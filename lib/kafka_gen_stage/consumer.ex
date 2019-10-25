defmodule KafkaGenStage.Consumer do
  @moduledoc """
  Producer GenStage for reading from Kafka topic, using [Klarna's Brod](https://github.com/klarna/brod).

  > Note that is its **consumer** from Kafka's perspective, but **producer** from GenStage's.

  ## Messages

  Events emited are in 4-tuple format

      @type msg_tuple :: {offset :: non_neg_integer(), timestamp :: non_neg_integer(),
      key :: binary(), value :: binary()}

  ## Options
  When starting, several options can modify behaviour of GenStage.

  * `:begin_offset` - where to start reading the topic, defaut to `:earliest`,
    or provide exact offset number (inclusive)

  * `:read_end_offset` - when to stop reading, also stops gen_stage and sent cancel to subscribers
    possible values:

    * exact integer of last offset to be read(inclusive)
    * `:latest` - will check what is offset of last message at time when GenStage is initializing
    * `:infinity` - does not stop reading by offset, **default**

  * `:stats_handler` - every second called function with some statistics, type is:
    `(%{count: non_neg_integer(), cursor: non_neg_integer()}, topic() -> :ok)`, you can use this
    function for monitoring of throughput etc...

  * `:stats_handler_interval` - if you dont want stats_hander to be called every second,
    provide desired interval (milliseconds)

  * `:bulk_transformer` - optionally bulk of events can be statelessly transformed before sending downstream.
    Function signature must be:
    ([msg_tuple], is_end_of_stream) :: [msg_tuple]

  * `:gen_stage_producer_options` - refer to options passed to underlaying GenStage, usefull for
    accumulating demand when partition dispatcher is used (`[demand: :accumulate]`).

  * `:partition` - one gen_stage reads from single partition, 0 by default

  ## Starting and stopping brod client

  Brod client should be either already started (provided as atom or pid) or provided as
  initializing function.

  Closing of brod client is out of scope of this gen_stage. If you want client to be started
  exclusively for this gen_stage, do it via initialize function, and manage lifecycle on your own,
  simple example being:

      fn -> :brod.start_link_client([{'localhost', 9092}] = _endpoints) end

  """

  use GenStage

  @partition 0
  @default_interval 1000

  alias KafkaGenStage.ConsumerLogic, as: Logic
  alias KafkaGenStage.Utils

  require Record
  require Logger

  import Record, only: [defrecord: 2, extract: 2]

  defrecord :kafka_message, extract(:kafka_message, from_lib: "brod/include/brod.hrl")
  defrecord :kafka_message_set, extract(:kafka_message_set, from_lib: "brod/include/brod.hrl")

  @typedoc "Last offset (inclusive) to be emmited by GenStage."
  @type end_offset :: Logic.end_offset()

  @typedoc "Starting option(see @moduledoc) which defines when to stop reading."
  @type read_end_offset :: :latest | end_offset()

  @typedoc "Format of read messages."
  @type msg_tuple :: Logic.msg_tuple()

  @typedoc "Runtime stats of consumer. Count of messeges received in last second."
  @type stats :: %{count: non_neg_integer(), cursor: non_neg_integer()}

  @typedoc "Kafka topic identifier."
  @type topic :: KafkaGenStage.topic()

  @typedoc "Function consuming runtime stats -> to sent to StatsD or so."
  @type stats_handler :: (stats(), topic() -> :ok)

  @typedoc "Function transforming message bulks before sending them downstream."
  @type bulk_transformer :: ([msg_tuple], boolean() -> [msg_tuple])

  @typedoc "Brod's type for where to start reading in kafka topic."
  @type begin_offset :: KafkaGenStage.begin_offset()

  @typedoc "Brod client passing or initialization."
  @type brod_client_init :: atom() | pid() | (() -> {:ok, atom() | pid()})

  @typedoc """
  All the startup option to configure genstage. See @moduledoc.
  """
  @type option ::
          {:begin_offset, begin_offset()}
          | {:read_end_offset, read_end_offset()}
          | {:gen_stage_producer_options, [GenStage.producer_option()]}
          | {:partition, integer()}
          | {:stats_handler, stats_handler}
          | {:bulk_transformer, bulk_transformer}
          | {:stats_handler_interval, pos_integer()}

  @typedoc "List of startup options."
  @type options :: [option()]

  defmodule State do
    @moduledoc false
    defstruct [
      :topic,
      :partition,
      :brod_client,
      :brod_client_mref,
      :consumer,
      :consumer_ref,
      :queue,
      :is_end_of_stream,
      :end_of_stream_offset_queue,
      :reading,
      :demand,
      :stats,
      :end_offset,
      :stats_handler,
      :stats_handler_interval,
      :bulk_transformer
    ]
  end

  @typedoc "Just documentation purposes of internal state typespec."
  @type state :: %State{
          topic: topic(),
          partition: non_neg_integer(),
          brod_client: atom() | pid(),
          brod_client_mref: reference(),
          consumer: pid(),
          consumer_ref: reference(),
          queue: :queue.queue(),
          is_end_of_stream: boolean(),
          end_of_stream_offset_queue: :queue.queue(),
          reading: boolean(),
          demand: non_neg_integer(),
          stats: stats(),
          end_offset: end_offset(),
          stats_handler: stats_handler(),
          stats_handler_interval: pos_integer(),
          bulk_transformer: bulk_transformer()
        }

  @doc """
  Start linked Consumer GenStage of topic (with underlying brod consumer).
  See option type documentation for possible options.
  """
  @spec start_link(brod_client_init(), topic(), options(), GenServer.options()) ::
          GenServer.on_start()
  def start_link(brod_client_init, topic, options \\ [], gen_server_options \\ []) do
    GenStage.start_link(__MODULE__, {brod_client_init, topic, options}, gen_server_options)
  end

  @doc """
  Return some running metadata such as current offset position in topic.
  """
  @spec get_insight(server :: term()) ::
          {:ok, %{offset_cursor: non_neg_integer(), topic: topic()}}
  def get_insight(reader) do
    GenStage.call(reader, :get_insight)
  end

  @impl true
  def init({brod_client_init, topic, options}) do
    # default options
    partition = options[:partition] || @partition
    begin_offset = options[:begin_offset] || :earliest
    gen_stage_producer_options = options[:gen_stage_producer_options]
    read_end_offset = options[:read_end_offset] || :infinity
    stats_handler = options[:stats_handler] || (&Utils.log_stats/2)
    stats_handler_interval = options[:stats_handler_interval] || @default_interval
    bulk_transformer = options[:bulk_transformer] || nil

    with {:ok, client} <- Utils.resolve_client(brod_client_init),
         :ok <- :brod_utils.assert_client(client),
         :ok <- :brod_utils.assert_topic(topic),
         {:ok, latest_offset} = Utils.resolve_offset(client, topic, partition, :latest),
         {:ok, earliest_offset} = Utils.resolve_offset(client, topic, partition, :earliest),
         :ok <- :brod.start_consumer(client, topic, begin_offset: begin_offset) do
      GenStage.async_info(self(), :subscribe_consumer)
      Process.send_after(self(), :time_to_report_stats, stats_handler_interval)

      state = %State{
        brod_client: client,
        topic: topic,
        partition: partition,
        brod_client_mref: Process.monitor(client),
        queue: :queue.new(),
        is_end_of_stream: begin_is_end_of_stream(earliest_offset, latest_offset, begin_offset),
        end_of_stream_offset_queue: :queue.new(),
        demand: 0,
        stats: %{count: 0, cursor: 0},
        reading: true,
        stats_handler: stats_handler,
        stats_handler_interval: stats_handler_interval,
        end_offset: resolve_end_offset(latest_offset, read_end_offset),
        bulk_transformer: bulk_transformer
      }

      case gen_stage_producer_options do
        nil -> {:producer, state}
        _ -> {:producer, state, gen_stage_producer_options}
      end
    else
      err -> {:stop, err}
    end
  end

  @impl true
  def handle_demand(
        new_demand,
        %State{
          queue: queue,
          demand: pending_demand,
          consumer: consumer_pid,
          bulk_transformer: bulk_transformer,
          is_end_of_stream: is_end_of_stream,
          end_of_stream_offset_queue: eos_queue
        } = state
      ) do
    {to_send, to_ack, demand, queue, eos_queue} =
      Logic.prepare_dispatch(
        queue,
        new_demand + pending_demand,
        bulk_transformer,
        is_end_of_stream,
        eos_queue
      )

    ack(consumer_pid, to_ack)

    unless state.reading,
      do: GenStage.async_info(self(), :reading_end)

    {:noreply, to_send,
     %State{
       state
       | queue: queue,
         demand: demand,
         stats: update_stats(state.stats, to_send, to_ack),
         end_of_stream_offset_queue: eos_queue
     }}
  end

  @impl true
  def handle_call(:get_insight, _from, %State{stats: stats, topic: topic} = state) do
    {:reply, {:ok, %{offset_cursor: stats.cursor, topic: topic}}, [], state}
  end

  @impl true
  def handle_info(:subscribe_consumer, %State{} = state) do
    case :brod.subscribe(state.brod_client, self(), state.topic, state.partition, []) do
      {:ok, consumer_pid} ->
        ref = Process.monitor(consumer_pid)
        {:noreply, [], %State{state | consumer: consumer_pid, consumer_ref: ref}}

      {:error, reason} ->
        {:stop, reason, state}
    end
  end

  def handle_info(
        {consumer_pid,
         kafka_message_set(topic: topic, messages: messages, high_wm_offset: high_offset)},
        %State{
          consumer: consumer_pid,
          topic: topic,
          queue: queue,
          demand: demand,
          end_offset: end_offset,
          bulk_transformer: bulk_transformer,
          end_of_stream_offset_queue: eos_queue
        } = state
      ) do
    kafka_message(offset: last_offset) = List.last(messages)

    is_end_of_stream = last_offset >= min(high_offset - 1, end_offset)

    eos_queue =
      if is_end_of_stream do
        :queue.in(last_offset, eos_queue)
      else
        eos_queue
      end

    queue = Logic.enqueue(queue, messages |> Stream.map(&kafka_msg_record_to_tuple/1), end_offset)

    {to_send, to_ack, demand, queue, eos_queue} =
      Logic.prepare_dispatch(
        queue,
        demand,
        bulk_transformer,
        is_end_of_stream,
        eos_queue
      )

    :ok = ack(consumer_pid, to_ack)

    if last_offset >= end_offset,
      do: GenStage.async_info(self(), :reading_end)

    {:noreply, to_send,
     %State{
       state
       | demand: demand,
         queue: queue,
         stats: update_stats(state.stats, to_send, to_ack),
         is_end_of_stream: is_end_of_stream,
         end_of_stream_offset_queue: eos_queue
     }}
  end

  def handle_info({:DOWN, ref, :process, _object, reason}, %State{brod_client_mref: ref} = state) do
    Logger.warn("Brod client #{inspect(state.brod_client)} DOWN, stopping consumer gen_stage.")

    {:stop, reason, state}
  end

  def handle_info({:DOWN, ref, :process, _object, reason}, %State{consumer_ref: ref} = state) do
    Logger.warn(
      "Brod consumer of #{inspect(state.brod_client)} for #{state.config.topic} DOWN, stopping consumer gen_stage"
    )

    {:stop, reason, state}
  end

  def handle_info(
        :time_to_report_stats,
        %State{stats: stats, topic: topic, stats_handler: stats_handler} = state
      ) do
    stats_handler.(stats, topic)
    Process.send_after(self(), :time_to_report_stats, state.stats_handler_interval)
    {:noreply, [], %State{state | stats: %{stats | count: 0}}}
  end

  def handle_info(:reading_end, %State{} = state) do
    if :queue.is_empty(state.queue) do
      {:stop, :normal, state}
    else
      {:noreply, [], %State{state | reading: false}}
    end
  end

  @impl true
  def terminate(_reason, %State{consumer: pid}) do
    :brod_consumer.stop(pid)
  end

  # the topic is empty
  defp begin_is_end_of_stream(0, 0, _), do: true
  # reading from the last message, automatically is at the end of stream
  defp begin_is_end_of_stream(_earliest, _latest, :latest), do: true
  # reading from the first message, is at the end of stream only if the first message is also the last
  defp begin_is_end_of_stream(earliest, latest, :earliest), do: latest == earliest
  # reading from the middle
  defp begin_is_end_of_stream(_earliest, latest, begin) when is_integer(begin), do: latest <= begin

  defp resolve_end_offset(latest, :latest), do: latest - 1
  defp resolve_end_offset(_latest, :infinity), do: :infinity
  defp resolve_end_offset(_latest, read_end) when is_integer(read_end), do: :infinity

  defp update_stats(%{count: count, cursor: cursor} = stats, to_send, to_ack) do
    %{stats | count: count + length(to_send), cursor: update_cursor(to_ack, cursor)}
  end

  defp update_cursor(to_ack, current) do
    case to_ack do
      :no_ack = _nothing_to_send ->
        current

      ack ->
        ack
    end
  end

  defp kafka_msg_record_to_tuple(kafka_msg) do
    kafka_message(value: value, offset: offset, key: key, ts: ts) = kafka_msg
    {offset, ts, key, value}
  end

  defp ack(_pid, :no_ack), do: :ok
  defp ack(pid, offset) when is_integer(offset), do: :brod.consume_ack(pid, offset)
end
