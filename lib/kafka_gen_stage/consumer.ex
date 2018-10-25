defmodule KafkaGenStage.Consumer do
  @moduledoc """
  Producer Gen_Stage for reading from Kafka topic, using [Klarna's Brod](https://github.com/klarna/brod).

  ## Starting and stopping brod client

  Brod client shoudl be either already started (provided as atom or pid),
  or provided as initializing function.

  Closing of brod client is out of scope of this gen_stage.
  If you want client to be started exclusively for this gen_stage, do it via initialize function,
  and manage lifecycle on your own, simple example being:

  `fn -> :brod.start_link_client([{'localhost', 9092}] = _endpoints) end`.

  ## Options
  When starting, sevaral options can modify behaviour of GenStage

  - **begin_offset**: where to start reading the topic, defaut to `:earliest`,
    or provide exact offset number (inclusive)
  - **read_end_offset**: when to stop reading, also stops gen_stage and sent cancel to subscribers
    possible values:
    - exact integer of last offset to be read(inclusive)
    - `:latest` : will check what is offset of last message at time when gen_stage is initializing
    - `:infinity` : does not stop reading by offset, **default**
  - **stats_handler**: every second called function with some statistics, type is:
    (%{count: non_neg_integer(), cursor: non_neg_integer()}, topic() -> :ok)
    you can use this function for monitoring of throughput etc...
  - **gen_stage_producer_options**: refer to options passed to genstage, usefull for accumulating
  demand when partition dispatcher is used `[demand: :accumulate]`
  - **partition**: one gen_stage reads from single partition, 0 by default

  """

  use GenStage

  @partition 0

  alias KafkaGenStage.ConsumerLogic, as: Logic
  alias KafkaGenStage.Utils

  require Record
  require Logger

  import Record, only: [defrecord: 2, extract: 2]

  defrecord :kafka_message, extract(:kafka_message, from_lib: "brod/include/brod.hrl")
  defrecord :kafka_message_set, extract(:kafka_message_set, from_lib: "brod/include/brod.hrl")

  @type end_offset :: Logic.end_offset()
  @type read_end_offset :: :latest | end_offset()
  @type msg_tuple :: Logic.msg_tuple()
  @type stats :: %{count: non_neg_integer(), cursor: non_neg_integer()}
  @type topic :: KafkaGenStage.topic()
  @type stats_handler :: (stats(), topic() -> :ok)
  @type begin_offset :: KafkaGenStage.begin_offset()
  @type brod_client_init :: atom() | pid() | (() -> {:ok, atom() | pid()})

  @typedoc """
  See @moduledoc.
  """
  @type option ::
          {:begin_offset, begin_offset()}
          | {:read_end_offset, read_end_offset()}
          | {:gen_stage_producer_options, [GenStage.producer_option()]}
          | {:partition, integer()}
          | {:stats_handler, (integer(), topic() -> :ok)}

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
      :reading,
      :demand,
      :stats,
      :end_offset,
      :stats_handler
    ]
  end

  @type state :: %State{
          topic: topic(),
          partition: non_neg_integer(),
          brod_client: atom() | pid(),
          brod_client_mref: reference(),
          consumer: pid(),
          consumer_ref: reference(),
          queue: :queue.queue(),
          reading: :halt | :cont,
          demand: non_neg_integer(),
          stats: stats(),
          end_offset: end_offset(),
          stats_handler: stats_handler()
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
  def get_insight(reader) do
    GenStage.call(reader, :get_insight)
  end

  defp resolve_end_offset(read_end_offset, client, topic, partition) do
    case read_end_offset do
      offset when is_integer(offset) ->
        offset

      :infinity ->
        :infinity

      :latest ->
        {:ok, offset} = Utils.resolve_offset(client, topic, partition, :latest)
        # it next offset to be assigned, we have to use the one before
        offset - 1
    end
  end

  def init({brod_client_init, topic, options}) do
    # default options
    partition = options[:partition] || @partition
    begin_offset = options[:begin_offset] || :earliest
    gen_stage_producer_options = options[:gen_stage_producer_options] || [demand: :forward]
    read_end_offset = options[:read_end_offset] || :infinity
    stats_handler = options[:stats_handler] || (&Utils.log_stats/2)

    with {:ok, client} <- Utils.resolve_client(brod_client_init),
         :ok <- :brod_utils.assert_client(client),
         :ok <- :brod_utils.assert_topic(topic),
         :ok <- :brod.start_consumer(client, topic, begin_offset: begin_offset) do
      GenStage.async_info(self(), :subscribe_consumer)
      Process.send_after(self(), :time_to_report_stats, 1000)

      state = %State{
        brod_client: client,
        topic: topic,
        partition: partition,
        brod_client_mref: Process.monitor(client),
        queue: :queue.new(),
        demand: 0,
        stats: %{count: 0, cursor: 0},
        reading: :cont,
        stats_handler: stats_handler,
        end_offset: resolve_end_offset(read_end_offset, client, topic, partition)
      }

      {:producer, state, gen_stage_producer_options}
    else
      err -> {:stop, err}
    end
  end

  def handle_demand(
        new_demand,
        %State{queue: queue, demand: pending_demand, consumer: consumer_pid} = state
      ) do
    {to_send, to_ack, demand, queue} = Logic.prepare_dispatch(queue, new_demand + pending_demand)
    ack(consumer_pid, to_ack)

    if state.reading == :halt do
      GenStage.async_info(self(), :reading_end)
    end

    {:noreply, to_send,
     %State{state | queue: queue, demand: demand, stats: update_stats(state.stats, to_send)}}
  end

  def handle_call(:get_insight, _from, %State{stats: stats, topic: topic} = state) do
    {:reply, {:ok, %{offset_cursor: stats.cursor, topic: topic}}, [], state}
  end

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
        {consumer_pid, kafka_message_set(topic: topic, messages: messages)},
        %State{
          consumer: consumer_pid,
          topic: topic,
          queue: queue,
          demand: demand,
          end_offset: end_offset
        } = state
      ) do
    {reading_flag, queue} =
      Logic.messages_into_queue(messages, queue, end_offset, &kafka_msg_record_to_tuple/1)

    if reading_flag == :halt do
      GenStage.async_info(self(), :reading_end)
    end

    {to_send, to_ack, demand, queue} = Logic.prepare_dispatch(queue, demand)
    :ok = ack(consumer_pid, to_ack)

    {:noreply, to_send,
     %State{state | demand: demand, queue: queue, stats: update_stats(state.stats, to_send)}}
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
    Process.send_after(self(), :time_to_report_stats, 1000)
    {:noreply, [], %State{state | stats: %{stats | count: 0}}}
  end

  def handle_info(:reading_end, %State{} = state) do
    if :queue.is_empty(state.queue) do
      {:stop, :normal, state}
    else
      {:noreply, [], %State{state | reading: :halt}}
    end
  end

  def terminate(_reason, %State{consumer: pid}) do
    :brod_consumer.stop(pid)
  end

  defp update_stats(%{count: count, cursor: cursor} = stats, to_send) do
    %{stats | count: count + length(to_send), cursor: update_cursor(to_send, cursor)}
  end

  defp update_cursor(to_send, current) do
    case to_send do
      [] = _nothing_to_send ->
        current

      _something_to_send ->
        {cursor, _, _, _} = List.last(to_send)
        cursor
    end
  end

  defp kafka_msg_record_to_tuple(kafka_msg) do
    kafka_message(value: value, offset: offset, key: key, ts: ts) = kafka_msg
    {offset, ts, key, value}
  end

  defp ack(_pid, :no_ack), do: :ok
  defp ack(pid, offset) when is_integer(offset), do: :brod.consume_ack(pid, offset)
end
