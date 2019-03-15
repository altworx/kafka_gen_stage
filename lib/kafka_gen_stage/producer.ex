defmodule KafkaGenStage.Producer do
  @moduledoc """
  Consumer GenStage for writing event into Kafka topic, using [Klarna's Brod](https://github.com/klarna/brod).

  > Note that is its **Producer** from Kafka's perspective, but **consumer** from GenStage's.

  GenStage handles demand manually, adhering to kafka acks.
  Because of that, currently allows to subscribe to only one producer,as logic how to distribte demand between
  several would need to be implemented.

  ## Incoming events must be in one of these formats
  Value to be written have to be `binary()`

   - 3-tuple form **{timestamp, kafka_key, value}**
   - 2-tuple form **{timestamp, value}**
   - binary value, is inserted without timestamp with empty key

  ### Note about canceling gen_stage:

  As of gen_stage 0.14, the gen_stage_producer_options can contain only single valid option:
  `:subscribe_to`, which is list of `GenStage.subscription_option`. See subscribing from init in
  GenStage docs.

  We mostly want to adjust subscription cancel option to :transient or :temporary,
  as we want this process to confirm all emited messages made it to kafka.
  Producer is designed to end itself after it does so, if the producer stage downstream ends.

  So this GenStage overrides default option, so default(when not specified) *cancel* behaviour is
  `:transient` instead of `:permanent` as is usual for genstage

  By explicitly stating `cancel: :permanent`, will be respected (thus gen_stage will end without
  waiting for pending acks from kafka).

  """

  # in future may use smart dynamic size based on bulk capacity
  @default_bulk_size 100
  @default_interval 1000
  @partition 0

  require Logger
  use GenStage
  alias KafkaGenStage.Utils

  @typedoc "Kafka topic identifier."
  @type topic :: KafkaGenStage.topic()

  @typedoc "Runtime stats of consumer stage. Count of messages emited in last second."
  @type stats :: %{count: non_neg_integer()}

  @typedoc "Function consuming runtime stats -> to sent to StatsD or so."
  @type stats_handler :: (stats(), topic() -> :ok)

  @typedoc "Brod client passing or initialization."
  @type brod_client_init :: atom() | pid() | (() -> {:ok, atom() | pid()})

  @typedoc "Common 3-tuple identifying destination topic-partition"
  @type client_topic_partition :: {atom() | pid(), topic(), non_neg_integer()}

  @typedoc """
  All the startup option to configure genstage. See @moduledoc.
  """
  @type option ::
          {:bulk_size, pos_integer()}
          | {:gen_stage_consumer_options, [GenStage.consumer_option()]}
          | {:stats_handler, stats_handler()}
          | {:partition, non_neg_integer()}
          | {:stats_handler_interval, pos_integer()}

  @typedoc "List of startup options."
  @type options :: [option()]

  defmodule State do
    @moduledoc false
    defstruct [
      :client_topic_partition,
      :brod_client_ref,
      :brod_producer,
      :producer,
      :stats,
      :refs,
      :bulk_size,
      :stats_handler,
      :stats_handler_interval
    ]
  end

  @typedoc "Just documentation purposes of internal state typespec."
  @type state :: %State{
          client_topic_partition: client_topic_partition(),
          brod_client_ref: reference(),
          brod_producer: {pid(), reference()},
          producer: term(),
          stats: stats(),
          refs: :queue.queue(),
          bulk_size: non_neg_integer(),
          stats_handler: stats_handler(),
          stats_handler_interval: pos_integer()
        }
  @doc """
  Start linked Producer GenStage for topic (with underlying brod consumer).
  See option type documentation for possible options.
  """
  @spec start_link(brod_client_init(), topic(), options(), GenServer.options()) ::
          GenServer.on_start()
  def start_link(brod_client_init, topic, options \\ [], gen_server_options \\ []) do
    GenStage.start_link(__MODULE__, {brod_client_init, topic, options}, gen_server_options)
  end

  @doc """
  Get meta-info about genstage forwarder.
  Currently only topic name :).
  """
  @spec get_insight(server :: term()) :: {:ok, %{topic: topic()}}
  def get_insight(forwarder) do
    GenStage.call(forwarder, :get_insight)
  end

  def init({brod_client_init, topic, options}) do
    bulk_size = options[:bulk_size] || @default_bulk_size
    stats_handler = options[:stats_handler] || (&Utils.log_stats/2)
    stats_handler_interval = options[:stats_handler_interval] || @default_interval
    partition = options[:partition] || @partition

    gen_stage_consumer_options =
      (options[:gen_stage_consumer_options] || []) |> Utils.adjust_gen_stage_consumer_options()

    with {:ok, brod_client} <- Utils.resolve_client(brod_client_init),
         :ok <- :brod_utils.assert_client(brod_client),
         # potential long call -> maybe should be factored to self message,
         # with buffering of handled_events
         {:ok, brod_producer} <- start_brod_producer(brod_client, topic, partition) do
      Process.send_after(self(), :time_to_report_stats, stats_handler_interval)

      {:consumer,
       %State{
         client_topic_partition: {brod_client, topic, partition},
         brod_client_ref: Process.monitor(brod_client),
         brod_producer: {brod_producer, Process.monitor(brod_producer)},
         refs: :queue.new(),
         stats_handler: stats_handler,
         stats_handler_interval: stats_handler_interval,
         stats: %{count: 0},
         bulk_size: bulk_size
       }, gen_stage_consumer_options}
    else
      err -> {:stop, err}
    end
  end

  def handle_events(
        messages,
        _from,
        %State{refs: refs, client_topic_partition: client_topic_partition, stats: stats} = state
      ) do
    refs = produce(client_topic_partition, refs, messages)
    stats = %{stats | count: stats.count + length(messages)}
    {:noreply, [], %State{state | refs: refs, stats: stats}}
  end

  def handle_call(:get_insight, _from, %State{client_topic_partition: {_, topic, _}} = state) do
    {:reply, {:ok, %{topic: topic}, [], state}}
  end

  def handle_subscribe(:producer, opts, from, state) do
    if state.producer == nil do
      pending_demand = opts[:max_demand] || 1000
      GenStage.ask(from, pending_demand)
      {:manual, %State{state | producer: from}}
    else
      {:stop, {:error, :kafka_forwarder_already_subscribed, state.topic}, state}
    end
  end

  def handle_cancel(_reason, _from, state) do
    if :queue.is_empty(state.refs) do
      {:stop, :normal, state}
    else
      {:noreply, [], %State{state | producer: nil}}
    end
  end

  def handle_info({:brod_produce_reply, ref, _offset, :brod_produce_req_acked}, state) do
    %State{refs: refs, producer: producer} = state
    {{:value, {^ref, bulk_size}}, refs} = :queue.out(refs)
    new_state = %State{state | refs: refs}

    cond do
      state.producer != nil ->
        GenStage.ask(producer, bulk_size)
        {:noreply, [], new_state}

      :queue.is_empty(refs) ->
        {:stop, :normal, new_state}

      true ->
        {:noreply, [], new_state}
    end
  end

  def handle_info(
        :time_to_report_stats,
        %State{stats: stats, client_topic_partition: {_, topic, _}, stats_handler: stats_handler} =
          state
      ) do
    stats_handler.(stats, topic)
    Process.send_after(self(), :time_to_report_stats, state.stats_handler_interval)
    {:noreply, [], %State{state | stats: %{stats | count: 0}}}
  end

  def terminate(_reason, %State{brod_producer: {producer_pid, ref}}) do
    Process.demonitor(ref)
    :brod_producer.stop(producer_pid)
  end

  defp produce({brod_client, topic, partition}, refs, messages) do
    partitioner_fun = fn _, _, _, _ -> {:ok, partition} end

    messages
    |> Stream.map(&input_message_format/1)
    |> Stream.chunk_every(@chunk_size)
    |> Enum.reduce(refs, fn bulk, acc ->
      {:ok, ref} = :brod.produce(brod_client, topic, partitioner_fun, "", bulk)
      :queue.in({ref, length(bulk)}, acc)
    end)
  end

  defp input_message_format({ts, val}) when is_integer(ts), do: {ts, "", val}
  defp input_message_format({_ts, _key, _val} = msg), do: msg
  defp input_message_format(val) when is_binary(val), do: {"", val}

  defp start_brod_producer(client, topic, partition),
    do: start_brod_producer(client, topic, partition, 1, nil)

  defp start_brod_producer(_client, topic, partition, retry_wait, error) when retry_wait > 16 do
    Logger.error("KafkaGenStage.Producer fail to create topic #{topic} because #{inspect(error)}")
    {:error, {:cannot_create_kafka_topic, topic, error}}
  end

  defp start_brod_producer(client, topic, partition, retry_wait, _) do
    with :ok <- :brod.start_producer(client, topic, _producer_config = []),
         {:ok, brod_producer} <- :brod.get_producer(client, topic, partition) do
      {:ok, brod_producer}
    else
      {:error, :LeaderNotAvailable} = error ->
        retry_for_topic_creation(client, topic, partition, retry_wait, error)

      {:error, :unknown_topic_or_partition} = error ->
        retry_for_topic_creation(client, topic, partition, retry_wait, error)

      {:error, _reason} = err ->
        err
    end
  end

  defp retry_for_topic_creation(client, topic, partition, retry_wait, error) do
    Process.sleep(retry_wait * 1000)
    start_brod_producer(client, topic, partition, retry_wait * 2, error)
  end
end
