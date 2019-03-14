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

  """

  # in future may use smart dynamic size based on bulk capacity
  @default_bulk_size 100
  @default_interval 1000
  @partition 0

  defmodule State do
    @moduledoc false
    defstruct [
      :topic,
      :brod_client,
      :producer,
      :sent_count,
      :refs,
      :bulk_size,
      :stats_handler,
      :stats_handler_interval
    ]
  end

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

  @typedoc """
  All the startup option to configure genstage. See @moduledoc.
  """
  @type option ::
          {:bulk_size, pos_integer()}
          | {:gen_stage_consumer_options, [GenStage.consumer_option()]}
          | {:stats_handler, stats_handler()}
          | {:stats_handler_interval, pos_integer()}

  @typedoc "List of startup options."
  @type options :: [option()]

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

  # As of gen_stage 0.14, the only valid option is `:subscribe_to`.
  # Which is list of `GenStage.subscription_option`.
  # We aim to adjust subscription cancel option to :transient or :temporary,
  # as we want this process to confirm all emited messages made it to kafka.
  # Producer is designed to end itself after it does so, if the producer stage downstream ends.
  # By explicitly stating the default `cancel: :permanent`, option will be
  # respected (thus gen_stage will end without waiting for pending acks from kafka).
  defp adjust_gen_stage_consumer_options(options) when is_list(options) do
    case Keyword.get(options, :subscribe_to) do
      nil ->
        options

      subscribers ->
        Keyword.put(options, :subscribe_to, Enum.map(subscribers, &adjust_subscribe_to/1))
    end
  end

  defp adjust_subscribe_to({name, subscription_options}) when is_list(subscription_options) do
    {name, Keyword.put_new(subscription_options, :cancel, :transient)}
  end

  defp adjust_subscribe_to(name) do
    {name, cancel: :transient}
  end

  def init({brod_client_init, topic, options}) do
    gen_stage_consumer_options =
      (options[:gen_stage_consumer_options] || []) |> adjust_gen_stage_consumer_options()

    bulk_size = options[:bulk_size] || @default_bulk_size
    stats_handler = options[:stats_handler] || (&Utils.log_stats/2)
    stats_handler_interval = options[:stats_handler_interval] || @default_interval

    with {:ok, brod_client} <- Utils.resolve_client(brod_client_init),
         :ok <- :brod_utils.assert_client(brod_client),
         :ok <- start_brod_producer(brod_client, topic) do
      Process.send_after(self(), :time_to_report_stats, stats_handler_interval)

      {:consumer,
       %State{
         topic: topic,
         brod_client: brod_client,
         refs: :queue.new(),
         stats_handler: stats_handler,
         stats_handler_interval: stats_handler_interval,
         sent_count: 0,
         bulk_size: bulk_size
       }, gen_stage_consumer_options}
    else
      err -> {:stop, err}
    end
  end

  def handle_events(messages, _from, state) do
    %State{refs: refs, sent_count: count, topic: topic, brod_client: brod_client} = state
    refs = produce(brod_client, topic, refs, messages)
    {:noreply, [], %State{state | refs: refs, sent_count: count + length(messages)}}
  end

  def handle_call(:get_insight, _from, %State{} = state) do
    {:reply, {:ok, %{topic: state.topic}, [], state}}
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

  def handle_cancel(reason, _from, state) do
    Logger.warn("Kafka forwarder stage for #{state.topic} cancelled because #{inspect(reason)}.")
    {:noreply, [], %State{state | producer: nil}}
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

  def handle_info(:time_to_report_stats, %State{sent_count: count, topic: topic} = state) do
    if count > 9 do
      Logger.info("Kafka topic #{topic} writing #{count} msgs/s.")
    end

    if count > 0 do
      Stats.counter(count, "kafka.producer.#{topic}.msgs")
    end

    Process.send_after(self(), :time_to_report_stats, 1000)
    {:noreply, [], %State{state | sent_count: 0}}
  end

  defp produce(brod_client, topic, refs, messages) do
    part_fun = fn _, _, _, _ -> {:ok, 0} end

    messages
    |> Stream.map(&input_message_format/1)
    |> Stream.chunk_every(@chunk_size)
    |> Enum.reduce(refs, fn bulk, acc ->
      {:ok, ref} = :brod.produce(brod_client, topic, part_fun, "", bulk)
      :queue.in({ref, length(bulk)}, acc)
    end)
  end

  defp input_message_format({ts, val}) when is_integer(ts), do: {ts, "", val}
  defp input_message_format({_ts, _key, _val} = msg), do: msg
  defp input_message_format(val) when is_binary(val), do: {"", val}

  defp start_brod_producer(client, topic), do: start_brod_producer(client, topic, 1, nil)

  defp start_brod_producer(_client, topic, retry_wait, error) when retry_wait > 16 do
    Logger.error("Forwarder cannot create kafka topic #{topic} because #{inspect(error)}")
    {:error, {:cannot_create_kafka_topic, topic, error}}
  end

  defp start_brod_producer(client, topic, retry_wait, _) do
    case :brod.start_producer(client, topic, _producer_config = []) do
      :ok ->
        :ok

      {:error, :LeaderNotAvailable} = error ->
        retry_for_topic_creation(client, topic, retry_wait, error)

      {:error, :unknown_topic_or_partition} = error ->
        retry_for_topic_creation(client, topic, retry_wait, error)

      {:error, reason} ->
        Logger.error("Unknown error when starting brod producer #{inspect(reason)}")
        {:error, reason}
    end
  end

  defp retry_for_topic_creation(client, topic, retry_wait, error) do
    Process.sleep(retry_wait * 1000)
    start_brod_producer(client, topic, retry_wait * 2, error)
  end
end
