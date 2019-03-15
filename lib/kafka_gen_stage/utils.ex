defmodule KafkaGenStage.Utils do
  @moduledoc """
  Various brod based helper apis over kafka.
  """

  require Logger

  @typedoc "Kafka topic identifier."
  @type topic :: KafkaGenStage.topic()

  @typedoc "Brod's type for where to start reading in kafka topic."
  @type begin_offset :: KafkaGenStage.begin_offset()

  @doc """
  Resolve timestamp or semantic offset to real offset, when started
  brod client is given.
  """
  @spec resolve_offset(atom(), topic(), integer(), begin_offset()) ::
          {:ok, integer()} | {:error, any()}
  def resolve_offset(client, topic, partition, time) do
    with {:ok, conn} <- :brod_client.get_leader_connection(client, topic, partition) do
      :brod_utils.resolve_offset(conn, topic, partition, time)
    end
  end

  @doc """
  If given function, function should return started kafka client ({:ok, pid}).
  Mostly used with fn -> :brod.start_link_client end.
  """
  @spec resolve_client(atom() | pid() | (() -> {:ok, atom() | pid()})) :: {:ok, atom() | pid()}
  def resolve_client(client) when is_atom(client) or is_pid(client), do: {:ok, client}

  def resolve_client(brod_init) when is_function(brod_init) do
    case brod_init.() do
      {:ok, client} = resolved when is_atom(client) or is_pid(client) -> resolved
      {:error, _reason} = error -> error
      bad_result -> {:error, {:brod_init_function_bad_result, bad_result}}
    end
  end

  @doc """
  Default stats handler for periodic info-report about genstage status.
  """
  def log_stats(%{count: count} = stats, topic) do
    if count > 0 do
      line =
        stats
        |> Map.put(:topic, topic)
        |> Enum.map(fn {key, value} -> "#{key}=#{value}" end)
        |> Enum.join(", ")

      Logger.info(line)
    end
  end

  @doc """
  As described in moduledoc of `KafkaGenStage.Producer`, for producer we want the subscripton
  option *cancel* to default to `:transient`. This function adjusts to that.
  """
  def adjust_gen_stage_consumer_options(options) when is_list(options) do
    case Keyword.get(options, :subscribe_to) do
      nil -> options
      subs -> Keyword.put(options, :subscribe_to, Enum.map(subs, &adjust_subscribe_to/1))
    end
  end

  defp adjust_subscribe_to({name, subscription_options}) when is_list(subscription_options) do
    {name, Keyword.put_new(subscription_options, :cancel, :transient)}
  end

  defp adjust_subscribe_to(name) do
    {name, cancel: :transient}
  end
end
