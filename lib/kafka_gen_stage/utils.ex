defmodule KafkaGenStage.Utils do
  @moduledoc """
  Various brod based helper apis over kafka.
  """

  require Logger

  @type topic :: KafkaGenStage.topic()
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
  def resolve_client(client_lazy) when is_function(client_lazy), do: client_lazy.()

  @doc """
  Default stats handler for periodic info-report about genstage status.
  """
  def log_stats(%{count: count, cursor: cursor}, topic) do
    if count > 0 do
      Logger.info("topic = #{topic}, throughput = #{count}, offset_cursor = #{cursor}")
    end
  end

end
