defmodule KafkaGenStage.ConsumerLogic do
  @moduledoc """
  Separates pure logic of consumer genstage to be unit tested.
  """

  @typedoc "Format of read messages."
  @type msg_tuple ::
          {offset :: non_neg_integer(), timestamp :: non_neg_integer(), key :: binary(),
           value :: binary()}

  @typedoc "Acking to prod consumer."
  @type ack :: non_neg_integer() | :no_ack

  @typedoc "Last offset (inclusive) to be emmited by GenStage."
  @type end_offset :: non_neg_integer() | :infinity

  @typedoc "Internal type for working with demand and event buffers."
  @type dispatch ::
          {msgs_to_send :: [msg_tuple()], ack_to_brod_consumer :: ack(),
           buffered_demand :: non_neg_integer(), buffered_msgs :: :queue.queue()}

  @doc """
  Handles caching of both, demand and events, as recomended in *Buffering Demand* in gen_stage docs.
  Thus called from both, `KafkaGenStage.Consumer.handle_demand/2` and
  `KafkaGenStage.Consumer.handle_info/2` where events from *brod_consumer* arrive.
  """
  @spec prepare_dispatch(buffered_msgs :: :queue.queue(), buffered_demand :: non_neg_integer()) ::
          dispatch()
  def prepare_dispatch(queue, 0), do: {[], :no_ack, 0, queue}

  def prepare_dispatch(queue, demand) do
    case :queue.out(queue) do
      {{:value, msg}, queue} -> prepare_dispatch(queue, demand - 1, [msg])
      {:empty, queue} -> {[], :no_ack, demand, queue}
    end
  end

  @spec prepare_dispatch(
          buffered_msgs :: :queue.queue(),
          buffered_demand :: non_neg_integer(),
          msgs_to_send :: [msg_tuple()]
        ) :: dispatch()
  defp prepare_dispatch(queue, 0, [{last_offset, _, _, _} | _] = to_send) do
    {Enum.reverse(to_send), last_offset, 0, queue}
  end

  defp prepare_dispatch(queue, demand, [{last_offset, _, _, _} | _] = to_send) do
    case :queue.out(queue) do
      {{:value, msg}, queue} -> prepare_dispatch(queue, demand - 1, [msg | to_send])
      {:empty, queue} -> {Enum.reverse(to_send), last_offset, demand, queue}
    end
  end

  @doc """
  Insert incoming messages into buffer, up to end_offset(inclusive)
  """
  @spec messages_into_queue(
          messages :: [any()],
          buffered_message :: :queue.queue(msg_tuple()),
          inclusive_end_offset :: end_offset(),
          (msg :: term() -> msg_tuple())
        ) :: {:cont | :halt, :queue.queue(msg_tuple())}
  def messages_into_queue(messages, queue, end_offset, msg_to_tuple \\ &(&1)) do
    Enum.reduce(messages, {:cont, queue}, fn msg, {flag, queue} ->
      message_into_queue_reducer(msg_to_tuple.(msg), flag, end_offset, queue)
    end)
  end

  @spec message_into_queue_reducer(
          msg_tuple(),
          :cont | :halt,
          :infinity | integer(),
          :queue.queue(msg_tuple())
        ) :: {:cont | :halt, :queue.queue(msg_tuple())}
  defp message_into_queue_reducer(msg, flag, :infinity, queue), do: {flag, :queue.in(msg, queue)}
  defp message_into_queue_reducer(_, :halt, _, queue), do: {:halt, queue}

  defp message_into_queue_reducer({offset, _, _, _} = msg, :cont = flag, end_offset, queue) do
    cond do
      offset < end_offset -> {flag, :queue.in(msg, queue)}
      offset == end_offset -> {:halt, :queue.in(msg, queue)}
      offset > end_offset -> {:halt, queue}
    end
  end
end
