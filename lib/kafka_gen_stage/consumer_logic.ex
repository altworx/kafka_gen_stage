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
           buffered_msgs :: :queue.queue()}

  @typedoc "API type for working with demand and event buffers."
  @type return_dispatch ::
          {msgs_to_send :: [msg_tuple()], ack_to_brod_consumer :: ack(),
           buffered_demand :: non_neg_integer(), buffered_msgs :: :queue.queue()}

  @doc """
  Handles caching of both, demand and events, as recomended in *Buffering Demand* in gen_stage docs.
  Thus called from both, `KafkaGenStage.Consumer.handle_demand/2` and
  `KafkaGenStage.Consumer.handle_info/2` where events from *brod_consumer* arrive.
  """
  @spec prepare_dispatch(
          buffered_msgs :: :queue.queue(),
          buffered_demand :: non_neg_integer(),
          bulk_transformer :: ([msg_tuple()] -> [msg_tuple()]),
          is_end_of_stream :: boolean()
        ) ::
          return_dispatch()
  def prepare_dispatch(queue, 0, _bulk_transformer, _is_end_of_stream) do
    {[], :no_ack, 0, queue}
  end

  def prepare_dispatch(queue, demand, bulk_transformer, is_end_of_stream) do
    {to_send, to_ack, queue} = dequeue(queue, demand, [], :no_ack)
    to_send = to_send |> Enum.reverse() |> transform_bulk(bulk_transformer, is_end_of_stream)
    demand = lower_demand(demand, to_send)
    {to_send, to_ack, demand, queue}
  end

  defp transform_bulk(to_send, nil, _is_end_of_stream) do
    to_send
  end

  defp transform_bulk(to_send, bulk_transformer, is_end_of_stream) do
    bulk_transformer.(to_send, is_end_of_stream)
  end

  @spec dequeue(
          buffered_msgs :: :queue.queue(),
          buffered_demand :: non_neg_integer(),
          msgs_to_send :: [term()],
          to_ack :: ack()
        ) :: dispatch()
  defp dequeue(queue, 0, to_send, to_ack) do
    {to_send, to_ack, queue}
  end

  defp dequeue(queue, demand, to_send, to_ack) do
    case :queue.out(queue) do
      {{:value, {offset, _, _, _} = msg}, queue} ->
        dequeue(
          queue,
          demand - 1,
          [msg | to_send],
          offset
        )

      {:empty, queue} ->
        {to_send, to_ack, queue}
    end
  end

  # its ok to send more than demanded events
  defp lower_demand(demand, to_send) do
    new_demand = demand - length(to_send)
    if new_demand < 0, do: 0, else: new_demand
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
  def messages_into_queue(messages, queue, end_offset, msg_to_tuple \\ & &1) do
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
