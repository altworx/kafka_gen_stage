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
           buffered_demand :: non_neg_integer(), buffered_msgs :: :queue.queue(),
           transformer_state :: term()}

  @doc """
  Handles caching of both, demand and events, as recomended in *Buffering Demand* in gen_stage docs.
  Thus called from both, `KafkaGenStage.Consumer.handle_demand/2` and
  `KafkaGenStage.Consumer.handle_info/2` where events from *brod_consumer* arrive.
  """
  @spec prepare_dispatch(
          buffered_msgs :: :queue.queue(),
          buffered_demand :: non_neg_integer(),
          transformer :: (msg_tuple(), term() -> {term(), term()}),
          transformer_state :: term,
          bulk_transformer :: ([msg_tuple()] -> [msg_tuple()])
        ) ::
          dispatch()
  def prepare_dispatch(queue, 0, _transformer, state, _bulk_transformer) do
    {[], :no_ack, 0, queue, state}
  end

  def prepare_dispatch(queue, demand, transformer, state, bulk_transformer) do
    case :queue.out(queue) do
      {{:value, msg}, queue} ->
        {to_send, new_state, to_ack} = transform(msg, transformer, state)
        new_demand = lower_demand(demand, to_send)

        {to_send, to_ack, _demand, queue, new_transformer_state} =
          prepare_dispatch(queue, new_demand, Enum.reverse(to_send), to_ack, transformer, new_state)

        new_to_send = transform_bulk(to_send, bulk_transformer)
        final_demand = lower_demand(demand, new_to_send)
        {new_to_send, to_ack, final_demand, queue, new_transformer_state}

      {:empty, queue} ->
        {[], :no_ack, demand, queue, state}
    end
  end

  defp transform_bulk(to_send, nil) do
    to_send
  end

  defp transform_bulk(to_send, bulk_transformer) do
    bulk_transformer.(to_send)
  end

  @spec prepare_dispatch(
          buffered_msgs :: :queue.queue(),
          buffered_demand :: non_neg_integer(),
          msgs_to_send :: [term()],
          to_ack :: ack(),
          transformer :: (msg_tuple(), term() -> {term(), term()}),
          transformer_state :: term
        ) :: dispatch()
  defp prepare_dispatch(queue, 0, to_send, to_ack, _transformer, state) do
    {Enum.reverse(to_send), to_ack, 0, queue, state}
  end

  defp prepare_dispatch(queue, demand, to_send, to_ack, transformer, state) do
    case :queue.out(queue) do
      {{:value, msg}, queue} ->
        {more_to_send, new_state, next_to_ack} = transform(msg, transformer, state)
        new_demand = lower_demand(demand, more_to_send)
        new_to_send = Enum.reduce(more_to_send, to_send, fn msg, acc -> [msg | acc] end)
        prepare_dispatch(queue, new_demand, new_to_send, next_to_ack, transformer, new_state)

      {:empty, queue} ->
        {Enum.reverse(to_send), to_ack, demand, queue, state}
    end
  end

  @spec transform(msg_tuple(), fun(), term()) :: {[term()], term(), ack()}
  def transform({offset, _, _, _} = msg, transformer, state) do
    case transformer.(msg, state) do
      {result, new_state} when is_list(result) -> {result, new_state, offset}
      {non_list_result, new_state} -> {[non_list_result], new_state, offset}
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
