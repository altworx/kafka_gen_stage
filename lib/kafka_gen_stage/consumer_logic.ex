defmodule KafkaGenStage.ConsumerLogic do
  @moduledoc """
  Separates pure logic of consumer genstage to be unit tested.
  """

  @type msg_tuple ::
          {offset :: pos_integer(), timestamp :: pos_integer(), key :: binary(),
           value :: binary()}
  @type ack :: pos_integer() | :no_ack

  @type dispatch ::
          {msgs_to_send :: [msg_tuple()], ack_to_brod_consumer :: ack(),
           buffered_demand :: pos_integer(), buffered_msgs :: :queue.queue()}

  @doc """
  Handles caching of both, demand and events, as recomended in *Buffering Demand* in gen_stage docs.
  Thus called from both, `KafkaGenStage.Consumer.handle_demand/2` and
  `KafkaGenStage.Consumer.handle_info/2` where events from *brod_consumer* arrive.
  """
  @spec prepare_dispatch(buffered_msgs :: :queue.queue(), buffered_demand :: pos_integer()) ::
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
          buffered_demand :: pos_integer(),
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

  @spec message_into_queue_reducer(
          msg_tuple(),
          :cont | :halt,
          :infinity | integer(),
          :queue.queue(msg_tuple())
        ) :: {:cont | :halt, :queue.queue(msg_tuple())}
  def message_into_queue_reducer(msg, flag, :infinity, queue), do: {flag, :queue.in(msg, queue)}
  def message_into_queue_reducer(_, :halt, _, queue), do: {:halt, queue}

  def message_into_queue_reducer({offset, _, _, _} = msg, :cont = flag, end_offset, queue) do
    cond do
      offset < end_offset -> {flag, :queue.in(msg, queue)}
      offset == end_offset -> {:halt, :queue.in(msg, queue)}
      offset > end_offset -> {:halt, queue}
    end
  end
end
