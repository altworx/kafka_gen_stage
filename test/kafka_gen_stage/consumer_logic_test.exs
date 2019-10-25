defmodule KafkaGenStage.ConsumerLogicTest do
  @moduledoc false
  use ExUnit.Case, async: true

  alias KafkaGenStage.ConsumerLogic, as: Logic

  @msg0 {0, 1_540_458_185_657, "kafkey0", "kafval0"}
  @msg1 {1, 1_540_458_185_658, "kafkey1", "kafval1"}
  @msg2 {2, 1_540_458_185_659, "kafkey2", "kafval2"}
  @msgs [@msg0, @msg1, @msg2]
  @empty_queue :queue.new()

  defp bulk_transformer(bulk, _is_end_of_stream), do: bulk

  defp add_end_of_stream(bulk, is_end_of_stream) do
    if is_end_of_stream do
      bulk ++ [:end_of_stream]
    else
      bulk
    end
  end

  test "prepare_dispatch with no demand -> no msgs to dispatch" do
    buffer = :queue.from_list(@msgs)

    assert Logic.prepare_dispatch(buffer, 0, nil, false, @empty_queue) ==
             {[], :no_ack, 0, buffer, @empty_queue}
  end

  test "prepare_dispatch with demand and envents -> return demand of events" do
    buffer = :queue.from_list(@msgs)

    {to_send, ack, remaining_demand, remaining_buffer, @empty_queue} =
      Logic.prepare_dispatch(buffer, 2, &bulk_transformer/2, false, @empty_queue)

    assert to_send == [@msg0, @msg1]
    assert ack == 1
    assert remaining_demand == 0
    assert :queue.to_list(remaining_buffer) == [@msg2]
  end

  test "prepare_dispatch - bulk_transformer appends a message" do
    {to_send, ack, remaining_demand, remaining_buffer, @empty_queue} =
      Logic.prepare_dispatch(
        :queue.from_list(@msgs),
        5,
        fn x, _is_end_of_stream -> x ++ [@msg2] end,
        false,
        @empty_queue
      )

    assert to_send == [@msg0, @msg1, @msg2, @msg2]
    assert ack == 2
    assert remaining_demand == 1
    assert :queue.to_list(remaining_buffer) == []
  end

  test "prepare_dispatch with more demand than events -> return all events and remaining demand" do
    expected = {@msgs, 2, 2, :queue.new(), @empty_queue}

    assert Logic.prepare_dispatch(
             :queue.from_list(@msgs),
             5,
             &bulk_transformer/2,
             false,
             @empty_queue
           ) ==
             expected
  end

  test "prepare_dispatch empty buffer , generate end_of_stream" do
    assert Logic.prepare_dispatch(
             @empty_queue,
             5,
             &add_end_of_stream/2,
             true,
             @empty_queue
           ) ==
             {[:end_of_stream], :no_ack, 4, @empty_queue, @empty_queue}
  end

  test "end_of_stream is set only for the last bulk" do
    # if queue still contains some elements after dequeueing, end_of_stream
    # should not be set for the currently processed bulk
    {output, _, _, queue, eos_queue} =
      Logic.prepare_dispatch(
        :queue.from_list(@msgs),
        2,
        &add_end_of_stream/2,
        true,
        :queue.from_list([2])
      )

    assert output == Enum.take(@msgs, 2)

    {output, _, _, _, @empty_queue} =
      Logic.prepare_dispatch(queue, 2, &add_end_of_stream/2, true, eos_queue)

    assert output == [@msg2, :end_of_stream]
  end

  test "end_of_stream queue is cleaned properly" do
    # multiple message sets come from kafka, each is last at that point, and thus multiple offsets
    # are stored in eos queue
    # later demand comes and all the messages are dequeued and put into one big GenStage bulk, the #
    # bulk contains multiple EOS offsets, and thus has to recursively clean the EOS queue

    {_, _, _, _, eos_queue} =
      Logic.prepare_dispatch(
        :queue.from_list(@msgs),
        3,
        nil,
        false,
        :queue.from_list([0, 1, 2, 3])
      )

    assert :queue.to_list(eos_queue) == [3]
  end

  test "enqueue inserts all on infinity end offset" do
    queue = :queue.from_list([@msg0])
    queue = Logic.enqueue(queue, [@msg1, @msg2], :infinity)
    assert :queue.to_list(queue) == @msgs
  end

  test "enqueue inserts until end offset inclusive" do
    queue = Logic.enqueue(:queue.new(), @msgs, 1)
    assert :queue.to_list(queue) == [@msg0, @msg1]
  end

  test "enqueue doesn't insert after end offset" do
    queue = :queue.from_list([@msg0, @msg1])
    queue = Logic.enqueue(queue, [@msg2], 1)
    assert :queue.to_list(queue) == [@msg0, @msg1]
  end
end
