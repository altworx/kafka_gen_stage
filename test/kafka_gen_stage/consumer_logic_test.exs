defmodule KafkaGenStage.ConsumerLogicTest do
  @moduledoc false
  use ExUnit.Case, async: true

  alias KafkaGenStage.ConsumerLogic, as: Logic

  @msg0 {0, 1_540_458_185_657, "kafkey0", "kafval0"}
  @msg1 {1, 1_540_458_185_658, "kafkey1", "kafval1"}
  @msg2 {2, 1_540_458_185_659, "kafkey2", "kafval2"}
  @msgs [@msg0, @msg1, @msg2]

  def simple_transformer(msg, state), do: {msg, state}
  def bulk_transformer(bulk, _high_wm), do: bulk

  def add_end_of_stream(bulk, is_end_of_stream) do
    if is_end_of_stream do
      bulk ++ [:end_of_stream]
    else
      bulk
    end
  end

  test "prepare_dispatch with no demand -> no msgs to dispatch" do
    buffer = :queue.from_list(@msgs)

    assert Logic.prepare_dispatch(buffer, 0, nil, false) ==
             {[], :no_ack, 0, buffer}
  end

  test "prepare_dispatch with demand and envents -> return demand of events" do
    buffer = :queue.from_list(@msgs)

    {to_send, ack, remaining_demand, remaining_buffer} =
      Logic.prepare_dispatch(buffer, 2, &bulk_transformer/2, false)

    assert to_send == [@msg0, @msg1]
    assert ack == 1
    assert remaining_demand == 0
    assert :queue.to_list(remaining_buffer) == [@msg2]
  end

  test "prepare_dispatch - bulk_transformer appends a message" do
    {to_send, ack, remaining_demand, remaining_buffer} =
      Logic.prepare_dispatch(
        :queue.from_list(@msgs),
        5,
        fn x, _high_wm -> x ++ [@msg2] end,
        false
      )

    assert to_send == [@msg0, @msg1, @msg2, @msg2]
    assert ack == 2
    assert remaining_demand == 1
    assert :queue.to_list(remaining_buffer) == []
  end

  test "prepare_dispatch with more demand than events -> return all events and remaining demand" do
    expected = {@msgs, 2, 2, :queue.new()}

    assert Logic.prepare_dispatch(
             :queue.from_list(@msgs),
             5,
             &bulk_transformer/2,
             false
           ) ==
             expected
  end

  test "prepare_dispatch empty buffer , generate end_of_stream" do
    assert Logic.prepare_dispatch(
             :queue.new(),
             5,
             &add_end_of_stream/2,
             true
           ) ==
             {[:end_of_stream], :no_ack, 4, :queue.new()}
  end

  test "messages_into_queue inserts all on infinity end offset" do
    buffer = :queue.from_list([@msg0])
    {:cont, new_buffer} = Logic.messages_into_queue([@msg1, @msg2], buffer, :infinity)
    assert :queue.to_list(new_buffer) == @msgs
  end

  test "messages_into_queue insert until end offset inclusive" do
    {:halt, buffer} = Logic.messages_into_queue(@msgs, :queue.new(), 1)
    assert :queue.to_list(buffer) == [@msg0, @msg1]
  end

  test "message_into_queue dont insert after end offset" do
    buffer = :queue.from_list([@msg0, @msg1])
    {:halt, new_buffer} = Logic.messages_into_queue([@msg2], buffer, 1)
    assert :queue.to_list(new_buffer) == [@msg0, @msg1]
  end
end
