defmodule KafkaGenStage.ConsumerLogicTest do
  @moduledoc false
  use ExUnit.Case, async: true

  alias KafkaGenStage.ConsumerLogic, as: Logic

  @msg0 {0, 1_540_458_185_657, "kafkey0", "kafval0"}
  @msg1 {1, 1_540_458_185_658, "kafkey1", "kafval1"}
  @msg2 {2, 1_540_458_185_659, "kafkey2", "kafval2"}
  @msgs [@msg0, @msg1, @msg2]

  test "prepare_dispatch with no demand -> no msgs to dispatch" do
    buffer = :queue.from_list(@msgs)
    assert Logic.prepare_dispatch(buffer, 0) == {[], :no_ack, 0, buffer}
  end

  test "prepare_dispatch with demand and envents -> return demand of events" do
    buffer = :queue.from_list(@msgs)
    {to_send, ack, remaining_demand, remaining_buffer} = Logic.prepare_dispatch(buffer, 2)

    assert to_send == [@msg0, @msg1]
    assert ack == 1
    assert remaining_demand == 0
    assert :queue.to_list(remaining_buffer) == [@msg2]
  end

  test "prepare_dispatch with more demand than events -> return all events and remaining demand" do
    expected = {@msgs, 2, 2, :queue.new()}
    assert Logic.prepare_dispatch(:queue.from_list(@msgs), 5) == expected
  end

  test "prepare_dispatch empty buffer just keeps demand" do
    assert Logic.prepare_dispatch(:queue.new(), 5) == {[], :no_ack, 5, :queue.new()}
  end


end
