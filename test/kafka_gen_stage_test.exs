defmodule KafkaGenStageTest do
  use ExUnit.Case
  doctest KafkaGenStage

  test "greets the world" do
    assert KafkaGenStage.hello() == :world
  end
end
