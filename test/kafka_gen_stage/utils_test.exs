defmodule KafkaGenStage.UtilsTest do
  use ExUnit.Case

  import KafkaGenStage.Utils

  test "adjust_gen_stage_consumer_options for single name" do
    options = [subscribe_to: :name_of_producer_stage]
    assert [subscribe_to: {:name_of_producer_stage, cancel: :transient}] ==
      adjust_gen_stage_consumer_options(options)
  end
end
