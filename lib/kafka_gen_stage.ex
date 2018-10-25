defmodule KafkaGenStage do
  @moduledoc """
  Documentation for KafkaGenStage.

  See `KafkaGenStage.Consumer` for how to source messages from Kafka.

      consumer_stage = KafkaGenStage.Consumer.start_link(
        fn -> :brod.start_link_client([{'localhost', 9092}]) end,
        "topic_to_consume", read_until: :latest
      )

  Then see `KafkaGenStage.Producer` for how to feed messages to Kafka. (Soon)
  """

  @typedoc "Kafka topic identifier."
  @type topic :: String.t()

  @typedoc "Brod's type for where to start reading in kafka topic."
  @type begin_offset :: integer() | :earliest | :latest

end
