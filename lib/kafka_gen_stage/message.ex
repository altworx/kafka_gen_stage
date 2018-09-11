defmodule KafkaGenStage.Message do
  @moduledoc """
  Raw kafka message struct for mapping from brod kafka_message record.
  """

  defstruct [:offset, :timestamp, :key, :value]

  @type t :: %KafkaGenStage.Message{
    offset: integer(),
    timestamp: integer(),
    key: String.t(),
    value: String.t()
  }
end
