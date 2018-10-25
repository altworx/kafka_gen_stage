# KafkaGenStage
Fast GenStages for reading and writting into Apache Kafka.
GenStages are built using [Klarna's Brod](https://github.com/klarna/brod).

Consumer is buffering events as well as demand.
Producer is controlling demand manually, synchronizing GenStage's *ask* with brod's *ack*.


```elixir

```

Documentation can be generated with [ExDoc](https://github.com/elixir-lang/ex_doc)
and published on [HexDocs](https://hexdocs.pm). Once published, the docs can
be found at [https://hexdocs.pm/kafka_gen_stage](https://hexdocs.pm/kafka_gen_stage).

