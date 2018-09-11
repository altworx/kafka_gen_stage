# KafkaGenStage
Fast GenStage wrapper around brod producer and consumer.

Consumer is buffering events as well as demand.
Producer is controlling demand manually and manages asks on acks.

## Consumer
Brod client lifecycle is not managed internally by gen_stage. Either provide started brod client or
use function as *brod_client_init* argument.


```elixir

```

Documentation can be generated with [ExDoc](https://github.com/elixir-lang/ex_doc)
and published on [HexDocs](https://hexdocs.pm). Once published, the docs can
be found at [https://hexdocs.pm/kafka_gen_stage](https://hexdocs.pm/kafka_gen_stage).

