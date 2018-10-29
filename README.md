# KafkaGenStage
Fast GenStages for reading and writting into [Apache Kafka](https://kafka.apache.org/).
GenStages are built using excelent feature-full [Klarna's Brod](https://github.com/klarna/brod) 
Kafka client.

Consumer is buffering events as well as demand.

Producer (**coming soon**) is controlling demand manually, synchronizing GenStage's *ask* with brod's *ack*.

## Example

```elixir
brod_init = fn -> :brod.start_link_client([{'localhost', 9092}]) end
{:ok, consumer} = KafkaGenStage.Consumer.start_link(brod_init, "topic_id", read_until: :latest)
```
