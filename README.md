# KafkaGenStage
Fast GenStages for reading and writting into [Apache Kafka](https://kafka.apache.org/).
GenStages are built using excelent feature-full [Klarna's Brod](https://github.com/klarna/brod) 
Kafka client.

> Unfortunately both Kafka & GenStage are using consumer/producer naming.
> Note that what is **consumer** from Kafka's perspective, is **producer** from GenStage's.

`KafkaGenStage.Consumer` (so`:producer` GenStage) is buffering events as well as demand.

`KafkaGenStage.Producer` (**coming soon**) is controlling demand manually, synchronizing GenStage's *ask* with brod's *ack*.

## Example

```elixir
{:ok, consumer} = KafkaGenStage.Consumer.start_link(:started_brod_client, "topic_id", read_until: :latest)
```
