# Usage: mix run examples/producer_consumer.exs
#
# Hit Ctrl+C twice to stop it.

defmodule ConsoleConsumer do
  use GenStage

  def init(:ok) do
    {:consumer, :the_state_does_not_matter}
  end

  def handle_events(events, _from, state) do
    # Wait for a second.
    :timer.sleep(1000)

    # Inspect the events.
    IO.inspect(events)

    # We are a consumer, so we would never emit items.
    {:noreply, [], state}
  end
end

{:ok, regen} =
  KafkaGenStage.Consumer.start_link(
    fn -> :brod.start_link_client([{'localhost', 9092}]) end,
    "cargo_esb_xml",
    [read_until: :latest],
    []
  )

{:ok, c} = GenStage.start_link(ConsoleConsumer, :ok)

GenStage.sync_subscribe(c, to: regen)
Process.sleep(:infinity)
