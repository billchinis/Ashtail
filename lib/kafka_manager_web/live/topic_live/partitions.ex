defmodule KafkaManagerWeb.TopicLive.Partitions do
  @moduledoc """
  AC-5: the Partitions sub-menu, showing each partition's leader, replicas
  and earliest/latest offset.
  """

  use KafkaManagerWeb, :live_view

  alias KafkaManager.Kafka
  alias KafkaManager.Kafka.BrokerError

  @impl true
  def mount(_params, _session, socket) do
    socket =
      socket
      |> assign(broker_error: nil, topic: nil)
      |> stream_configure(:partitions, dom_id: &("partition-" <> to_string(&1.id)))
      |> stream(:partitions, [])

    {:ok, socket}
  end

  @impl true
  def handle_params(%{"topic" => name}, _uri, socket) do
    socket =
      socket
      |> assign(topic_name: name)
      |> fetch_topic(name)

    {:noreply, socket}
  end

  defp fetch_topic(socket, name) do
    case Kafka.topic_summary(name) do
      {:ok, topic} ->
        socket
        |> assign(broker_error: nil, topic: topic)
        |> stream(:partitions, topic.partitions, reset: true)

      {:error, %BrokerError{} = error} ->
        assign(socket, broker_error: error, topic: nil)
    end
  end
end
