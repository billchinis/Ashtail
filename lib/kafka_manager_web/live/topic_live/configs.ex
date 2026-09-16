defmodule KafkaManagerWeb.TopicLive.Configs do
  @moduledoc """
  The Configs sub-menu, showing the topic's broker configuration.
  """

  use KafkaManagerWeb, :live_view

  alias KafkaManager.Kafka
  alias KafkaManager.Kafka.BrokerError

  @impl true
  def mount(_params, _session, socket) do
    socket =
      socket
      |> assign(broker_error: nil, topic: nil)
      |> stream_configure(:topic_configs, dom_id: &("config-" <> slug(&1.name)))
      |> stream(:topic_configs, [])

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
    case Kafka.get_topic(name) do
      {:ok, topic} ->
        socket
        |> assign(broker_error: nil, topic: topic)
        |> stream(:topic_configs, topic.config, reset: true)

      {:error, %BrokerError{} = error} ->
        assign(socket, broker_error: error, topic: nil)
    end
  end

  defp slug(name), do: String.replace(name, ~r/[^a-zA-Z0-9]+/, "-")
end
