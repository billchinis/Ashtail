defmodule KafkaManagerWeb.MessageLive.Index do
  @moduledoc """
  AC-6: the message browser, reading a chosen partition from a chosen offset.
  """

  use KafkaManagerWeb, :live_view

  alias KafkaManager.Kafka
  alias KafkaManager.Kafka.BrokerError

  @default_page_size 50

  @impl true
  def mount(_params, _session, socket) do
    socket =
      socket
      |> assign(broker_error: nil, topic_name: nil, partition: nil, earliest: nil, latest: nil)
      |> stream_configure(:messages, dom_id: &("message-" <> to_string(&1.offset)))
      |> stream(:messages, [])

    {:ok, socket}
  end

  @impl true
  def handle_params(%{"topic" => topic, "partition" => partition} = params, _uri, socket) do
    offset = parse_offset(params["offset"])
    partition = String.to_integer(partition)

    socket =
      socket
      |> assign(topic_name: topic, partition: partition, offset: offset)
      |> fetch_messages(topic, partition, offset)

    {:noreply, socket}
  end

  defp fetch_messages(socket, topic, partition, offset) do
    case Kafka.fetch_messages(topic, partition, offset, @default_page_size) do
      {:ok, %{messages: messages, earliest: earliest, latest: latest}} ->
        socket
        |> assign(broker_error: nil, earliest: earliest, latest: latest)
        |> stream(:messages, messages, reset: true)

      {:error, %BrokerError{} = error} ->
        assign(socket, broker_error: error)
    end
  end

  defp parse_offset(nil), do: 0

  defp parse_offset(value) do
    case Integer.parse(value) do
      {int, ""} -> int
      _ -> 0
    end
  end
end
