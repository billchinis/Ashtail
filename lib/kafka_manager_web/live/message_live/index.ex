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
      |> assign(
        broker_error: nil,
        topic_name: nil,
        partition: nil,
        offset: 0,
        page_size: @default_page_size,
        earliest: nil,
        latest: nil
      )
      |> stream_configure(:messages, dom_id: &("message-" <> to_string(&1.offset)))
      |> stream(:messages, [])

    {:ok, socket}
  end

  @impl true
  def handle_params(%{"topic" => topic, "partition" => partition} = params, _uri, socket) do
    offset = parse_offset(params["offset"])
    page_size = parse_page_size(params["page_size"])
    partition = String.to_integer(partition)

    socket =
      socket
      |> assign(topic_name: topic, partition: partition, offset: offset, page_size: page_size)
      |> fetch_messages(topic, partition, offset, page_size)

    {:noreply, socket}
  end

  @impl true
  def handle_event("page_size", %{"page_size" => page_size}, socket) do
    {:noreply,
     push_patch(socket,
       to:
         message_path(
           socket.assigns.topic_name,
           socket.assigns.partition,
           socket.assigns.offset,
           page_size
         )
     )}
  end

  def message_path(topic, partition, offset, page_size) do
    ~p"/topics/#{topic}/partitions/#{partition}?#{[offset: offset, page_size: page_size]}"
  end

  defp fetch_messages(socket, topic, partition, offset, page_size) do
    case Kafka.fetch_messages(topic, partition, offset, page_size) do
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

  defp parse_page_size(page_size) when page_size in ["20", "50"], do: String.to_integer(page_size)
  defp parse_page_size(_), do: @default_page_size
end
