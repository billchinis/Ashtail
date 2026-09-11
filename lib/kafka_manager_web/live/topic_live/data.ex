defmodule KafkaManagerWeb.TopicLive.Data do
  @moduledoc """
  AC-18: the Data sub-menu, the topic's landing view at `/topics/:topic`.
  Merges every partition's messages into one newest-first, paged list via
  `Kafka.read_topic/2` (docs/PLAN.md 2.4, 4.9), reusing R2's message row
  components. Filtering (AC-19, AC-20) and tailing (AC-21) are not wired
  yet; this run only builds page mode, paging and value expansion (P4), so
  those slot in later without redesigning this module.
  """

  use KafkaManagerWeb, :live_view

  alias KafkaManager.Kafka
  alias KafkaManager.Kafka.BrokerError
  alias KafkaManagerWeb.TopicLive.DataParams

  @default_page_size 50

  @impl true
  def mount(_params, _session, socket) do
    socket =
      socket
      |> assign(
        broker_error: nil,
        topic_name: nil,
        topic: nil,
        page_size: @default_page_size,
        cursor: nil,
        older: nil,
        newer: nil,
        scanned: 0,
        message_index: %{}
      )
      |> stream_configure(:messages,
        dom_id: &("message-" <> to_string(&1.partition) <> "-" <> to_string(&1.offset))
      )
      |> stream(:messages, [])

    {:ok, socket}
  end

  @impl true
  def handle_params(%{"topic" => topic} = params, _uri, socket) do
    %{page_size: page_size, cursor: cursor} = DataParams.parse(params)

    socket =
      socket
      |> assign(topic_name: topic, page_size: page_size, cursor: cursor)
      |> fetch_topic(topic)
      |> fetch_page(topic, page_size, cursor)

    {:noreply, socket}
  end

  @impl true
  def handle_event("page_size", %{"page_size" => page_size}, socket) do
    page_size = parse_page_size(page_size)

    {:noreply,
     push_patch(socket,
       to: DataParams.path(socket.assigns.topic_name, page_size, socket.assigns.cursor)
     )}
  end

  def handle_event("expand_value", %{"partition" => partition, "offset" => offset}, socket) do
    {:noreply, toggle_expanded(socket, partition, offset, true)}
  end

  def handle_event("collapse_value", %{"partition" => partition, "offset" => offset}, socket) do
    {:noreply, toggle_expanded(socket, partition, offset, false)}
  end

  defp fetch_topic(socket, topic) do
    case Kafka.topic_summary(topic) do
      {:ok, summary} ->
        assign(socket, topic: summary, broker_error: nil)

      {:error, %BrokerError{} = error} ->
        assign(socket, topic: nil, broker_error: error)
    end
  end

  # P3's two-fetch rule: a successful page read must not clear an error the
  # topic summary fetch already set (broker_error keeps the first error of
  # the two).
  defp fetch_page(socket, topic, page_size, cursor) do
    case Kafka.read_topic(topic, page_size: page_size, cursor: cursor) do
      {:ok, page} ->
        index = Map.new(page.messages, &{{&1.partition, &1.offset}, &1})
        rows = Enum.map(page.messages, &to_row(&1, false))

        socket
        |> assign(
          older: page.older,
          newer: page.newer,
          scanned: page.scanned,
          message_index: index
        )
        |> stream(:messages, rows, reset: true)

      {:error, %BrokerError{} = error} ->
        assign(socket, broker_error: first_error(socket.assigns.broker_error, error))
    end
  end

  defp first_error(nil, new_error), do: new_error
  defp first_error(existing_error, _new_error), do: existing_error

  # Client-supplied `partition`/`offset` (`phx-value-*`) that are not valid
  # integers must not crash the page: they simply cannot match any row, the
  # same as a well-formed pair that is not currently in `:message_index`.
  defp toggle_expanded(socket, partition_string, offset_string, expanded?) do
    with {partition, ""} <- Integer.parse(partition_string),
         {offset, ""} <- Integer.parse(offset_string) do
      apply_toggle(socket, {partition, offset}, expanded?)
    else
      _ -> socket
    end
  end

  # P4 (PLAN 4.1, 2026-09-11): the toggle handler re-inserts only the
  # affected row, looked up from the bounded `:message_index` map keyed by
  # `{partition, offset}`.
  defp apply_toggle(socket, key, expanded?) do
    case socket.assigns.message_index[key] do
      nil -> socket
      message -> stream_insert(socket, :messages, to_row(message, expanded?))
    end
  end

  defp to_row(message, expanded?) do
    message
    |> Map.from_struct()
    |> Map.put(:expanded?, expanded?)
  end

  defp parse_page_size(page_size) when page_size in ["20", "50"], do: String.to_integer(page_size)
  defp parse_page_size(_page_size), do: @default_page_size
end
