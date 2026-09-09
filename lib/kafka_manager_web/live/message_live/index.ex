defmodule KafkaManagerWeb.MessageLive.Index do
  @moduledoc """
  AC-6: the message browser, reading a chosen partition from a chosen offset.
  AC-9: an optional tail, a LiveView-owned timer that polls for messages
  produced after the page opened and appends them to the stream in place.
  """

  use KafkaManagerWeb, :live_view

  alias KafkaManager.Kafka
  alias KafkaManager.Kafka.BrokerError

  @default_page_size 50
  @tail_interval_ms 1_000
  @tail_limit 500

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
        latest: nil,
        message_index: %{},
        tailing?: false,
        tail_offset: nil,
        tail_ref: nil
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

  def handle_event("expand_value", %{"offset" => offset}, socket) do
    {:noreply, toggle_expanded(socket, String.to_integer(offset), true)}
  end

  def handle_event("collapse_value", %{"offset" => offset}, socket) do
    {:noreply, toggle_expanded(socket, String.to_integer(offset), false)}
  end

  def handle_event("toggle_tail", _params, socket) do
    {:noreply, toggle_tail(socket, not socket.assigns.tailing?)}
  end

  @impl true
  def handle_info(:tail_tick, socket) do
    if socket.assigns.tailing? do
      {:noreply, tail_tick(socket)}
    else
      {:noreply, socket}
    end
  end

  def message_path(topic, partition, offset, page_size) do
    ~p"/topics/#{topic}/partitions/#{partition}?#{[offset: offset, page_size: page_size]}"
  end

  defp fetch_messages(socket, topic, partition, offset, page_size) do
    case Kafka.fetch_messages(topic, partition, offset, page_size) do
      {:ok, %{messages: messages, earliest: earliest, latest: latest}} ->
        index = Map.new(messages, &{&1.offset, &1})
        rows = Enum.map(messages, &to_row(&1, false))

        socket
        |> assign(broker_error: nil, earliest: earliest, latest: latest, message_index: index)
        |> stream(:messages, rows, reset: true)

      {:error, %BrokerError{} = error} ->
        assign(socket, broker_error: error)
    end
  end

  # P4 (PLAN 4.1): the toggle handler re-inserts only the affected row,
  # looked up from the bounded `:message_index` map built at fetch time.
  defp toggle_expanded(socket, offset, expanded?) do
    case socket.assigns.message_index[offset] do
      nil -> socket
      message -> stream_insert(socket, :messages, to_row(message, expanded?))
    end
  end

  defp to_row(message, expanded?) do
    message
    |> Map.from_struct()
    |> Map.put(:expanded?, expanded?)
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

  # PLAN 4.5: tailing?, tail_offset and tail_ref are UI-mode assigns, not URL
  # state, so toggling never `push_patch`es.
  defp toggle_tail(socket, true) do
    tail_offset = socket.assigns.latest || socket.assigns.offset
    tail_ref = if connected?(socket), do: schedule_tail(), else: nil

    assign(socket, tailing?: true, tail_offset: tail_offset, tail_ref: tail_ref)
  end

  defp toggle_tail(socket, false) do
    cancel_tail(socket.assigns.tail_ref)
    assign(socket, tailing?: false, tail_ref: nil)
  end

  defp schedule_tail, do: Process.send_after(self(), :tail_tick, @tail_interval_ms)

  defp cancel_tail(nil), do: :ok
  defp cancel_tail(ref), do: Process.cancel_timer(ref)

  defp tail_tick(socket) do
    %{topic_name: topic, partition: partition, tail_offset: tail_offset} = socket.assigns

    case Kafka.fetch_messages(topic, partition, tail_offset, @tail_limit) do
      {:ok, %{messages: messages}} ->
        socket
        |> assign(broker_error: nil)
        |> append_tail_messages(messages)
        |> assign(tail_ref: schedule_tail())

      {:error, %BrokerError{} = error} ->
        assign(socket, broker_error: error, tailing?: false, tail_ref: nil)
    end
  end

  defp append_tail_messages(socket, []), do: socket

  defp append_tail_messages(socket, messages) do
    index_additions = Map.new(messages, &{&1.offset, &1})
    next_offset = messages |> List.last() |> Map.fetch!(:offset) |> Kernel.+(1)

    socket = assign(socket, tail_offset: next_offset)

    socket =
      update(socket, :message_index, &Map.merge(&1, index_additions))

    Enum.reduce(messages, socket, fn message, acc ->
      stream_insert(acc, :messages, to_row(message, false), at: -1)
    end)
  end
end
