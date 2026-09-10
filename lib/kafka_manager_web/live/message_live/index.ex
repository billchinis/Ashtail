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

  defmodule InvalidPartitionError do
    @moduledoc """
    Raised for a `:partition` path segment that is not a non-negative
    integer (e.g. `/topics/orders/partitions/x`). Carries `plug_status: 404`
    so Phoenix renders the app's normal "not found" page instead of an
    unhandled 500 on this public route.
    """
    defexception message: "invalid partition", plug_status: 404
  end

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
    case parse_partition(partition) do
      {:ok, partition} ->
        offset = parse_offset(params["offset"])
        page_size = parse_page_size(params["page_size"])

        socket =
          socket
          |> assign(
            topic_name: topic,
            partition: partition,
            offset: offset,
            page_size: page_size
          )
          |> fetch_messages(topic, partition, offset, page_size)

        {:noreply, socket}

      :error ->
        raise InvalidPartitionError, message: "invalid partition: #{inspect(partition)}"
    end
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
    {:noreply, toggle_expanded_by_string(socket, offset, true)}
  end

  def handle_event("collapse_value", %{"offset" => offset}, socket) do
    {:noreply, toggle_expanded_by_string(socket, offset, false)}
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

  # A client-supplied `offset` (`phx-value-offset`) that is not a valid
  # integer must not crash the page via `String.to_integer/1`: it simply
  # cannot match any row, the same as an offset that is a real integer but
  # not currently in `:message_index`.
  defp toggle_expanded_by_string(socket, offset_string, expanded?) do
    case Integer.parse(offset_string) do
      {offset, ""} -> toggle_expanded(socket, offset, expanded?)
      _ -> socket
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

  # A malformed `:partition` path segment (e.g. "x" in
  # `/topics/orders/partitions/x`) must not crash via `String.to_integer/1`
  # on this public route.
  defp parse_partition(value) do
    case Integer.parse(value) do
      {int, ""} when int >= 0 -> {:ok, int}
      _ -> :error
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

  # `:message_index` is a lookup table for what is currently on screen
  # (PLAN 4.1, P4 — "bounded by page size"), not an ever-growing log: every
  # tick merges in the new messages, then the oldest entries beyond
  # `page_size` are dropped so a long-running tail does not grow the
  # LiveView's heap (values included) without bound. The `:messages` stream
  # is trimmed to the same window (`limit: -page_size` keeps the most
  # recent `page_size` rows, evicting the oldest), so the expander keeps
  # working for exactly what is on screen and never for a row that has
  # scrolled off.
  defp append_tail_messages(socket, messages) do
    page_size = socket.assigns.page_size
    index_additions = Map.new(messages, &{&1.offset, &1})
    next_offset = messages |> List.last() |> Map.fetch!(:offset) |> Kernel.+(1)

    socket =
      socket
      |> assign(tail_offset: next_offset)
      |> update(:message_index, &trim_index(Map.merge(&1, index_additions), page_size))

    Enum.reduce(messages, socket, fn message, acc ->
      stream_insert(acc, :messages, to_row(message, false), at: -1, limit: -page_size)
    end)
  end

  defp trim_index(index, limit) when map_size(index) <= limit, do: index

  defp trim_index(index, limit) do
    index
    |> Enum.sort_by(fn {offset, _message} -> offset end, :desc)
    |> Enum.take(limit)
    |> Map.new()
  end
end
