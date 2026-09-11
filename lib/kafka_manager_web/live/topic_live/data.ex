defmodule KafkaManagerWeb.TopicLive.Data do
  @moduledoc """
  The Data sub-menu, the topic's landing view at `/topics/:topic`. Merges
  every partition's messages into one newest-first, paged list via
  `Kafka.read_topic/2` (docs/PLAN.md 2.4, 4.9), reusing R2's message row
  components.

  AC-18 built page mode, paging and value expansion (P4). AC-19 adds the
  key/value/header/partition filter, which reads through the same function
  in scan mode: any active filter runs `Kafka.read_topic/2` in a
  `start_async` task instead of synchronously, so an uncapped scan never
  blocks the page (docs/PLAN.md 4.9, P8). AC-20 adds the time range: `from`
  and `to` are query params like every other filter field, and the form's
  Time range preset select is UI-only — it fills `from`/`to` on change but
  never patches or reads on its own (docs/PLAN.md 4.9). Tailing (AC-21) is
  not wired yet.
  """

  use KafkaManagerWeb, :live_view

  alias KafkaManager.Kafka
  alias KafkaManager.Kafka.{BrokerError, Filter}
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
        filter: Filter.none(),
        filter_form: to_form(%{}, as: :filter),
        filter_errors: %{},
        filter_params: %{},
        older: nil,
        newer: nil,
        page_high: nil,
        scan_id: nil,
        scan_state: :complete,
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
    %{page_size: page_size, cursor: cursor, filter_params: filter_params} =
      DataParams.parse(params)

    scan_id = make_ref()

    socket =
      socket
      |> maybe_cancel_scan()
      |> assign(
        topic_name: topic,
        page_size: page_size,
        cursor: cursor,
        filter_params: filter_params,
        filter_form: to_form(filter_params, as: :filter),
        scan_id: scan_id
      )
      |> fetch_topic(topic)
      |> load(topic, page_size, cursor, filter_params, scan_id)

    {:noreply, socket}
  end

  @impl true
  def handle_event("filter_change", %{"filter" => filter_params} = params, socket) do
    filter_params = maybe_fill_range(params["_target"], filter_params)
    {:noreply, assign(socket, filter_form: to_form(filter_params, as: :filter))}
  end

  def handle_event("page_size", %{"page_size" => page_size}, socket) do
    page_size = parse_page_size(page_size)

    {:noreply,
     push_patch(socket,
       to:
         DataParams.path(
           socket.assigns.topic_name,
           socket.assigns.filter_params,
           page_size,
           socket.assigns.cursor
         )
     )}
  end

  def handle_event("apply", %{"filter" => filter_params}, socket) do
    {:noreply,
     push_patch(socket,
       to: DataParams.path(socket.assigns.topic_name, filter_params, socket.assigns.page_size)
     )}
  end

  def handle_event("clear", _params, socket) do
    {:noreply,
     push_patch(socket,
       to: DataParams.path(socket.assigns.topic_name, %{}, socket.assigns.page_size)
     )}
  end

  def handle_event("expand_value", %{"partition" => partition, "offset" => offset}, socket) do
    {:noreply, toggle_expanded(socket, partition, offset, true)}
  end

  def handle_event("collapse_value", %{"partition" => partition, "offset" => offset}, socket) do
    {:noreply, toggle_expanded(socket, partition, offset, false)}
  end

  @impl true
  def handle_info(
        {:scan_progress, scan_id, %{scanned: scanned, messages: rows}},
        %{assigns: %{scan_id: scan_id}} = socket
      ) do
    additions = Map.new(rows, &{{&1.partition, &1.offset}, &1})

    socket =
      socket
      |> assign(
        scanned: scanned,
        message_index: Map.merge(socket.assigns.message_index, additions)
      )
      |> insert_rows(rows)

    {:noreply, socket}
  end

  # Progress from a superseded scan (a new search already minted a fresh
  # `scan_id`, or the scan was cancelled): drop it (docs/PLAN.md 4.9).
  def handle_info({:scan_progress, _stale_id, _progress}, socket), do: {:noreply, socket}

  @impl true
  def handle_async(
        {:scan, scan_id},
        {:ok, result},
        %{assigns: %{scan_id: scan_id}} = socket
      ) do
    socket =
      case result do
        {:ok, page} ->
          apply_page(socket, page)

        {:error, %BrokerError{} = error} ->
          socket
          |> assign(broker_error: first_error(socket.assigns.broker_error, error))
          |> assign(scan_state: :complete)
      end

    {:noreply, socket}
  end

  def handle_async({:scan, scan_id}, {:exit, reason}, %{assigns: %{scan_id: scan_id}}) do
    # A crash inside the read engine, not a broker failure: surfacing it as
    # a %BrokerError{} would hide a real bug (docs/PLAN.md 4.9).
    raise "Data view scan crashed: #{inspect(reason)}"
  end

  # A result for a superseded scan (cancelled by a new search): drop it,
  # including the `{:exit, {:shutdown, :cancel}}` a cancelled scan delivers.
  def handle_async({:scan, _stale_id}, _result, socket), do: {:noreply, socket}

  defp maybe_cancel_scan(%{assigns: %{scan_state: :running, scan_id: scan_id}} = socket)
       when not is_nil(scan_id) do
    cancel_async(socket, {:scan, scan_id})
  end

  defp maybe_cancel_scan(socket), do: socket

  defp fetch_topic(socket, topic) do
    case Kafka.topic_summary(topic) do
      {:ok, summary} ->
        assign(socket, topic: summary, broker_error: nil)

      {:error, %BrokerError{} = error} ->
        assign(socket, topic: nil, broker_error: error)
    end
  end

  defp load(socket, topic, page_size, cursor, filter_params, scan_id) do
    case Kafka.parse_filter(filter_params) do
      {:error, errors} ->
        socket
        |> assign(filter: Filter.none(), filter_errors: errors, scan_state: :complete, scanned: 0)
        |> stream(:messages, [], reset: true)

      {:ok, filter} ->
        socket = assign(socket, filter: filter, filter_errors: %{})

        if Filter.active?(filter) do
          start_scan(socket, topic, filter, page_size, cursor, scan_id)
        else
          fetch_page(socket, topic, filter, page_size, cursor)
        end
    end
  end

  defp start_scan(socket, topic, filter, page_size, cursor, scan_id) do
    live_view = self()

    socket
    |> assign(scan_state: :running, scanned: 0, message_index: %{})
    |> stream(:messages, [], reset: true)
    |> start_async({:scan, scan_id}, fn ->
      Kafka.read_topic(topic,
        page_size: page_size,
        cursor: cursor,
        filter: filter,
        on_progress: fn progress -> send(live_view, {:scan_progress, scan_id, progress}) end
      )
    end)
  end

  # P3's two-fetch rule: a successful page read must not clear an error the
  # topic summary fetch already set (broker_error keeps the first error of
  # the two).
  defp fetch_page(socket, topic, filter, page_size, cursor) do
    case Kafka.read_topic(topic, page_size: page_size, cursor: cursor, filter: filter) do
      {:ok, page} ->
        apply_page(socket, page)

      {:error, %BrokerError{} = error} ->
        socket
        |> assign(broker_error: first_error(socket.assigns.broker_error, error))
        |> assign(scan_state: :complete, scanned: 0)
    end
  end

  defp apply_page(socket, page) do
    index = Map.new(page.messages, &{{&1.partition, &1.offset}, &1})
    rows = Enum.map(page.messages, &to_row(&1, false))

    socket
    |> assign(
      older: page.older,
      newer: page.newer,
      scanned: page.scanned,
      message_index: index,
      scan_state: :complete,
      page_high: page_high(socket, page)
    )
    |> assign_halted_error(page.halted)
    |> stream(:messages, rows, reset: true)
  end

  defp page_high(%{assigns: %{cursor: nil}}, page) do
    Map.new(page.range, fn {p, {_low, high}} -> {p, high} end)
  end

  defp page_high(%{assigns: %{page_high: page_high}}, _page), do: page_high

  defp assign_halted_error(socket, nil), do: socket

  defp assign_halted_error(socket, {:match_limit, field}) do
    message =
      "This pattern needs too much backtracking to evaluate (for example nested repeats " <>
        "such as `(a+)+`). Simplify it."

    assign(socket, filter_errors: Map.put(socket.assigns.filter_errors, field, message))
  end

  defp insert_rows(socket, rows) do
    Enum.reduce(rows, socket, fn message, socket ->
      stream_insert(socket, :messages, to_row(message, false), at: -1)
    end)
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

  defp partition_options(nil), do: []
  defp partition_options(%{partition_count: count}), do: 0..(count - 1)

  defp applied_filter_count(filter_params) do
    ["key", "value", "header", "partition", "from", "to"]
    |> Enum.map(&Map.get(filter_params, &1))
    |> Enum.count(&(&1 not in [nil, ""]))
  end

  # Selecting the Time range preset fills From/To with a range ending now
  # (DESIGN.md Assumption 9); it never patches or reads on its own
  # (docs/PLAN.md 4.9). Editing From or To by hand leaves the form alone,
  # which is what makes that the custom range.
  defp maybe_fill_range(["filter", "range"], filter_params) do
    case range_seconds(filter_params["range"]) do
      nil -> filter_params
      :clear -> Map.merge(filter_params, %{"from" => "", "to" => ""})
      seconds -> Map.merge(filter_params, range_bounds(seconds))
    end
  end

  defp maybe_fill_range(_target, filter_params), do: filter_params

  defp range_seconds(""), do: :clear
  defp range_seconds("15m"), do: 15 * 60
  defp range_seconds("1h"), do: 60 * 60
  defp range_seconds("24h"), do: 24 * 60 * 60
  defp range_seconds("7d"), do: 7 * 24 * 60 * 60
  defp range_seconds(_other), do: nil

  defp range_bounds(seconds) do
    to = DateTime.utc_now() |> DateTime.truncate(:millisecond)
    from = DateTime.add(to, -seconds, :second)
    %{"from" => iso_timestamp(from), "to" => iso_timestamp(to)}
  end
end
