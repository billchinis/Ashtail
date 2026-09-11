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
  and `to` are query params like every other filter field, entered directly
  (no preset control). AC-21 extends the
  per-partition browser's tail (docs/PLAN.md 4.5) across every scoped
  partition (docs/PLAN.md 4.10): a LiveView-owned timer reads forward from
  `tail_from` (a per-partition offset map) through the same
  `Kafka.read_topic/2`, so the active filter applies to tailed rows exactly
  as it does to a page or a scan. New rows are prepended and the stream is
  trimmed to `page_size`. Turning tailing on returns to page 1, and the
  tail starts exactly where that page ended, once it has loaded.
  """

  use KafkaManagerWeb, :live_view

  alias KafkaManager.Kafka
  alias KafkaManager.Kafka.{BrokerError, Filter}
  alias KafkaManagerWeb.TopicLive.DataParams

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
        message_index: %{},
        tailing?: false,
        tail_from: nil,
        tail_ref: nil,
        json_rows: [],
        next_json_id: 0
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

    {json_rows, next_json_id} = rebuild_json_rows(filter_params["json"])

    socket =
      socket
      |> maybe_cancel_scan()
      |> assign(
        topic_name: topic,
        page_size: page_size,
        cursor: cursor,
        filter_params: filter_params,
        filter_form: to_form(filter_params, as: :filter),
        json_rows: json_rows,
        next_json_id: next_json_id,
        scan_id: scan_id
      )
      |> maybe_stop_tail(cursor)
      |> fetch_topic(topic)
      |> load(topic, page_size, cursor, filter_params, scan_id)

    {:noreply, socket}
  end

  @impl true
  def handle_event("filter_change", %{"filter" => filter_params}, socket) do
    socket =
      socket
      |> assign(filter_form: to_form(filter_params, as: :filter))
      |> sync_json_rows(filter_params["json"])

    {:noreply, socket}
  end

  def handle_event("add_json_condition", _params, socket) do
    id = socket.assigns.next_json_id
    row = %{id: id, path: "", op: "equals", value: ""}

    {:noreply, assign(socket, json_rows: socket.assigns.json_rows ++ [row], next_json_id: id + 1)}
  end

  def handle_event("remove_json_condition", %{"row" => row}, socket) when is_binary(row) do
    {:noreply, remove_json_row(socket, row)}
  end

  # A non-binary `row` (a crafted payload) must not crash `Integer.parse/1`
  # in `remove_json_row/2`, the same as the offset handlers above (fix run
  # item 7): it simply cannot match any row, so it is a no-op.
  def handle_event("remove_json_condition", _params, socket), do: {:noreply, socket}

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
        # A rejected filter must not tail at all (docs/PLAN.md 4.10): the
        # form still shows the rejected filter, so tailing on with no filter
        # would silently show every message. `older`/`newer` are reset too,
        # since no read happened and the previous filter's cursors no
        # longer apply (docs/PLAN.md 4.9).
        socket
        |> assign(
          filter: Filter.none(),
          filter_errors: errors,
          scan_state: :complete,
          scanned: 0,
          older: nil,
          newer: nil,
          page_high: nil
        )
        |> stop_tail()
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
    # `older`/`newer` are reset so the previous read's pager links (possibly
    # from a different filter) do not stay on screen while this scan runs
    # (docs/PLAN.md 4.9). `page_high` is reset so a tail switched on while
    # this scan is still running cannot arm itself from a stale, previous
    # filter's high-water mark (docs/PLAN.md 4.10) — `toggle_tail/2` only
    # trusts `page_high` once it reflects this read.
    |> assign(
      scan_state: :running,
      scanned: 0,
      message_index: %{},
      older: nil,
      newer: nil,
      page_high: nil
    )
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
    high = page_high(socket, page)

    socket
    |> assign(
      older: page.older,
      newer: page.newer,
      scanned: page.scanned,
      message_index: index,
      scan_state: :complete,
      page_high: high
    )
    |> assign_halted_error(page.halted)
    |> maybe_arm_tail(high)
    |> stream(:messages, rows, reset: true)
  end

  defp page_high(%{assigns: %{cursor: nil}}, page) do
    Map.new(page.range, fn {p, {_low, high}} -> {p, high} end)
  end

  defp page_high(%{assigns: %{page_high: page_high}}, _page), do: page_high

  # Arms the tail the moment a page-1 read completes while `tailing?` is
  # true and `tail_from` is still `nil` (docs/PLAN.md 4.10): that is either
  # the read that just completed after tailing was switched on, or the
  # page-1 reload `toggle_tail/2` forced when tailing was switched on from
  # a cursor page. Once armed, later page-1 reads (a new filter, a new
  # scan) leave `tail_from` alone.
  defp maybe_arm_tail(socket, high) do
    %{tailing?: tailing?, cursor: cursor, tail_from: tail_from} = socket.assigns

    if tailing? and cursor == nil and is_nil(tail_from) do
      tail_ref = if connected?(socket), do: schedule_tail(), else: nil
      assign(socket, tail_from: high, tail_ref: tail_ref)
    else
      socket
    end
  end

  # Paging away from page 1 ends the tail (docs/PLAN.md 4.9, 4.10): a
  # non-`nil` cursor in the freshly parsed URL stops it before the new page
  # loads.
  defp maybe_stop_tail(socket, nil), do: socket
  defp maybe_stop_tail(socket, _cursor), do: stop_tail(socket)

  # Cancels the timer and turns tailing off, if it is on. Idempotent, so
  # every caller (paging away, a rejected filter, the toggle button) can
  # reach for it without first checking `tailing?` itself.
  defp stop_tail(%{assigns: %{tailing?: true}} = socket) do
    cancel_tail(socket.assigns.tail_ref)
    assign(socket, tailing?: false, tail_ref: nil, tail_from: nil)
  end

  defp stop_tail(socket), do: socket

  # PLAN 4.10: turning tailing on returns to page 1. On page 1 with a
  # completed read, the tail is gap-free from the start: `tail_from` is set
  # to that read's `high` map straight away. From a cursor page, or while
  # page 1's own scan is still running, `tail_from` stays `nil` and
  # `maybe_arm_tail/2` sets it once a page-1 read completes.
  defp toggle_tail(socket, false), do: stop_tail(socket)

  # A currently rejected filter must never tail (fix run item 4): with no
  # valid filter, `load/5` never runs a read, so `tail_from` could never be
  # armed and the indicator would be stuck showing "live" forever. Refusing
  # the toggle outright, instead of falling through to the catch-all clause
  # below, keeps `tailing?` false and the indicator correctly off.
  defp toggle_tail(%{assigns: %{filter_errors: filter_errors}} = socket, true)
       when filter_errors != %{} do
    socket
  end

  # `scan_state: :complete` is required, not just a non-`nil` `page_high`
  # (docs/PLAN.md 4.10): `start_scan/6` clears `page_high` before a scan
  # runs, but the explicit guard also protects against a future read path
  # that leaves a stale `page_high` in place while `scan_state` is
  # `:running`. Without it, switching the tail on mid-scan could arm
  # `tail_from` from a previous filter's completed page-1 read instead of
  # waiting for the running scan's own result.
  defp toggle_tail(
         %{assigns: %{cursor: nil, page_high: page_high, scan_state: :complete}} = socket,
         true
       )
       when not is_nil(page_high) do
    tail_ref = if connected?(socket), do: schedule_tail(), else: nil
    assign(socket, tailing?: true, tail_from: page_high, tail_ref: tail_ref)
  end

  defp toggle_tail(%{assigns: %{cursor: cursor}} = socket, true) when not is_nil(cursor) do
    socket
    |> assign(tailing?: true, tail_from: nil, tail_ref: nil)
    |> push_patch(
      to:
        DataParams.path(
          socket.assigns.topic_name,
          socket.assigns.filter_params,
          socket.assigns.page_size
        )
    )
  end

  defp toggle_tail(socket, true) do
    assign(socket, tailing?: true, tail_from: nil, tail_ref: nil)
  end

  defp schedule_tail, do: Process.send_after(self(), :tail_tick, tail_interval_ms())

  # Configurable, not just `@tail_interval_ms`, so a test can set it far
  # longer than its own run time (fix run item 3): otherwise a real,
  # correctly-timed tick firing mid-test can mask a bug in what a tick was
  # armed from, by quietly catching a stale value up to the right one before
  # the test gets to look at it.
  defp tail_interval_ms,
    do: Application.get_env(:kafka_manager, :data_tail_interval_ms, @tail_interval_ms)

  defp cancel_tail(nil), do: :ok
  defp cancel_tail(ref), do: Process.cancel_timer(ref)

  # While `tail_from` is not yet set (waiting for page 1) or a scan is
  # running, reschedule and do nothing (docs/PLAN.md 4.10).
  defp tail_tick(%{assigns: %{tail_from: nil}} = socket) do
    assign(socket, tail_ref: schedule_tail())
  end

  defp tail_tick(%{assigns: %{scan_state: :running}} = socket) do
    assign(socket, tail_ref: schedule_tail())
  end

  defp tail_tick(socket) do
    %{topic_name: topic, filter: filter, tail_from: tail_from} = socket.assigns

    case Kafka.read_topic(topic,
           cursor: {:after, tail_from},
           filter: filter,
           page_size: @tail_limit,
           max_scanned: @tail_limit
         ) do
      {:ok, page} ->
        socket
        |> assign(broker_error: nil)
        |> insert_tail_rows(page.messages)
        |> assign(tail_from: range_high(page.range), tail_ref: schedule_tail())

      {:error, %BrokerError{} = error} ->
        assign(socket, broker_error: error, tailing?: false, tail_ref: nil)
    end
  end

  defp range_high(range), do: Map.new(range, fn {p, {_low, high}} -> {p, high} end)

  # Newest first ends on top: inserting the oldest of this tick's rows
  # first, each at the head, leaves the newest sitting above it last
  # (docs/PLAN.md 4.10). `:message_index` is trimmed to the same bounded
  # window (P4) so the expander keeps working for exactly what is on
  # screen.
  defp insert_tail_rows(socket, messages) do
    page_size = socket.assigns.page_size
    additions = Map.new(messages, &{{&1.partition, &1.offset}, &1})

    socket =
      update(socket, :message_index, &trim_message_index(Map.merge(&1, additions), page_size))

    messages
    |> Enum.reverse()
    |> Enum.reduce(socket, fn message, acc ->
      stream_insert(acc, :messages, to_row(message, false), at: 0, limit: page_size)
    end)
  end

  defp trim_message_index(index, limit) when map_size(index) <= limit, do: index

  defp trim_message_index(index, limit) do
    index
    |> Enum.sort_by(fn {_key, message} -> merge_key(message) end, :desc)
    |> Enum.take(limit)
    |> Map.new()
  end

  defp merge_key(%{timestamp: timestamp, partition: partition, offset: offset}),
    do: {DateTime.to_unix(timestamp, :millisecond), partition, offset}

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

  # `handle_params/3` is the only source of applied JSON rows (docs/PLAN.md
  # 4.11): a row's id always equals its position, since `DataParams.parse/1`
  # already dropped blank-path rows and renumbered the rest. `next_json_id`
  # continues from there, so ids are never reused across an apply.
  defp rebuild_json_rows(json_conditions) do
    rows =
      json_conditions
      |> Enum.with_index()
      |> Enum.map(fn {row, id} ->
        %{id: id, path: row["path"], op: row["op"], value: row["value"]}
      end)

    {rows, length(rows)}
  end

  # Copies typed `path`/`op`/`value` into the matching row by id, so a
  # re-render (an added row, scan progress) never reverts what a person is
  # typing (docs/PLAN.md 4.11). An id the form does not know, or a
  # non-binary field, is ignored.
  defp sync_json_rows(socket, json_params) when is_map(json_params) do
    rows =
      Enum.map(socket.assigns.json_rows, fn row ->
        case Map.get(json_params, Integer.to_string(row.id)) do
          %{} = fields ->
            %{
              row
              | path: string_or(fields["path"], row.path),
                op: string_or(fields["op"], row.op),
                value: string_or(fields["value"], row.value)
            }

          _other ->
            row
        end
      end)

    assign(socket, json_rows: rows)
  end

  defp sync_json_rows(socket, _json_params), do: socket

  defp string_or(value, _default) when is_binary(value), do: value
  defp string_or(_value, default), do: default

  # A client-supplied row id (`phx-value-row`) that is not a valid integer,
  # or matches no row, must not crash the page (the produce form's
  # `remove_header_row/2` is the model). Zero rows is a valid state, unlike
  # the produce form's last header row, so there is no length guard here.
  defp remove_json_row(socket, row) do
    case Integer.parse(row) do
      {id, ""} ->
        assign(socket, json_rows: Enum.reject(socket.assigns.json_rows, &(&1.id == id)))

      _other ->
        socket
    end
  end

  # Counts the applied filters, from `filter_params` (the URL), never the
  # unapplied form (docs/PLAN.md 4.11). Every JSON row surviving
  # `DataParams`' normaliser already has a non-blank path, so it always
  # counts as one filter.
  defp applied_filter_count(filter_params) do
    scalar_count =
      ["key", "value", "header", "partition", "from", "to"]
      |> Enum.map(&Map.get(filter_params, &1))
      |> Enum.count(&(&1 not in [nil, ""]))

    scalar_count + length(Map.get(filter_params, "json", []))
  end
end
