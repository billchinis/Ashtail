defmodule KafkaManager.Kafka.TopicReader do
  @moduledoc """
  The merged read engine behind `KafkaManager.Kafka.read_topic/2`
  (docs/PLAN.md 2.4): a lazy k-way merge of every scoped partition's
  messages into one newest-first (or, for a forward cursor, oldest-first
  then reversed) page.

  It calls `Client.metadata/1`, `Client.list_offsets/3` and
  `Messages.read_range/5` only, never `Client.fetch/5` directly, which keeps
  the fetch-loop rules of `Messages` in one place.

  AC-18 wired the bounds, the cursor and the merge for the unfiltered case.
  AC-19 adds the filter predicate (`KafkaManager.Kafka.Filter`), progress
  reporting (`on_progress`), the scan budget (`max_scanned`) and the
  backtracking-limit halt (`halted`). AC-20 adds the time-range bounds
  (`filter.from`, `filter.to`): each set end costs one more batched
  `ListOffsets` call by timestamp, jumping straight to the start offset
  instead of scanning from the beginning (docs/PLAN.md 2.4).
  """

  alias KafkaManager.Kafka.{BrokerError, Client, Config, Filter, Message, Messages}

  @default_page_size 50
  @scan_chunk 500

  @typedoc "A per-partition offset map, the shape the URL cursor carries."
  @type offset_map :: %{non_neg_integer() => integer()}

  @typedoc "`nil` (page 1, newest), `{:before, m}` (Older) or `{:after, m}` (Newer)."
  @type cursor :: nil | {:before, offset_map()} | {:after, offset_map()}

  @doc """
  Reads one page of `topic`, merged newest first across every partition. See
  `KafkaManager.Kafka.read_topic/2` for the returned shape.
  """
  @spec read(Config.t(), String.t(), keyword()) :: {:ok, map()} | {:error, BrokerError.t()}
  def read(%Config{} = config, topic, opts \\ []) when is_binary(topic) do
    page_size = Keyword.get(opts, :page_size, @default_page_size)
    cursor = Keyword.get(opts, :cursor)
    filter = Keyword.get(opts, :filter, Filter.none())
    max_scanned = Keyword.get(opts, :max_scanned, :infinity)
    on_progress = Keyword.get(opts, :on_progress)

    with {:ok, metadata} <- Client.metadata(config),
         {:ok, partition_ids} <- topic_partition_ids(metadata, config, topic, filter),
         {:ok, floor, ceiling} <- bounds(config, topic, partition_ids, filter) do
      direction = direction(cursor)
      # A filtered scan reads in bigger chunks than an unfiltered page, since
      # most chunks will not fill the page (docs/PLAN.md 2.4, "Refill").
      chunk = if Filter.active?(filter), do: @scan_chunk, else: page_size

      ctx = %{
        floor: floor,
        ceiling: ceiling,
        direction: direction,
        page_size: page_size,
        chunk: chunk,
        filter: filter,
        max_scanned: max_scanned,
        on_progress: on_progress
      }

      run(config, topic, partition_ids, cursor, ctx)
    end
  end

  # `floor`/`ceiling` narrow to `filter.from`/`filter.to` when set
  # (docs/PLAN.md 2.4). `to` is inclusive, so its `ListOffsets` lookup uses
  # `to + 1 ms` as the exclusive end. A lookup with no matching offset (1.4)
  # means "latest", which leaves that end unrestricted; a `floor` past its
  # `ceiling` is clamped down to `ceiling`, leaving the partition empty.
  defp bounds(config, topic, partition_ids, filter) do
    pairs = Enum.map(partition_ids, &{topic, &1})

    with {:ok, earliest} <- Client.list_offsets(config, pairs, :earliest),
         {:ok, latest} <- Client.list_offsets(config, pairs, :latest),
         {:ok, earliest_map} <- offsets_map(config, topic, partition_ids, earliest),
         {:ok, latest_map} <- offsets_map(config, topic, partition_ids, latest),
         {:ok, from_map} <- time_bound(config, topic, partition_ids, filter.from, latest_map),
         {:ok, to_map} <-
           time_bound(config, topic, partition_ids, to_exclusive(filter.to), latest_map) do
      floor = merge_bound(partition_ids, earliest_map, from_map, &max/2)
      ceiling = merge_bound(partition_ids, latest_map, to_map, &min/2)
      {:ok, clamp_floor(partition_ids, floor, ceiling), ceiling}
    end
  end

  defp to_exclusive(nil), do: nil
  defp to_exclusive(%DateTime{} = to), do: DateTime.add(to, 1, :millisecond)

  defp time_bound(_config, _topic, _partition_ids, nil, _latest_map), do: {:ok, nil}

  defp time_bound(config, topic, partition_ids, %DateTime{} = at, latest_map) do
    ms = DateTime.to_unix(at, :millisecond)
    pairs = Enum.map(partition_ids, &{topic, &1})

    with {:ok, offsets} <- Client.list_offsets(config, pairs, {:timestamp, ms}),
         {:ok, map} <- offsets_map(config, topic, partition_ids, offsets) do
      {:ok, Map.new(map, fn {p, offset} -> {p, offset || Map.fetch!(latest_map, p)} end)}
    end
  end

  defp merge_bound(_partition_ids, base_map, nil, _fun), do: base_map

  defp merge_bound(partition_ids, base_map, bound_map, fun) do
    Map.new(partition_ids, fn p ->
      {p, fun.(Map.fetch!(base_map, p), Map.fetch!(bound_map, p))}
    end)
  end

  defp clamp_floor(partition_ids, floor, ceiling) do
    Map.new(partition_ids, fn p -> {p, min(Map.fetch!(floor, p), Map.fetch!(ceiling, p))} end)
  end

  defp run(config, topic, partition_ids, cursor, ctx) do
    %{floor: floor, ceiling: ceiling, direction: direction} = ctx
    starts = init_starts(partition_ids, floor, ceiling, cursor, direction)
    state = init_state(starts, floor, ceiling, direction)

    case step(config, topic, ctx, state, [], 0, []) do
      {:ok, final_state, messages, scanned, halted} ->
        # `step/6` returns messages in emission order: newest first for a
        # backward read, oldest first for a forward one. The result is
        # always newest first (docs/PLAN.md 2.4), so a forward read's
        # emission order is reversed here.
        ordered = if direction == :forward, do: Enum.reverse(messages), else: messages

        {:ok,
         build_result(final_state, starts, floor, ceiling, direction, ordered, scanned, halted)}

      {:error, _} = error ->
        error
    end
  end

  defp topic_partition_ids(%{topics: topics}, config, topic, filter) do
    case Enum.find(topics, &(&1.name == topic)) do
      nil ->
        {:error, unknown_topic_error(config, topic)}

      %{partitions: partitions} ->
        ids = Enum.map(partitions, & &1.partition_index)
        {:ok, scope_partitions(ids, filter.partition)}
    end
  end

  # Scope is every one of the topic's partitions, or, with `filter.partition`
  # set, that one partition only (docs/PLAN.md 2.4, "Terms"). A partition not
  # on the topic simply scopes to nothing, which reads as an empty page.
  defp scope_partitions(ids, nil), do: ids
  defp scope_partitions(ids, partition), do: Enum.filter(ids, &(&1 == partition))

  defp unknown_topic_error(config, topic) do
    %BrokerError{
      address: Config.address(config),
      reason: :unknown_topic,
      message: "Topic #{topic} does not exist on this cluster."
    }
  end

  defp offsets_map(config, topic, partition_ids, offsets) do
    Enum.reduce_while(partition_ids, {:ok, %{}}, fn p, {:ok, acc} ->
      case Map.fetch(offsets, {topic, p}) do
        {:ok, value} -> {:cont, {:ok, Map.put(acc, p, value)}}
        :error -> {:halt, {:error, missing_offset_error(config, topic, p)}}
      end
    end)
  end

  defp missing_offset_error(config, topic, partition) do
    %BrokerError{
      address: Config.address(config),
      reason: :missing_offset,
      message:
        "No offset was returned for #{topic}/#{partition}. It may have changed " <>
          "since the topic's metadata was fetched."
    }
  end

  defp direction({:after, _}), do: :forward
  defp direction(_), do: :backward

  defp init_starts(partition_ids, _floor, ceiling, nil, :backward) do
    Map.new(partition_ids, &{&1, ceiling[&1]})
  end

  defp init_starts(partition_ids, floor, ceiling, {:before, m}, :backward) do
    Map.new(partition_ids, fn p ->
      value = Map.get(m, p, floor[p])
      {p, clamp(value, floor[p], ceiling[p])}
    end)
  end

  defp init_starts(partition_ids, floor, ceiling, {:after, m}, :forward) do
    Map.new(partition_ids, fn p ->
      value = Map.get(m, p, ceiling[p])
      {p, clamp(value, floor[p], ceiling[p])}
    end)
  end

  defp init_state(starts, floor, ceiling, direction) do
    Map.new(starts, fn {p, start} ->
      exhausted? =
        case direction do
          :backward -> start <= floor[p]
          :forward -> start >= ceiling[p]
        end

      {p, %{next: start, buffer: [], exhausted?: exhausted?}}
    end)
  end

  # Refill/emit loop (docs/PLAN.md 2.4, "Algorithm"). Emits until
  # `ctx.page_size` messages have been collected or every scoped partition is
  # exhausted with an empty buffer. `ctx` carries the loop-invariant
  # `floor`/`ceiling`/`direction`/`page_size`/`chunk`/`filter`/`max_scanned`/
  # `on_progress`. Stops early, with `halted` set, the moment a regular
  # expression hits its backtracking limit (docs/PLAN.md 2.5); stops early,
  # with `halted` `nil`, once the scan budget (`max_scanned`) is spent.
  # `pending` carries the messages emitted since the last progress report, in
  # emission order (newest first): only rows the merge has actually emitted
  # may ever reach `on_progress`, never rows merely buffered by a refill
  # (docs/PLAN.md 2.4, "Refill" step 4).
  defp step(config, topic, ctx, state, page, scanned, pending) do
    cond do
      length(page) >= ctx.page_size -> stopped(state, page, scanned)
      all_exhausted_empty?(state) -> stopped(state, page, scanned)
      ready_to_emit?(state) -> emit(config, topic, ctx, state, page, scanned, pending)
      budget_spent?(ctx, scanned) -> stopped(state, page, scanned)
      true -> refill_and_continue(config, topic, ctx, state, page, scanned, pending)
    end
  end

  defp stopped(state, page, scanned), do: {:ok, state, Enum.reverse(page), scanned, nil}

  defp budget_spent?(%{max_scanned: :infinity}, _scanned), do: false
  defp budget_spent?(%{max_scanned: max_scanned}, scanned), do: scanned >= max_scanned

  defp emit(config, topic, ctx, state, page, scanned, pending) do
    {msg, state2} = pop_best(state, ctx.direction)
    step(config, topic, ctx, state2, [msg | page], scanned, [msg | pending])
  end

  defp refill_and_continue(config, topic, ctx, state, page, scanned, pending) do
    case refill_all(config, topic, ctx, state, scanned) do
      {:ok, state2, added, _matched, halted} ->
        scanned2 = scanned + added
        report_progress(ctx, scanned2, Enum.reverse(pending))
        continue_or_halt(config, topic, ctx, state2, page, scanned2, halted)

      {:error, _} = error ->
        error
    end
  end

  defp continue_or_halt(_config, _topic, _ctx, state, page, scanned, halted)
       when halted != nil do
    {:ok, state, Enum.reverse(page), scanned, halted}
  end

  defp continue_or_halt(config, topic, ctx, state, page, scanned, nil) do
    step(config, topic, ctx, state, page, scanned, [])
  end

  defp report_progress(%{on_progress: nil}, _scanned, _emitted), do: :ok

  defp report_progress(%{on_progress: on_progress, direction: direction}, scanned, emitted) do
    # Messages are included for backward reads only: a forward read's
    # emission order is oldest first, so its progress is reported without
    # rows (docs/PLAN.md 2.4, "Refill" step 4).
    messages = if direction == :backward, do: emitted, else: []
    on_progress.(%{scanned: scanned, messages: messages})
  end

  defp all_exhausted_empty?(state) do
    Enum.all?(state, fn {_p, %{exhausted?: exhausted?, buffer: buffer}} ->
      exhausted? and buffer == []
    end)
  end

  @doc false
  @spec merge_key(Message.t()) :: {integer(), non_neg_integer(), integer()}
  def merge_key(%{partition: p, offset: o, timestamp: ts}) do
    {DateTime.to_unix(ts, :millisecond), p, o}
  end

  # Every non-exhausted scoped partition must hold a buffered message before
  # anything is emitted, which is what makes `pop_best/2`'s head comparison
  # exact (docs/PLAN.md 2.4, "Emit").
  @doc false
  @spec ready_to_emit?(map()) :: boolean()
  def ready_to_emit?(state) do
    Enum.all?(state, fn {_p, %{exhausted?: exhausted?, buffer: buffer}} ->
      exhausted? or buffer != []
    end)
  end

  # The tie order (docs/PLAN.md 2.4, "Merge order"): the largest
  # `{timestamp_ms, partition, offset}` first for a backward read, the
  # smallest first for a forward read, so a timestamp tie breaks by
  # partition, then offset, both descending when read newest first.
  @doc false
  @spec pop_best(map(), :backward | :forward) :: {Message.t(), map()}
  def pop_best(state, direction) do
    candidates = for {p, %{buffer: [head | _]}} <- state, do: {p, head}

    {p, msg} =
      case direction do
        :backward -> Enum.max_by(candidates, fn {_p, m} -> merge_key(m) end)
        :forward -> Enum.min_by(candidates, fn {_p, m} -> merge_key(m) end)
      end

    {msg, update_in(state[p].buffer, &tl/1)}
  end

  # Refills every partition that needs it for one round, stopping at the
  # first error or the first backtracking-limit halt: the merge state must
  # not silently drop the partitions after it (docs/PLAN.md 2.5). The scan
  # budget (`max_scanned`) is a total across every partition, not per
  # partition: `scanned` is the count already spent before this round, and
  # each partition's chunk is clamped to what is left of it, stopping the
  # round early once the budget runs out rather than reading a full chunk
  # from every partition regardless of `scanned`.
  defp refill_all(config, topic, ctx, state, scanned) do
    needing = for {p, %{buffer: [], exhausted?: false}} <- state, do: p

    Enum.reduce_while(needing, {:ok, state, 0, [], nil}, fn p,
                                                            {:ok, acc_state, added, matched, nil} ->
      refill_step(config, topic, ctx, p, acc_state, added, matched, scanned)
    end)
  end

  defp refill_step(config, topic, ctx, p, acc_state, added, matched, scanned) do
    case remaining_budget(ctx, scanned + added) do
      0 -> {:halt, {:ok, acc_state, added, matched, nil}}
      remaining -> refill_one(config, topic, p, ctx, acc_state, remaining, added, matched)
    end
  end

  defp refill_one(config, topic, p, ctx, acc_state, remaining, added, matched) do
    case refill_partition(config, topic, p, ctx, acc_state, remaining) do
      {:ok, new_state, count, new_matched, nil} ->
        {:cont, {:ok, new_state, added + count, matched ++ new_matched, nil}}

      {:ok, new_state, count, new_matched, halted} ->
        {:halt, {:ok, new_state, added + count, matched ++ new_matched, halted}}

      {:error, _} = error ->
        {:halt, error}
    end
  end

  defp remaining_budget(%{max_scanned: :infinity}, _scanned), do: :infinity
  defp remaining_budget(%{max_scanned: max_scanned}, scanned), do: max(max_scanned - scanned, 0)

  defp refill_partition(config, topic, p, ctx, state, remaining) do
    %{next: next} = Map.fetch!(state, p)
    chunk = clamp_chunk(ctx.chunk, remaining)
    {lo, hi} = chunk_bounds(next, ctx.floor[p], ctx.ceiling[p], ctx.direction, chunk)

    case Messages.read_range(config, topic, p, lo, hi) do
      {:ok, messages} ->
        {matched, scanned, halted} = filter_chunk(messages, ctx.filter)
        new_next = far_edge(ctx.direction, lo, hi)
        new_exhausted? = exhausted?(ctx.direction, new_next, ctx.floor[p], ctx.ceiling[p])
        buffer = order_buffer(ctx.direction, matched)

        new_state =
          put_in(state[p], %{next: new_next, buffer: buffer, exhausted?: new_exhausted?})

        {:ok, new_state, scanned, matched, halted}

      {:error, _} = error ->
        error
    end
  end

  # Every message read counts toward `scanned`, whether or not it passes the
  # filter (docs/PLAN.md 2.4, "Refill" step 1). Stops at the first
  # backtracking-limit hit, discarding the rest of the chunk: a halted scan
  # ends the whole read (docs/PLAN.md 2.5).
  defp filter_chunk(messages, filter) do
    result =
      Enum.reduce_while(messages, {[], 0}, fn message, {acc, scanned} ->
        case Filter.match(filter, message) do
          :match -> {:cont, {[message | acc], scanned + 1}}
          :nomatch -> {:cont, {acc, scanned + 1}}
          {:match_limit, field} -> {:halt, {:halted, Enum.reverse(acc), scanned + 1, field}}
        end
      end)

    case result do
      {:halted, matched, scanned, field} -> {matched, scanned, {:match_limit, field}}
      {acc, scanned} -> {Enum.reverse(acc), scanned, nil}
    end
  end

  defp clamp_chunk(chunk, :infinity), do: chunk
  defp clamp_chunk(chunk, remaining), do: min(chunk, remaining)

  defp chunk_bounds(next, floor, _ceiling, :backward, chunk), do: {max(floor, next - chunk), next}

  defp chunk_bounds(next, _floor, ceiling, :forward, chunk),
    do: {next, min(ceiling, next + chunk)}

  defp far_edge(:backward, lo, _hi), do: lo
  defp far_edge(:forward, _lo, hi), do: hi

  defp order_buffer(:backward, matched), do: Enum.reverse(matched)
  defp order_buffer(:forward, matched), do: matched

  defp exhausted?(:backward, next, floor, _ceiling), do: next <= floor
  defp exhausted?(:forward, next, _floor, ceiling), do: next >= ceiling

  defp build_result(state, starts, floor, ceiling, direction, messages, scanned, halted) do
    {low, high} = ranges(state, starts, direction)

    range =
      Map.new(Map.keys(state), fn p -> {p, {Map.fetch!(low, p), Map.fetch!(high, p)}} end)

    %{
      messages: messages,
      range: range,
      floor: floor,
      ceiling: ceiling,
      older: older(low, floor),
      newer: newer(high, ceiling),
      scanned: scanned,
      halted: halted
    }
  end

  defp ranges(state, starts, :backward) do
    high = starts

    low =
      Map.new(state, fn {p, %{buffer: buffer, next: next}} ->
        case buffer do
          [head | _] -> {p, head.offset + 1}
          [] -> {p, next}
        end
      end)

    {low, high}
  end

  defp ranges(state, starts, :forward) do
    low = starts

    high =
      Map.new(state, fn {p, %{buffer: buffer, next: next}} ->
        case buffer do
          [head | _] -> {p, head.offset}
          [] -> {p, next}
        end
      end)

    {low, high}
  end

  defp older(low, floor) do
    if Enum.all?(low, fn {p, v} -> v == floor[p] end), do: nil, else: {:before, low}
  end

  defp newer(high, ceiling) do
    if Enum.all?(high, fn {p, v} -> v == ceiling[p] end), do: nil, else: {:after, high}
  end

  defp clamp(value, min, max), do: value |> max(min) |> min(max)
end
