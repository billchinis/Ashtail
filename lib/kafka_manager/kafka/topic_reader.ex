defmodule KafkaManager.Kafka.TopicReader do
  @moduledoc """
  The merged read engine behind `KafkaManager.Kafka.read_topic/2`
  (docs/PLAN.md 2.4): a lazy k-way merge of every scoped partition's
  messages into one newest-first (or, for a forward cursor, oldest-first
  then reversed) page.

  It calls `Client.metadata/1`, `Client.list_offsets/3` and
  `Messages.read_range/5` only, never `Client.fetch/5` directly, which keeps
  the fetch-loop rules of `Messages` in one place.

  This build (AC-18) wires the bounds, the cursor and the merge for the
  unfiltered case: every scoped partition is the whole topic, and every
  message matches. The `filter`/`max_scanned`/`on_progress`/`halted` pieces
  documented in `KafkaManager.Kafka.read_topic/2`'s final shape (2.1) arrive
  with AC-19..AC-21.
  """

  alias KafkaManager.Kafka.{BrokerError, Client, Config, Messages}

  @default_page_size 50

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

    with {:ok, metadata} <- Client.metadata(config),
         {:ok, partition_ids} <- topic_partition_ids(metadata, config, topic),
         {:ok, floor, ceiling} <- bounds(config, topic, partition_ids) do
      run(config, topic, partition_ids, floor, ceiling, cursor, page_size)
    end
  end

  defp bounds(config, topic, partition_ids) do
    pairs = Enum.map(partition_ids, &{topic, &1})

    with {:ok, earliest} <- Client.list_offsets(config, pairs, :earliest),
         {:ok, latest} <- Client.list_offsets(config, pairs, :latest),
         {:ok, floor} <- offsets_map(config, topic, partition_ids, earliest),
         {:ok, ceiling} <- offsets_map(config, topic, partition_ids, latest) do
      {:ok, floor, ceiling}
    end
  end

  defp run(config, topic, partition_ids, floor, ceiling, cursor, page_size) do
    direction = direction(cursor)
    starts = init_starts(partition_ids, floor, ceiling, cursor, direction)
    state = init_state(starts, floor, ceiling, direction)
    ctx = %{floor: floor, ceiling: ceiling, direction: direction, page_size: page_size}

    case step(config, topic, ctx, state, [], 0) do
      {:ok, final_state, messages, scanned} ->
        # `step/6` returns messages in emission order: newest first for a
        # backward read, oldest first for a forward one. The result is
        # always newest first (docs/PLAN.md 2.4), so a forward read's
        # emission order is reversed here.
        ordered = if direction == :forward, do: Enum.reverse(messages), else: messages
        {:ok, build_result(final_state, starts, floor, ceiling, direction, ordered, scanned)}

      {:error, _} = error ->
        error
    end
  end

  defp topic_partition_ids(%{topics: topics}, config, topic) do
    case Enum.find(topics, &(&1.name == topic)) do
      nil -> {:error, unknown_topic_error(config, topic)}
      %{partitions: partitions} -> {:ok, Enum.map(partitions, & &1.partition_index)}
    end
  end

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
  # `floor`/`ceiling`/`direction`/`page_size`.
  defp step(config, topic, ctx, state, page, scanned) do
    cond do
      length(page) >= ctx.page_size ->
        {:ok, state, Enum.reverse(page), scanned}

      all_exhausted_empty?(state) ->
        {:ok, state, Enum.reverse(page), scanned}

      ready_to_emit?(state) ->
        {msg, state2} = pop_best(state, ctx.direction)
        step(config, topic, ctx, state2, [msg | page], scanned)

      true ->
        case refill_all(config, topic, ctx, state) do
          {:ok, state2, added} -> step(config, topic, ctx, state2, page, scanned + added)
          {:error, _} = error -> error
        end
    end
  end

  defp all_exhausted_empty?(state) do
    Enum.all?(state, fn {_p, %{exhausted?: exhausted?, buffer: buffer}} ->
      exhausted? and buffer == []
    end)
  end

  defp ready_to_emit?(state) do
    Enum.all?(state, fn {_p, %{exhausted?: exhausted?, buffer: buffer}} ->
      exhausted? or buffer != []
    end)
  end

  defp pop_best(state, direction) do
    candidates = for {p, %{buffer: [head | _]}} <- state, do: {p, head}

    {p, msg} =
      case direction do
        :backward -> Enum.max_by(candidates, fn {_p, m} -> merge_key(m) end)
        :forward -> Enum.min_by(candidates, fn {_p, m} -> merge_key(m) end)
      end

    {msg, update_in(state[p].buffer, &tl/1)}
  end

  defp merge_key(%{partition: p, offset: o, timestamp: ts}) do
    {DateTime.to_unix(ts, :millisecond), p, o}
  end

  defp refill_all(config, topic, ctx, state) do
    needing = for {p, %{buffer: [], exhausted?: false}} <- state, do: p

    Enum.reduce_while(needing, {:ok, state, 0}, fn p, {:ok, acc_state, added} ->
      case refill_partition(config, topic, p, ctx, acc_state) do
        {:ok, new_state, count} -> {:cont, {:ok, new_state, added + count}}
        {:error, _} = error -> {:halt, error}
      end
    end)
  end

  defp refill_partition(config, topic, p, ctx, state) do
    %{next: next} = Map.fetch!(state, p)
    {lo, hi} = chunk_bounds(next, ctx.floor[p], ctx.ceiling[p], ctx.direction, ctx.page_size)

    case Messages.read_range(config, topic, p, lo, hi) do
      {:ok, messages} ->
        {new_next, buffer} = refilled(ctx.direction, lo, hi, messages)
        new_exhausted? = exhausted?(ctx.direction, new_next, ctx.floor[p], ctx.ceiling[p])

        new_state =
          put_in(state[p], %{next: new_next, buffer: buffer, exhausted?: new_exhausted?})

        {:ok, new_state, length(messages)}

      {:error, _} = error ->
        error
    end
  end

  defp chunk_bounds(next, floor, _ceiling, :backward, chunk), do: {max(floor, next - chunk), next}

  defp chunk_bounds(next, _floor, ceiling, :forward, chunk),
    do: {next, min(ceiling, next + chunk)}

  defp refilled(:backward, lo, _hi, messages), do: {lo, Enum.reverse(messages)}
  defp refilled(:forward, _lo, hi, messages), do: {hi, messages}

  defp exhausted?(:backward, next, floor, _ceiling), do: next <= floor
  defp exhausted?(:forward, next, _floor, ceiling), do: next >= ceiling

  defp build_result(state, starts, floor, ceiling, direction, messages, scanned) do
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
      halted: nil
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
