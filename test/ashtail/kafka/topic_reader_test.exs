defmodule Ashtail.Kafka.TopicReaderTest do
  @moduledoc """
  Unit tests for the pure parts of the merge engine, plus regressions
  against the live seeded broker:

  - a scan's `on_progress` callback must only ever report rows the merge has
    actually emitted, in their final newest-first order, and the cumulative
    count it reports must never exceed the page size (a "cancelled" scan
    must not briefly show more rows than the page allows);
  - the scan budget (`max_scanned`) is a total across every scoped
    partition, not a per-partition allowance, and it must never freeze the
    tail: whenever unread messages exist, a tick must emit at least one
    message and move the cursor, not just stay under budget.
  """

  use ExUnit.Case, async: true

  alias Ashtail.Kafka.{Filter, Message, TopicReader}
  alias AshtailWeb.TopicLive.DataParams

  describe "the pure merge primitives" do
    test "pop_best/2 breaks a timestamp tie by partition, descending" do
      ts = 1_700_000_000_000

      state = %{
        0 => %{buffer: [message(0, 5, ts)], exhausted?: true, next: 5},
        1 => %{buffer: [message(1, 9, ts)], exhausted?: true, next: 9},
        2 => %{buffer: [message(2, 1, ts)], exhausted?: true, next: 1}
      }

      {first, state} = TopicReader.pop_best(state, :backward)
      {second, state} = TopicReader.pop_best(state, :backward)
      {third, _state} = TopicReader.pop_best(state, :backward)

      assert Enum.map([first, second, third], & &1.partition) == [2, 1, 0]
    end

    test "merge_key/1 breaks a same-partition, same-timestamp tie by offset, descending" do
      ts = 1_700_000_000_000
      higher_offset = message(3, 42, ts)
      lower_offset = message(3, 7, ts)

      assert TopicReader.merge_key(higher_offset) > TopicReader.merge_key(lower_offset)
    end

    test "pop_best/2 pops a single scoped partition's buffer in its own order" do
      ts = 1_700_000_000_000

      state = %{
        0 => %{
          buffer: [message(0, 9, ts), message(0, 5, ts), message(0, 1, ts)],
          exhausted?: true,
          next: 1
        }
      }

      {first, state} = TopicReader.pop_best(state, :backward)
      {second, state} = TopicReader.pop_best(state, :backward)
      {third, _state} = TopicReader.pop_best(state, :backward)

      assert Enum.map([first, second, third], & &1.offset) == [9, 5, 1]
    end

    test "pop_best/2 breaks a timestamp tie by partition, ascending, for a forward cursor" do
      ts = 1_700_000_000_000

      state = %{
        0 => %{buffer: [message(0, 5, ts)], exhausted?: true, next: 5},
        1 => %{buffer: [message(1, 9, ts)], exhausted?: true, next: 9},
        2 => %{buffer: [message(2, 1, ts)], exhausted?: true, next: 1}
      }

      {first, state} = TopicReader.pop_best(state, :forward)
      {second, state} = TopicReader.pop_best(state, :forward)
      {third, _state} = TopicReader.pop_best(state, :forward)

      assert Enum.map([first, second, third], & &1.partition) == [0, 1, 2]
    end

    test "ready_to_emit?/1 waits while a non-exhausted partition's buffer is empty" do
      ts = 1_700_000_000_000

      not_ready = %{
        0 => %{buffer: [], exhausted?: false, next: 10},
        1 => %{buffer: [message(1, 0, ts)], exhausted?: true, next: 0}
      }

      refute TopicReader.ready_to_emit?(not_ready)

      ready = put_in(not_ready[0].buffer, [message(0, 10, ts)])
      assert TopicReader.ready_to_emit?(ready)

      # A partition with an empty buffer is also ready once it is exhausted:
      # there is nothing left to wait for.
      exhausted_and_empty = put_in(not_ready[0].exhausted?, true)
      assert TopicReader.ready_to_emit?(exhausted_and_empty)
    end

    test "a cursor round-trips through the URL's P.O-P.O format" do
      cursor = {:before, %{0 => 58, 1 => 100, 4 => 3}}
      path = DataParams.path("orders", %{}, 50, cursor)
      %{query: query} = URI.parse(path)
      params = query |> URI.decode_query() |> Map.put("topic", "orders")

      assert %{cursor: ^cursor} = DataParams.parse(params)
    end
  end

  describe "on_progress" do
    test "reports only rows already emitted, in final order, never more than the page size" do
      config = Ashtail.Kafka.config()
      page_size = 50

      {:ok, page} =
        TopicReader.read(config, "orders",
          page_size: page_size,
          filter: Filter.none(),
          # Test-only: forces several refill rounds so the scan cannot finish in
          # a single round, which is the only way this test can actually
          # exercise `on_progress` reporting rows at all — on every seeded
          # topic, an unbounded chunk fills the page in one round, so the only
          # progress report ever fired would carry no rows, and this test would
          # hold trivially even if `on_progress` never reported an emitted row
          # correctly.
          scan_chunk: 10,
          on_progress: fn progress -> send(self(), {:progress, progress}) end
        )

      progresses = flush_progress([])
      assert progresses != []

      reported = Enum.flat_map(progresses, & &1.messages)
      assert reported != []
      assert length(reported) <= page_size

      # Every reported row is part of the final, authoritative page, in the
      # same newest-first order it appears there — never a row that was
      # merely matched by a refill and later discarded from the page.
      assert reported == Enum.take(page.messages, length(reported))
    end
  end

  describe "scan_chunk validation" do
    test "a non-positive scan_chunk raises ArgumentError instead of looping forever" do
      config = Ashtail.Kafka.config()

      assert_raise ArgumentError, ~r/scan_chunk must be a positive integer/, fn ->
        TopicReader.read(config, "orders", filter: Filter.none(), scan_chunk: 0)
      end
    end
  end

  describe "the tail budget guarantees progress" do
    test "three ticks from offset 0 each check at most 500 messages, emit at least " <>
           "one, and move the cursor" do
      config = Ashtail.Kafka.config()
      cursor0 = {:after, Map.new(0..5, &{&1, 0})}

      {cursor1, page1} = tail_tick(config, cursor0)
      assert page1.scanned <= 500
      assert page1.messages != []
      assert cursor1 != cursor0

      {cursor2, page2} = tail_tick(config, cursor1)
      assert page2.scanned <= 500
      assert page2.messages != []
      assert cursor2 != cursor1

      {_cursor3, page3} = tail_tick(config, cursor2)
      assert page3.scanned <= 500
      assert page3.messages != []

      # Concatenate the three ticks in tick order, each un-reversed back to
      # chronological (oldest-first) order, and compare against a single
      # untruncated forward read of the same range. An implementation that
      # skipped a buffered-but-unemitted message between ticks, or emitted one
      # out of merge order, would still satisfy the three loose assertions above
      # (something non-empty, under budget, cursor moved) but would fail this
      # one.
      emitted =
        Enum.reverse(page1.messages) ++
          Enum.reverse(page2.messages) ++ Enum.reverse(page3.messages)

      {:ok, full} =
        TopicReader.read(config, "orders",
          cursor: cursor0,
          page_size: 100_000,
          filter: Filter.none(),
          max_scanned: :infinity
        )

      assert Enum.reverse(full.messages) == emitted
    end

    test "the total scanned in one tick never exceeds the budget, even when a filter " <>
           "forces a second refill round on a multi-partition topic" do
      config = Ashtail.Kafka.config()
      # `orders` has 6 partitions of 100 messages each. A filter that matches
      # nothing forces every partition to stay in `needing` after round 1, which
      # is exactly the shape that made `fair_chunk`'s old
      # floor-of-one-per-partition rule push a second round's total past the 500
      # cap.
      {:ok, filter} =
        Filter.parse(%{"key" => "^does-not-exist-in-orders$", "key_mode" => "regex"})

      {:ok, page} =
        TopicReader.read(config, "orders",
          cursor: nil,
          page_size: 10_000,
          filter: filter,
          max_scanned: 500
        )

      assert page.scanned <= 500
    end
  end

  # Mirrors `TopicLive.Data`'s own tail-tick cursor:
  # `{:after, high}`, where `high` is every scoped partition's `range` upper
  # bound from the previous tick.
  defp tail_tick(config, cursor) do
    {:ok, page} =
      TopicReader.read(config, "orders",
        cursor: cursor,
        page_size: 10_000,
        filter: Filter.none(),
        max_scanned: 500
      )

    high = Map.new(page.range, fn {p, {_low, high}} -> {p, high} end)
    {{:after, high}, page}
  end

  defp message(partition, offset, ts_ms) do
    %Message{
      offset: offset,
      key: nil,
      value: "v",
      timestamp: DateTime.from_unix!(ts_ms, :millisecond),
      headers: [],
      partition: partition
    }
  end

  defp flush_progress(acc) do
    receive do
      {:progress, progress} -> flush_progress([progress | acc])
    after
      0 -> Enum.reverse(acc)
    end
  end
end
