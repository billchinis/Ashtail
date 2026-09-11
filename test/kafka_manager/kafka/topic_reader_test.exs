defmodule KafkaManager.Kafka.TopicReaderTest do
  @moduledoc """
  Unit tests for the pure parts of the merge engine (docs/PLAN.md 6.7), plus
  two fix-run regressions against the live seeded broker:

  - a scan's `on_progress` callback must only ever report rows the merge has
    actually emitted, in their final newest-first order, and the cumulative
    count it reports must never exceed the page size (a "cancelled" scan
    must not briefly show more rows than the page allows);
  - the scan budget (`max_scanned`) is a total across every scoped
    partition, not a per-partition allowance.
  """

  use ExUnit.Case, async: true

  alias KafkaManager.Kafka.{Filter, Message, TopicReader}
  alias KafkaManagerWeb.TopicLive.DataParams

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

  describe "on_progress (fix run, item 1)" do
    test "reports only rows already emitted, never more than the page size" do
      config = KafkaManager.Kafka.config()
      page_size = 50

      {:ok, page} =
        TopicReader.read(config, "orders",
          page_size: page_size,
          filter: Filter.none(),
          on_progress: fn progress -> send(self(), {:progress, progress}) end
        )

      progresses = flush_progress([])
      assert progresses != []

      reported = Enum.flat_map(progresses, & &1.messages)
      assert length(reported) <= page_size

      # Every reported row is part of the final, authoritative page, in the
      # same newest-first order it appears there — never a row that was
      # merely buffered by a refill and later discarded from the page.
      assert reported == Enum.take(page.messages, length(reported))
    end
  end

  describe "the scan budget (fix run, item 2)" do
    test "max_scanned is a total across every scoped partition, not per partition" do
      config = KafkaManager.Kafka.config()
      cursor = {:after, Map.new(0..5, &{&1, 0})}

      {:ok, page} =
        TopicReader.read(config, "orders",
          cursor: cursor,
          page_size: 10_000,
          filter: Filter.none(),
          max_scanned: 500
        )

      assert page.scanned <= 500
    end
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
