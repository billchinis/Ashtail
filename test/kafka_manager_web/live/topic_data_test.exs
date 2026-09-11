defmodule KafkaManagerWeb.TopicDataTest do
  @moduledoc """
  AC-18: the Data sub-menu merges every partition's messages, newest first,
  with paging.
  """

  use KafkaManagerWeb.ConnCase, async: true

  import Phoenix.LiveViewTest

  test "the Data sub-menu merges every notifications partition newest first, with paging",
       %{conn: conn} do
    {:ok, view, html} = live(conn, ~p"/topics/notifications")

    rows = rows(html)
    assert length(rows) == 48
    assert Enum.map(rows, & &1.notification) == Enum.to_list(48..1//-1)

    for {row, n} <- Enum.zip(rows, 48..1//-1) do
      assert row.partition == expected_partition(n)
      assert row.offset == expected_offset(n)
      assert row.null_key?
      assert row.channel == expected_channel(n)
      assert row.timestamp =~ ~r/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$/
    end

    # The first four rows come from partitions 7, 2, 9, 4, each at offset 3.
    assert Enum.map(Enum.take(rows, 4), & &1.partition) == [7, 2, 9, 4]
    assert Enum.map(Enum.take(rows, 4), & &1.offset) == [3, 3, 3, 3]

    # The 13th row (Notification 36) is partition 7, offset 2.
    thirteenth = Enum.at(rows, 12)
    assert thirteenth.notification == 36
    assert thirteenth.partition == 7
    assert thirteenth.offset == 2

    timestamps = Enum.map(rows, &parse_ts/1)
    assert Enum.uniq(timestamps) == timestamps
    assert timestamps == Enum.sort(timestamps, :desc)

    # Select page size 20.
    html = view |> element("[data-page-size-form]") |> render_change(%{"page_size" => "20"})
    assert Enum.map(rows(html), & &1.notification) == Enum.to_list(48..29//-1)

    # Page forwards (Older) twice.
    html = view |> element("[data-next-page]") |> render_click()
    assert Enum.map(rows(html), & &1.notification) == Enum.to_list(28..9//-1)

    html = view |> element("[data-next-page]") |> render_click()
    assert Enum.map(rows(html), & &1.notification) == Enum.to_list(8..1//-1)
    refute html =~ "data-next-page"

    # Page backwards (Newer) once: renders 28..9 again.
    html = view |> element("[data-prev-page]") |> render_click()
    assert Enum.map(rows(html), & &1.notification) == Enum.to_list(28..9//-1)
  end

  defp expected_partition(n), do: rem((n - 1) * 5, 12)
  defp expected_offset(n), do: div(n - 1, 12)
  defp expected_channel(n) when rem(n, 5) == 0, do: "sms"
  defp expected_channel(_n), do: "email"

  defp parse_ts(%{timestamp: timestamp}) do
    {:ok, datetime, _offset} = DateTime.from_iso8601(timestamp)
    DateTime.to_unix(datetime, :millisecond)
  end

  defp rows(html) do
    ~r/<li\b(?=[^>]*\bdata-partition="(\d+)")(?=[^>]*\bdata-offset="(\d+)")(?=[^>]*\bdata-timestamp="([^"]*)")[^>]*>(.*?)<\/li>/s
    |> Regex.scan(html)
    |> Enum.map(fn [_full, partition, offset, timestamp, body] ->
      %{
        partition: String.to_integer(partition),
        offset: String.to_integer(offset),
        timestamp: timestamp,
        notification: notification_number(body),
        null_key?: body =~ "data-null-key",
        channel: channel(body)
      }
    end)
  end

  defp notification_number(body) do
    [_, number] = Regex.run(~r/Notification (\d+):/, body)
    String.to_integer(number)
  end

  defp channel(body) do
    case Regex.run(~r/channel=(\w+)/, body) do
      [_, value] -> value
      nil -> nil
    end
  end
end
