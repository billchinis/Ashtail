defmodule AshtailWeb.TopicDataTimeFilterTest do
  @moduledoc """
  The Data sub-menu filters by a from/to time range, both ends inclusive to
  the millisecond, combined with a header filter, and keeps every applied
  filter in the URL. The range's bounds are read from the broker at test
  time, through the app's own rendered `data-timestamp` values, never
  hard-coded: rpk cannot set a record timestamp. The filter bar offers no
  time-range preset control, only From and To.
  """

  use AshtailWeb.ConnCase, async: true

  import Phoenix.LiveViewTest

  @default_filter_params %{
    "key" => "",
    "key_mode" => "text",
    "value" => "",
    "value_mode" => "text",
    "header" => "",
    "header_value" => "",
    "header_mode" => "text",
    "partition" => "",
    "from" => "",
    "to" => ""
  }

  test "from/to range and a header combine, survive the URL, with no preset control", %{
    conn: conn
  } do
    {:ok, view, html} = live(conn, ~p"/topics/notifications")

    from = timestamp_of(html, 20)
    to = timestamp_of(html, 30)
    assert from != nil and to != nil
    assert String.ends_with?(from, "Z")
    assert String.ends_with?(to, "Z")

    {html, _path} = apply_filter(view, %{"from" => from, "to" => to})
    assert notifications(html) == Enum.to_list(30..20//-1)

    {html, path} =
      apply_filter(view, %{
        "from" => from,
        "to" => to,
        "header" => "channel",
        "header_value" => "sms"
      })

    assert notifications(html) == [30, 25, 20]

    {:ok, view2, _html} = live(conn, path)
    html2 = render_async(view2, 10_000)

    assert notifications(html2) == [30, 25, 20]
    assert filter_field(html2, "from") == from
    assert filter_field(html2, "to") == to
    assert String.ends_with?(filter_field(html2, "from"), "Z")
    assert String.ends_with?(filter_field(html2, "to"), "Z")
    assert filter_field(html2, "header") == "channel"
    assert filter_field(html2, "header_value") == "sms"

    refute html2 =~ ~s(name="filter[range]")

    html = view2 |> element("#filter-form button[phx-click='clear']") |> render_click()
    assert notifications(html) == Enum.to_list(48..1//-1)
  end

  defp apply_filter(view, overrides) do
    params = Map.merge(@default_filter_params, overrides)

    view
    |> form("#filter-form", %{"filter" => params})
    |> render_submit()

    path = assert_patch(view)
    html = render_async(view, 10_000)

    {html, path}
  end

  defp timestamp_of(html, n) do
    ~r/<li\b(?=[^>]*\bdata-timestamp="([^"]*)")[^>]*>(.*?)<\/li>/s
    |> Regex.scan(html)
    |> Enum.find_value(fn [_full, timestamp, body] ->
      if Regex.match?(~r/Notification #{n}:/, body), do: timestamp
    end)
  end

  defp notifications(html) do
    ~r/<li\b[^>]*>(.*?)<\/li>/s
    |> Regex.scan(html)
    |> Enum.flat_map(fn [_full, body] ->
      case Regex.run(~r/Notification (\d+):/, body) do
        [_, n] -> [String.to_integer(n)]
        nil -> []
      end
    end)
  end

  defp filter_field(html, field) do
    case Regex.run(~r/name="filter\[#{field}\]"[^>]*?value="([^"]*)"/s, html) do
      [_, value] -> value
      nil -> nil
    end
  end
end
