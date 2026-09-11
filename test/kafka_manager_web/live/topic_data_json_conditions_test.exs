defmodule KafkaManagerWeb.TopicDataJsonConditionsTest do
  @moduledoc """
  AC-24: several JSON field condition rows combine with AND, with each
  other and with the other filters, an invalid path on one row errors only
  that row, removing a row keeps the other rows' typed values, and the
  applied rows survive a round trip through the URL into a fresh LiveView
  (docs/PLAN.md 2.5.1, 4.9, 4.11). `payments` is the AC-22/AC-23 reshape:
  `refunded` is JSON `true` exactly when N mod 3 = 1, and the text
  `"method":"card"` appears in the JSON values exactly when N is divisible
  by 3.
  """

  use KafkaManagerWeb.ConnCase, async: true

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

  test "AND across rows, a per-row path error, removal, and the URL round trip", %{conn: conn} do
    {:ok, view, _html} = live(conn, ~p"/topics/payments")

    add_row(view)
    add_row(view)

    {html, _path} =
      apply_rows(view, %{}, %{
        "0" => %{"path" => "refunded", "op" => "equals", "value" => "true"},
        "1" => %{"path" => "note", "op" => "exists", "value" => ""}
      })

    assert scan_state(html) == "complete"
    assert scanned(html) == 120
    refute html =~ "data-filter-error"
    assert keys(html) == Enum.map([109, 85, 61, 37, 13], &pay_key/1)

    add_row(view)

    {html, _path} =
      apply_rows(view, %{}, %{
        "0" => %{"path" => "refunded", "op" => "equals", "value" => "true"},
        "1" => %{"path" => "note", "op" => "exists", "value" => ""},
        "2" => %{"path" => "items[one].qty", "op" => "equals", "value" => "3"}
      })

    assert html =~ ~s(data-filter-error="json-2")
    refute html =~ ~s(data-filter-error="json-0")
    refute html =~ ~s(data-filter-error="json-1")
    assert rows(html) == []
    assert scanned(html) == 0
    assert Process.alive?(view.pid)

    {html, _path} =
      apply_rows(view, %{}, %{
        "0" => %{"path" => "refunded", "op" => "equals", "value" => "true"},
        "1" => %{"path" => "note", "op" => "exists", "value" => ""},
        "2" => %{"path" => "items[1].qty", "op" => "equals", "value" => "3"}
      })

    assert scan_state(html) == "complete"
    refute html =~ "data-filter-error"
    assert keys(html) == [pay_key(37)]

    remove_row(view, 0)

    {html, _path} =
      apply_rows(view, %{}, %{
        "1" => %{"path" => "note", "op" => "exists", "value" => ""},
        "2" => %{"path" => "items[1].qty", "op" => "equals", "value" => "3"}
      })

    assert scan_state(html) == "complete"
    assert condition_ids(html) == [0, 1]
    assert json_field(html, 0, "path") == "note"
    assert json_op(html, 0) == "exists"
    assert json_field(html, 1, "path") == "items[1].qty"
    assert json_op(html, 1) == "equals"
    assert json_field(html, 1, "value") == "3"
    assert keys(html) == Enum.map([93, 37], &pay_key/1)

    {html, path} =
      apply_rows(view, %{"value" => ~s("method":"card")}, %{
        "0" => %{"path" => "note", "op" => "exists", "value" => ""},
        "1" => %{"path" => "items[1].qty", "op" => "equals", "value" => "3"}
      })

    assert scan_state(html) == "complete"
    assert keys(html) == [pay_key(93)]

    {:ok, view2, _html} = live(conn, path)
    html2 = render_async(view2, 10_000)

    assert keys(html2) == [pay_key(93)]
    assert filter_field(html2, "value") == ~s("method":"card")
    assert condition_ids(html2) == [0, 1]
    assert json_field(html2, 0, "path") == "note"
    assert json_op(html2, 0) == "exists"
    assert json_field(html2, 1, "path") == "items[1].qty"
    assert json_op(html2, 1) == "equals"
    assert json_field(html2, 1, "value") == "3"

    html = view2 |> element("#filter-form button[phx-click='clear']") |> render_click()

    refute html =~ "data-json-condition"
    assert keys(html) == Enum.map(120..71//-1, &pay_key/1)
  end

  defp pay_key(n), do: "pay-" <> String.pad_leading(Integer.to_string(n), 3, "0")

  defp add_row(view) do
    view |> element("[data-add-json-condition]") |> render_click()
  end

  defp remove_row(view, id) do
    view
    |> element(~s([data-json-condition="#{id}"] [data-remove-json-condition]))
    |> render_click()
  end

  defp apply_rows(view, overrides, json_rows) do
    params =
      @default_filter_params
      |> Map.merge(overrides)
      |> Map.put("json", json_rows)

    view
    |> form("#filter-form", %{"filter" => params})
    |> render_submit()

    path = assert_patch(view)
    html = render_async(view, 10_000)

    {html, path}
  end

  defp rows(html) do
    ~r/<li\b(?=[^>]*\bdata-partition="(\d+)")(?=[^>]*\bdata-offset="(\d+)")[^>]*>(.*?)<\/li>/s
    |> Regex.scan(html)
    |> Enum.map(fn [_full, partition, offset, body] ->
      %{partition: String.to_integer(partition), offset: String.to_integer(offset), body: body}
    end)
  end

  defp keys(html) do
    Enum.map(rows(html), fn %{body: body} ->
      case Regex.run(~r/title="([^"]*)"/, body) do
        [_, key] -> key
        nil -> nil
      end
    end)
  end

  defp scan_state(html) do
    [_, state] = Regex.run(~r/data-scan-state="([^"]*)"/, html)
    state
  end

  defp scanned(html) do
    [_, n] = Regex.run(~r/data-scanned="(\d+)"/, html)
    String.to_integer(n)
  end

  defp condition_ids(html) do
    ~r/data-json-condition="(\d+)"/
    |> Regex.scan(html)
    |> Enum.map(fn [_, id] -> String.to_integer(id) end)
  end

  defp json_field(html, id, field) do
    case Regex.run(~r/name="filter\[json\]\[#{id}\]\[#{field}\]"[^>]*value="([^"]*)"/, html) do
      [_, value] -> value
      nil -> nil
    end
  end

  defp json_op(html, id) do
    [_, block] =
      Regex.run(~r/<select name="filter\[json\]\[#{id}\]\[op\]"[^>]*>(.*?)<\/select>/s, html)

    Enum.find(~w(equals contains regex exists), fn op ->
      block =~ ~r/<option value="#{op}" selected/
    end)
  end

  defp filter_field(html, field) do
    case Regex.run(~r/name="filter\[#{field}\]"[^>]*?value="([^"]*)"/s, html) do
      [_, value] -> unescape_html(value)
      nil -> nil
    end
  end

  defp unescape_html(value) do
    value
    |> String.replace("&quot;", "\"")
    |> String.replace("&#39;", "'")
    |> String.replace("&amp;", "&")
  end
end
