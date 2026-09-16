defmodule AshtailWeb.TopicDataJsonOperatorsTest do
  @moduledoc """
  A single JSON field condition row, edited in place, matches with `equals`,
  `contains` and `regex` on a nested path and an array index, and an invalid
  regular expression on that row halts the search and renders a per-row error
  without running a scan.
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

  test "equals on a nested path and an array index, contains, and an invalid then fixed regex",
       %{conn: conn} do
    {:ok, view, _html} = live(conn, ~p"/topics/payments")

    html = view |> element("[data-add-json-condition]") |> render_click()
    assert html =~ ~s(data-json-condition="0")

    html =
      apply_json(view, %{"path" => "shipping.country", "op" => "equals", "value" => "GR"})

    assert scan_state(html) == "complete"
    assert scanned(html) == 120
    refute html =~ "data-filter-error"
    assert keys(html) == Enum.map(shipping_gr_ns(), &pay_key/1)

    html =
      apply_json(view, %{"path" => "items[1].qty", "op" => "equals", "value" => "3"})

    assert scan_state(html) == "complete"
    assert scanned(html) == 120
    assert keys(html) == Enum.map(items1_qty3_ns(), &pay_key/1)

    html =
      apply_json(view, %{"path" => "note", "op" => "contains", "value" => "GIFT"})

    assert scan_state(html) == "complete"
    assert scanned(html) == 120
    assert keys(html) == Enum.map(note_contains_gift_ns(), &pay_key/1)

    html =
      apply_json(view, %{"path" => "customer.id", "op" => "regex", "value" => "cust-("})

    assert html =~ ~s(data-filter-error="json-0")
    assert rows(html) == []
    assert scanned(html) == 0
    assert Process.alive?(view.pid)

    html =
      apply_json(view, %{"path" => "customer.id", "op" => "regex", "value" => "^cust-0\\d7$"})

    assert scan_state(html) == "complete"
    assert scanned(html) == 120
    refute html =~ "data-filter-error"
    assert keys(html) == Enum.map(customer_id_regex_ns(), &pay_key/1)
  end

  defp shipping_gr_ns, do: [99, 88, 77, 66, 55, 44, 33, 22, 11]
  defp items1_qty3_ns, do: [107, 93, 79, 65, 51, 37, 23, 9]
  defp note_contains_gift_ns, do: [117, 101, 85, 69, 53, 37, 21, 5]
  defp customer_id_regex_ns, do: [97, 87, 77, 67, 57, 47, 37, 27, 17, 7]

  defp pay_key(n), do: "pay-" <> String.pad_leading(Integer.to_string(n), 3, "0")

  defp apply_json(view, row) do
    params = Map.put(@default_filter_params, "json", %{"0" => row})

    view
    |> form("#filter-form", %{"filter" => params})
    |> render_submit()

    render_async(view, 10_000)
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
end
