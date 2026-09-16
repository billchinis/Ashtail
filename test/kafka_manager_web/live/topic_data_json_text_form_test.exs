defmodule KafkaManagerWeb.TopicDataJsonTextFormTest do
  @moduledoc """
  `contains` and `regex` match a non-string scalar by its JSON text form —
  a number as its decoded text, a boolean as `true`/`false` — while objects
  and arrays still never match either operator.
  """

  use KafkaManagerWeb.ConnCase, async: true

  import Phoenix.LiveViewTest

  @audit_topic "audit-log-with-a-very-long-topic-name-that-stresses-table-layout-and-headers-0123456789"

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

  test "contains and regex fall back to a scalar's JSON text form, never an object or array",
       %{conn: conn} do
    {:ok, view, _html} = live(conn, ~p"/topics/#{@audit_topic}")

    add_row(view)

    html = apply_rows(view, %{"0" => %{"path" => "n", "op" => "contains", "value" => "4"}})

    assert scan_state(html) == "complete"
    assert scanned(html) == 40
    refute html =~ "data-filter-error"
    assert offsets(html) == [39, 33, 23, 13, 3]

    add_row(view)

    html =
      apply_rows(view, %{
        "0" => %{"path" => "n", "op" => "contains", "value" => "4"},
        "1" => %{"path" => "n", "op" => "contains", "value" => "0"}
      })

    assert scan_state(html) == "complete"
    assert scanned(html) == 40
    assert offsets(html) == [39]

    remove_row(view, 1)

    html = apply_rows(view, %{"0" => %{"path" => "n", "op" => "contains", "value" => "40"}})

    assert scan_state(html) == "complete"
    assert scanned(html) == 40
    assert offsets(html) == [39]

    html = apply_rows(view, %{"0" => %{"path" => "n", "op" => "regex", "value" => "^4$"}})

    assert scan_state(html) == "complete"
    assert scanned(html) == 40
    assert offsets(html) == [3]

    {:ok, payments_view, _html} = live(conn, ~p"/topics/payments")

    add_row(payments_view)

    html =
      apply_rows(payments_view, %{
        "0" => %{"path" => "amount", "op" => "regex", "value" => "^14\\.0$"}
      })

    assert scan_state(html) == "complete"
    assert scanned(html) == 120
    assert keys(html) == [pay_key(1)]

    html =
      apply_rows(payments_view, %{
        "0" => %{"path" => "amount", "op" => "regex", "value" => "^14\\.00$"}
      })

    assert scan_state(html) == "complete"
    assert scanned(html) == 120
    assert keys(html) == []

    add_row(payments_view)

    html =
      apply_rows(payments_view, %{
        "0" => %{"path" => "refunded", "op" => "contains", "value" => "RUE"},
        "1" => %{"path" => "note", "op" => "exists", "value" => ""}
      })

    assert scan_state(html) == "complete"
    assert scanned(html) == 120
    refute html =~ "data-filter-error"
    assert keys(html) == Enum.map([109, 85, 61, 37, 13], &pay_key/1)

    html =
      apply_rows(payments_view, %{
        "0" => %{"path" => "refunded", "op" => "regex", "value" => "^false$"},
        "1" => %{"path" => "note", "op" => "exists", "value" => ""}
      })

    assert scan_state(html) == "complete"
    assert scanned(html) == 120

    assert keys(html) ==
             Enum.map([117, 101, 93, 77, 69, 53, 45, 29, 21, 5], &pay_key/1)

    remove_row(payments_view, 1)

    html =
      apply_rows(payments_view, %{
        "0" => %{"path" => "items", "op" => "contains", "value" => "sku"}
      })

    assert scan_state(html) == "complete"
    assert scanned(html) == 120
    refute html =~ "data-filter-error"
    assert keys(html) == []

    html =
      apply_rows(payments_view, %{
        "0" => %{"path" => "customer", "op" => "regex", "value" => "cust"}
      })

    assert scan_state(html) == "complete"
    assert scanned(html) == 120
    refute html =~ "data-filter-error"
    assert keys(html) == []

    assert Process.alive?(view.pid)
    assert Process.alive?(payments_view.pid)
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

  defp apply_rows(view, json_rows) do
    params = Map.put(@default_filter_params, "json", json_rows)

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

  defp offsets(html), do: Enum.map(rows(html), & &1.offset)

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
