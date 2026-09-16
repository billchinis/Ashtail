defmodule KafkaManagerWeb.TopicDataJsonExistsTest do
  @moduledoc """
  A JSON field condition, added and edited through the Value fieldset's
  "Add JSON field" control, matches only messages whose value is JSON and
  has the given path, combined with AND with the other filters. `payments`
  mixes 108 JSON values with 12 that are not (plain text, truncated JSON,
  null), seeded specifically for the JSON field filter.
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

  test "the key filter, then amount exists, then note exists on the edited row", %{conn: conn} do
    {:ok, view, html} = live(conn, ~p"/topics/payments")

    value_fieldset =
      html
      |> LazyHTML.from_document()
      |> LazyHTML.query(~s{fieldset:has(input[name="filter[value]"])})

    assert LazyHTML.query(value_fieldset, "[data-add-json-condition]") |> Enum.count() == 1

    html = apply_filter(view, %{"key" => "0$", "key_mode" => "regex"})
    assert scan_state(html) == "complete"
    assert keys(html) == Enum.map(multiples_of(10), &pay_key/1)

    html = view |> element("[data-add-json-condition]") |> render_click()

    assert html =~ ~s(data-json-condition="0")
    assert html =~ ~s(aria-label="JSON path 1")
    assert html =~ ~s(aria-label="Operator 1")
    assert html =~ ~s(aria-label="JSON value 1")
    assert html =~ ~s(data-remove-json-condition)

    assert Regex.scan(~r/<option value="(equals|contains|regex|exists)"[^>]*>\1<\/option>/, html)
           |> Enum.map(fn [_full, op] -> op end)
           |> Enum.take(4) == ~w(equals contains regex exists)

    html =
      apply_filter(view, %{"key" => "0$", "key_mode" => "regex"},
        json: %{"0" => %{"path" => "amount", "op" => "exists", "value" => ""}}
      )

    assert scan_state(html) == "complete"
    assert scanned(html) == 120
    assert rows(html) == []
    refute html =~ "data-filter-error"
    assert Process.alive?(view.pid)

    html =
      apply_filter(view, %{},
        json: %{"0" => %{"path" => "note", "op" => "exists", "value" => ""}}
      )

    assert scan_state(html) == "complete"
    assert scanned(html) == 120
    assert keys(html) == Enum.map(mod8_5(), &pay_key/1)
  end

  defp multiples_of(n), do: Enum.filter(120..1//-1, &(rem(&1, n) == 0))
  defp mod8_5, do: Enum.filter(120..1//-1, &(rem(&1, 8) == 5))

  defp pay_key(n), do: "pay-" <> String.pad_leading(Integer.to_string(n), 3, "0")

  defp apply_filter(view, overrides, opts \\ []) do
    params = Map.merge(@default_filter_params, overrides)
    params = if json = opts[:json], do: Map.put(params, "json", json), else: params

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
