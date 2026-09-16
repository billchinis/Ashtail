defmodule AshtailWeb.TopicDataJsonErrorByIdTest do
  @moduledoc """
  A JSON condition row's error is looked up by the row's id, not by its display
  position. After an apply leaves a row error in place, removing an earlier row
  (a client-side action that never patches or re-applies) shifts every later
  row's position down by one. The error must stay on the row that caused it,
  identified by id, even though its `data-filter-error` label (the row's current
  0-based position) changes to match.
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

  test "removing an earlier row keeps the error on the row that caused it", %{conn: conn} do
    {:ok, view, _html} = live(conn, ~p"/topics/payments")

    add_row(view)
    add_row(view)

    {html, _path} =
      apply_rows(view, %{
        "0" => %{"path" => "refunded", "op" => "equals", "value" => "true"},
        "1" => %{"path" => "items[one].qty", "op" => "equals", "value" => "3"}
      })

    assert html =~ ~s(data-filter-error="json-1")
    refute html =~ ~s(data-filter-error="json-0")
    error_message = error_text(html, 1)
    assert error_message =~ "Invalid path"
    assert json_field(html, 1, "path") == "items[one].qty"

    html = remove_row(view, 0)

    assert condition_ids(html) == [1]
    refute html =~ ~s(data-filter-error="json-1")
    assert html =~ ~s(data-filter-error="json-0")
    assert error_text(html, 0) == error_message
    assert json_field(html, 1, "path") == "items[one].qty"
  end

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

    path = assert_patch(view)
    html = render_async(view, 10_000)

    {html, path}
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

  defp error_text(html, position) do
    [_, text] =
      Regex.run(
        ~r/data-filter-error="json-#{position}"[^>]*>\s*<span[^>]*><\/span>\s*(.*?)<\/p>/s,
        html
      )

    String.trim(text)
  end
end
