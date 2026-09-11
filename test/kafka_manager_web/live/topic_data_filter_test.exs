defmodule KafkaManagerWeb.TopicDataFilterTest do
  @moduledoc """
  AC-19: the Data sub-menu filters key, value and header as plain text or
  regular expression, combined with AND with the partition filter, across
  the whole topic (docs/PLAN.md 2.4, 2.5, 4.9). Every submission that
  carries an active filter starts an async scan (docs/PLAN.md 6.6), so each
  assertion below waits for it through `render_async/2` and reads the
  rendered scan status before reading the rows.
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
    "partition" => ""
  }

  test "orders and notifications filters narrow the merged Data view", %{conn: conn} do
    {:ok, view, html} = live(conn, ~p"/topics/orders")
    assert filter_form_precedes_list?(html)

    html = apply_filter(view, %{"key" => "ORDER-005"})
    assert scan_state(html) == "complete"
    assert keys(html) == Enum.map(59..50//-1, &order_key/1)
    assert Enum.all?(rows(html), &(&1.partition == 0))
    assert Enum.map(rows(html), & &1.offset) == Enum.to_list(58..49//-1)

    html = apply_filter(view, %{"key" => "^order-0001$"})
    assert scan_state(html) == "complete"
    refute html =~ "data-broker-error"
    refute html =~ "data-filter-error"
    assert rows(html) == []

    html = apply_filter(view, %{"key" => "order-(", "key_mode" => "regex"})
    assert html =~ ~s(data-filter-error="key")
    assert Process.alive?(view.pid)

    html = apply_filter(view, %{"key" => "^order-0001$", "key_mode" => "regex"})
    assert keys(html) == ["order-0001"]
    assert [%{partition: 0, offset: 0}] = rows(html)
    assert scan_state(html) == "complete"
    assert scanned(html) == 600

    html = apply_filter(view, %{"value" => ~s("status":"cancelled"), "partition" => "2"})
    assert scan_state(html) == "complete"
    assert Enum.all?(rows(html), &(&1.partition == 2))
    assert keys(html) == Enum.map(299..203//-4, &order_key/1)
    assert Enum.map(rows(html), & &1.offset) == Enum.to_list(98..2//-4)

    {:ok, view2, _html} = live(conn, ~p"/topics/notifications")

    html = apply_filter(view2, %{"header" => "channel", "header_value" => "SMS"})
    assert scan_state(html) == "complete"
    assert notifications(html) == Enum.to_list(45..5//-5)

    html =
      apply_filter(view2, %{
        "header" => "channel",
        "header_mode" => "regex",
        "header_value" => "^sms$",
        "value" => "^Notification [12][0-9]:",
        "value_mode" => "regex"
      })

    assert scan_state(html) == "complete"
    assert notifications(html) == [25, 20, 15, 10]

    html = apply_filter(view2, %{"value" => "no-such-text"})
    assert scan_state(html) == "complete"
    assert rows(html) == []
    assert scanned(html) == 48

    html = apply_filter(view2, %{})
    assert scan_state(html) == "complete"
    assert notifications(html) == Enum.to_list(48..1//-1)
  end

  defp order_key(n), do: "order-" <> String.pad_leading(Integer.to_string(n), 4, "0")

  defp apply_filter(view, overrides) do
    params = Map.merge(@default_filter_params, overrides)

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

  defp notifications(html) do
    Enum.map(rows(html), fn %{body: body} ->
      [_, n] = Regex.run(~r/Notification (\d+):/, body)
      String.to_integer(n)
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

  defp filter_form_precedes_list?(html) do
    {form_index, _} = :binary.match(html, "id=\"filter-form\"")
    {list_index, _} = :binary.match(html, "id=\"messages\"")
    form_index < list_index
  end
end
