defmodule KafkaManagerWeb.MessageRenderingTest do
  @moduledoc """
  AC-8: null keys show a marker and long values truncate behind an expander.
  """

  use KafkaManagerWeb.ConnCase, async: true

  import Phoenix.LiveViewTest

  @audit_topic "audit-log-with-a-very-long-topic-name-that-stresses-table-layout-and-headers-0123456789"

  test "null keys render a marker and audit values truncate behind an expander", %{conn: conn} do
    {:ok, view, html} = live(conn, ~p"/topics/notifications/partitions/0")

    for offset <- 0..3 do
      row = view |> element("[data-offset='#{offset}']") |> render()
      assert row =~ "data-null-key"
      assert row =~ "&lt;null&gt;"
      refute row =~ ~r/<td>\s*<\/td>/
    end

    refute html =~ ~r/<td>\s*<\/td>/

    {:ok, audit_view, audit_html} = live(conn, ~p"/topics/#{@audit_topic}/partitions/0")

    stored_value_length = stored_value_length()

    for offset <- 0..39 do
      row = audit_view |> element("[data-offset='#{offset}']") |> render()
      assert has_element?(audit_view, "[data-offset='#{offset}'] [data-expand-value]")
      assert truncated_value_length(row) < stored_value_length
    end

    refute audit_html =~ "unbroken long value to stress wrapping"

    audit_view
    |> element("[data-offset='0'] [data-expand-value]")
    |> render_click()

    expanded_row = audit_view |> element("[data-offset='0']") |> render()
    assert expanded_row =~ "unbroken long value to stress wrapping"
    assert String.length(full_value(expanded_row)) == stored_value_length
  end

  defp stored_value_length do
    long_value = String.duplicate("x", 2048)
    value = ~s({"n":1,"blob":"#{long_value}","note":"unbroken long value to stress wrapping"})
    String.length(value)
  end

  defp truncated_value_length(row) do
    [_, value] = Regex.run(~r/<span[^>]*data-value-preview[^>]*>(.*?)<\/span>/s, row)
    String.length(value)
  end

  defp full_value(row) do
    [_, value] = Regex.run(~r/<span[^>]*data-value-full[^>]*>(.*?)<\/span>/s, row)
    unescape(value)
  end

  defp unescape(value) do
    value
    |> String.replace("&quot;", "\"")
    |> String.replace("&lt;", "<")
    |> String.replace("&gt;", ">")
    |> String.replace("&amp;", "&")
  end
end
