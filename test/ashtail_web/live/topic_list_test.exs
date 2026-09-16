defmodule AshtailWeb.TopicListTest do
  @moduledoc """
  The topic list shows partition count, replication factor and message count.
  """

  use AshtailWeb.ConnCase, async: true

  import Phoenix.LiveViewTest

  @audit_topic "audit-log-with-a-very-long-topic-name-that-stresses-table-layout-and-headers-0123456789"

  test "the topic list shows partition count, replication factor and message count", %{
    conn: conn
  } do
    {:ok, view, html} = live(conn, ~p"/")

    assert count_topic_rows(html) == 20

    assert_counts(view, "orders", "6", "1", "600")
    assert_counts(view, "notifications", "12", "1", "48")
    assert_counts(view, "empty-topic", "2", "1", "0")

    assert has_element?(view, "[data-topic='events.compacted']")
    assert has_element?(view, "[data-topic='scratch']")
    assert has_element?(view, "[data-topic='#{@audit_topic}']")
  end

  # Each count cell holds the bare number; the unit words are gone, so the
  # column header is the only label on desktop.
  defp assert_counts(view, topic, partitions, replication, messages) do
    row = "[data-topic='#{topic}']"

    assert view |> element("#{row} [data-partitions]") |> render() |> text() == partitions
    assert view |> element("#{row} [data-replication]") |> render() |> text() == replication
    assert view |> element("#{row} [data-messages]") |> render() |> text() == messages

    row_html = view |> element(row) |> render()
    refute row_html =~ ~r/\d+ partitions/
    refute row_html =~ ~r/RF \d/
    refute row_html =~ ~r/\d+ messages/
  end

  defp text(html), do: html |> LazyHTML.from_fragment() |> LazyHTML.text() |> String.trim()

  defp count_topic_rows(html) do
    ~r/data-topic="/ |> Regex.scan(html) |> length()
  end
end
