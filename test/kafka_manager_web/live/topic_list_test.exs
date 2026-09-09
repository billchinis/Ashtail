defmodule KafkaManagerWeb.TopicListTest do
  @moduledoc """
  AC-2: the topic list shows partition count, replication factor and message
  count.
  """

  use KafkaManagerWeb.ConnCase, async: true

  import Phoenix.LiveViewTest

  @audit_topic "audit-log-with-a-very-long-topic-name-that-stresses-table-layout-and-headers-0123456789"

  test "the topic list shows partition count, replication factor and message count", %{
    conn: conn
  } do
    {:ok, view, html} = live(conn, ~p"/")

    assert count_topic_rows(html) == 20

    orders_row = view |> element("[data-topic='orders']") |> render()
    assert orders_row =~ "6 partitions"
    assert orders_row =~ "RF 1"
    assert orders_row =~ "600 messages"

    notifications_row = view |> element("[data-topic='notifications']") |> render()
    assert notifications_row =~ "12 partitions"
    assert notifications_row =~ "RF 1"
    assert notifications_row =~ "48 messages"

    empty_topic_row = view |> element("[data-topic='empty-topic']") |> render()
    assert empty_topic_row =~ "2 partitions"
    assert empty_topic_row =~ "RF 1"
    assert empty_topic_row =~ "0 messages"

    assert has_element?(view, "[data-topic='events.compacted']")
    assert has_element?(view, "[data-topic='scratch']")
    assert has_element?(view, "[data-topic='#{@audit_topic}']")
  end

  defp count_topic_rows(html) do
    ~r/data-topic="/ |> Regex.scan(html) |> length()
  end
end
