defmodule KafkaManagerWeb.TopicGroupsTest do
  @moduledoc """
  AC-16: the Consumer Groups sub-menu lists the groups consuming the topic.
  """

  use KafkaManagerWeb.ConnCase, async: true

  import Phoenix.LiveViewTest

  test "the Consumer Groups sub-menu lists only the groups that read the topic, scoped to it",
       %{conn: conn} do
    {:ok, orders_view, orders_html} = live(conn, ~p"/topics/orders/groups")

    assert count_group_rows(orders_html) == 2

    orders_service = orders_view |> element("[data-group='orders-service']") |> render()
    assert orders_service =~ ~r/data-group-state="Empty"/
    assert total_lag(orders_service) == 0
    assert orders_service =~ ~s(href="/groups/orders-service")

    lagging_analytics = orders_view |> element("[data-group='lagging-analytics']") |> render()
    assert total_lag(lagging_analytics) == 550
    assert lagging_analytics =~ ~s(href="/groups/lagging-analytics")

    refute has_element?(orders_view, "[data-group='payments-worker']")
    refute has_element?(orders_view, "[data-group='live-tailer']")

    {:ok, notifications_view, notifications_html} = live(conn, ~p"/topics/notifications/groups")

    assert count_group_rows(notifications_html) == 1

    live_tailer = notifications_view |> element("[data-group='live-tailer']") |> render()
    assert live_tailer =~ ~r/data-group-state="Stable"/
    assert live_tailer =~ ~s(href="/groups/live-tailer")

    {:ok, _empty_view, empty_html} = live(conn, ~p"/topics/empty-topic/groups")

    assert count_group_rows(empty_html) == 0
  end

  defp count_group_rows(html), do: ~r/data-group="/ |> Regex.scan(html) |> length()

  defp total_lag(row) do
    [_, value] = Regex.run(~r/tabular-nums font-semibold[^"]*">(\d+)</, row)
    String.to_integer(value)
  end
end
