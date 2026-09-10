defmodule KafkaManagerWeb.GroupListTest do
  @moduledoc """
  AC-11: the consumer group list shows state, total lag and inline
  per-partition lag.
  """

  use KafkaManagerWeb.ConnCase, async: true

  import Phoenix.LiveViewTest

  test "the group list shows state, total lag, and expands per-partition lag inline", %{
    conn: conn
  } do
    {:ok, view, _html} = live(conn, ~p"/groups")

    for id <- ["orders-service", "lagging-analytics", "payments-worker", "live-tailer"] do
      assert has_element?(view, "[data-group='#{id}']")
    end

    orders_service = view |> element("[data-group='orders-service']") |> render()
    assert orders_service =~ ~r/data-group-state="Empty"/
    assert orders_service =~ "state-empty"
    assert total_lag(orders_service) == 0

    lagging_analytics = view |> element("[data-group='lagging-analytics']") |> render()
    assert total_lag(lagging_analytics) == 550

    payments_worker = view |> element("[data-group='payments-worker']") |> render()
    assert total_lag(payments_worker) == 30

    live_tailer = view |> element("[data-group='live-tailer']") |> render()
    assert live_tailer =~ ~r/data-group-state="Stable"/
    assert live_tailer =~ "state-stable"

    refute has_element?(view, "[data-group='lagging-analytics'] [data-partition]")

    view
    |> element("[data-group='lagging-analytics'] [data-expand-group]")
    |> render_click()

    expanded = view |> element("[data-group='lagging-analytics']") |> render()

    partition_rows =
      Regex.scan(~r/<tr[^>]*data-partition="(\d+)"[^>]*>(.*?)<\/tr>/s, expanded)

    assert length(partition_rows) == 6

    assert Enum.all?(partition_rows, fn [full_row, _, _] ->
             full_row =~ ~s(data-topic="orders")
           end)

    total = partition_rows |> Enum.map(fn [full_row, _, _] -> lag_of(full_row) end) |> Enum.sum()
    assert total == 550
  end

  defp total_lag(row) do
    [_, value] = Regex.run(~r/data-total-lag="(-?\d+)"/, row)
    String.to_integer(value)
  end

  defp lag_of(row) do
    [_, value] = Regex.run(~r/data-lag="(-?\d+)"/, row)
    String.to_integer(value)
  end
end
