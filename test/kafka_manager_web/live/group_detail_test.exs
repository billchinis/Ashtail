defmodule KafkaManagerWeb.GroupDetailTest do
  @moduledoc """
  AC-12: consumer group detail shows lag for every assigned partition.
  """

  use KafkaManagerWeb.ConnCase, async: true

  import Phoenix.LiveViewTest

  test "shows a partition row with topic, committed offset and lag for every assigned partition",
       %{conn: conn} do
    {:ok, view, _html} = live(conn, ~p"/groups/lagging-analytics")

    rows = Regex.scan(~r/<tr[^>]*data-partition="(\d+)"[^>]*>(.*?)<\/tr>/s, render(view))

    assert length(rows) == 6

    assert Enum.all?(rows, fn [full_row, _, _] -> full_row =~ ~s(data-topic="orders") end)

    assert Enum.all?(rows, fn [full_row, _, _] ->
             Regex.match?(~r/data-committed-offset="-?\d+"/, full_row)
           end)

    total_lag =
      rows
      |> Enum.map(fn [full_row, _, _] ->
        [_, value] = Regex.run(~r/data-lag="(-?\d+)"/, full_row)
        String.to_integer(value)
      end)
      |> Enum.sum()

    assert total_lag == 550
  end
end
