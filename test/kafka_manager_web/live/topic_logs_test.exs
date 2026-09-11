defmodule KafkaManagerWeb.TopicLogsTest do
  @moduledoc """
  AC-17: the Logs sub-menu shows log directory usage for every partition
  replica.
  """

  use KafkaManagerWeb.ConnCase, async: true

  import Phoenix.LiveViewTest

  test "the Logs sub-menu shows one row per partition replica with broker, log dir, size and offset lag",
       %{conn: conn} do
    {:ok, view, html} = live(conn, ~p"/topics/orders/logs")

    assert count_log_rows(html) == 6

    for id <- 0..5 do
      row = view |> element("[data-partition='#{id}']") |> render()

      assert row =~ ~r/data-broker="\d+"/
      assert row =~ ~r{/[^"<]+}

      [_, size] = Regex.run(~r/data-size-bytes="(\d+)"/, row)
      assert String.to_integer(size) > 0

      [_, offset_lag] = Regex.run(~r/data-offset-lag="(-?\d+)"/, row)
      assert is_integer(String.to_integer(offset_lag))
    end
  end

  defp count_log_rows(html), do: ~r/data-partition="/ |> Regex.scan(html) |> length()
end
