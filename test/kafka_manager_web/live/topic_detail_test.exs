defmodule KafkaManagerWeb.TopicDetailTest do
  @moduledoc """
  AC-5: topic detail shows per-partition offsets and the topic configuration.
  """

  use KafkaManagerWeb.ConnCase, async: true

  import Phoenix.LiveViewTest

  test "topic detail shows per-partition offsets and the topic configuration", %{conn: conn} do
    {:ok, view, html} = live(conn, ~p"/topics/payments")

    assert count_partition_rows(html) == 3

    latest_sum =
      for id <- [0, 1, 2] do
        row = view |> element("[data-partition='#{id}']") |> render()
        assert row =~ ~r/data-earliest="0"/
        [_, latest] = Regex.run(~r/data-latest="(\d+)"/, row)
        String.to_integer(latest)
      end
      |> Enum.sum()

    assert latest_sum == 120

    retention_row = view |> element("[data-config-name='retention.ms']") |> render()
    assert retention_row =~ "604800000"

    cleanup_row = view |> element("[data-config-name='cleanup.policy']") |> render()
    assert cleanup_row =~ "delete"
  end

  defp count_partition_rows(html) do
    ~r/data-partition="/ |> Regex.scan(html) |> length()
  end
end
