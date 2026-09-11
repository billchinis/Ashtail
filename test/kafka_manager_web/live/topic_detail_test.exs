defmodule KafkaManagerWeb.TopicDetailTest do
  @moduledoc """
  AC-5: the Partitions and Configs sub-menus show per-partition offsets and
  the topic configuration.
  """

  use KafkaManagerWeb.ConnCase, async: true

  import Phoenix.LiveViewTest

  test "the Partitions sub-menu shows per-partition offsets and the Configs sub-menu shows the topic configuration",
       %{conn: conn} do
    {:ok, partitions_view, partitions_html} = live(conn, ~p"/topics/payments/partitions")

    assert count_partition_rows(partitions_html) == 3
    refute partitions_html =~ "data-config-name"

    latest_sum =
      for id <- [0, 1, 2] do
        row = partitions_view |> element("[data-partition='#{id}']") |> render()
        assert row =~ ~r/data-earliest="0"/
        [_, latest] = Regex.run(~r/data-latest="(\d+)"/, row)
        String.to_integer(latest)
      end
      |> Enum.sum()

    assert latest_sum == 120

    {:ok, configs_view, configs_html} = live(conn, ~p"/topics/payments/configs")

    refute configs_html =~ "data-partition"

    retention_row = configs_view |> element("[data-config-name='retention.ms']") |> render()
    assert retention_row =~ "604800000"

    cleanup_row = configs_view |> element("[data-config-name='cleanup.policy']") |> render()
    assert cleanup_row =~ "delete"
  end

  defp count_partition_rows(html) do
    ~r/data-partition="/ |> Regex.scan(html) |> length()
  end
end
