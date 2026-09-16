defmodule KafkaManagerWeb.TopicLogsTest do
  @moduledoc """
  The Logs sub-menu shows log directory usage for every partition replica.
  """

  use KafkaManagerWeb.ConnCase, async: true

  import Phoenix.LiveViewTest

  alias KafkaManager.Kafka

  test "the Logs sub-menu shows one row per partition replica with broker, log dir, size and offset lag",
       %{conn: conn} do
    # Independent fact, not the page's own `topic_log_dirs` call: the app's
    # topic metadata reports which broker leads each partition. At
    # replication factor 1 that broker is the sole replica.
    {:ok, %{partitions: partitions}} = Kafka.topic_summary("orders")
    assert length(partitions) == 6

    leader_by_partition = Map.new(partitions, &{&1.id, &1.leader})

    {:ok, view, html} = live(conn, ~p"/topics/orders/logs")

    assert count_log_rows(html) == 6

    for id <- 0..5 do
      row = view |> element("[data-partition='#{id}']") |> render()

      [_, broker] = Regex.run(~r/data-broker="(\d+)"/, row)
      assert String.to_integer(broker) == leader_by_partition[id]

      [_, log_dir] = Regex.run(~r/data-log-dir="([^"]*)"/, row)
      assert log_dir != ""
      assert String.starts_with?(log_dir, "/")

      [_, size] = Regex.run(~r/data-size-bytes="(\d+)"/, row)
      assert String.to_integer(size) > 0

      [_, offset_lag] = Regex.run(~r/data-offset-lag="(-?\d+)"/, row)
      assert is_integer(String.to_integer(offset_lag))
    end
  end

  defp count_log_rows(html), do: ~r/data-partition="/ |> Regex.scan(html) |> length()
end
