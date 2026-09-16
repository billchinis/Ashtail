defmodule Ashtail.Kafka.GroupsSortTest do
  @moduledoc """
  Sorting the consumer group list by id, state, member count or total lag.
  """

  use ExUnit.Case, async: true

  alias Ashtail.Kafka.{Group, Groups}

  defp group(id, state, members, lag) do
    %Group{
      id: id,
      state: state,
      protocol_type: "consumer",
      member_count: members,
      total_lag: lag
    }
  end

  defp sorted(key, dir) do
    [
      group("b", "Stable", 2, 10),
      group("a", "Empty", 0, 10),
      group("c", "Dead", 1, 0)
    ]
    |> Groups.sort_groups(key, dir)
    |> Enum.map(& &1.id)
  end

  test "sorts by id" do
    assert sorted(:id, :asc) == ["a", "b", "c"]
    assert sorted(:id, :desc) == ["c", "b", "a"]
  end

  test "sorts by state name" do
    assert sorted(:state, :asc) == ["c", "a", "b"]
    assert sorted(:state, :desc) == ["b", "a", "c"]
  end

  test "sorts by member count" do
    assert sorted(:members, :desc) == ["b", "c", "a"]
  end

  test "sorts by total lag, ties by id" do
    assert sorted(:lag, :desc) == ["a", "b", "c"]
    assert sorted(:lag, :asc) == ["c", "a", "b"]
  end
end
