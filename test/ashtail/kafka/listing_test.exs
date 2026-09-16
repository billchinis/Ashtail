defmodule Ashtail.Kafka.ListingTest do
  @moduledoc """
  Sorting and paging shared by the topic and consumer group lists.
  """

  use ExUnit.Case, async: true

  alias Ashtail.Kafka.Listing

  defp items do
    [%{name: "b", n: 2}, %{name: "a", n: 2}, %{name: "c", n: 1}]
  end

  defp sorted(dir) do
    items() |> Listing.sort(& &1.n, & &1.name, dir) |> Enum.map(& &1.name)
  end

  test "sort/4 orders by the value in either direction, ties by name ascending" do
    assert sorted(:asc) == ["c", "a", "b"]
    assert sorted(:desc) == ["a", "b", "c"]
  end

  test "sort/4 on the name itself reverses fully when descending" do
    names = items() |> Listing.sort(& &1.name, & &1.name, :desc) |> Enum.map(& &1.name)
    assert names == ["c", "b", "a"]
  end

  test "paginate/3 slices a page and reports the totals" do
    assert %{items: [3, 4], total: 5, page: 2, page_size: 2, page_count: 3} =
             Listing.paginate([1, 2, 3, 4, 5], 2, 2)
  end

  test "paginate/3 clamps the page into range, with one page for an empty list" do
    assert %{items: [5], page: 3} = Listing.paginate([1, 2, 3, 4, 5], 99, 2)
    assert %{items: [1, 2], page: 1} = Listing.paginate([1, 2, 3, 4, 5], 0, 2)
    assert %{items: [], total: 0, page: 1, page_count: 1} = Listing.paginate([], 4, 20)
  end
end
