defmodule Ashtail.Kafka.Listing do
  @moduledoc """
  Sorting and paging for the topic and consumer group lists. Kafka has no
  server-side paging for either, so both lists are fetched whole and cut
  down here.
  """

  @type page(item) :: %{
          items: [item],
          total: non_neg_integer(),
          page: pos_integer(),
          page_size: pos_integer(),
          page_count: pos_integer()
        }

  @doc """
  Sorts `items` by `value.(item)` in direction `dir`. Items with equal values
  are ordered by `name.(item)` ascending, whatever the direction.
  """
  @spec sort([item], (item -> term()), (item -> String.t()), :asc | :desc) :: [item]
        when item: term()
  def sort(items, value, name, dir) do
    Enum.sort(items, fn a, b ->
      case {value.(a), value.(b)} do
        {same, same} -> name.(a) <= name.(b)
        {x, y} when dir == :asc -> x < y
        {x, y} -> x > y
      end
    end)
  end

  @doc """
  One page of `items`. The requested page is clamped into range, and an
  empty list still has one (empty) page.
  """
  @spec paginate([item], integer(), pos_integer()) :: page(item) when item: term()
  def paginate(items, page, page_size) do
    total = length(items)
    page_count = max(1, div(total + page_size - 1, page_size))
    page = page |> max(1) |> min(page_count)

    %{
      items: Enum.slice(items, (page - 1) * page_size, page_size),
      total: total,
      page: page,
      page_size: page_size,
      page_count: page_count
    }
  end
end
