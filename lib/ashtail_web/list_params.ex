defmodule AshtailWeb.ListParams do
  @moduledoc """
  URL parameters shared by the paginated, sortable lists (topics and
  consumer groups): `page`, `page_size` (20 or 50) and `sort`/`dir`.
  """

  @page_sizes [20, 50]
  @default_page_size 20

  def default_page_size, do: @default_page_size

  @doc "A positive page number, or 1."
  def page(value) when is_binary(value) do
    case Integer.parse(value) do
      {int, _} when int > 0 -> int
      _ -> 1
    end
  end

  def page(_value), do: 1

  @doc "20 or 50, or the default 20."
  def page_size(value) do
    case Integer.parse(to_string(value)) do
      {size, ""} when size in @page_sizes -> size
      _ -> @default_page_size
    end
  end

  @doc """
  `{key, dir}` for a `sort` and `dir` pair from the URL, where `sort` must
  name one of `keys` and `dir` must be `"asc"` or `"desc"`. Anything else is
  `{default, :asc}`.
  """
  def sort(sort, dir, keys, default) do
    key = Enum.find(keys, &(Atom.to_string(&1) == sort))

    case {key, dir} do
      {nil, _} -> {default, :asc}
      {key, "asc"} -> {key, :asc}
      {key, "desc"} -> {key, :desc}
      _ -> {default, :asc}
    end
  end

  @doc """
  `{key, dir}` for a `"key:dir"` value from a mobile sort menu.
  """
  def sort_option(value, keys, default) do
    case String.split(to_string(value), ":") do
      [sort, dir] -> sort(sort, dir, keys, default)
      _ -> {default, :asc}
    end
  end

  @doc """
  The direction a click on `column`'s header should sort in: the active
  column flips; text columns (`text_keys`) start A–Z; count columns start
  largest-first.
  """
  def next_dir(column, current_sort, current_dir, text_keys) do
    cond do
      column == current_sort and current_dir == :asc -> :desc
      column == current_sort -> :asc
      column in text_keys -> :asc
      true -> :desc
    end
  end
end
