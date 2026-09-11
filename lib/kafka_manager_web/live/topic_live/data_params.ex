defmodule KafkaManagerWeb.TopicLive.DataParams do
  @moduledoc """
  Pure URL <-> params translation for the Data sub-menu (docs/PLAN.md 4.9):
  page size, the `before`/`after` cursor, and the filter query params (key,
  value, header, partition; `from`/`to` join them at AC-20, through the same
  generic pass-through below, needing no further change here). Nothing else
  builds a Data view URL.

  (2026-09-12, AC-22) JSON field condition rows (`json[i][path|op|value]`)
  go through one private normaliser, shared by `parse/1` and `path/4`
  (docs/PLAN.md 4.11): it accepts either the index map the URL and the form
  both produce, or the list `parse/1` itself returns, sorts by integer
  index, drops anything malformed instead of raising, blanks a non-binary
  field, defaults a blank operator to `"equals"`, and drops (then
  renumbers) a row with a blank path. The same pass also coerces every
  scalar filter field to a string, which fixes a real crash: `?key[x]=1`
  used to reach `Filter.parse/1`'s `Regex.escape/1` with a map.
  """

  use KafkaManagerWeb, :verified_routes

  @default_page_size 50
  @default_mode "text"

  @doc """
  Parses the Data view's query params into `%{page_size:, cursor:,
  filter_params:}`. A malformed page size or cursor falls back to its
  default. `filter_params` is the raw string-keyed subset handed to
  `Kafka.parse_filter/1` and to the filter form. Never raises.
  """
  @spec parse(map()) :: %{
          page_size: pos_integer(),
          cursor: nil | {:before, map()} | {:after, map()},
          filter_params: %{String.t() => String.t()}
        }
  def parse(params) do
    %{
      page_size: parse_page_size(params["page_size"]),
      cursor: parse_cursor(params),
      filter_params: filter_params(params)
    }
  end

  @filter_keys ~w(key key_mode value value_mode header header_value header_mode partition from to)

  defp filter_params(params) do
    @filter_keys
    |> Map.new(fn key -> {key, blank_to_string(params[key])} end)
    |> Map.put("json", normalize_json(params["json"]))
  end

  # A scalar filter field must always reach `Filter.parse/1` as a string,
  # never as the map a malformed query string like `?key[x]=1` produces
  # (docs/PLAN.md 4.11) — `params[key] || ""` alone does not catch this,
  # because a map is truthy.
  defp blank_to_string(value) when is_binary(value), do: value
  defp blank_to_string(_value), do: ""

  @doc """
  The JSON condition rows normaliser (docs/PLAN.md 4.11), exposed for
  `path/4` and `parse/1` to share. Accepts the index map the URL and the
  form both produce, or the list `parse/1` returns; anything else reads as
  no rows. Returns an ordered list of `%{"path" => .., "op" => .., "value"
  => ..}` string maps, blank-path rows dropped and the rest renumbered by
  their position in the result.
  """
  @spec normalize_json(term()) :: [%{String.t() => String.t()}]
  def normalize_json(rows) when is_map(rows) do
    rows
    |> Enum.filter(fn {key, value} -> canonical_index?(key) and is_map(value) end)
    |> Enum.sort_by(fn {key, _value} -> String.to_integer(key) end)
    |> Enum.map(fn {_key, value} -> normalize_json_row(value) end)
    |> drop_blank_paths()
  end

  def normalize_json(rows) when is_list(rows) do
    rows
    |> Enum.filter(&is_map/1)
    |> Enum.map(&normalize_json_row/1)
    |> drop_blank_paths()
  end

  def normalize_json(_other), do: []

  defp canonical_index?(key) when is_binary(key) do
    case Integer.parse(key) do
      {n, ""} when n >= 0 -> Integer.to_string(n) == key
      _other -> false
    end
  end

  defp canonical_index?(_key), do: false

  defp normalize_json_row(row) do
    %{
      "path" => row |> Map.get("path") |> blank_to_string() |> String.trim(),
      "op" => row |> Map.get("op") |> blank_to_string() |> blank_default("equals"),
      "value" => row |> Map.get("value") |> blank_to_string()
    }
  end

  defp blank_default("", default), do: default
  defp blank_default(value, _default), do: value

  defp drop_blank_paths(rows), do: Enum.reject(rows, &(&1["path"] == ""))

  @doc """
  Builds the Data view's URL for `topic`, dropping `page_size` when it is
  the default, `cursor` when it is `nil`, and every blank or default-mode
  filter field. `filter_params` is a raw string-keyed map, as returned by
  `parse/1` or submitted by the filter form.
  """
  @spec path(
          String.t(),
          %{String.t() => String.t()},
          pos_integer(),
          nil | {:before, map()} | {:after, map()}
        ) :: String.t()
  def path(topic, filter_params, page_size, cursor \\ nil) do
    case query_params(filter_params, page_size, cursor) do
      [] -> ~p"/topics/#{topic}"
      query -> ~p"/topics/#{topic}?#{query}"
    end
  end

  defp query_params(filter_params, page_size, cursor) do
    []
    |> maybe_put_page_size(page_size)
    |> maybe_put_cursor(cursor)
    |> put_filter_params(filter_params)
    |> put_json_conditions(filter_params)
  end

  defp put_filter_params(query, filter_params) do
    query
    |> maybe_put_pattern(:key, filter_params["key"], filter_params["key_mode"])
    |> maybe_put_pattern(:value, filter_params["value"], filter_params["value_mode"])
    |> maybe_put_header(
      filter_params["header"],
      filter_params["header_value"],
      filter_params["header_mode"]
    )
    |> maybe_put(:partition, filter_params["partition"])
    |> maybe_put(:from, filter_params["from"])
    |> maybe_put(:to, filter_params["to"])
  end

  # One `json:` entry, last in the query, as a keyword list so the row
  # order is deterministic (docs/PLAN.md 4.11). `path` and `op` are always
  # written, even the default `op`, so a shared URL reads without knowing
  # the defaults. `value` is written only when non-blank and the op is not
  # `exists`. Runs `filter_params["json"]` through the same normaliser
  # `parse/1` uses, so this also accepts the raw index map a form submits.
  defp put_json_conditions(query, filter_params) do
    case normalize_json(filter_params["json"]) do
      [] ->
        query

      rows ->
        entry =
          rows
          |> Enum.with_index()
          |> Enum.map(fn {row, index} -> {to_string(index), json_row_query(row)} end)

        query ++ [json: entry]
    end
  end

  defp json_row_query(%{"path" => path, "op" => op, "value" => value}) do
    base = [{"path", path}, {"op", op}]

    if op != "exists" and value != "" do
      base ++ [{"value", value}]
    else
      base
    end
  end

  defp maybe_put_pattern(query, field, value, mode) do
    case blank_to_nil(value) do
      nil -> query
      value -> query |> Kernel.++([{field, value}]) |> maybe_put_mode(mode_field(field), mode)
    end
  end

  defp mode_field(:key), do: :key_mode
  defp mode_field(:value), do: :value_mode

  defp maybe_put_mode(query, field, mode) do
    if mode in [nil, "", @default_mode], do: query, else: query ++ [{field, mode}]
  end

  defp maybe_put_header(query, name, value, mode) do
    case blank_to_nil(name) do
      nil ->
        query

      name ->
        query = query ++ [header: name]

        case blank_to_nil(value) do
          nil -> query
          value -> query |> Kernel.++(header_value: value) |> maybe_put_mode(:header_mode, mode)
        end
    end
  end

  defp maybe_put(query, field, value) do
    case blank_to_nil(value) do
      nil -> query
      value -> query ++ [{field, value}]
    end
  end

  defp blank_to_nil(nil), do: nil
  defp blank_to_nil(""), do: nil
  defp blank_to_nil(value), do: value

  defp maybe_put_page_size(query, @default_page_size), do: query
  defp maybe_put_page_size(query, page_size), do: query ++ [page_size: page_size]

  defp maybe_put_cursor(query, nil), do: query
  defp maybe_put_cursor(query, {:before, m}), do: query ++ [before: encode_cursor(m)]
  defp maybe_put_cursor(query, {:after, m}), do: query ++ [after: encode_cursor(m)]

  defp encode_cursor(m) do
    m
    |> Enum.sort_by(fn {partition, _offset} -> partition end)
    |> Enum.map_join("-", fn {partition, offset} -> "#{partition}.#{offset}" end)
  end

  defp parse_page_size(page_size) when page_size in ["20", "50"], do: String.to_integer(page_size)
  defp parse_page_size(_page_size), do: @default_page_size

  defp parse_cursor(%{"before" => value}) when is_binary(value) and value != "" do
    case decode_cursor(value) do
      {:ok, map} -> {:before, map}
      :error -> nil
    end
  end

  defp parse_cursor(%{"after" => value}) when is_binary(value) and value != "" do
    case decode_cursor(value) do
      {:ok, map} -> {:after, map}
      :error -> nil
    end
  end

  defp parse_cursor(_params), do: nil

  defp decode_cursor(value) do
    value
    |> String.split("-")
    |> Enum.reduce_while({:ok, %{}}, fn pair, {:ok, acc} ->
      case decode_pair(pair) do
        {:ok, partition, offset} -> {:cont, {:ok, Map.put(acc, partition, offset)}}
        :error -> {:halt, :error}
      end
    end)
  end

  defp decode_pair(pair) do
    with [partition_string, offset_string] <- String.split(pair, "."),
         {partition, ""} <- Integer.parse(partition_string),
         {offset, ""} <- Integer.parse(offset_string),
         true <- partition >= 0 do
      {:ok, partition, offset}
    else
      _ -> :error
    end
  end
end
