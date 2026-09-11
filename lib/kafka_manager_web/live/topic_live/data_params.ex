defmodule KafkaManagerWeb.TopicLive.DataParams do
  @moduledoc """
  Pure URL <-> params translation for the Data sub-menu (docs/PLAN.md 4.9):
  page size, the `before`/`after` cursor, and the filter query params (key,
  value, header, partition; `from`/`to` join them at AC-20, through the same
  generic pass-through below, needing no further change here). Nothing else
  builds a Data view URL.
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
    Map.new(@filter_keys, fn key -> {key, params[key] || ""} end)
  end

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
