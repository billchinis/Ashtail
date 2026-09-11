defmodule KafkaManagerWeb.TopicLive.DataParams do
  @moduledoc """
  Pure URL <-> params translation for the Data sub-menu (docs/PLAN.md 4.9):
  page size and the `before`/`after` cursor. AC-18 owns page size and the
  cursor; AC-19 and AC-20 extend this module with the filter parameters.
  Nothing else builds a Data view URL.
  """

  use KafkaManagerWeb, :verified_routes

  @default_page_size 50

  @doc """
  Parses the Data view's query params into `%{page_size:, cursor:}`. A
  malformed page size or cursor falls back to its default. Never raises.
  """
  @spec parse(map()) :: %{
          page_size: pos_integer(),
          cursor: nil | {:before, map()} | {:after, map()}
        }
  def parse(params) do
    %{page_size: parse_page_size(params["page_size"]), cursor: parse_cursor(params)}
  end

  @doc """
  Builds the Data view's URL for `topic`, dropping `page_size` when it is
  the default and `cursor` when it is `nil`.
  """
  @spec path(String.t(), pos_integer(), nil | {:before, map()} | {:after, map()}) :: String.t()
  def path(topic, page_size, cursor \\ nil) do
    case query_params(page_size, cursor) do
      [] -> ~p"/topics/#{topic}"
      query -> ~p"/topics/#{topic}?#{query}"
    end
  end

  defp query_params(page_size, cursor) do
    []
    |> maybe_put_page_size(page_size)
    |> maybe_put_cursor(cursor)
  end

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
