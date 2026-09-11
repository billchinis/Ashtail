defmodule KafkaManager.Kafka.JsonPath do
  @moduledoc """
  The path syntax for a JSON field condition (docs/PLAN.md 2.5.1): dot
  notation for object keys plus `[n]` for array indexes, for example
  `customer.id` or `items[0].sku`. Pure, two functions, no dependency on
  `Filter` — only `Filter` calls this module.

      path  := first ( "." key index* )*
      first := key index* | index+        a path may start with [n]
      key   := one or more characters other than ".", "[" and "]"
      index := "[" digit+ "]"             non-negative decimal

  `parse/1` is a hand-written recursive-descent parser over the trimmed
  binary, not a regular expression, so every failure can name its cause.
  """

  @type segment :: String.t() | non_neg_integer()

  @doc """
  Parses a trimmed JSON path into its segments, keys and indexes in order.
  Every failure names its cause, so `Filter`'s row error can quote it.
  """
  @spec parse(String.t()) :: {:ok, [segment()]} | {:error, String.t()}
  def parse(path) when is_binary(path) do
    with {:ok, first, rest} <- parse_first(path),
         {:ok, more} <- parse_more(rest) do
      {:ok, first ++ more}
    end
  end

  # `first := key index* | index+` — a path may open with a key or with a
  # top-level array index.
  defp parse_first("[" <> _rest = path) do
    parse_indexes(path, [])
  end

  defp parse_first(path) do
    with {:ok, key, rest} <- parse_key(path),
         {:ok, indexes, rest} <- parse_indexes(rest, []) do
      {:ok, [key | indexes], rest}
    end
  end

  # `( "." key index* )*` — every following segment starts with a dot.
  # Anything else after a key or a closing bracket (a stray character, a
  # bracket with no leading dot after a key) is the one "anything after ]
  # other than . or [" failure (docs/PLAN.md 2.5.1).
  defp parse_more(""), do: {:ok, []}

  defp parse_more("." <> rest) do
    with {:ok, key, rest} <- parse_key(rest),
         {:ok, indexes, rest} <- parse_indexes(rest, []),
         {:ok, more} <- parse_more(rest) do
      {:ok, [key | indexes] ++ more}
    end
  end

  defp parse_more(path), do: {:error, "'#{path}' must be preceded by . or start with ["}

  # A key is one or more characters other than ".", "[" and "]". Stops at
  # the first character that starts a new segment or ends the path.
  defp parse_key(path) do
    case take_key(path, "") do
      {"", _rest} -> {:error, key_error(path)}
      {key, rest} -> {:ok, key, rest}
    end
  end

  defp take_key(<<c, rest::binary>>, acc) when c not in [?., ?[, ?]] do
    take_key(rest, acc <> <<c>>)
  end

  defp take_key(rest, acc), do: {acc, rest}

  defp key_error(""), do: "a path segment cannot be empty"
  defp key_error(path), do: "'#{path}' is missing a key"

  # `index*` — zero or more `[n]` groups immediately following a key or the
  # previous index.
  defp parse_indexes("[" <> rest, acc) do
    case take_index(rest, "") do
      {digits, "]" <> after_bracket} ->
        case Integer.parse(digits) do
          {n, ""} when n >= 0 ->
            parse_indexes(after_bracket, [n | acc])

          _ when digits == "" ->
            {:error, "[] is empty; an index must be a non-negative integer"}

          _ ->
            {:error, "'[#{digits}]' is not an array index"}
        end

      {digits, tail} ->
        {:error, "'[#{digits}#{tail}' is missing a closing ]"}
    end
  end

  defp parse_indexes(rest, acc), do: {:ok, Enum.reverse(acc), rest}

  # Reads up to the next "]" or the end of the path, without judging the
  # characters yet, so a non-digit index (e.g. "one") can still be quoted
  # whole in the error above.
  defp take_index("]" <> _rest = closing, acc), do: {acc, closing}
  defp take_index("", acc), do: {acc, ""}
  defp take_index(<<c, rest::binary>>, acc), do: take_index(rest, acc <> <<c>>)

  @doc """
  Looks up `path` inside `term`. A string segment on a map is
  `Map.fetch/2`; an integer segment on a list is `Enum.fetch/2`; any other
  pairing (a missing key, an out-of-range index, a key on a list, an index
  on a map, or the path running into a scalar) is `:error`. A JSON `null`
  at the end of the path is `{:ok, nil}`.
  """
  @spec fetch(term(), [segment()]) :: {:ok, term()} | :error
  def fetch(term, []), do: {:ok, term}

  def fetch(map, [key | rest]) when is_map(map) and is_binary(key) do
    case Map.fetch(map, key) do
      {:ok, value} -> fetch(value, rest)
      :error -> :error
    end
  end

  def fetch(list, [index | rest]) when is_list(list) and is_integer(index) do
    case Enum.fetch(list, index) do
      {:ok, value} -> fetch(value, rest)
      :error -> :error
    end
  end

  def fetch(_term, [_segment | _rest]), do: :error
end
