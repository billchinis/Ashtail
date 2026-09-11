defmodule KafkaManager.Kafka.Filter do
  @moduledoc """
  A Data sub-menu filter: one condition each for key, value and header, plus
  an optional partition (docs/PLAN.md 2.5). Pure — no broker access, no
  config. `parse/1` compiles every regular expression once; the compiled
  filter is then passed unchanged into the scan task's closure and to every
  tail tick, never recompiled per message.

  `from`/`to` (the time range) are not part of this build (AC-19); they
  arrive at AC-20.
  """

  alias KafkaManager.Kafka.Message

  @match_limit 100_000
  @match_limit_recursion 10_000

  defstruct key: nil, value: nil, header: nil, partition: nil

  @typedoc "Plain text or regular expression, compiled once."
  @type pattern :: %{mode: :text | :regex, source: String.t(), regex: Regex.t()}

  @typedoc "A header name, matched exactly, plus an optional value pattern."
  @type header_filter :: %{name: String.t(), value: nil | pattern()}

  @type t :: %__MODULE__{
          key: nil | pattern(),
          value: nil | pattern(),
          header: nil | header_filter(),
          partition: nil | non_neg_integer()
        }

  @doc "The empty filter: every field inactive, every message matches."
  @spec none() :: t()
  def none, do: %__MODULE__{}

  @doc "True when any field of `filter` narrows the read."
  @spec active?(t()) :: boolean()
  def active?(%__MODULE__{key: nil, value: nil, header: nil, partition: nil}), do: false
  def active?(%__MODULE__{}), do: true

  @doc """
  Parses the Data view's filter query params into a `%Filter{}`. Every
  regular expression is compiled here, once per search. Returns every field
  error found, keyed by `"key"`, `"value"`, `"header"` or `"partition"`
  (docs/PLAN.md 2.5).
  """
  @spec parse(map()) :: {:ok, t()} | {:error, %{String.t() => String.t()}}
  def parse(params) when is_map(params) do
    fields = %{
      key: parse_pattern(get(params, "key"), get(params, "key_mode")),
      value: parse_pattern(get(params, "value"), get(params, "value_mode")),
      header: parse_header(params),
      partition: parse_partition(get(params, "partition"))
    }

    errors =
      Enum.reduce(fields, %{}, fn
        {field, {:error, message}}, acc -> Map.put(acc, Atom.to_string(field), message)
        {_field, {:ok, _value}}, acc -> acc
      end)

    if errors == %{} do
      {:ok,
       %__MODULE__{
         key: unwrap(fields.key),
         value: unwrap(fields.value),
         header: unwrap(fields.header),
         partition: unwrap(fields.partition)
       }}
    else
      {:error, errors}
    end
  end

  @doc """
  Matches one message against `filter`. Every active field must match
  (AND). Evaluates the cheapest checks first: key, header, then value
  (docs/PLAN.md 2.5 — the time check ahead of these arrives at AC-20). A
  regular expression that hits its backtracking limit stops evaluation and
  returns `{:match_limit, field}` immediately, without checking the
  remaining fields.
  """
  @spec match(t(), Message.t()) :: :match | :nomatch | {:match_limit, String.t()}
  def match(%__MODULE__{} = filter, %Message{} = message) do
    with :match <- match_key(filter.key, message.key),
         :match <- match_header(filter.header, message.headers) do
      match_value(filter.value, message.value)
    end
  end

  defp get(params, key), do: Map.get(params, key)

  defp unwrap({:ok, value}), do: value

  defp parse_pattern(nil, _mode), do: {:ok, nil}
  defp parse_pattern("", _mode), do: {:ok, nil}

  defp parse_pattern(source, mode) do
    {regex_source, opts} =
      case mode do
        "regex" -> {source, ""}
        _text -> {Regex.escape(source), "i"}
      end

    case Regex.compile(regex_source, opts) do
      {:ok, regex} ->
        {:ok, %{mode: pattern_mode(mode), source: source, regex: regex}}

      {:error, {reason, position}} ->
        {:error, "Invalid regular expression: #{reason} at position #{position}"}
    end
  end

  defp pattern_mode("regex"), do: :regex
  defp pattern_mode(_mode), do: :text

  defp parse_header(params) do
    name = blank_to_nil(get(params, "header"))
    value_source = blank_to_nil(get(params, "header_value"))
    mode = get(params, "header_mode")

    cond do
      is_nil(name) and is_nil(value_source) ->
        {:ok, nil}

      is_nil(name) ->
        {:error, "A header filter needs a header name."}

      true ->
        case parse_pattern(value_source, mode) do
          {:ok, pattern} -> {:ok, %{name: name, value: pattern}}
          {:error, _message} = error -> error
        end
    end
  end

  defp parse_partition(value) do
    case blank_to_nil(value) do
      nil ->
        {:ok, nil}

      string ->
        case Integer.parse(string) do
          {n, ""} when n >= 0 -> {:ok, n}
          _other -> {:error, "Partition must be a non-negative integer."}
        end
    end
  end

  defp blank_to_nil(nil), do: nil
  defp blank_to_nil(""), do: nil
  defp blank_to_nil(value), do: value

  defp match_key(nil, _key), do: :match
  # A null key never matches a key filter (docs/PLAN.md 2.5).
  defp match_key(_pattern, nil), do: :nomatch
  defp match_key(pattern, key), do: run_regex(pattern.regex, key, "key")

  defp match_header(nil, _headers), do: :match

  defp match_header(%{name: name, value: value_pattern}, headers) do
    case Enum.filter(headers, fn {header_name, _value} -> header_name == name end) do
      [] -> :nomatch
      matching -> match_header_value(matching, value_pattern)
    end
  end

  # A blank header value matches any message carrying the header.
  defp match_header_value(_matching, nil), do: :match

  defp match_header_value(matching, pattern) do
    Enum.reduce_while(matching, :nomatch, fn {_name, value}, acc ->
      case run_regex(pattern.regex, value, "header") do
        :match -> {:halt, :match}
        :nomatch -> {:cont, acc}
        {:match_limit, _field} = limit -> {:halt, limit}
      end
    end)
  end

  defp match_value(nil, _value), do: :match
  defp match_value(pattern, value), do: run_regex(pattern.regex, value, "value")

  defp run_regex(compiled, subject, field) do
    opts = [
      :report_errors,
      {:capture, :none},
      {:match_limit, @match_limit},
      {:match_limit_recursion, @match_limit_recursion}
    ]

    case :re.run(subject, compiled.re_pattern, opts) do
      :match -> :match
      :nomatch -> :nomatch
      {:error, :match_limit} -> {:match_limit, field}
      {:error, :match_limit_recursion} -> {:match_limit, field}
    end
  end
end
