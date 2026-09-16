defmodule Ashtail.Kafka.Filter do
  @moduledoc """
  A Data sub-menu filter: one condition each for key, value and header, an
  optional partition, and an ordered list of JSON field conditions. Pure — no
  broker access, no config. `parse/1` compiles every regular expression once;
  the compiled filter is then passed unchanged into the scan task's closure and
  to every tail tick, never recompiled per message.

  `from`/`to` are the inclusive UTC time range, to the millisecond. They narrow
  the read window through `ListOffsets` in `TopicReader`, but `match/2` also
  re-checks them against every message's own timestamp: a bound only limits
  where reading starts, not which messages qualify.

  JSON field conditions test a path into the message value, decoded as JSON at
  most once per message and only when at least one condition is present.
  `Ashtail.Kafka.JsonPath` owns the path grammar and lookup; this module
  owns parsing the row (operator, value, compiling `contains`/`regex`) and
  evaluating it against the decoded term.
  """

  alias Ashtail.Kafka.{JsonPath, Message}

  @match_limit 100_000
  @match_limit_recursion 10_000

  defstruct key: nil, value: nil, header: nil, partition: nil, from: nil, to: nil, json: []

  @typedoc "Plain text or regular expression, compiled once."
  @type pattern :: %{mode: :text | :regex, source: String.t(), regex: Regex.t()}

  @typedoc "A header name, matched exactly, plus an optional value pattern."
  @type header_filter :: %{name: String.t(), value: nil | pattern()}

  @typedoc "One JSON field condition, in row order."
  @type json_condition :: %{
          index: non_neg_integer(),
          source: String.t(),
          path: [String.t() | non_neg_integer()],
          op: :equals | :contains | :regex | :exists,
          value: nil | String.t(),
          number: nil | number(),
          regex: nil | Regex.t()
        }

  @type t :: %__MODULE__{
          key: nil | pattern(),
          value: nil | pattern(),
          header: nil | header_filter(),
          partition: nil | non_neg_integer(),
          from: nil | DateTime.t(),
          to: nil | DateTime.t(),
          json: [json_condition()]
        }

  @doc "The empty filter: every field inactive, every message matches."
  @spec none() :: t()
  def none, do: %__MODULE__{}

  @doc "True when any field of `filter` narrows the read."
  @spec active?(t()) :: boolean()
  def active?(%__MODULE__{
        key: nil,
        value: nil,
        header: nil,
        partition: nil,
        from: nil,
        to: nil,
        json: []
      }),
      do: false

  def active?(%__MODULE__{}), do: true

  @doc """
  Parses the Data view's filter query params into a `%Filter{}`. Every
  regular expression is compiled here, once per search. Returns every field
  error found, keyed by `"key"`, `"value"`, `"header"`, `"partition"`,
  `"from"`, `"to"` or `"json-<i>"`, `i` the
  0-based position of a JSON condition row in `params["json"]`.
  """
  @spec parse(map()) :: {:ok, t()} | {:error, %{String.t() => String.t()}}
  def parse(params) when is_map(params) do
    fields = %{
      key: parse_pattern(get(params, "key"), get(params, "key_mode")),
      value: parse_pattern(get(params, "value"), get(params, "value_mode")),
      header: parse_header(params),
      partition: parse_partition(get(params, "partition")),
      from: parse_datetime(get(params, "from"), "From"),
      to: parse_datetime(get(params, "to"), "To")
    }

    errors =
      Enum.reduce(fields, %{}, fn
        {field, {:error, message}}, acc -> Map.put(acc, Atom.to_string(field), message)
        {_field, {:ok, _value}}, acc -> acc
      end)
      |> maybe_range_error(fields)

    {json_conditions, json_errors} = parse_json_conditions(params)
    errors = Map.merge(errors, json_errors)

    if errors == %{} do
      {:ok,
       %__MODULE__{
         key: unwrap(fields.key),
         value: unwrap(fields.value),
         header: unwrap(fields.header),
         partition: unwrap(fields.partition),
         from: unwrap(fields.from),
         to: unwrap(fields.to),
         json: json_conditions
       }}
    else
      {:error, errors}
    end
  end

  # `from` after `to` is a range error keyed to `"to"`,
  # checked only once both fields parsed cleanly on their own.
  defp maybe_range_error(errors, fields) do
    with false <- Map.has_key?(errors, "from"),
         false <- Map.has_key?(errors, "to"),
         {:ok, %DateTime{} = from} <- fields.from,
         {:ok, %DateTime{} = to} <- fields.to,
         :gt <- DateTime.compare(from, to) do
      Map.put(errors, "to", "From must be at or before To.")
    else
      _ -> errors
    end
  end

  @doc """
  Matches one message against `filter`. Every active field must match
  (AND). Evaluates the cheapest checks first: time, key, header, value, then
  JSON conditions last, the most expensive check.
  A regular expression that hits its backtracking limit stops evaluation and
  returns `{:match_limit, field}` immediately, without checking the
  remaining fields or conditions.
  """
  @spec match(t(), Message.t()) :: :match | :nomatch | {:match_limit, String.t()}
  def match(%__MODULE__{} = filter, %Message{} = message) do
    with :match <- match_time(filter.from, filter.to, message.timestamp),
         :match <- match_key(filter.key, message.key),
         :match <- match_header(filter.header, message.headers),
         :match <- match_value(filter.value, message.value) do
      match_json(filter.json, message.value)
    end
  end

  # --- JSON field conditions ---------------------------------------------

  # `params["json"]`: a list of string-keyed row maps. Anything else there
  # (missing, a map, a string) reads as no rows, and a non-map element is
  # skipped like a blank row — `DataParams` already guarantees a clean list,
  # but `Filter` repeats the rule so it is safe called on its own.
  defp parse_json_conditions(params) do
    rows =
      case get(params, "json") do
        rows when is_list(rows) -> rows
        _other -> []
      end

    {conditions, errors} =
      rows
      |> Enum.with_index()
      |> Enum.reduce({[], %{}}, fn {row, index}, {conditions, errors} ->
        case parse_json_row(row, index) do
          :skip -> {conditions, errors}
          {:ok, condition} -> {[condition | conditions], errors}
          {:error, message} -> {conditions, Map.put(errors, "json-#{index}", message)}
        end
      end)

    {Enum.reverse(conditions), errors}
  end

  defp parse_json_row(row, _index) when not is_map(row), do: :skip

  defp parse_json_row(row, index) do
    path_source = row |> Map.get("path") |> trimmed()

    if path_source == "" do
      :skip
    else
      with {:ok, op} <- parse_json_op(Map.get(row, "op")),
           {:ok, path} <- parse_json_path(path_source),
           {:ok, value} <- parse_json_value(op, Map.get(row, "value")),
           {:ok, compiled} <- compile_json_op(op, value) do
        {:ok,
         %{
           index: index,
           source: path_source,
           path: path,
           op: op,
           value: if(op == :exists, do: nil, else: value),
           number: Map.get(compiled, :number),
           regex: Map.get(compiled, :regex)
         }}
      end
    end
  end

  defp trimmed(value) when is_binary(value), do: String.trim(value)
  defp trimmed(_value), do: ""

  defp parse_json_op(nil), do: {:ok, :equals}
  defp parse_json_op(""), do: {:ok, :equals}
  defp parse_json_op("equals"), do: {:ok, :equals}
  defp parse_json_op("contains"), do: {:ok, :contains}
  defp parse_json_op("regex"), do: {:ok, :regex}
  defp parse_json_op("exists"), do: {:ok, :exists}

  defp parse_json_op(_other),
    do: {:error, "Choose an operator: equals, contains, regex or exists."}

  defp parse_json_path(source) do
    case JsonPath.parse(source) do
      {:ok, path} ->
        {:ok, path}

      {:error, reason} ->
        {:error,
         "Invalid path: #{reason}. Use keys separated by dots and [n] for array items, " <>
           "for example customer.id or items[0].sku."}
    end
  end

  defp parse_json_value(:exists, _raw), do: {:ok, nil}

  defp parse_json_value(op, raw) do
    value = if is_binary(raw), do: raw, else: ""

    if value == "" do
      {:error, "#{op} needs a value."}
    else
      {:ok, value}
    end
  end

  defp compile_json_op(:exists, _value), do: {:ok, %{}}

  defp compile_json_op(:equals, value) do
    number =
      case Jason.decode(value) do
        {:ok, num} when is_number(num) -> num
        _other -> nil
      end

    {:ok, %{number: number}}
  end

  defp compile_json_op(:contains, value), do: compile_json_regex(value, "text")
  defp compile_json_op(:regex, value), do: compile_json_regex(value, "regex")

  defp compile_json_regex(value, mode) do
    case parse_pattern(value, mode) do
      {:ok, %{regex: regex}} -> {:ok, %{regex: regex}}
      {:error, _message} = error -> error
    end
  end

  defp match_json([], _value), do: :match

  defp match_json(conditions, value) do
    case decode_json_root(value) do
      {:ok, term} -> match_json_conditions(conditions, term)
      :nomatch -> :nomatch
    end
  end

  defp match_json_conditions(conditions, term) do
    Enum.reduce_while(conditions, :match, fn condition, _acc ->
      case match_json_condition(condition, term) do
        :match -> {:cont, :match}
        :nomatch -> {:halt, :nomatch}
        {:match_limit, _field} = limit -> {:halt, limit}
      end
    end)
  end

  # A path always needs an object or an array at the root. Checking the
  # first non-whitespace byte first means plain text, empty and null values
  # and scalar JSON never pay for a decode.
  defp decode_json_root(value) do
    case first_significant_byte(value) do
      c when c in [?{, ?[] ->
        case Jason.decode(value) do
          {:ok, term} -> {:ok, term}
          {:error, _reason} -> :nomatch
        end

      _other ->
        :nomatch
    end
  end

  defp first_significant_byte(<<c, rest::binary>>) when c in [?\s, ?\t, ?\r, ?\n],
    do: first_significant_byte(rest)

  defp first_significant_byte(<<c, _rest::binary>>), do: c
  defp first_significant_byte(<<>>), do: nil

  defp match_json_condition(%{op: :exists, path: path}, term) do
    case JsonPath.fetch(term, path) do
      {:ok, _value} -> :match
      :error -> :nomatch
    end
  end

  defp match_json_condition(%{op: :equals, path: path, value: value, number: number}, term) do
    case JsonPath.fetch(term, path) do
      {:ok, fetched} -> if json_equals?(fetched, value, number), do: :match, else: :nomatch
      :error -> :nomatch
    end
  end

  defp match_json_condition(%{op: op, path: path, regex: regex, index: index}, term)
       when op in [:contains, :regex] do
    case JsonPath.fetch(term, path) do
      {:ok, fetched} ->
        case json_text_form(fetched) do
          {:ok, text} -> run_regex(regex, text, "json-#{index}")
          :error -> :nomatch
        end

      :error ->
        :nomatch
    end
  end

  # The subject `contains` and `regex` match: a string's own content, a
  # number's or boolean's decoded text form, `null` for JSON null. Objects
  # and arrays never match either operator.
  defp json_text_form(t) when is_binary(t), do: {:ok, t}
  defp json_text_form(t) when is_number(t), do: {:ok, json_number_text(t)}
  defp json_text_form(t) when is_boolean(t), do: {:ok, to_string(t)}
  defp json_text_form(nil), do: {:ok, "null"}
  defp json_text_form(_other), do: :error

  defp json_number_text(t) when is_float(t), do: :erlang.float_to_binary(t, [:compact, :short])
  defp json_number_text(t) when is_integer(t), do: Integer.to_string(t)

  defp json_equals?(t, value, _number) when is_binary(t), do: t == value
  defp json_equals?(t, _value, number) when is_number(t) and not is_nil(number), do: t == number
  defp json_equals?(t, value, _number) when is_boolean(t), do: to_string(t) == value
  defp json_equals?(nil, value, _number), do: value == "null"
  defp json_equals?(_t, _value, _number), do: false

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

  defp parse_datetime(nil, _label), do: {:ok, nil}
  defp parse_datetime("", _label), do: {:ok, nil}

  defp parse_datetime(string, label) do
    case DateTime.from_iso8601(string) do
      {:ok, datetime, _utc_offset} -> {:ok, DateTime.truncate(datetime, :millisecond)}
      {:error, _reason} -> {:error, "#{label} must look like YYYY-MM-DDTHH:MM:SS.sssZ"}
    end
  end

  defp match_time(nil, nil, _timestamp), do: :match

  defp match_time(from, to, timestamp) do
    ms = DateTime.to_unix(timestamp, :millisecond)

    at_or_after_from = is_nil(from) or ms >= DateTime.to_unix(from, :millisecond)
    at_or_before_to = is_nil(to) or ms <= DateTime.to_unix(to, :millisecond)

    if at_or_after_from and at_or_before_to, do: :match, else: :nomatch
  end

  defp match_key(nil, _key), do: :match
  # A null key never matches a key filter.
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
