defmodule KafkaManager.Kafka.FilterTest do
  @moduledoc """
  Pins the parts of `KafkaManager.Kafka.Filter` that seed data cannot reach
  through a LiveView test: the PCRE2 backtracking limit on a catastrophic
  pattern, an invalid pattern's error shape, plain text's literal `^`/`$`, a
  null key never matching, and a header value with no header name.
  """

  use ExUnit.Case, async: true

  alias KafkaManager.Kafka.{Filter, Message}

  defp message(attrs) do
    defaults = %{
      offset: 0,
      key: "some-key",
      value: "some-value",
      timestamp: DateTime.utc_now(),
      headers: [],
      partition: 0
    }

    struct!(Message, Map.merge(defaults, Map.new(attrs)))
  end

  test "a catastrophic pattern hits the backtracking limit instead of hanging" do
    subject = String.duplicate("a", 30) <> "b"
    assert {:ok, filter} = Filter.parse(%{"key" => "(a+)+$", "key_mode" => "regex"})

    {elapsed_us, result} =
      :timer.tc(fn -> Filter.match(filter, message(key: subject)) end)

    assert {:match_limit, "key"} = result
    assert elapsed_us < 1_000_000
  end

  test "an invalid pattern yields a field-keyed error, not a raise" do
    assert {:error, %{"key" => message}} =
             Filter.parse(%{"key" => "order-(", "key_mode" => "regex"})

    assert message =~ "Invalid regular expression"
  end

  test "plain text treats ^ and $ literally" do
    assert {:ok, filter} = Filter.parse(%{"key" => "^order-0001$"})

    assert Filter.match(filter, message(key: "^order-0001$")) == :match
    assert Filter.match(filter, message(key: "order-0001")) == :nomatch
  end

  test "a nil key never matches a key filter" do
    assert {:ok, filter} = Filter.parse(%{"key" => ".", "key_mode" => "regex"})
    assert Filter.match(filter, message(key: nil)) == :nomatch
  end

  test "a header value with no header name is a form error" do
    assert {:error, %{"header" => message}} = Filter.parse(%{"header_value" => "sms"})
    assert message =~ "header name"
  end

  describe "JSON field conditions" do
    defp json_filter(rows) do
      assert {:ok, filter} = Filter.parse(%{"json" => rows})
      filter
    end

    defp json_condition(attrs) do
      %{"path" => "amount", "op" => "equals", "value" => ""} |> Map.merge(attrs)
    end

    test "equals matches a number against the number, its string form, and a float" do
      filter =
        json_filter([json_condition(%{"path" => "amount", "op" => "equals", "value" => "5"})])

      assert Filter.match(filter, message(value: ~s({"amount":5}))) == :match
      assert Filter.match(filter, message(value: ~s({"amount":"5"}))) == :match
      assert Filter.match(filter, message(value: ~s({"amount":5.0}))) == :match
      assert Filter.match(filter, message(value: ~s({"amount":6}))) == :nomatch
    end

    test "equals compares numbers numerically, ignoring the literal's decimals" do
      filter =
        json_filter([json_condition(%{"path" => "amount", "op" => "equals", "value" => "622"})])

      assert Filter.match(filter, message(value: ~s({"amount":622.00}))) == :match
    end

    test "equals matches a boolean against true/false and against the same text" do
      filter =
        json_filter([json_condition(%{"path" => "paid", "op" => "equals", "value" => "true"})])

      assert Filter.match(filter, message(value: ~s({"paid":true}))) == :match
      assert Filter.match(filter, message(value: ~s({"paid":"true"}))) == :match
      assert Filter.match(filter, message(value: ~s({"paid":false}))) == :nomatch
    end

    test "equals matches JSON null against the value null" do
      filter =
        json_filter([json_condition(%{"path" => "note", "op" => "equals", "value" => "null"})])

      assert Filter.match(filter, message(value: ~s({"note":null}))) == :match
      assert Filter.match(filter, message(value: ~s({"note":"something"}))) == :nomatch
    end

    test "equals is case-sensitive" do
      filter =
        json_filter([json_condition(%{"path" => "status", "op" => "equals", "value" => "Open"})])

      assert Filter.match(filter, message(value: ~s({"status":"Open"}))) == :match
      assert Filter.match(filter, message(value: ~s({"status":"open"}))) == :nomatch
    end

    test "equals never matches an object or an array" do
      filter =
        json_filter([json_condition(%{"path" => "items", "op" => "equals", "value" => "x"})])

      assert Filter.match(filter, message(value: ~s({"items":{"a":1}}))) == :nomatch
      assert Filter.match(filter, message(value: ~s({"items":[1,2]}))) == :nomatch
    end

    test "contains is a case-insensitive substring on a string's own content" do
      filter =
        json_filter([json_condition(%{"path" => "note", "op" => "contains", "value" => "GIFT"})])

      assert Filter.match(filter, message(value: ~s({"note":"Gift wrap"}))) == :match
      assert Filter.match(filter, message(value: ~s({"note":"ring bell"}))) == :nomatch
    end

    test "exists matches a present key, including one holding JSON null, but not a missing one" do
      filter = json_filter([json_condition(%{"path" => "note", "op" => "exists"})])
      assert Filter.match(filter, message(value: ~s({"note":null}))) == :match
      assert Filter.match(filter, message(value: ~s({"note":"hi"}))) == :match
      assert Filter.match(filter, message(value: ~s({"other":1}))) == :nomatch
    end

    test "values that are not JSON, or fail to decode, never match, and nothing raises" do
      filter = json_filter([json_condition(%{"path" => "amount", "op" => "exists"})])

      for value <- ["plain text", "", <<0xFF>>, ~s({"a":1), ~s({"amount":1e400})] do
        assert Filter.match(filter, message(value: value)) == :nomatch
      end
    end

    test "a blank path is ignored: no condition and no error" do
      assert {:ok, filter} =
               Filter.parse(%{"json" => [%{"path" => "  ", "op" => "equals", "value" => "1"}]})

      assert filter.json == []
      assert Filter.active?(filter) == false
    end

    test "a blank value on equals, contains or regex is a row error" do
      assert {:error, %{"json-0" => message}} =
               Filter.parse(%{"json" => [json_condition(%{"op" => "equals", "value" => ""})]})

      assert message =~ "equals"
    end

    test "an unknown operator is a row error" do
      assert {:error, %{"json-0" => message}} =
               Filter.parse(%{"json" => [json_condition(%{"op" => "frobnicate"})]})

      assert message =~ "Choose an operator"
    end

    test "an invalid path errors only its own row, by position" do
      assert {:error, errors} =
               Filter.parse(%{
                 "json" => [
                   json_condition(%{"path" => "note", "op" => "exists"}),
                   json_condition(%{"path" => "a..b", "op" => "exists"})
                 ]
               })

      assert Map.keys(errors) == ["json-1"]
      assert errors["json-1"] =~ "Invalid path"
    end

    test "a catastrophic regex condition halts on that row" do
      subject = String.duplicate("a", 30) <> "b"

      filter =
        json_filter([json_condition(%{"path" => "note", "op" => "regex", "value" => "(a+)+$"})])

      assert Filter.match(filter, message(value: ~s({"note":"#{subject}"}))) ==
               {:match_limit, "json-0"}
    end

    test "active?/1 is true with only a JSON condition" do
      filter = json_filter([json_condition(%{"path" => "note", "op" => "exists"})])
      assert Filter.active?(filter) == true
    end
  end

  describe "contains and regex fall back to a scalar's JSON text form" do
    test "contains matches a number by its decoded text form" do
      filter =
        json_filter([json_condition(%{"path" => "n", "op" => "contains", "value" => "4"})])

      assert Filter.match(filter, message(value: ~s({"n":40}))) == :match
      assert Filter.match(filter, message(value: ~s({"n":14.0}))) == :match
      assert Filter.match(filter, message(value: ~s({"n":5}))) == :nomatch
    end

    test "regex matches a float by its decoded text form, not the literal's decimals" do
      filter =
        json_filter([
          json_condition(%{"path" => "amount", "op" => "regex", "value" => "^14\\.0$"})
        ])

      assert Filter.match(filter, message(value: ~s({"amount":14.00}))) == :match

      filter2 =
        json_filter([
          json_condition(%{"path" => "amount", "op" => "regex", "value" => "^14\\.00$"})
        ])

      assert Filter.match(filter2, message(value: ~s({"amount":14.00}))) == :nomatch
    end

    test "contains and regex match a boolean as true or false" do
      contains_filter =
        json_filter([json_condition(%{"path" => "paid", "op" => "contains", "value" => "RUE"})])

      assert Filter.match(contains_filter, message(value: ~s({"paid":true}))) == :match
      assert Filter.match(contains_filter, message(value: ~s({"paid":false}))) == :nomatch

      regex_filter =
        json_filter([json_condition(%{"path" => "paid", "op" => "regex", "value" => "^false$"})])

      assert Filter.match(regex_filter, message(value: ~s({"paid":false}))) == :match
      assert Filter.match(regex_filter, message(value: ~s({"paid":true}))) == :nomatch
    end

    test "contains and regex match JSON null as the text null" do
      contains_filter =
        json_filter([json_condition(%{"path" => "note", "op" => "contains", "value" => "ul"})])

      assert Filter.match(contains_filter, message(value: ~s({"note":null}))) == :match

      regex_filter =
        json_filter([json_condition(%{"path" => "note", "op" => "regex", "value" => "^null$"})])

      assert Filter.match(regex_filter, message(value: ~s({"note":null}))) == :match
    end

    test "contains and regex never match an object or an array" do
      contains_filter =
        json_filter([json_condition(%{"path" => "items", "op" => "contains", "value" => "sku"})])

      assert Filter.match(contains_filter, message(value: ~s({"items":[{"sku":"A"}]}))) ==
               :nomatch

      regex_filter =
        json_filter([json_condition(%{"path" => "customer", "op" => "regex", "value" => "cust"})])

      assert Filter.match(regex_filter, message(value: ~s({"customer":{"id":"cust-001"}}))) ==
               :nomatch
    end
  end
end
