defmodule KafkaManager.Kafka.FilterTest do
  @moduledoc """
  Pins the parts of `KafkaManager.Kafka.Filter` that seed data cannot reach
  through an AC test (docs/PLAN.md 6.7): the PCRE2 backtracking limit on a
  catastrophic pattern, an invalid pattern's error shape, plain text's
  literal `^`/`$`, a null key never matching, and a header value with no
  header name.
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
end
