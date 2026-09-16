defmodule Ashtail.Kafka.JsonPathTest do
  @moduledoc """
  Pins `Ashtail.Kafka.JsonPath`'s grammar and lookup: every valid path
  shape, every invalid form the grammar rejects, and `fetch/2`'s one rule (a
  string segment on a map, an integer segment on a list, anything else is
  `:error`) covering a missing key, an out-of-range index, a key on a list, an
  index on a map, a scalar mid-path and a JSON `null` leaf.
  """

  use ExUnit.Case, async: true

  alias Ashtail.Kafka.JsonPath

  describe "parse/1 valid paths" do
    test "a bare key" do
      assert JsonPath.parse("amount") == {:ok, ["amount"]}
    end

    test "dotted keys" do
      assert JsonPath.parse("customer.id") == {:ok, ["customer", "id"]}
    end

    test "a key followed by an index" do
      assert JsonPath.parse("items[0].sku") == {:ok, ["items", 0, "sku"]}
    end

    test "a path starting with an index" do
      assert JsonPath.parse("[0].id") == {:ok, [0, "id"]}
    end

    test "a single top-level index" do
      assert JsonPath.parse("[0]") == {:ok, [0]}
    end

    test "consecutive indexes" do
      assert JsonPath.parse("a[0][1]") == {:ok, ["a", 0, 1]}
    end
  end

  describe "parse/1 invalid paths" do
    test "an empty segment between two dots" do
      assert {:error, _reason} = JsonPath.parse("a..b")
    end

    test "a leading dot" do
      assert {:error, _reason} = JsonPath.parse(".a")
    end

    test "a trailing dot" do
      assert {:error, _reason} = JsonPath.parse("a.")
    end

    test "a dot followed by a bracket" do
      assert {:error, _reason} = JsonPath.parse("a.[0]")
    end

    test "an unclosed bracket" do
      assert {:error, _reason} = JsonPath.parse("a[0")
    end

    test "an empty bracket" do
      assert {:error, _reason} = JsonPath.parse("a[]")
    end

    test "a non-digit index names the offending index" do
      assert {:error, reason} = JsonPath.parse("a[one]")
      assert reason =~ "[one]"
      assert reason =~ "not an array index"
    end

    test "a negative index" do
      assert {:error, _reason} = JsonPath.parse("a[-1]")
    end

    test "a stray closing bracket" do
      assert {:error, _reason} = JsonPath.parse("a]")
    end

    test "text after a closing bracket other than . or [" do
      assert {:error, _reason} = JsonPath.parse("a[0]b")
    end
  end

  describe "fetch/2" do
    test "a missing key is :error" do
      {:ok, path} = JsonPath.parse("missing")
      assert JsonPath.fetch(%{"amount" => 5}, path) == :error
    end

    test "an out-of-range index is :error" do
      {:ok, path} = JsonPath.parse("[5]")
      assert JsonPath.fetch([1, 2, 3], path) == :error
    end

    test "a key on a list is :error" do
      {:ok, path} = JsonPath.parse("items.sku")
      assert JsonPath.fetch(%{"items" => [%{"sku" => "A-1"}]}, path) == :error
    end

    test "an index on a map is :error" do
      {:ok, path} = JsonPath.parse("[0]")
      assert JsonPath.fetch(%{"0" => "x"}, path) == :error
    end

    test "a path running into a scalar is :error" do
      {:ok, path} = JsonPath.parse("amount.cents")
      assert JsonPath.fetch(%{"amount" => 5}, path) == :error
    end

    test "a JSON null leaf is {:ok, nil}" do
      {:ok, path} = JsonPath.parse("note")
      assert JsonPath.fetch(%{"note" => nil}, path) == {:ok, nil}
    end
  end
end
