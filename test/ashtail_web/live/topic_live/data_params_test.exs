defmodule AshtailWeb.TopicLive.DataParamsTest do
  @moduledoc """
  Pins `AshtailWeb.TopicLive.DataParams`'s JSON-row normaliser: ordering by
  integer key, ignoring malformed shapes instead of crashing, dropping and
  renumbering blank-path rows, the `exists` operator leaving its value out of
  the URL, and the round trip through `path/4` and back through `parse/1`. Also
  the pre-existing `?key[x]=1` crash that the same scalar-field coercion fixes.
  """

  use ExUnit.Case, async: true

  alias AshtailWeb.TopicLive.DataParams

  describe "parse/1 JSON rows" do
    test "rows are ordered by integer key, not string order" do
      params = %{
        "json" => %{
          "10" => %{"path" => "b", "op" => "exists"},
          "2" => %{"path" => "a", "op" => "exists"}
        }
      }

      assert %{filter_params: %{"json" => rows}} = DataParams.parse(params)
      assert Enum.map(rows, & &1["path"]) == ["a", "b"]
    end

    test "a non-canonical index key is ignored" do
      params = %{
        "json" => %{
          "01" => %{"path" => "leading-zero", "op" => "exists"},
          "-1" => %{"path" => "negative", "op" => "exists"},
          "x" => %{"path" => "not-a-number", "op" => "exists"},
          "0" => %{"path" => "kept", "op" => "exists"}
        }
      }

      assert %{filter_params: %{"json" => rows}} = DataParams.parse(params)
      assert Enum.map(rows, & &1["path"]) == ["kept"]
    end

    test "a non-map row is ignored" do
      params = %{"json" => %{"0" => "not a map", "1" => %{"path" => "kept", "op" => "exists"}}}

      assert %{filter_params: %{"json" => rows}} = DataParams.parse(params)
      assert Enum.map(rows, & &1["path"]) == ["kept"]
    end

    test "a non-binary field is blanked, and a blank op defaults to equals" do
      params = %{"json" => %{"0" => %{"path" => "amount", "op" => 5, "value" => %{}}}}

      assert %{filter_params: %{"json" => [row]}} = DataParams.parse(params)
      assert row == %{"path" => "amount", "op" => "equals", "value" => ""}
    end

    test "an unknown op is kept as it is, not defaulted" do
      params = %{"json" => %{"0" => %{"path" => "amount", "op" => "frobnicate"}}}

      assert %{filter_params: %{"json" => [row]}} = DataParams.parse(params)
      assert row["op"] == "frobnicate"
    end

    test "blank-path rows are dropped and the rest renumbered" do
      params = %{
        "json" => %{
          "0" => %{"path" => "  ", "op" => "exists"},
          "1" => %{"path" => "note", "op" => "exists"},
          "2" => %{"path" => "  ", "op" => "exists"},
          "3" => %{"path" => "amount", "op" => "exists"}
        }
      }

      assert %{filter_params: %{"json" => rows}} = DataParams.parse(params)
      assert Enum.map(rows, & &1["path"]) == ["note", "amount"]
    end

    test "an unrecognised shape (a string, a list of non-maps) reads as no rows" do
      assert %{filter_params: %{"json" => []}} = DataParams.parse(%{"json" => "not-a-map"})
      assert %{filter_params: %{"json" => []}} = DataParams.parse(%{"json" => [1, "two"]})
      assert %{filter_params: %{"json" => []}} = DataParams.parse(%{})
    end
  end

  describe "path/4 JSON rows" do
    defp decode_json_query(url) do
      %URI{query: query} = URI.parse(url)
      query |> Plug.Conn.Query.decode() |> Map.get("json", %{})
    end

    test "an exists row's value is left out of the URL" do
      filter_params = %{"json" => [%{"path" => "note", "op" => "exists", "value" => "ignored"}]}

      url = DataParams.path("payments", filter_params, 50)

      assert %{"0" => row} = decode_json_query(url)
      assert row == %{"path" => "note", "op" => "exists"}
    end

    test "a value operator keeps its value in the URL" do
      filter_params = %{"json" => [%{"path" => "amount", "op" => "equals", "value" => "3"}]}

      url = DataParams.path("payments", filter_params, 50)

      assert %{"0" => row} = decode_json_query(url)
      assert row == %{"path" => "amount", "op" => "equals", "value" => "3"}
    end

    test "no rows means no json entry in the URL" do
      url = DataParams.path("payments", %{"json" => []}, 50)
      refute url =~ "json"
    end

    test "parse/1 of path/4's URL returns the same rows" do
      filter_params = %{
        "json" => [
          %{"path" => "note", "op" => "exists", "value" => ""},
          %{"path" => "items[1].qty", "op" => "equals", "value" => "3"}
        ]
      }

      url = DataParams.path("payments", filter_params, 50)
      %URI{query: query} = URI.parse(url)
      decoded = Plug.Conn.Query.decode(query)

      assert %{filter_params: %{"json" => rows}} = DataParams.parse(decoded)
      assert rows == filter_params["json"]
    end
  end

  describe "the ?key[x]=1 regression" do
    test "a map value for a scalar filter field reaches filter_params as an empty string" do
      params = %{"key" => %{"x" => "1"}}

      assert %{filter_params: %{"key" => ""}} = DataParams.parse(params)
    end
  end
end
