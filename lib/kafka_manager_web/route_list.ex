defmodule KafkaManagerWeb.RouteList do
  @moduledoc """
  The GET routes covered by the smoke test (test/smoke_test.exs) and the
  screenshot pass (`mix screenshots`). Both read from here so they can never
  drift apart.

  `@params` maps path parameter names to values that exist in the seed data
  (priv/kafka/seed.sh). A route with a parameter missing from this map fails
  loudly: extend the map, do not skip the route.
  """

  @params %{
    "topic" => "orders",
    "partition" => "0",
    "group" => "orders-service"
  }

  @skip_prefixes ["/dev"]

  @doc "Seed-backed values for path parameters."
  def params, do: @params

  @doc "Every GET route as `%{path: String.t(), live?: boolean()}` with params substituted."
  def routes(params \\ @params) do
    KafkaManagerWeb.Router.__routes__()
    |> Enum.filter(&(&1.verb == :get))
    |> Enum.reject(&skip?(&1.path))
    |> Enum.uniq_by(& &1.path)
    |> Enum.map(fn route ->
      %{
        path: substitute(route.path, params),
        live?: Map.has_key?(route.metadata, :phoenix_live_view)
      }
    end)
  end

  @doc "Every GET route path with params substituted."
  def paths(params \\ @params), do: Enum.map(routes(params), & &1.path)

  defp skip?(path) do
    Enum.any?(@skip_prefixes, &String.starts_with?(path, &1)) or String.contains?(path, "*")
  end

  defp substitute(path, params) do
    path
    |> String.split("/")
    |> Enum.map_join("/", fn
      ":" <> name ->
        Map.get(params, name) ||
          raise ArgumentError,
                "route #{path} has parameter :#{name} with no seed value; " <>
                  "add it to @params in #{inspect(__MODULE__)}"

      segment ->
        segment
    end)
  end
end
