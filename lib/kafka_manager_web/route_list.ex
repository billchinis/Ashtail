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

  # The long-named audit topic (~90 chars) from priv/kafka/seed.sh, seeded with
  # 200-char keys, 2 KB values and long header names. Its name is URL-encoded
  # (via `URI.encode/1`, path-safe) before it goes into a literal path below,
  # the same way `~p` encodes segments interpolated from a LiveView template.
  @audit_topic "audit-log-with-a-very-long-topic-name-that-stresses-table-layout-and-headers-0123456789"

  # Additional seed-backed paths covering the hostile seed content that
  # `@params` above never reaches (it only ever substitutes `orders`
  # partition 0 and group `orders-service`): the audit topic's long keys and
  # 2 KB values, `notifications`' null keys, and a group with real lag. Kept
  # here, alongside `@params`, so `mix screenshots` and the smoke test never
  # have to invent their own extra paths.
  @extra_routes [
    %{path: "/topics/#{URI.encode(@audit_topic)}", live?: true},
    %{path: "/topics/#{URI.encode(@audit_topic)}/partitions/0", live?: true},
    %{path: "/topics/notifications/partitions/0", live?: true},
    %{path: "/groups/lagging-analytics", live?: true}
  ]

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

  @doc """
  Extra seed-backed paths (already-substituted, literal) covering seed
  content `@params` cannot reach because it only ever supplies one value per
  path parameter: the audit topic's long keys and 2 KB values, the
  `notifications` topic's null keys, and the `lagging-analytics` group's
  real per-partition lag.
  """
  def extra_routes, do: @extra_routes

  @doc "`routes/1` plus `extra_routes/0`, for callers that want full coverage."
  def all_routes(params \\ @params), do: routes(params) ++ @extra_routes

  @doc "`paths/1` plus `extra_routes/0`'s paths, for callers that want full coverage."
  def all_paths(params \\ @params), do: Enum.map(all_routes(params), & &1.path)

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
