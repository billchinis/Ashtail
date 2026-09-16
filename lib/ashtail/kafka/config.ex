defmodule Ashtail.Kafka.Config do
  @moduledoc """
  Resolves the cluster connection settings from environment variables.

  This is the only module that names the `KAFKA_*` environment variables. It
  is a pure resolver: `resolve/2` takes the environment and the per-environment
  defaults as explicit arguments so it can be tested without touching the real
  process environment, and so nothing caches a stale broker list.
  """

  @enforce_keys [:brokers, :client_id, :connect_timeout, :request_timeout, :tls, :sasl]
  defstruct [:brokers, :client_id, :connect_timeout, :request_timeout, :tls, :sasl]

  @type sasl_mechanism :: :plain | :scram_sha_256 | :scram_sha_512

  @type t :: %__MODULE__{
          brokers: [{String.t(), pos_integer()}],
          client_id: String.t(),
          connect_timeout: pos_integer(),
          request_timeout: pos_integer(),
          tls: boolean(),
          sasl: nil | {sasl_mechanism(), String.t(), String.t()}
        }

  @mechanisms %{
    "plain" => :plain,
    "scram-sha-256" => :scram_sha_256,
    "scram-sha-512" => :scram_sha_512
  }

  @doc """
  Resolves the config from an environment map and a defaults keyword list.

  Returns `{:ok, %Config{}}` or `{:error, message}` where `message` names the
  offending environment variable.
  """
  @spec resolve(map(), keyword()) :: {:ok, t()} | {:error, String.t()}
  def resolve(env, defaults) when is_map(env) do
    with {:ok, brokers} <- resolve_brokers(env, defaults),
         {:ok, client_id} <- {:ok, resolve_string(env, "KAFKA_CLIENT_ID", defaults[:client_id])},
         {:ok, connect_timeout} <-
           resolve_integer(env, "KAFKA_CONNECT_TIMEOUT_MS", defaults[:connect_timeout]),
         {:ok, request_timeout} <-
           resolve_integer(env, "KAFKA_REQUEST_TIMEOUT_MS", defaults[:request_timeout]),
         {:ok, tls} <- resolve_tls(env, defaults),
         {:ok, sasl} <- resolve_sasl(env, defaults) do
      {:ok,
       %__MODULE__{
         brokers: brokers,
         client_id: client_id,
         connect_timeout: connect_timeout,
         request_timeout: request_timeout,
         tls: tls,
         sasl: sasl
       }}
    end
  end

  @doc """
  Same as `resolve/2`, reading the real system environment.
  """
  @spec resolve(keyword()) :: {:ok, t()} | {:error, String.t()}
  def resolve(defaults) when is_list(defaults), do: resolve(System.get_env(), defaults)

  @doc """
  Same as `resolve/1,2` but raises `RuntimeError` on error. Used at boot only.
  """
  @spec resolve!() :: t()
  def resolve!, do: resolve!(app_defaults())

  @spec resolve!(map() | keyword()) :: t()
  def resolve!(env_or_defaults) when is_map(env_or_defaults),
    do: resolve!(env_or_defaults, app_defaults())

  def resolve!(defaults) when is_list(defaults), do: resolve!(System.get_env(), defaults)

  @spec resolve!(map(), keyword()) :: t()
  def resolve!(env, defaults) do
    case resolve(env, defaults) do
      {:ok, config} -> config
      {:error, message} -> raise message
    end
  end

  @doc """
  The bootstrap broker list joined as `"host:port"` pairs, comma-separated.
  """
  @spec address(t()) :: String.t()
  def address(%__MODULE__{brokers: brokers}) do
    Enum.map_join(brokers, ", ", fn {host, port} -> "#{host}:#{port}" end)
  end

  @doc """
  The map handed to `:brod`/`:kpro` connection options.
  """
  @spec conn_config(t()) :: map()
  def conn_config(%__MODULE__{} = config) do
    %{
      client_id: config.client_id,
      connect_timeout: config.connect_timeout,
      request_timeout: config.request_timeout,
      ssl: config.tls,
      sasl: sasl_opt(config.sasl)
    }
  end

  # kpro's SASL code matches the *Erlang* atom `undefined` to mean "no auth".
  # Elixir's `nil` is a different atom and does not match that clause, so it
  # must be translated at the boundary or every plaintext connection attempts
  # (and fails) a SASL handshake.
  defp sasl_opt(nil), do: :undefined
  defp sasl_opt(sasl), do: sasl

  defp app_defaults do
    Application.get_env(:ashtail, :kafka_defaults, [])
  end

  defp resolve_brokers(env, defaults) do
    case Map.fetch(env, "KAFKA_BROKERS") do
      {:ok, value} -> parse_brokers(value)
      :error -> resolve_default_brokers(defaults[:brokers])
    end
  end

  defp resolve_default_brokers(:required) do
    {:error, "KAFKA_BROKERS is required but was not set"}
  end

  defp resolve_default_brokers(value) when is_binary(value) do
    parse_brokers(value)
  end

  defp parse_brokers(value) do
    entries = value |> String.split(",") |> Enum.map(&String.trim/1)

    case Enum.reduce_while(entries, [], &parse_broker_entry/2) do
      {:error, _} = error -> error
      brokers -> {:ok, Enum.reverse(brokers)}
    end
  end

  defp parse_broker_entry(entry, acc) do
    case parse_broker(entry) do
      {:ok, broker} -> {:cont, [broker | acc]}
      :error -> {:halt, {:error, "KAFKA_BROKERS is invalid: #{inspect(entry)}"}}
    end
  end

  defp parse_broker(entry) do
    with [host, port_str] when host != "" <- String.split(entry, ":"),
         {port, ""} when port > 0 <- Integer.parse(port_str) do
      {:ok, {host, port}}
    else
      _ -> :error
    end
  end

  defp resolve_string(env, var, default) do
    Map.get(env, var, default)
  end

  defp resolve_integer(env, var, default) do
    case Map.fetch(env, var) do
      {:ok, value} ->
        case Integer.parse(value) do
          {int, ""} when int > 0 -> {:ok, int}
          _ -> {:error, "#{var} must be a positive integer, got: #{inspect(value)}"}
        end

      :error ->
        {:ok, default}
    end
  end

  defp resolve_tls(env, defaults) do
    case Map.fetch(env, "KAFKA_TLS") do
      {:ok, "true"} -> {:ok, true}
      {:ok, "false"} -> {:ok, false}
      {:ok, value} -> {:error, "KAFKA_TLS must be true or false, got: #{inspect(value)}"}
      :error -> {:ok, defaults[:tls]}
    end
  end

  defp resolve_sasl(env, defaults) do
    mechanism = Map.get(env, "KAFKA_SASL_MECHANISM", defaults[:sasl_mechanism])

    case mechanism do
      nil ->
        {:ok, nil}

      mechanism_string when is_binary(mechanism_string) ->
        resolve_sasl_mechanism(mechanism_string, env, defaults)
    end
  end

  defp resolve_sasl_mechanism(mechanism_string, env, defaults) do
    case Map.fetch(@mechanisms, mechanism_string) do
      {:ok, mechanism} ->
        username = Map.get(env, "KAFKA_SASL_USERNAME", defaults[:sasl_username])
        password = Map.get(env, "KAFKA_SASL_PASSWORD", defaults[:sasl_password])
        {:ok, {mechanism, username, password}}

      :error ->
        {:error,
         "KAFKA_SASL_MECHANISM must be one of plain, scram-sha-256, scram-sha-512, got: " <>
           inspect(mechanism_string)}
    end
  end
end
