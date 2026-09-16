defmodule KafkaManager.BrokerHelpers do
  @moduledoc """
  Test-only ways to reach past the app's own boundaries: producing into
  `scratch` (the only topic tests may write to) through the app's own
  `Kafka.produce/3`, never through `rpk`/docker directly, and pointing a test
  at a different broker address than the one the app booted with.
  """

  alias KafkaManager.Kafka
  alias KafkaManager.Kafka.Config

  @doc """
  A thin wrapper over `Kafka.produce/3`, used by the tail test to write the
  `tail-probe` message.
  """
  @spec produce_probe(String.t(), non_neg_integer(), map()) ::
          {:ok, map()} | {:error, KafkaManager.Kafka.BrokerError.t()}
  def produce_probe(topic, partition, attrs) when is_binary(topic) and is_map(attrs) do
    Kafka.produce(topic, partition, attrs)
  end

  @doc """
  Points the app's resolved config at `brokers_string` (a `KAFKA_BROKERS`-style
  value, e.g. `"localhost:19099"`) for the remainder of the calling test.

  Sets the `KAFKA_BROKERS` environment variable, re-resolves the config and
  stores it where `KafkaManager.Kafka.config/0` reads it from, and registers
  an `on_exit` callback that restores both the environment variable and the
  previous config struct so no other test is left pointing at a dead broker.
  """
  @spec override_brokers(String.t()) :: :ok
  def override_brokers(brokers_string) when is_binary(brokers_string) do
    previous_env = System.get_env("KAFKA_BROKERS")
    previous_config = Application.fetch_env!(:kafka_manager, :kafka_config)

    System.put_env("KAFKA_BROKERS", brokers_string)
    config = Config.resolve!()
    Application.put_env(:kafka_manager, :kafka_config, config)

    ExUnit.Callbacks.on_exit(fn ->
      restore_env("KAFKA_BROKERS", previous_env)
      Application.put_env(:kafka_manager, :kafka_config, previous_config)
    end)

    :ok
  end

  defp restore_env(var, nil), do: System.delete_env(var)
  defp restore_env(var, value), do: System.put_env(var, value)
end
