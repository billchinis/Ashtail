defmodule Ashtail.Kafka.ConfigTest do
  @moduledoc """
  Cluster connection settings are read from environment variables.
  """

  use ExUnit.Case, async: true

  alias Ashtail.Kafka.Config

  @dev_defaults [
    brokers: "localhost:19092",
    client_id: "ashtail",
    connect_timeout: 5000,
    request_timeout: 10_000,
    tls: false,
    sasl_mechanism: nil,
    sasl_username: nil,
    sasl_password: nil
  ]

  test "unset environment variables resolve to the documented dev/test defaults" do
    assert {:ok, %Config{} = config} = Config.resolve(%{}, @dev_defaults)

    assert config.brokers == [{"localhost", 19_092}]
    assert config.client_id == "ashtail"
    assert config.connect_timeout == 5000
    assert config.request_timeout == 10_000
    assert config.tls == false
    assert config.sasl == nil
  end

  test "fully-set environment variables resolve to the given values" do
    env = %{
      "KAFKA_BROKERS" => "a.example:9093, b.example:9093",
      "KAFKA_CLIENT_ID" => "probe",
      "KAFKA_CONNECT_TIMEOUT_MS" => "1234",
      "KAFKA_REQUEST_TIMEOUT_MS" => "4321",
      "KAFKA_TLS" => "true",
      "KAFKA_SASL_MECHANISM" => "scram-sha-512",
      "KAFKA_SASL_USERNAME" => "u",
      "KAFKA_SASL_PASSWORD" => "p"
    }

    assert {:ok, %Config{} = config} = Config.resolve(env, @dev_defaults)

    assert config.brokers == [{"a.example", 9093}, {"b.example", 9093}]
    assert config.client_id == "probe"
    assert config.connect_timeout === 1234
    assert config.request_timeout === 4321
    assert is_integer(config.connect_timeout)
    assert is_integer(config.request_timeout)
    assert config.tls == true
    assert config.sasl == {:scram_sha_512, "u", "p"}
  end

  test "an unparsable KAFKA_BROKERS fails with an error naming the variable" do
    env = %{"KAFKA_BROKERS" => "nonsense"}

    assert {:error, message} = Config.resolve(env, @dev_defaults)
    assert message =~ "KAFKA_BROKERS"
  end
end
