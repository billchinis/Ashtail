defmodule KafkaManager.Application do
  # See https://elixir.hexdocs.pm/Application.html
  # for more information on OTP Applications
  @moduledoc false

  use Application

  @impl true
  def start(_type, _args) do
    config = KafkaManager.Kafka.Config.resolve!()
    Application.put_env(:kafka_manager, :kafka_config, config)

    children = [
      KafkaManagerWeb.Telemetry,
      {DNSCluster, query: Application.get_env(:kafka_manager, :dns_cluster_query) || :ignore},
      {Phoenix.PubSub, name: KafkaManager.PubSub},
      # Start a worker by calling: KafkaManager.Worker.start_link(arg)
      # {KafkaManager.Worker, arg},
      # Start to serve requests, typically the last entry
      KafkaManagerWeb.Endpoint
    ]

    # See https://elixir.hexdocs.pm/Supervisor.html
    # for other strategies and supported options
    opts = [strategy: :one_for_one, name: KafkaManager.Supervisor]
    Supervisor.start_link(children, opts)
  end

  # Tell Phoenix to update the endpoint configuration
  # whenever the application is updated.
  @impl true
  def config_change(changed, _new, removed) do
    KafkaManagerWeb.Endpoint.config_change(changed, removed)
    :ok
  end
end
