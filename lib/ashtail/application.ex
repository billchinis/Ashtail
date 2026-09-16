defmodule Ashtail.Application do
  # See https://elixir.hexdocs.pm/Application.html
  # for more information on OTP Applications
  @moduledoc false

  use Application

  @impl true
  def start(_type, _args) do
    config = Ashtail.Kafka.Config.resolve!()
    Application.put_env(:ashtail, :kafka_config, config)

    children = [
      AshtailWeb.Telemetry,
      {DNSCluster, query: Application.get_env(:ashtail, :dns_cluster_query) || :ignore},
      {Phoenix.PubSub, name: Ashtail.PubSub},
      {Task.Supervisor, name: Ashtail.Kafka.TaskSupervisor},
      # Start to serve requests, typically the last entry
      AshtailWeb.Endpoint
    ]

    # See https://elixir.hexdocs.pm/Supervisor.html
    # for other strategies and supported options
    opts = [strategy: :one_for_one, name: Ashtail.Supervisor]
    Supervisor.start_link(children, opts)
  end

  # Tell Phoenix to update the endpoint configuration
  # whenever the application is updated.
  @impl true
  def config_change(changed, _new, removed) do
    AshtailWeb.Endpoint.config_change(changed, removed)
    :ok
  end
end
