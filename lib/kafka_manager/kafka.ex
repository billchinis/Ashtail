defmodule KafkaManager.Kafka do
  @moduledoc """
  The only module the web layer calls. A thin facade: it fills in the
  resolved `Config` and delegates to the context modules
  (`KafkaManager.Kafka.Topics`, `.Messages`, `.Groups`). If a LiveView needs
  something new, add a function here rather than reaching past it.
  """

  alias KafkaManager.Kafka.Topics

  @doc """
  The cluster connection settings resolved at boot by
  `KafkaManager.Application`.
  """
  @spec config() :: KafkaManager.Kafka.Config.t()
  def config, do: Application.fetch_env!(:kafka_manager, :kafka_config)

  @doc """
  Lists topics. See `KafkaManager.Kafka.Topics.list_topics/2` for `opts` and
  the returned shape.
  """
  @spec list_topics(keyword()) :: {:ok, map()} | {:error, KafkaManager.Kafka.BrokerError.t()}
  def list_topics(opts \\ []), do: Topics.list_topics(config(), opts)

  @doc """
  A single topic's per-partition offsets and broker configuration. See
  `KafkaManager.Kafka.Topics.get_topic/2`.
  """
  @spec get_topic(String.t()) ::
          {:ok, KafkaManager.Kafka.Topic.t()} | {:error, KafkaManager.Kafka.BrokerError.t()}
  def get_topic(name), do: Topics.get_topic(config(), name)
end
