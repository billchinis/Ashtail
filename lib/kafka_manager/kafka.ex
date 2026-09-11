defmodule KafkaManager.Kafka do
  @moduledoc """
  The only module the web layer calls. A thin facade: it fills in the
  resolved `Config` and delegates to the context modules
  (`KafkaManager.Kafka.Topics`, `.Messages`, `.Groups`). If a LiveView needs
  something new, add a function here rather than reaching past it.
  """

  alias KafkaManager.Kafka.{Groups, Messages, Topics}

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

  @doc """
  A topic's header summary: per-partition offsets, with no broker
  configuration fetched. Used by every topic sub-menu page (except Configs,
  which needs `get_topic/1`'s superset). See
  `KafkaManager.Kafka.Topics.topic_summary/2`.
  """
  @spec topic_summary(String.t()) ::
          {:ok, KafkaManager.Kafka.Topic.t()} | {:error, KafkaManager.Kafka.BrokerError.t()}
  def topic_summary(name), do: Topics.topic_summary(config(), name)

  @doc """
  A page of messages from a chosen partition starting at a chosen offset.
  See `KafkaManager.Kafka.Messages.fetch_messages/5`.
  """
  @spec fetch_messages(String.t(), non_neg_integer(), integer(), pos_integer()) ::
          {:ok, map()} | {:error, KafkaManager.Kafka.BrokerError.t()}
  def fetch_messages(topic, partition, from_offset, limit),
    do: Messages.fetch_messages(config(), topic, partition, from_offset, limit)

  @doc """
  Produces one message to a chosen topic/partition. `partition == nil` lets
  the context choose. See `KafkaManager.Kafka.Messages.produce/4`.
  """
  @spec produce(String.t(), non_neg_integer() | nil, map()) ::
          {:ok, map()} | {:error, KafkaManager.Kafka.BrokerError.t()}
  def produce(topic, partition, attrs), do: Messages.produce(config(), topic, partition, attrs)

  @doc """
  Lists every consumer group with its state, total lag and per-partition lag
  breakdown. See `KafkaManager.Kafka.Groups.list_groups/1`.
  """
  @spec list_groups() ::
          {:ok, [KafkaManager.Kafka.Group.t()]} | {:error, KafkaManager.Kafka.BrokerError.t()}
  def list_groups, do: Groups.list_groups(config())

  @doc """
  A single consumer group's state, total lag and per-partition lag
  breakdown. See `KafkaManager.Kafka.Groups.get_group/2`.
  """
  @spec get_group(String.t()) ::
          {:ok, KafkaManager.Kafka.Group.t()} | {:error, KafkaManager.Kafka.BrokerError.t()}
  def get_group(group_id), do: Groups.get_group(config(), group_id)

  @doc """
  Every consumer group that reads the given topic, with its state and its
  lag on that topic only. See `KafkaManager.Kafka.Groups.topic_groups/2`.
  """
  @spec topic_groups(String.t()) ::
          {:ok, [KafkaManager.Kafka.Group.t()]} | {:error, KafkaManager.Kafka.BrokerError.t()}
  def topic_groups(name), do: Groups.topic_groups(config(), name)
end
