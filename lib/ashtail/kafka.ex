defmodule Ashtail.Kafka do
  @moduledoc """
  The only module the web layer calls. A thin facade: it fills in the
  resolved `Config` and delegates to the context modules
  (`Ashtail.Kafka.Topics`, `.Messages`, `.Groups`). If a LiveView needs
  something new, add a function here rather than reaching past it.
  """

  alias Ashtail.Kafka.{Filter, Groups, Messages, Topics, TopicReader}

  @doc """
  The cluster connection settings resolved at boot by
  `Ashtail.Application`.
  """
  @spec config() :: Ashtail.Kafka.Config.t()
  def config, do: Application.fetch_env!(:ashtail, :kafka_config)

  @doc """
  Lists topics. See `Ashtail.Kafka.Topics.list_topics/2` for `opts` and
  the returned shape.
  """
  @spec list_topics(keyword()) :: {:ok, map()} | {:error, Ashtail.Kafka.BrokerError.t()}
  def list_topics(opts \\ []), do: Topics.list_topics(config(), opts)

  @doc """
  A single topic's per-partition offsets and broker configuration. See
  `Ashtail.Kafka.Topics.get_topic/2`.
  """
  @spec get_topic(String.t()) ::
          {:ok, Ashtail.Kafka.Topic.t()} | {:error, Ashtail.Kafka.BrokerError.t()}
  def get_topic(name), do: Topics.get_topic(config(), name)

  @doc """
  A topic's header summary: per-partition offsets, with no broker
  configuration fetched. Used by every topic sub-menu page (except Configs,
  which needs `get_topic/1`'s superset). See
  `Ashtail.Kafka.Topics.topic_summary/2`.
  """
  @spec topic_summary(String.t()) ::
          {:ok, Ashtail.Kafka.Topic.t()} | {:error, Ashtail.Kafka.BrokerError.t()}
  def topic_summary(name), do: Topics.topic_summary(config(), name)

  @doc """
  A page of messages from a chosen partition starting at a chosen offset.
  See `Ashtail.Kafka.Messages.fetch_messages/5`.
  """
  @spec fetch_messages(String.t(), non_neg_integer(), integer(), pos_integer()) ::
          {:ok, map()} | {:error, Ashtail.Kafka.BrokerError.t()}
  def fetch_messages(topic, partition, from_offset, limit),
    do: Messages.fetch_messages(config(), topic, partition, from_offset, limit)

  @doc """
  Produces one message to a chosen topic/partition. `partition == nil` lets
  the context choose. See `Ashtail.Kafka.Messages.produce/4`.
  """
  @spec produce(String.t(), non_neg_integer() | nil, map()) ::
          {:ok, map()} | {:error, Ashtail.Kafka.BrokerError.t()}
  def produce(topic, partition, attrs), do: Messages.produce(config(), topic, partition, attrs)

  @doc """
  Lists every consumer group with its state, total lag and per-partition lag
  breakdown. See `Ashtail.Kafka.Groups.list_groups/1`.
  """
  @spec list_groups() ::
          {:ok, [Ashtail.Kafka.Group.t()]} | {:error, Ashtail.Kafka.BrokerError.t()}
  def list_groups, do: Groups.list_groups(config())

  @doc """
  A single consumer group's state, total lag and per-partition lag
  breakdown. See `Ashtail.Kafka.Groups.get_group/2`.
  """
  @spec get_group(String.t()) ::
          {:ok, Ashtail.Kafka.Group.t()} | {:error, Ashtail.Kafka.BrokerError.t()}
  def get_group(group_id), do: Groups.get_group(config(), group_id)

  @doc """
  Every consumer group that reads the given topic, with its state and its
  lag on that topic only. See `Ashtail.Kafka.Groups.topic_groups/2`.
  """
  @spec topic_groups(String.t()) ::
          {:ok, [Ashtail.Kafka.Group.t()]} | {:error, Ashtail.Kafka.BrokerError.t()}
  def topic_groups(name), do: Groups.topic_groups(config(), name)

  @doc """
  Log directory usage for every partition replica of a topic. See
  `Ashtail.Kafka.Topics.topic_log_dirs/2`.
  """
  @spec topic_log_dirs(String.t()) ::
          {:ok,
           [
             %{
               partition: non_neg_integer(),
               broker: integer(),
               log_dir: String.t(),
               size_bytes: non_neg_integer(),
               offset_lag: integer()
             }
           ]}
          | {:error, Ashtail.Kafka.BrokerError.t()}
  def topic_log_dirs(name), do: Topics.topic_log_dirs(config(), name)

  @doc """
  One page of a topic's messages, merged across every partition and sorted
  newest first. The Data sub-menu's only read path. See
  `Ashtail.Kafka.TopicReader.read/3`.
  """
  @spec read_topic(String.t(), keyword()) ::
          {:ok, map()} | {:error, Ashtail.Kafka.BrokerError.t()}
  def read_topic(name, opts \\ []), do: TopicReader.read(config(), name, opts)

  @doc """
  Parses the Data sub-menu's filter query params into a `%Filter{}`. Pure, no
  broker access. `params["json"]` is the ordered list of JSON field condition
  rows `DataParams` normalises; a row error is keyed `"json-<i>"`, `i` its
  0-based position. See `Ashtail.Kafka.Filter.parse/1`.
  """
  @spec parse_filter(map()) :: {:ok, Filter.t()} | {:error, %{String.t() => String.t()}}
  def parse_filter(params), do: Filter.parse(params)
end
