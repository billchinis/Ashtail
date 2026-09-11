defmodule KafkaManagerWeb.TopicComponents do
  @moduledoc """
  The shared header every topic sub-menu page renders first inside
  `<Layouts.app>` (docs/PLAN.md P6, docs/DESIGN.md "topic page header and
  sub-menu"): the eyebrow, the serif `h1`, the stats strip, the tab row and
  the Produce button, followed by the one broker-error card these pages
  render.

  The tab list is defined once, here, in the order Data, Partitions,
  Consumer Groups, Configs, Logs. Each run whose route lands adds exactly
  its own entry to `tabs/1` (docs/PLAN.md 7.1) — at R1 that is Data only,
  the current `/topics/:topic` route.
  """

  use Phoenix.Component
  use KafkaManagerWeb, :verified_routes

  import KafkaManagerWeb.CoreComponents, only: [icon: 1]
  import KafkaManagerWeb.BrokerComponents, only: [broker_error: 1]

  @doc """
  Renders the topic sub-menu header. `topic` is a `%KafkaManager.Kafka.Topic{}`
  or `nil`; the stats read "—" when it is `nil`. `active` is the current
  page's tab atom.
  """
  attr :topic_name, :string, required: true
  attr :topic, :any, default: nil
  attr :active, :atom, required: true
  attr :broker_error, :any, default: nil

  def topic_header(assigns) do
    ~H"""
    <div class="space-y-6">
      <div>
        <.link navigate={~p"/"} class="link link-hover text-sm text-base-content/60">
          ← Topics
        </.link>
        <div class="flex items-end justify-between gap-6 mt-1">
          <h1 class="min-w-0 font-serif text-2xl sm:text-3xl font-semibold tracking-tight break-all">
            Topic {@topic_name}
          </h1>
        </div>
      </div>

      <div class="stats stats-horizontal bg-base-100 shadow-sm w-full sm:w-fit">
        <div class="stat px-3 py-4 sm:px-4">
          <div class="stat-title text-xs">Partitions</div>
          <div class="stat-value text-2xl font-semibold tabular-nums">
            {stat_value(@topic, :partition_count)}
          </div>
        </div>
        <div class="stat px-3 py-4 sm:px-4">
          <div class="stat-title text-xs">RF</div>
          <div class="stat-value text-2xl font-semibold tabular-nums">
            {stat_value(@topic, :replication_factor)}
          </div>
        </div>
        <div class="stat px-3 py-4 sm:px-4">
          <div class="stat-title text-xs">Messages</div>
          <div class="stat-value text-2xl font-semibold tabular-nums">
            {stat_value(@topic, :message_count)}
          </div>
        </div>
      </div>

      <div class="flex flex-col sm:flex-row sm:items-center sm:justify-between gap-4">
        <nav
          aria-label="Topic sections"
          class="tabs tabs-box tabs-sm w-full overflow-x-auto flex-nowrap sm:tabs-md sm:w-fit sm:overflow-visible"
          phx-hook=".ScrollActiveTab"
          id={"topic-tabs-#{@topic_name}"}
        >
          <.link
            :for={tab <- tabs(@topic_name)}
            navigate={tab.path}
            class="tab shrink-0 whitespace-nowrap"
            aria-current={tab.action == @active && "page"}
          >
            {tab.label}
          </.link>
        </nav>
        <script :type={Phoenix.LiveView.ColocatedHook} name=".ScrollActiveTab">
          export default {
            mounted() {
              this.scroll()
            },
            updated() {
              this.scroll()
            },
            scroll() {
              const active = this.el.querySelector("[aria-current='page']")
              if (active) active.scrollIntoView({block: "nearest", inline: "nearest"})
            }
          }
        </script>

        <.link
          navigate={~p"/topics/#{@topic_name}/produce"}
          data-produce-link
          class="btn btn-primary btn-sm rounded-full self-start sm:self-auto phx-click-loading:opacity-60"
        >
          <.icon name="hero-paper-airplane" class="size-4" /> Produce
        </.link>
      </div>

      <.broker_error error={@broker_error} />
    </div>
    """
  end

  defp tabs(topic_name) do
    [
      %{action: :data, label: "Data", path: ~p"/topics/#{topic_name}"},
      %{action: :partitions, label: "Partitions", path: ~p"/topics/#{topic_name}/partitions"},
      %{action: :groups, label: "Consumer Groups", path: ~p"/topics/#{topic_name}/groups"},
      %{action: :configs, label: "Configs", path: ~p"/topics/#{topic_name}/configs"},
      %{action: :logs, label: "Logs", path: ~p"/topics/#{topic_name}/logs"}
    ]
  end

  defp stat_value(nil, _key), do: "—"
  defp stat_value(topic, key), do: Map.fetch!(topic, key)
end
