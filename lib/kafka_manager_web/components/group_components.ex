defmodule KafkaManagerWeb.GroupComponents do
  @moduledoc """
  Shared CSS-class helpers for a consumer group's state badge and lag
  colouring (DESIGN.md), used by both `GroupLive.Index` and `GroupLive.Show`
  so the two pages cannot drift apart.
  """

  @doc """
  The DECISIONS.md colour mapping for a group's raw Kafka state, as a
  daisyUI badge modifier class, for the coloured badge in the State cell
  (DESIGN.md).
  """
  def badge_class("Stable"), do: "badge-success"
  def badge_class("PreparingRebalance"), do: "badge-warning"
  def badge_class("CompletingRebalance"), do: "badge-warning"
  def badge_class("Empty"), do: "badge-neutral text-base-content"
  def badge_class("Dead"), do: "badge-error"
  def badge_class(_other), do: "badge-ghost"

  @doc """
  Lag colour rule (DESIGN.md): non-zero lag is a warning, zero lag is muted.
  """
  def lag_class(lag) when is_integer(lag) and lag > 0, do: "text-warning font-semibold"
  def lag_class(_lag), do: "text-base-content/50"
end
