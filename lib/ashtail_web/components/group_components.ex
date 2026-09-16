defmodule AshtailWeb.GroupComponents do
  @moduledoc """
  Shared rendering for a consumer group's state and lag, used everywhere a
  group's state or lag is shown so the pages cannot drift apart: `state_pill/1`
  and `lag/1` are the current function components.
  """

  use Phoenix.Component

  @doc """
  The consumer group state pill: a coloured `status` dot plus a `badge-soft`
  badge, with the state word kept in `base-content` (the light theme's
  success/warning text colours fail 4.5:1 at this size, so colour carries the
  dot and the tint, never the word). `size` defaults to `badge-sm`; the group
  detail stats strip is the one caller that passes `badge-md`.
  """
  attr :state, :string, required: true
  attr :size, :string, default: "badge-sm"

  def state_pill(assigns) do
    ~H"""
    <span class={["badge badge-soft gap-1.5 text-base-content", @size, pill_color(@state)]}>
      <span class={["status status-sm", pill_dot(@state)]} />{@state}
    </span>
    """
  end

  defp pill_color("Stable"), do: "badge-success"
  defp pill_color("PreparingRebalance"), do: "badge-warning"
  defp pill_color("CompletingRebalance"), do: "badge-warning"
  defp pill_color("Empty"), do: "badge-neutral"
  defp pill_color("Dead"), do: "badge-error"
  defp pill_color(_other), do: "badge-ghost"

  defp pill_dot("Stable"), do: "status-success"
  defp pill_dot("PreparingRebalance"), do: "status-warning"
  defp pill_dot("CompletingRebalance"), do: "status-warning"
  defp pill_dot("Empty"), do: "status-neutral"
  defp pill_dot("Dead"), do: "status-error"
  defp pill_dot(_other), do: ""

  @doc """
  A lag numeral: `size={:large}` (the default) is a bare `text-xl`-or-bigger
  numeral for group and topic totals; non-zero is `text-warning`, zero is
  `text-base-content/40`. `size={:small}` is an ink numeral for the
  per-partition tables, with a leading `status status-warning` dot when the lag
  is non-zero and no dot when it is zero. The caller sets the numeral's own text
  size (`text-xl`, `text-2xl`, ...) through `class`.
  """
  attr :lag, :integer, required: true
  attr :size, :atom, default: :large, values: [:large, :small]
  attr :class, :any, default: nil

  def lag(%{size: :large} = assigns) do
    ~H"""
    <span class={[
      "tabular-nums font-semibold",
      @lag > 0 && "text-warning",
      @lag == 0 && "text-base-content/40",
      @class
    ]}>{@lag}</span>
    """
  end

  def lag(%{size: :small} = assigns) do
    ~H"""
    <span class={["inline-flex items-center gap-1.5 tabular-nums", @class]}>
      <span :if={@lag > 0} class="status status-sm status-warning" />{@lag}
    </span>
    """
  end
end
