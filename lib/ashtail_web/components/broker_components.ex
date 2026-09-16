defmodule AshtailWeb.BrokerComponents do
  @moduledoc """
  The single, uniform way a broker failure is shown on every page.
  """

  use Phoenix.Component

  import AshtailWeb.CoreComponents, only: [icon: 1]

  @doc """
  Renders nothing for `nil`. Otherwise a page-level alert naming the broker
  address and giving advice, never an inspected error tuple.
  """
  attr :error, :any, default: nil

  def broker_error(%{error: nil} = assigns), do: ~H""

  def broker_error(assigns) do
    ~H"""
    <div
      id="broker-error"
      role="alert"
      data-broker-error
      class="card bg-base-100 shadow-sm border-l-4 border-error"
    >
      <div class="card-body flex-row gap-3 p-5">
        <.icon name="hero-exclamation-triangle" class="size-5 shrink-0 text-error" />
        <div>
          <p class="font-semibold">
            Could not reach the broker at <strong class="break-all">{@error.address}</strong>.
          </p>
          <p class="text-sm text-base-content/70">{@error.message}</p>
        </div>
      </div>
    </div>
    """
  end
end
