defmodule KafkaManagerWeb.BrokerComponents do
  @moduledoc """
  The single, uniform way a broker failure is shown on every page.
  """

  use Phoenix.Component

  import KafkaManagerWeb.CoreComponents, only: [icon: 1]

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
      class="alert alert-error alert-vertical sm:alert-horizontal w-full"
    >
      <.icon name="hero-exclamation-triangle" class="size-5 shrink-0" />
      <div>
        <p class="font-semibold">
          Could not reach the broker at <strong>{@error.address}</strong>.
        </p>
        <p class="text-sm opacity-80">{@error.message}</p>
      </div>
    </div>
    """
  end
end
