defmodule Ashtail.Kafka.BrokerError do
  @moduledoc """
  A uniform broker-failure shape. Every context function that talks to the
  broker returns `{:error, %BrokerError{}}` instead of raising, a bare
  `:error`, or `{:error, atom()}`, so every page can render the same
  `<.broker_error />` component.
  """

  @enforce_keys [:address, :reason, :message]
  defstruct [:address, :reason, :message]

  @type t :: %__MODULE__{
          address: String.t(),
          reason: term(),
          message: String.t()
        }
end
