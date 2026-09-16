defmodule Ashtail.TcpProxyTest do
  @moduledoc """
  Regression: `cut/1` and the async `{:register_sockets, ...}` cast a
  freshly accepted connection sends can be processed in either order. A
  registration that lands *after* `cut/1` has already run must not leave
  its socket pair open forever (nothing will ever close it otherwise).
  """

  use ExUnit.Case, async: true

  alias Ashtail.TcpProxy

  test "a registration that arrives after cut is closed immediately" do
    proxy = start_supervised!({TcpProxy, to: {"localhost", 19_092}})

    TcpProxy.cut(proxy)

    {:ok, listen_socket} = :gen_tcp.listen(0, [:binary, packet: :raw, active: false])
    {:ok, port} = :inet.port(listen_socket)

    {:ok, client_socket} =
      :gen_tcp.connect({127, 0, 0, 1}, port, [:binary, packet: :raw, active: false])

    {:ok, server_socket} = :gen_tcp.accept(listen_socket)

    # Simulate the ordering the real race allows: a connection finishes
    # accepting/connecting and casts its registration only after `cut/1`
    # has already been handled by the GenServer.
    GenServer.cast(proxy, {:register_sockets, [client_socket, server_socket]})

    assert eventually_closed?(client_socket)
    assert eventually_closed?(server_socket)

    :gen_tcp.close(listen_socket)
  end

  defp eventually_closed?(socket, retries \\ 20)

  defp eventually_closed?(_socket, 0), do: false

  defp eventually_closed?(socket, retries) do
    case :gen_tcp.send(socket, "x") do
      {:error, :closed} ->
        true

      _ ->
        Process.sleep(10)
        eventually_closed?(socket, retries - 1)
    end
  end
end
