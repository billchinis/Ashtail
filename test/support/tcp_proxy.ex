defmodule KafkaManager.TcpProxy do
  @moduledoc """
  A test-owned TCP proxy. Listens on an ephemeral port, forwards every
  connection byte-for-byte to `to`, and can sever every forwarded connection and
  stop listening on demand, without ever touching the shared Redpanda container
  other tests depend on.
  """

  use GenServer

  @type option :: {:to, {String.t(), :inet.port_number()}}

  @spec start_link([option()]) :: GenServer.on_start()
  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts)
  end

  @doc "The ephemeral port the proxy is listening on."
  @spec port(GenServer.server()) :: :inet.port_number()
  def port(proxy), do: GenServer.call(proxy, :port)

  @doc """
  Closes every forwarded connection and stops listening, so any further
  connection attempt (including a reconnect) gets `:econnrefused`.
  """
  @spec cut(GenServer.server()) :: :ok
  def cut(proxy), do: GenServer.call(proxy, :cut)

  @impl true
  def init(opts) do
    {host, upstream_port} = Keyword.fetch!(opts, :to)

    {:ok, listen_socket} =
      :gen_tcp.listen(0, [:binary, packet: :raw, active: false, reuseaddr: true])

    owner = self()
    spawn(fn -> accept_loop(listen_socket, host, upstream_port, owner) end)

    {:ok, %{listen_socket: listen_socket, sockets: [], cut?: false}}
  end

  @impl true
  def handle_call(:port, _from, state) do
    {:ok, port} = :inet.port(state.listen_socket)
    {:reply, port, state}
  end

  def handle_call(:cut, _from, state) do
    close_quietly(state.listen_socket)
    Enum.each(state.sockets, &close_quietly/1)
    {:reply, :ok, %{state | sockets: [], cut?: true}}
  end

  # `register_sockets` is an async cast, so it can be processed before or
  # after a `cut` call sent concurrently by the test. If `cut` was already
  # handled (`cut?: true`), a connection that finished accepting/connecting
  # just after it must be closed immediately instead of being stored and
  # left open forever: `cut` already closed everything registered *before*
  # it ran, and nothing will ever close this pair for it.
  @impl true
  def handle_cast({:register_sockets, sockets}, %{cut?: true} = state) do
    Enum.each(sockets, &close_quietly/1)
    {:noreply, state}
  end

  def handle_cast({:register_sockets, sockets}, state) do
    {:noreply, %{state | sockets: sockets ++ state.sockets}}
  end

  defp accept_loop(listen_socket, host, upstream_port, owner) do
    case :gen_tcp.accept(listen_socket) do
      {:ok, client_socket} ->
        spawn_connection(client_socket, host, upstream_port, owner)
        accept_loop(listen_socket, host, upstream_port, owner)

      {:error, _reason} ->
        :ok
    end
  end

  defp spawn_connection(client_socket, host, upstream_port, owner) do
    connect_opts = [:binary, packet: :raw, active: false]

    case :gen_tcp.connect(String.to_charlist(host), upstream_port, connect_opts) do
      {:ok, upstream_socket} ->
        GenServer.cast(owner, {:register_sockets, [client_socket, upstream_socket]})

        pid =
          spawn(fn ->
            receive do
              :start -> :ok
            end

            :ok = :inet.setopts(client_socket, active: true)
            :ok = :inet.setopts(upstream_socket, active: true)
            forward_loop(client_socket, upstream_socket)
          end)

        :ok = :gen_tcp.controlling_process(client_socket, pid)
        :ok = :gen_tcp.controlling_process(upstream_socket, pid)
        send(pid, :start)

      {:error, _reason} ->
        close_quietly(client_socket)
    end
  end

  defp forward_loop(a, b) do
    receive do
      {:tcp, ^a, data} ->
        :gen_tcp.send(b, data)
        forward_loop(a, b)

      {:tcp, ^b, data} ->
        :gen_tcp.send(a, data)
        forward_loop(a, b)

      {:tcp_closed, _socket} ->
        close_quietly(a)
        close_quietly(b)

      {:tcp_error, _socket, _reason} ->
        close_quietly(a)
        close_quietly(b)
    end
  end

  defp close_quietly(socket), do: :gen_tcp.close(socket)
end
