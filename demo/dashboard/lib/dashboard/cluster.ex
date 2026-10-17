defmodule Dashboard.Cluster do
  @moduledoc """
  Watches the cluster for rex compute nodes.

  The Rust nodes find us through the local EPMD (`rex::cluster::LocalEpmd`)
  and connect; we also poll EPMD ourselves so a dashboard started after the
  compute nodes picks them up at once. Every change is broadcast on the
  `"cluster"` PubSub topic as `{:cluster, nodes}`.
  """
  use GenServer
  require Logger

  @topic "cluster"
  @poll_ms 2_000

  defstruct nodes: %{}

  def start_link(_), do: GenServer.start_link(__MODULE__, [], name: __MODULE__)

  @doc "Compute nodes currently connected, with the info they reported."
  def compute_nodes, do: GenServer.call(__MODULE__, :nodes)

  def subscribe, do: Phoenix.PubSub.subscribe(Dashboard.PubSub, @topic)

  @doc "Is this a rex compute node? They register as compute*@host."
  def compute_node?(node), do: String.starts_with?(Atom.to_string(node), "compute")

  @impl true
  def init(_) do
    :ok = :net_kernel.monitor_nodes(true, node_type: :all)
    send(self(), :poll)
    {:ok, %__MODULE__{}}
  end

  @impl true
  def handle_call(:nodes, _from, state), do: {:reply, state.nodes, state}

  @impl true
  def handle_info({:nodeup, node, _}, state) do
    if compute_node?(node) do
      Logger.info("compute node up: #{node}")
      {:noreply, refresh(state, node)}
    else
      {:noreply, state}
    end
  end

  def handle_info({:nodedown, node, _}, state) do
    if Map.has_key?(state.nodes, node) do
      Logger.warning("compute node down: #{node}")
      {:noreply, publish(%{state | nodes: Map.delete(state.nodes, node)})}
    else
      {:noreply, state}
    end
  end

  def handle_info(:poll, state) do
    Process.send_after(self(), :poll, @poll_ms)
    # Connect to any compute* name registered with the local EPMD.
    [_, host] = node() |> Atom.to_string() |> String.split("@")

    case :net_adm.names() do
      {:ok, names} ->
        for {name, _port} <- names, String.starts_with?(to_string(name), "compute") do
          Node.connect(:"#{name}@#{host}")
        end

      _ ->
        :ok
    end

    # Refresh the counters of every connected compute node, including ones
    # that connected before we started watching.
    connected = Node.list([:hidden, :visible]) |> Enum.filter(&compute_node?/1)
    state = Enum.reduce(connected, state, &refresh(&2, &1))
    {:noreply, state}
  end

  defp refresh(state, node) do
    try do
      info = :erpc.call(node, Rex.Compute, :info, [], 2_000)
      publish(%{state | nodes: Map.put(state.nodes, node, info)})
    catch
      _, reason ->
        Logger.debug("info from #{node} failed: #{inspect(reason)}")
        state
    end
  end

  defp publish(state) do
    Phoenix.PubSub.broadcast(Dashboard.PubSub, @topic, {:cluster, state.nodes})
    state
  end
end
