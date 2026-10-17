# Start libcluster's Gossip strategy and wait for a rex node to appear.
#   elixir --sname app --cookie rexcookie elixir/gossip_test.exs [addr broadcast_only [secret]]
Mix.install([{:libcluster, "~> 3.5"}])
# args: [multicast_addr] [broadcast_only true|false] [secret]
{addr, bcast, secret} = case System.argv() do
  [a, b | rest] -> {a, b == "true", List.first(rest)}
  _ -> {"233.252.1.32", false, nil}
end
config = [multicast_addr: addr, broadcast_only: bcast] ++ if(secret, do: [secret: secret], else: [])
topologies = [gossip: [strategy: Cluster.Strategy.Gossip, config: config]]
{:ok, _} = Cluster.Supervisor.start_link([topologies, [name: Test.ClusterSupervisor]])
IO.puts("gossip started on #{node()} secret=#{inspect(secret)}")

wait = fn wait, n ->
  case Node.list(:hidden) ++ Node.list() do
    [] when n > 0 -> Process.sleep(500); wait.(wait, n - 1)
    nodes -> nodes
  end
end

case wait.(wait, 60) do
  [] -> IO.puts("FAIL: no rex node discovered in 30s")
  [rex | _] = nodes ->
    IO.puts("discovered: #{inspect(nodes)}")
    IO.puts("erpc: #{inspect(:erpc.call(rex, Rex.Math, :add, [20, 22]))}")
    IO.puts("rex sees: #{inspect(:erpc.call(rex, :erlang, :nodes, []))}")
end
