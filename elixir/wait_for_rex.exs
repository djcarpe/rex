# Wait for a rex node to connect to *us* (rex initiates), then use it.
#   elixir --sname app --cookie rexcookie elixir/wait_for_rex.exs [visible]
visible = System.argv() == ["visible"]
:net_kernel.monitor_nodes(true, node_type: :all)
IO.puts("waiting on #{node()} for a rex node (visible=#{visible})")
receive do
  {:nodeup, n, info} ->
    IO.puts("nodeup #{inspect(n)} #{inspect(info)}")
    IO.puts("Node.list(): #{inspect(Node.list())}  hidden: #{inspect(Node.list(:hidden))}")
    IO.puts("erpc: #{inspect(:erpc.call(n, Rex.Math, :add, [1, 1]))}")
    IO.puts("ping: #{inspect(Node.ping(n))}")
    if visible do
      t = Task.async(fn -> :global.sync() end)
      IO.puts("global.sync: #{inspect(Task.yield(t, 3000) || Task.shutdown(t))}")
      IO.puts("global register: #{inspect(:global.register_name(:gtest, self()))}")
    end
    # rex calling back into us: Rex.Cluster.peer_nodes asks our node for erlang:nodes()
    IO.puts("peer_nodes: #{inspect(:erpc.call(n, Rex.Cluster, :peer_nodes, []))}")
after
  30_000 -> IO.puts("FAIL: rex never connected")
end
