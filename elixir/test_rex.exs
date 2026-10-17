# Exercise a running rex node from Elixir.
#   elixir --sname app --cookie rexcookie elixir/test_rex.exs rex@desktop
rex = case System.argv() do
  [n | _] -> String.to_atom(n)
  _ -> :"rex@#{String.trim(elem(System.cmd("hostname", []), 0))}"
end

defmodule T do
  def check(label, fun) do
    result = try do
      {:ok, fun.()}
    rescue
      e -> {:raised, e}
    catch
      kind, value -> {kind, value}
    end
    IO.puts("#{label}: #{inspect(result, limit: :infinity, printable_limit: 200)}")
    result
  end
end

T.check("connect", fn -> Node.connect(rex) end)
T.check("ping", fn -> Node.ping(rex) end)
T.check("hidden nodes", fn -> Node.list(:hidden) end)
T.check("erpc add", fn -> :erpc.call(rex, Rex.Math, :add, [1, 2]) end)
T.check("erpc sum mixed", fn -> :erpc.call(rex, Rex.Math, :sum, [[1, 2, 3.5]]) end)
T.check("erpc big int", fn -> :erpc.call(rex, Rex.Math, :mul, [4_000_000_000, 3]) end)
T.check("rpc fib", fn -> :rpc.call(rex, Rex.Math, :fib, [50]) end)
T.check("block_call upcase", fn -> :rpc.block_call(rex, Rex.Strings, :upcase, ["hello"]) end)
T.check("word_count", fn -> :erpc.call(rex, Rex.Strings, :word_count, ["a b a"]) end)
T.check("charlist reverse", fn -> :erpc.call(rex, Rex.Strings, :reverse, [~c"abc"]) end)
T.check("rex modules", fn -> :erpc.call(rex, :rex, :modules, []) end)
T.check("rex functions", fn -> :erpc.call(rex, :rex, :functions, [Rex.Math]) end)
T.check("erlang node", fn -> :rpc.call(rex, :erlang, :node, []) end)
T.check("erlang nodes", fn -> :rpc.call(rex, :erlang, :nodes, []) end)
T.check("kv put", fn -> :erpc.call(rex, Rex.KV, :put, [:k, %{"b" => [1, 2.5, <<1, 2>>], a: 1}]) end)
T.check("kv get", fn -> :erpc.call(rex, Rex.KV, :get, [:k]) end)
T.check("kv get missing", fn -> :erpc.call(rex, Rex.KV, :get, [:missing]) end)
T.check("peer_nodes (rust -> elixir rpc)", fn -> :erpc.call(rex, Rex.Cluster, :peer_nodes, []) end)
T.check("greet (prints via group leader)", fn -> :erpc.call(rex, Rex.Cluster, :greet, ["elixir"]) end)
Process.sleep(100)

T.check("divide by zero", fn -> :erpc.call(rex, Rex.Math, :divide, [1, 0]) end)
T.check("badarg message", fn -> :erpc.call(rex, Rex.Math, :fib, [-1]) end)
T.check("undef function", fn -> :erpc.call(rex, Rex.Math, :nope, []) end)
T.check("undef module", fn -> :erpc.call(rex, Nope, :nope, []) end)
T.check("rpc undef", fn -> :rpc.call(rex, Nope, :nope, []) end)
T.check("erpc timeout", fn -> :erpc.call(rex, Rex.Cluster, :sleep, [2000], 300) end)
T.check("multicall", fn -> :erpc.multicall([rex, node()], :erlang, :node, []) end)

T.check("erpc cast", fn -> :erpc.cast(rex, Rex.KV, :put, [:cast_key, 1]) end)
T.check("rpc cast", fn -> :rpc.cast(rex, Rex.KV, :put, [:rpc_cast_key, 2]) end)
Process.sleep(200)
T.check("kv all", fn -> :erpc.call(rex, Rex.KV, :all, []) end)

T.check("Node.spawn + monitor", fn ->
  pid = Node.spawn(rex, Rex.Cluster, :sleep, [100])
  ref = Process.monitor(pid)
  receive do
    {:DOWN, ^ref, :process, ^pid, reason} -> {pid, reason}
  after
    2000 -> :no_down
  end
end)

T.check("Node.spawn_link crash", fn ->
  Process.flag(:trap_exit, true)
  pid = Node.spawn_link(rex, Rex.Math, :divide, [1, 0])
  receive do
    {:EXIT, ^pid, reason} -> reason
  after
    2000 -> :no_exit
  end
end)

T.check("send to registered name + echo", fn ->
  send({:rex_inbox, rex}, {self(), :hello})
  receive do
    {:echo, x} -> x
  after
    2000 -> :no_echo
  end
end)

T.check("monitor missing name", fn ->
  ref = :erlang.monitor(:process, {:nonexistent, rex})
  receive do
    {:DOWN, ^ref, :process, obj, reason} -> {obj, reason}
  after
    2000 -> :no_down
  end
end)

T.check("GenServer.call to missing name", fn -> GenServer.call({:nonexistent, rex}, :hi, 1000) end)
T.check("send_request/receive_response", fn ->
  req = :erpc.send_request(rex, Rex.Math, :add, [40, 2])
  :erpc.receive_response(req, 2000)
end)
T.check("disconnect", fn -> Node.disconnect(rex) end)
Process.sleep(200)
T.check("reconnect via rpc autoconnect", fn -> :rpc.call(rex, :rex, :ping, []) end)
T.check("hidden nodes after", fn -> Node.list(:hidden) end)
IO.puts("done")
