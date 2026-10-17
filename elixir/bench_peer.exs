# Elixir baseline peer: the same surface as examples/bench_node.rs, in Elixir.
#   elixir --sname base --cookie rexcookie elixir/bench_peer.exs
defmodule Rex.Bench do
  def echo(x), do: x
  def sum(list), do: Enum.sum(list)
  def size(bin), do: byte_size(bin)
end

defmodule Peer do
  def echo_loop do
    receive do
      {:"$gen_call", from, req} -> GenServer.reply(from, req)
      {from, ref, payload} -> send(from, {ref, payload})
      _ -> :ok
    end
    echo_loop()
  end

  def sink_loop(count, bytes) do
    receive do
      {:msg, bin} when is_binary(bin) -> sink_loop(count + 1, bytes + byte_size(bin))
      {:msg, _} -> sink_loop(count + 1, bytes)
      {:"$gen_call", from, :flush} -> GenServer.reply(from, {count, bytes}); sink_loop(0, 0)
      {from, ref, :flush} when is_pid(from) -> send(from, {ref, count, bytes}); sink_loop(0, 0)
      _ -> sink_loop(count, bytes)
    end
  end
end

Process.register(spawn(&Peer.echo_loop/0), :rex_bench_echo)
Process.register(spawn(fn -> Peer.sink_loop(0, 0) end), :rex_bench_sink)
IO.puts("bench peer #{node()} ready")
Process.sleep(:infinity)
