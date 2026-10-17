# Drive the benchmarks from Elixir against a rex node and an Elixir baseline.
#   elixir --sname driver --cookie rexcookie elixir/bench.exs rexbench@host base@host
[rex, base] = Enum.map(System.argv(), &String.to_atom/1)
true = Node.connect(rex)
true = Node.connect(base)

defmodule B do
  def fmt_bytes(n) when n >= 1_048_576, do: "#{div(n, 1_048_576)} MiB"
  def fmt_bytes(n) when n >= 1024, do: "#{div(n, 1024)} KiB"
  def fmt_bytes(n), do: "#{n} B"

  def pct(sorted, p) do
    idx = round((length(sorted) - 1) * p)
    Enum.at(sorted, idx)
  end

  # Run `fun` n times sequentially; return {elapsed_s, sorted latencies in µs}.
  def seq(n, fun) do
    t0 = System.monotonic_time(:microsecond)
    lats = for _ <- 1..n do
      t = System.monotonic_time(:microsecond)
      fun.()
      System.monotonic_time(:microsecond) - t
    end
    {(System.monotonic_time(:microsecond) - t0) / 1_000_000, Enum.sort(lats)}
  end

  # Run n calls spread over `conc` concurrent processes; return elapsed_s.
  def conc(n, conc, fun) do
    per = div(n, conc)
    t0 = System.monotonic_time(:microsecond)
    1..conc
    |> Enum.map(fn _ -> Task.async(fn -> for _ <- 1..per, do: fun.() end) end)
    |> Enum.each(&Task.await(&1, :infinity))
    (System.monotonic_time(:microsecond) - t0) / 1_000_000
  end

  def row(label, size, n, elapsed, bytes_moved, lats \\ nil) do
    rate = n / elapsed
    mbs = if bytes_moved > 0, do: :erlang.float_to_binary(bytes_moved / elapsed / 1_000_000, decimals: 1), else: "-"
    {p50, p99} = if lats, do: {"#{pct(lats, 0.5)}", "#{pct(lats, 0.99)}"}, else: {"-", "-"}
    IO.puts("| #{label} | #{size} | #{n} | #{round(rate)} | #{mbs} | #{p50} | #{p99} |")
  end

  def echo_msg(target, payload) do
    ref = make_ref()
    send({:rex_bench_echo, target}, {self(), ref, payload})
    receive do
      {^ref, ^payload} -> :ok
    after 60_000 -> raise "echo timeout"
    end
  end

  def flood(target, payload, n) do
    for _ <- 1..n, do: send({:rex_bench_sink, target}, {:msg, payload})
    ref = make_ref()
    send({:rex_bench_sink, target}, {self(), ref, :flush})
    receive do
      {^ref, count, _bytes} -> count
    after 120_000 -> raise "flush timeout"
    end
  end
end

sizes_seq = [{0, 3000}, {1024, 3000}, {65_536, 1000}, {1_048_576, 100}, {8 * 1_048_576, 10}]
structured = Enum.map(1..10_000, fn i -> {i, "item-#{i}", %{id: i, ok: true}} end)
ints = Enum.to_list(1..100_000)

for {label, target} <- [{"rex (Rust)", rex}, {"Elixir baseline", base}] do
  IO.puts("\n## Elixir -> #{label} (#{node()} -> #{target})\n")
  IO.puts("| benchmark | size | n | calls/s | MB/s | p50 µs | p99 µs |")
  IO.puts("|---|---|---|---|---|---|---|")

  for {size, n} <- sizes_seq do
    payload = :binary.copy(<<0xAB>>, size)
    {el, lats} = B.seq(n, fn -> ^payload = :erpc.call(target, Rex.Bench, :echo, [payload]) end)
    B.row("erpc echo, 1 caller", B.fmt_bytes(size), n, el, 2 * size * n, lats)
  end
  for {size, n} <- [{1024, 20_000}, {65_536, 2_000}] do
    payload = :binary.copy(<<0xAB>>, size)
    el = B.conc(n, 32, fn -> ^payload = :erpc.call(target, Rex.Bench, :echo, [payload]) end)
    B.row("erpc echo, 32 callers", B.fmt_bytes(size), n, el, 2 * size * n)
  end
  {el, lats} = B.seq(200, fn -> ^structured = :erpc.call(target, Rex.Bench, :echo, [structured]) end)
  B.row("erpc echo 10k tuples+maps", "#{byte_size(:erlang.term_to_binary(structured))} B wire", 200, el, 2 * byte_size(:erlang.term_to_binary(structured)) * 200, lats)
  {el, lats} = B.seq(200, fn -> 5_000_050_000 = :erpc.call(target, Rex.Bench, :sum, [ints]) end)
  B.row("erpc sum 100k ints (decode only)", "#{byte_size(:erlang.term_to_binary(ints))} B wire", 200, el, byte_size(:erlang.term_to_binary(ints)) * 200, lats)

  for {size, n} <- sizes_seq do
    payload = :binary.copy(<<0xCD>>, size)
    {el, lats} = B.seq(n, fn -> B.echo_msg(target, payload) end)
    B.row("raw message echo", B.fmt_bytes(size), n, el, 2 * size * n, lats)
  end
  for {size, n} <- [{0, 300_000}, {1024, 200_000}, {65_536, 10_000}, {1_048_576, 300}] do
    payload = :binary.copy(<<0xEF>>, size)
    t0 = System.monotonic_time(:microsecond)
    ^n = B.flood(target, payload, n)
    el = (System.monotonic_time(:microsecond) - t0) / 1_000_000
    B.row("one-way flood + flush", B.fmt_bytes(size), n, el, size * n)
  end
end
IO.puts("")
