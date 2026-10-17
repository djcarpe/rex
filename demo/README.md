# rex demo: Rust compute nodes in a BEAM cluster

![dashboard](dashboard.png)

Two pieces:

- **`compute/`** — a Rust binary that joins the cluster with rex as
  `compute<N>@host` and serves `Rex.Compute.tile/1`: it renders a Mandelbrot
  tile to PNG on a blocking thread pool (one node serves many callers at once)
  and pushes progress back to the caller while it works. No NIF, no port, no
  Erlang in the binary.
- **`dashboard/`** — a Phoenix LiveView app. It discovers compute nodes, fans a
  render out over all of them with `:erpc.call`, shows tiles as they land with
  per-node throughput, streams the Rust-side progress, and re-queues tiles when
  a node dies mid-render. A "render in Elixir" option runs the same loop on
  the BEAM for comparison.

What it shows, in order:

1. **Discovery.** Start a compute node and it appears on the page: the Rust
   side runs `rex::cluster::LocalEpmd` (the libcluster strategy), the Elixir
   side gets `nodeup` from `:net_kernel.monitor_nodes/2`, and polls EPMD too.
2. **Throughput.** Render 8×8 tiles of 256 px at 20 000 iterations. Each tile
   is an `:erpc.call` into Rust; every node reports cores, tiles served and
   iterations. Switch "render on" to Elixir with a smaller grid to feel the
   difference.
3. **Progress from Rust.** While a tile renders, the Rust node sends
   `{:tile_progress, id, node, pct}` to `{:render_progress, dashboard_node}`
   and the bar under the tile moves.
4. **Failure.** Kill a compute node (Ctrl-C) mid-render. Its in-flight tiles
   come back as `:noconnection` exceptions, go back on the queue, and land on
   the remaining nodes. Start it again and it is back in the pool within two
   seconds. The BEAM's supervision story, with Rust doing the work.
5. **Hot reload.** Edit the LiveView while a render runs; Phoenix reloads it,
   the Rust nodes never notice.

## Run it

```sh
# 1. two compute nodes (any number works; names must start with "compute")
cd demo/compute
cargo run --release -- compute1@$(hostname) --cookie rexcookie &
cargo run --release -- compute2@$(hostname) --cookie rexcookie &

# 2. the dashboard, as a distributed node with the same cookie
cd ../dashboard
mix setup            # first time only
elixir --sname dashboard --cookie rexcookie -S mix phx.server
```

Open <http://localhost:4000>, press **Render**, then kill one of the compute
nodes while it runs.

Everything is on one host here, so the two compute nodes share its cores. On
separate machines (or k3s pods, see the top-level README) each node brings
its own; the dashboard sizes the in-flight work per node from the `cores`
each one reports.

## How the pieces talk

| | |
|---|---|
| dashboard → Rust | `:erpc.call(node, Rex.Compute, :tile, [%{id, x0, y0, x1, y1, size, max_iter}])` — a `spawn_request` on the wire, a tokio task on the Rust side |
| Rust → dashboard | `node.send_named("render_progress", caller_node, {:tile_progress, ...})` from inside the handler |
| node info | `:erpc.call(node, Rex.Compute, :info, [])` every 2 s |
| node death | `{:nodedown, node, _}` on the dashboard; `{:erpc, :noconnection}` for the tiles in flight |
