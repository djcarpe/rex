defmodule DashboardWeb.RenderLive do
  @moduledoc """
  The demo page: compute nodes on top, a render form, the tile grid filling
  in as Rust nodes answer, per-node throughput, and an event log that shows
  tiles being re-queued when a node is killed.
  """
  use DashboardWeb, :live_view

  alias Dashboard.{Cluster, Render}

  @colors ~w(#f97316 #22c55e #3b82f6 #a855f7 #ec4899 #14b8a6 #eab308 #ef4444)

  @impl true
  def mount(_params, _session, socket) do
    if connected?(socket) do
      Cluster.subscribe()
      Render.subscribe()
      :timer.send_interval(500, :tick)
    end

    {:ok,
     socket
     |> assign(nodes: Cluster.compute_nodes(), run: Render.current(), now: now())
     |> assign(form: to_form(%{"grid" => "8", "size" => "256", "max_iter" => "20000", "target" => "rust"}))}
  end

  @impl true
  def handle_event("start", params, socket) do
    opts = %{
      grid: int(params["grid"], 8, 1, 16),
      size: int(params["size"], 256, 16, 1024),
      max_iter: int(params["max_iter"], 20_000, 10, 100_000),
      target: if(params["target"] == "elixir", do: :elixir, else: :rust)
    }

    socket = assign(socket, form: to_form(params))

    case Render.start(opts) do
      :ok -> {:noreply, socket}
      {:error, :busy} -> {:noreply, put_flash(socket, :error, "A render is already running")}
    end
  end

  def handle_event("cancel", _params, socket) do
    Render.cancel()
    {:noreply, socket}
  end

  @impl true
  def handle_info({:cluster, nodes}, socket), do: {:noreply, assign(socket, nodes: nodes)}
  def handle_info({:render, run}, socket), do: {:noreply, assign(socket, run: run, now: now())}
  def handle_info(:tick, socket), do: {:noreply, assign(socket, now: now())}

  defp now, do: System.monotonic_time(:millisecond)

  defp int(v, default, lo, hi) do
    case Integer.parse(to_string(v)) do
      {n, _} -> n |> max(lo) |> min(hi)
      _ -> default
    end
  end

  # ---- view helpers -------------------------------------------------------

  defp color(nil, _), do: "#52525b"

  defp color(node, nodes) do
    names = nodes |> Map.keys() |> Enum.sort()
    idx = Enum.find_index(names, &(&1 == node)) || rem(:erlang.phash2(node), length(@colors))
    Enum.at(@colors, rem(idx, length(@colors)))
  end

  defp elapsed_ms(nil, _now), do: 0
  defp elapsed_ms(%{started_at: t0, finished_at: nil}, now), do: max(now - t0, 1)
  defp elapsed_ms(%{started_at: t0, finished_at: t1}, _now), do: max(t1 - t0, 1)

  defp done(run), do: run.tiles |> Map.values() |> Enum.count(&(&1.status == :done))
  defp running(run), do: run.tiles |> Map.values() |> Enum.count(&(&1.status == :running))

  defp iterations(run), do: run.node_stats |> Map.values() |> Enum.map(& &1.iterations) |> Enum.sum()

  defp rate(count, ms), do: Float.round(count * 1000 / max(ms, 1), 1)

  defp human(n) when n >= 1_000_000_000, do: "#{Float.round(n / 1_000_000_000, 2)} G"
  defp human(n) when n >= 1_000_000, do: "#{Float.round(n / 1_000_000, 1)} M"
  defp human(n) when n >= 1_000, do: "#{Float.round(n / 1_000, 1)} k"
  defp human(n), do: "#{n}"

  defp short_node(node), do: node |> Atom.to_string() |> String.split("@") |> hd()

  @impl true
  def render(assigns) do
    ~H"""
    <div class="min-h-screen px-6 py-6 space-y-6">
      <header class="flex items-baseline justify-between">
        <div>
          <h1 class="text-2xl font-bold">rex demo: Rust compute nodes in a BEAM cluster</h1>
          <p class="text-sm opacity-70">
            This page is Phoenix LiveView on <code>{node()}</code>. Every tile below is rendered by a plain Rust process
            that joined the cluster through <code>rex</code> and answers <code>:erpc.call</code> like any Erlang node.
          </p>
        </div>
      </header>

      <section>
        <h2 class="text-lg font-semibold mb-2">Compute nodes</h2>
        <div :if={@nodes == %{}} class="alert alert-warning">
          No compute nodes connected. Start one with
          <code>cd demo/compute && cargo run --release -- compute1@$(hostname) --cookie rexcookie</code>
        </div>
        <div class="grid grid-cols-1 md:grid-cols-3 gap-3">
          <div :for={{node, info} <- Enum.sort(@nodes)} class="card bg-base-200 shadow" style={"border-left: 6px solid #{color(node, @nodes)}"}>
            <div class="card-body py-3 px-4">
              <div class="flex justify-between items-baseline">
                <span class="font-mono font-semibold">{node}</span>
                <span class="badge badge-success">up</span>
              </div>
              <div class="text-sm grid grid-cols-2 gap-x-3">
                <span>cores</span><span class="text-right">{info[:cores]}</span>
                <span>tiles served</span><span class="text-right">{info[:tiles]}</span>
                <span>in flight</span><span class="text-right">{info[:busy]}</span>
                <span>iterations</span><span class="text-right">{human(info[:iterations] || 0)}</span>
                <%= if @run && @run.node_stats[node] do %>
                  <span>this render</span>
                  <span class="text-right">
                    {@run.node_stats[node].tiles} tiles, {rate(@run.node_stats[node].tiles, elapsed_ms(@run, @now))}/s
                  </span>
                <% end %>
              </div>
            </div>
          </div>
        </div>
      </section>

      <section class="card bg-base-200 shadow">
        <div class="card-body py-4 px-5">
          <.form for={@form} phx-submit="start" class="flex flex-wrap items-end gap-4">
            <label class="form-control">
              <span class="label-text">grid (tiles per side)</span>
              <input type="number" name="grid" value={@form[:grid].value} min="1" max="16" class="input input-bordered w-28" />
            </label>
            <label class="form-control">
              <span class="label-text">tile size (px)</span>
              <input type="number" name="size" value={@form[:size].value} min="16" max="1024" step="16" class="input input-bordered w-28" />
            </label>
            <label class="form-control">
              <span class="label-text">max iterations</span>
              <input type="number" name="max_iter" value={@form[:max_iter].value} min="10" max="100000" step="10" class="input input-bordered w-32" />
            </label>
            <label class="form-control">
              <span class="label-text">render on</span>
              <select name="target" class="select select-bordered w-44">
                <option value="rust" selected={@form[:target].value == "rust"}>Rust nodes (rex)</option>
                <option value="elixir" selected={@form[:target].value == "elixir"}>this node, in Elixir</option>
              </select>
            </label>
            <button type="submit" class="btn btn-primary" disabled={@run && @run.status == :running}>Render</button>
            <button type="button" phx-click="cancel" class="btn btn-ghost" disabled={!(@run && @run.status == :running)}>Cancel</button>
          </.form>
        </div>
      </section>

      <%= if @run do %>
        <section class="grid grid-cols-2 md:grid-cols-6 gap-3">
          <div class="stat bg-base-200 rounded-box py-2">
            <div class="stat-title">status</div>
            <div class="stat-value text-lg">{@run.status}</div>
            <div class="stat-desc">on {@run.target}</div>
          </div>
          <div class="stat bg-base-200 rounded-box py-2">
            <div class="stat-title">tiles</div>
            <div class="stat-value text-lg">{done(@run)} / {map_size(@run.tiles)}</div>
            <div class="stat-desc">{running(@run)} in flight, {@run.requeued} requeued</div>
          </div>
          <div class="stat bg-base-200 rounded-box py-2">
            <div class="stat-title">elapsed</div>
            <div class="stat-value text-lg">{Float.round(elapsed_ms(@run, @now) / 1000, 1)} s</div>
          </div>
          <div class="stat bg-base-200 rounded-box py-2">
            <div class="stat-title">tiles / s</div>
            <div class="stat-value text-lg">{rate(done(@run), elapsed_ms(@run, @now))}</div>
          </div>
          <div class="stat bg-base-200 rounded-box py-2">
            <div class="stat-title">pixels / s</div>
            <div class="stat-value text-lg">{human(trunc(done(@run) * @run.size * @run.size * 1000 / max(elapsed_ms(@run, @now), 1)))}</div>
          </div>
          <div class="stat bg-base-200 rounded-box py-2">
            <div class="stat-title">iterations / s</div>
            <div class="stat-value text-lg">{human(trunc(iterations(@run) * 1000 / max(elapsed_ms(@run, @now), 1)))}</div>
          </div>
        </section>

        <section class="grid grid-cols-1 lg:grid-cols-[2fr_1fr] gap-6">
          <div>
            <div class="grid gap-[2px] bg-base-300 p-[2px]" style={"grid-template-columns: repeat(#{@run.grid}, minmax(0, 1fr));"}>
              <div
                :for={tile <- @run.tiles |> Map.values() |> Enum.sort_by(& &1.id)}
                class="relative aspect-square overflow-hidden bg-base-100"
                style={"box-shadow: inset 0 0 0 3px #{color(tile.node, @nodes)}"}
                title={"tile #{tile.id} · #{tile.status} · #{tile.node || "-"} · #{tile.ms && Float.round(tile.ms, 1)} ms"}
              >
                <img :if={tile.png} src={"data:image/png;base64,#{tile.png}"} class="w-full h-full block" />
                <div :if={tile.status == :running} class="absolute inset-x-0 bottom-0 bg-base-100/60">
                  <div class="h-2" style={"width: #{tile.pct}%; background: #{color(tile.node, @nodes)}"}></div>
                </div>
                <span :if={tile.status != :done} class="absolute top-1 left-1 text-[10px] font-mono opacity-60">
                  {tile.id} {if tile.status == :running, do: "#{short_node(tile.node)} #{tile.pct}%", else: "queued"}
                </span>
              </div>
            </div>
          </div>
          <div class="space-y-3">
            <h3 class="font-semibold">Events</h3>
            <ul class="text-xs font-mono space-y-1 max-h-96 overflow-auto">
              <li :for={{stamp, msg} <- @run.events}><span class="opacity-50">{stamp}</span> {msg}</li>
            </ul>
            <p class="text-sm opacity-70">
              Try it: kill a compute node with Ctrl-C mid-render. Its tiles are re-queued and land on the others;
              start it again and it is back in the pool on the next poll.
            </p>
          </div>
        </section>
      <% end %>
    </div>
    """
  end
end
