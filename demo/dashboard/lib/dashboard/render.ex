defmodule Dashboard.Render do
  @moduledoc """
  One render at a time: a grid of Mandelbrot tiles, fanned out over every
  compute node (or rendered locally in Elixir for comparison).

  Each tile is an `:erpc.call` to `Rex.Compute.tile/1` on a Rust node, run in
  a task so the BEAM side stays responsive. A node going away mid-tile shows
  up as an erpc exception; the tile goes back on the queue and lands on
  another node. Rust reports progress rows to the registered name
  `:render_progress` on this node while it works.

  State changes are broadcast on the `"render"` topic as `{:render, run}`.
  """
  use GenServer
  require Logger

  @topic "render"
  @per_node_default 4

  def start_link(_), do: GenServer.start_link(__MODULE__, [], name: __MODULE__)

  def subscribe, do: Phoenix.PubSub.subscribe(Dashboard.PubSub, @topic)

  def current, do: GenServer.call(__MODULE__, :current)

  @doc """
  Start a render. `opts`: `grid` (tiles per side), `size` (pixels per tile),
  `max_iter`, `target` (`:rust` or `:elixir`), and the window `x0 y0 x1 y1`.
  """
  def start(opts), do: GenServer.call(__MODULE__, {:start, opts})

  def cancel, do: GenServer.call(__MODULE__, :cancel)

  @impl true
  def init(_) do
    # Rust nodes send {:tile_progress, ...} to {:render_progress, node()};
    # a process can carry only one registered name, so a tiny forwarder
    # holds that one and relays to us.
    me = self()
    forwarder = spawn_link(fn -> forward_loop(me) end)
    Process.register(forwarder, :render_progress)
    {:ok, %{run: nil, tasks: %{}}}
  end

  defp forward_loop(to) do
    receive do
      msg -> send(to, msg)
    end

    forward_loop(to)
  end

  @impl true
  def handle_call(:current, _from, state), do: {:reply, state.run, state}

  def handle_call({:start, _opts}, _from, %{run: %{status: :running}} = state) do
    {:reply, {:error, :busy}, state}
  end

  def handle_call({:start, opts}, _from, state) do
    grid = Map.get(opts, :grid, 8)
    size = Map.get(opts, :size, 256)
    max_iter = Map.get(opts, :max_iter, 20_000)
    target = Map.get(opts, :target, :rust)
    {x0, y0, x1, y1} = Map.get(opts, :window, {-2.2, -1.5, 0.8, 1.5})

    tiles =
      for ty <- 0..(grid - 1), tx <- 0..(grid - 1), into: %{} do
        id = ty * grid + tx
        w = (x1 - x0) / grid
        h = (y1 - y0) / grid

        {id,
         %{
           id: id,
           tx: tx,
           ty: ty,
           req: %{id: id, x0: x0 + tx * w, y0: y0 + ty * h, x1: x0 + (tx + 1) * w, y1: y0 + (ty + 1) * h, size: size, max_iter: max_iter},
           status: :pending,
           node: nil,
           pct: 0,
           attempts: 0,
           png: nil,
           ms: nil
         }}
      end

    run = %{
      status: :running,
      target: target,
      grid: grid,
      size: size,
      max_iter: max_iter,
      tiles: tiles,
      started_at: System.monotonic_time(:millisecond),
      finished_at: nil,
      node_stats: %{},
      events: [],
      requeued: 0
    }

    state = %{state | run: run, tasks: %{}} |> log("render started: #{grid}x#{grid} tiles of #{size}px, #{max_iter} iterations, on #{target}")
    {:reply, :ok, schedule(state)}
  end

  def handle_call(:cancel, _from, %{run: nil} = state), do: {:reply, :ok, state}

  def handle_call(:cancel, _from, state) do
    for {ref, {_id, _node, pid}} <- state.tasks do
      Process.demonitor(ref, [:flush])
      Task.Supervisor.terminate_child(Dashboard.TaskSupervisor, pid)
    end

    run = %{state.run | status: :cancelled, finished_at: System.monotonic_time(:millisecond)}
    {:reply, :ok, publish(%{state | run: run, tasks: %{}} |> log("render cancelled"))}
  end

  # A task finished with its result.
  @impl true
  def handle_info({ref, {:ok, id, node, result}}, state) when is_map_key(state.tasks, ref) do
    Process.demonitor(ref, [:flush])
    state = update_in(state.tasks, &Map.delete(&1, ref))

    state =
      update_tile(state, id, fn t ->
        %{t | status: :done, node: node, pct: 100, png: Base.encode64(result.png), ms: result.ms}
      end)
      |> bump_stats(node, result)

    {:noreply, state |> finish_if_done() |> schedule()}
  end

  def handle_info({ref, {:error, id, node, reason}}, state) when is_map_key(state.tasks, ref) do
    Process.demonitor(ref, [:flush])
    state = update_in(state.tasks, &Map.delete(&1, ref))
    {:noreply, requeue(state, id, node, reason) |> schedule()}
  end

  # A task crashed (should not happen; errors are caught) – requeue its tile.
  def handle_info({:DOWN, ref, :process, _pid, reason}, state) when is_map_key(state.tasks, ref) do
    {{id, node, _pid}, tasks} = Map.pop(state.tasks, ref)
    {:noreply, requeue(%{state | tasks: tasks}, id, node, reason) |> schedule()}
  end

  # Progress from a Rust node: {:tile_progress, id, node, pct}.
  def handle_info({:tile_progress, id, node, pct}, %{run: %{status: :running}} = state) do
    state = update_tile(state, id, fn t -> if t.status == :running, do: %{t | pct: pct, node: node}, else: t end)
    {:noreply, publish(state)}
  end

  def handle_info({:tile_progress, _, _, _}, state), do: {:noreply, state}

  def handle_info(_other, state), do: {:noreply, state}

  # ---- scheduling ---------------------------------------------------------

  defp schedule(%{run: %{status: :running}} = state) do
    pending = state.run.tiles |> Map.values() |> Enum.filter(&(&1.status == :pending)) |> Enum.sort_by(& &1.id)
    inflight = Enum.frequencies_by(Map.values(state.tasks), fn {_id, node, _pid} -> node end)
    capacity = capacity(state.run.target, inflight)

    {state, _} =
      Enum.reduce_while(pending, {state, capacity}, fn tile, {state, capacity} ->
        case pick_node(capacity) do
          nil ->
            {:halt, {state, capacity}}

          node ->
            {:cont, {dispatch(state, tile, node), Map.update!(capacity, node, &(&1 - 1))}}
        end
      end)

    publish(state)
  end

  defp schedule(state), do: state

  # Free slots per node: reported cores (Rust) or schedulers (Elixir).
  defp capacity(:elixir, inflight) do
    %{node() => System.schedulers_online() - Map.get(inflight, node(), 0)}
  end

  defp capacity(:rust, inflight) do
    for {node, info} <- Dashboard.Cluster.compute_nodes(), into: %{} do
      cores = Map.get(info, :cores, @per_node_default)
      {node, cores - Map.get(inflight, node, 0)}
    end
  end

  defp pick_node(capacity) do
    case Enum.filter(capacity, fn {_n, free} -> free > 0 end) do
      [] -> nil
      free -> free |> Enum.max_by(fn {_n, free} -> free end) |> elem(0)
    end
  end

  defp dispatch(state, tile, node) do
    req = tile.req
    target = state.run.target
    me = self()

    task =
      Task.Supervisor.async_nolink(Dashboard.TaskSupervisor, fn ->
        try do
          result =
            case target do
              :rust ->
                :erpc.call(node, Rex.Compute, :tile, [req], 120_000)

              :elixir ->
                size = req.size
                Dashboard.Mandelbrot.tile(req, fn row ->
                  if rem(row, max(div(size, 10), 1)) == 0 or row == size,
                    do: send(me, {:tile_progress, req.id, node(), div(row * 100, size)})
                end)
            end

          {:ok, req.id, node, result}
        catch
          kind, reason -> {:error, req.id, node, {kind, reason}}
        end
      end)

    state
    |> put_in([:tasks, task.ref], {tile.id, node, task.pid})
    |> update_tile(tile.id, fn t -> %{t | status: :running, node: node, pct: 0, attempts: t.attempts + 1} end)
  end

  defp requeue(state, id, node, reason) do
    state
    |> update_tile(id, fn t -> %{t | status: :pending, node: nil, pct: 0} end)
    |> update_in([:run, :requeued], &(&1 + 1))
    |> log("tile #{id} on #{node} failed (#{short(reason)}), requeued")
  end

  defp finish_if_done(state) do
    if Enum.all?(Map.values(state.run.tiles), &(&1.status == :done)) do
      run = %{state.run | status: :done, finished_at: System.monotonic_time(:millisecond)}
      ms = run.finished_at - run.started_at
      log(%{state | run: run}, "render done in #{ms} ms")
    else
      state
    end
  end

  defp bump_stats(state, node, result) do
    update_in(state, [:run, :node_stats], fn stats ->
      Map.update(stats, node, %{tiles: 1, ms: result.ms, iterations: result.iterations}, fn s ->
        %{s | tiles: s.tiles + 1, ms: s.ms + result.ms, iterations: s.iterations + result.iterations}
      end)
    end)
  end

  defp update_tile(state, id, fun), do: update_in(state, [:run, :tiles, id], fun)

  defp log(state, msg) do
    Logger.info(msg)
    stamp = Calendar.strftime(DateTime.utc_now(), "%H:%M:%S")
    update_in(state, [:run, :events], &Enum.take([{stamp, msg} | &1], 30))
  end

  defp publish(state) do
    Phoenix.PubSub.broadcast(Dashboard.PubSub, @topic, {:render, state.run})
    state
  end

  defp short({:exit, {:exception, reason, _}}), do: inspect(reason)
  defp short({:error, {:erpc, reason}}), do: inspect(reason)
  defp short({:error, %{__exception__: true} = e}), do: Exception.message(e)
  defp short(other), do: other |> inspect() |> String.slice(0, 80)
end
