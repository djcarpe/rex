# Elixir side of the rex k3s demo: libcluster Kubernetes.DNS over the headless
# service `rex-demo`, then keep calling into whatever rex nodes show up.
Mix.install([{:libcluster, "~> 3.5"}])

topologies = [
  k8s: [
    strategy: Cluster.Strategy.Kubernetes.DNS,
    config: [service: "rex-demo", application_name: "app", polling_interval: 3_000]
  ]
]

{:ok, _} = Cluster.Supervisor.start_link([topologies, [name: Demo.ClusterSupervisor]])
IO.puts("[#{node()}] libcluster Kubernetes.DNS started")

defmodule Demo do
  def loop(seen) do
    hidden = Node.list(:hidden)
    for rex <- hidden do
      add = :erpc.call(rex, Rex.Math, :add, [40, 2])
      mods = :erpc.call(rex, :rex, :modules, [])
      wc = :erpc.call(rex, Rex.Strings, :word_count, ["rex in k3s rex works"])
      rexs_view = :erpc.call(rex, :erlang, :nodes, [])
      IO.puts("[#{node()}] rex=#{rex} add=#{inspect(add)} word_count=#{inspect(wc)} modules=#{inspect(mods)} rex_sees=#{inspect(rexs_view)}")
      unless rex in seen, do: :erpc.call(rex, Rex.Cluster, :greet, [Atom.to_string(node())])
    end
    IO.puts("[#{node()}] visible=#{inspect(Node.list())} hidden=#{inspect(hidden)}")
    Process.sleep(10_000)
    loop(Enum.uniq(seen ++ hidden))
  end
end

Demo.loop([])
