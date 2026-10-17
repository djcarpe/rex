//! A demo node exposing a few native modules.
//!
//! ```text
//! cargo run --example rex_node -- rex@desktop [--cookie C] [--visible] [--poll-interval SECS] [--no-epmd-spawn]
//!   discovery (any number of these):
//!     --connect a@h,b@h                  static list (Cluster.Strategy.Epmd)
//!     --local-epmd                       everything on the local EPMD
//!     --erlang-hosts [PATH]              .hosts.erlang file
//!     --gossip [--gossip-secret S] [--gossip-addr A] [--gossip-port P] [--gossip-broadcast]
//!     --dns-poll QUERY BASENAME
//!     --k8s-dns SERVICE BASENAME [--k8s-ns NS]
//!     --k8s-dns-srv SERVICE NAMESPACE BASENAME
//!     --k8s BASENAME SELECTOR [--k8s-ns NS] [--k8s-api URL] [--k8s-mode ip|dns|hostname]
//!           [--k8s-lookup endpoints|pods] [--k8s-service NAME] [--k8s-insecure] [--k8s-token T]
//!     --rancher BASENAME [--rancher-url URL]
//!     --nebula-lighthouse LH1,LH2 HOST1,HOST2 BASENAME | --nebula-dns HOST1,HOST2 BASENAME
//!     --nebula-static CONFIG.yml BASENAME   with [--cidr RANGE]... [--nebula-naming ip|name]
//!     --nameserver IP[:PORT][/tcp]       custom resolver for the DNS strategies
//!     --poll-static a@h,b@h              the generic Poll strategy with a fixed answer
//! ```
//! Then from `iex --sname app --cookie C`:
//! ```elixir
//! Node.connect(:"rex@desktop")
//! :erpc.call(:"rex@desktop", Rex.Math, :add, [1, 2])
//! :erpc.call(:"rex@desktop", Rex.KV, :put, [:k, "v"])
//! :erpc.call(:"rex@desktop", :rex, :modules, [])
//! send({:rex_inbox, :"rex@desktop"}, {self(), :hello}); flush()
//! ```

use rex::cluster::{
    DnsPoll, ErlangHosts, Gossip, Hosts, IpLookupMode, Kubernetes, KubernetesDns, KubernetesDnsSrv,
    LocalEpmd, Nebula, NebulaDiscovery, NebulaNaming, NodeNameMode, Poll, Rancher, Resolver,
};
use rex::{Exception, FnModule, Node, Signal, Term};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

fn int_arg(args: &[Term], i: usize) -> Result<i64, Exception> {
    args.get(i)
        .and_then(|t| t.as_int())
        .ok_or_else(Exception::badarg)
}

#[tokio::main]
async fn main() -> rex::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "rex=info".parse().unwrap()),
        )
        .init();

    let mut args = std::env::args().skip(1).peekable();
    let mut name = None;
    let mut cookie = std::env::var("REX_COOKIE").ok();
    let mut gossip = false;
    let mut gossip_cfg = Gossip::new();
    let mut visible = false;
    let mut connect: Vec<String> = Vec::new();
    let mut poll_interval = Duration::from_secs(5);
    let mut spawn_epmd = true;
    let mut local_epmd = false;
    let mut erlang_hosts: Option<Option<String>> = None;
    let mut dns_poll: Option<(String, String)> = None;
    let mut k8s_dns: Option<(String, String)> = None;
    let mut k8s_dns_srv: Option<(String, String, String)> = None;
    let mut k8s: Option<(String, String)> = None;
    let mut k8s_ns: Option<String> = None;
    let mut k8s_api: Option<String> = None;
    let mut k8s_mode = NodeNameMode::Ip;
    let mut k8s_lookup = IpLookupMode::Endpoints;
    let mut k8s_service: Option<String> = None;
    let mut k8s_insecure = false;
    let mut k8s_token: Option<String> = None;
    let mut rancher: Option<String> = None;
    let mut rancher_url: Option<String> = None;
    let mut nebula: Option<(NebulaDiscovery, String)> = None;
    let mut cidrs: Vec<String> = Vec::new();
    let mut nebula_naming = NebulaNaming::Ip;
    let mut nameservers: Vec<String> = Vec::new();
    let mut poll_static: Vec<String> = Vec::new();
    let split = |s: String| -> Vec<String> {
        s.split(',')
            .filter(|x| !x.is_empty())
            .map(str::to_string)
            .collect()
    };
    let servers = |s: String| -> Vec<std::net::SocketAddr> {
        s.split(',')
            .filter(|x| !x.is_empty())
            .map(|x| Resolver::parse_server(x).expect("nameserver").0)
            .collect()
    };
    while let Some(a) = args.next() {
        let mut next = || args.next().expect("missing argument");
        match a.as_str() {
            "--poll-interval" => {
                poll_interval = Duration::from_secs(next().parse().expect("seconds"))
            }
            "--local-epmd" => local_epmd = true,
            "--no-epmd-spawn" => spawn_epmd = false,
            "--erlang-hosts" => {
                erlang_hosts = Some(None);
                // optional path argument
                if args.peek().is_some_and(|p| !p.starts_with("--")) {
                    erlang_hosts = Some(args.next());
                }
            }
            "--dns-poll" => dns_poll = Some((next(), next())),
            "--k8s-dns" => k8s_dns = Some((next(), next())),
            "--k8s-dns-srv" => k8s_dns_srv = Some((next(), next(), next())),
            "--k8s" => k8s = Some((next(), next())),
            "--k8s-ns" => k8s_ns = Some(next()),
            "--k8s-api" => k8s_api = Some(next()),
            "--k8s-mode" => {
                k8s_mode = match next().as_str() {
                    "ip" => NodeNameMode::Ip,
                    "dns" => NodeNameMode::Dns,
                    "hostname" => NodeNameMode::Hostname,
                    other => panic!("unknown --k8s-mode {other}"),
                }
            }
            "--k8s-lookup" => {
                k8s_lookup = match next().as_str() {
                    "endpoints" => IpLookupMode::Endpoints,
                    "pods" => IpLookupMode::Pods,
                    other => panic!("unknown --k8s-lookup {other}"),
                }
            }
            "--k8s-service" => k8s_service = Some(next()),
            "--k8s-insecure" => k8s_insecure = true,
            "--k8s-token" => k8s_token = Some(next()),
            "--rancher" => rancher = Some(next()),
            "--rancher-url" => rancher_url = Some(next()),
            "--nebula-lighthouse" => {
                let (lh, hosts, base) = (next(), next(), next());
                nebula = Some((
                    NebulaDiscovery::LighthouseDns {
                        lighthouses: servers(lh),
                        hosts: split(hosts),
                    },
                    base,
                ));
            }
            "--nebula-dns" => {
                let (hosts, base) = (next(), next());
                nebula = Some((
                    NebulaDiscovery::Dns {
                        hosts: split(hosts),
                        nameservers: Vec::new(),
                    },
                    base,
                ));
            }
            "--nebula-static" => {
                let (path, base) = (next(), next());
                nebula = Some((
                    NebulaDiscovery::StaticHostMap {
                        path: path.into(),
                        include_lighthouses: true,
                        exclude: Vec::new(),
                    },
                    base,
                ));
            }
            "--cidr" => cidrs.push(next()),
            "--nebula-naming" => {
                nebula_naming = match next().as_str() {
                    "ip" => NebulaNaming::Ip,
                    "name" => NebulaNaming::Name,
                    other => panic!("unknown --nebula-naming {other}"),
                }
            }
            "--nameserver" => nameservers.push(next()),
            "--poll-static" => poll_static = split(next()),
            "--cookie" => cookie = args.next(),
            "--gossip" => gossip = true,
            "--gossip-secret" => gossip_cfg = gossip_cfg.secret(args.next().unwrap_or_default()),
            "--gossip-addr" => {
                gossip_cfg = gossip_cfg.multicast_addr(
                    args.next()
                        .unwrap_or_default()
                        .parse()
                        .expect("ipv4 address"),
                )
            }
            "--gossip-port" => {
                gossip_cfg = gossip_cfg.port(args.next().unwrap_or_default().parse().expect("port"))
            }
            "--gossip-broadcast" => gossip_cfg = gossip_cfg.broadcast_only(true),
            "--visible" => visible = true,
            "--connect" => connect.extend(
                args.next()
                    .unwrap_or_default()
                    .split(',')
                    .map(str::to_string),
            ),
            other => name = Some(other.to_string()),
        }
    }
    let name = name.unwrap_or_else(|| {
        let host = std::process::Command::new("hostname")
            .output()
            .ok()
            .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string())
            .filter(|h| !h.is_empty())
            .unwrap_or_else(|| "localhost".into());
        format!("rex@{host}")
    });

    // -- native "packages" ----------------------------------------------------

    let math = FnModule::elixir("Rex.Math")
        .function("add", 2, |args, _| {
            Ok(Term::Int(int_arg(&args, 0)? + int_arg(&args, 1)?))
        })
        .function("mul", 2, |args, _| {
            Ok(Term::Int(int_arg(&args, 0)? * int_arg(&args, 1)?))
        })
        .function("sum", 1, |args, _| {
            let list = args[0].as_list().ok_or_else(Exception::badarg)?;
            let mut acc = 0.0;
            let mut all_int = true;
            for t in list {
                match t {
                    Term::Int(i) => acc += *i as f64,
                    Term::Float(f) => {
                        acc += f;
                        all_int = false;
                    }
                    _ => return Err(Exception::badarg()),
                }
            }
            Ok(if all_int {
                Term::Int(acc as i64)
            } else {
                Term::Float(acc)
            })
        })
        .function("fib", 1, |args, _| {
            let n = int_arg(&args, 0)?;
            if !(0..=92).contains(&n) {
                return Err(Exception::badarg_msg("fib/1 takes 0..92"));
            }
            let (mut a, mut b) = (0i64, 1i64);
            for _ in 0..n {
                (a, b) = (b, a + b);
            }
            Ok(Term::Int(a))
        })
        .function("divide", 2, |args, _| {
            let d = int_arg(&args, 1)?;
            if d == 0 {
                return Err(Exception::error(Term::atom("badarith")));
            }
            Ok(Term::Int(int_arg(&args, 0)? / d))
        });

    let strings = FnModule::elixir("Rex.Strings")
        .function("upcase", 1, |args, _| {
            Ok(Term::string(
                args[0]
                    .to_text()
                    .ok_or_else(Exception::badarg)?
                    .to_uppercase(),
            ))
        })
        .function("reverse", 1, |args, _| {
            Ok(Term::string(
                args[0]
                    .to_text()
                    .ok_or_else(Exception::badarg)?
                    .chars()
                    .rev()
                    .collect::<String>(),
            ))
        })
        .function("word_count", 1, |args, _| {
            let text = args[0].to_text().ok_or_else(Exception::badarg)?;
            let mut counts: HashMap<String, i64> = HashMap::new();
            for w in text.split_whitespace() {
                *counts.entry(w.to_lowercase()).or_default() += 1;
            }
            let mut pairs: Vec<_> = counts.into_iter().collect();
            pairs.sort();
            Ok(Term::map(
                pairs
                    .into_iter()
                    .map(|(k, v)| (Term::string(k), Term::Int(v)))
                    .collect(),
            ))
        });

    // A stateful module: a key/value store living in the Rust process.
    // Keys are stored by their printed form so any term works as a key.
    let store: Arc<Mutex<HashMap<String, (Term, Term)>>> = Arc::new(Mutex::new(HashMap::new()));
    let kv = {
        let (put, get, del, all) = (store.clone(), store.clone(), store.clone(), store.clone());
        FnModule::elixir("Rex.KV")
            .function("put", 2, move |mut args, _| {
                let v = args.pop().unwrap();
                let k = args.pop().unwrap();
                put.lock().unwrap().insert(k.to_string(), (k, v));
                Ok(Term::atom("ok"))
            })
            .function("get", 1, move |args, _| {
                Ok(get
                    .lock()
                    .unwrap()
                    .get(&args[0].to_string())
                    .map(|(_, v)| v.clone())
                    .into())
            })
            .function("delete", 1, move |args, _| {
                Ok(del
                    .lock()
                    .unwrap()
                    .remove(&args[0].to_string())
                    .is_some()
                    .into())
            })
            .function("all", 0, move |_, _| {
                let m = all.lock().unwrap();
                Ok(Term::map(m.values().cloned().collect()))
            })
    };

    // Async functions: talk back to the cluster while serving a call.
    let cluster = FnModule::elixir("Rex.Cluster")
        .async_function("peer_nodes", 0, |_, ctx| async move {
            // Ask the caller's node what it is connected to.
            ctx.node
                .rpc_call(
                    ctx.peer.as_str(),
                    "erlang",
                    "nodes",
                    vec![],
                    Duration::from_secs(5),
                )
                .await
                .map_err(|e| Exception::error(Term::string(e.to_string())))
        })
        .async_function("greet", 1, |args, ctx| async move {
            let who = args[0].to_text().unwrap_or_else(|| args[0].to_string());
            // Prints on the *caller's* shell via its group leader.
            let _ = ctx.puts(format!("hello {who}, from {}", ctx.node.name()));
            Ok(Term::atom("ok"))
        })
        .async_function("sleep", 1, |args, _| async move {
            let ms = int_arg(&args, 0)?;
            tokio::time::sleep(Duration::from_millis(ms.max(0) as u64)).await;
            Ok(Term::atom("ok"))
        });

    let node = Node::builder(&name)
        .cookie(
            cookie
                .clone()
                .or_else(read_cookie)
                .unwrap_or_else(|| "rexcookie".into()),
        )
        .hidden(!visible)
        .start_epmd(spawn_epmd)
        .module(math)
        .module(strings)
        .module(kv)
        .module(cluster)
        .start()
        .await?;
    println!(
        "rex node {} up on port {} (cookie {})",
        node.name(),
        node.listen_port(),
        if cookie.is_some() {
            "from args/env"
        } else {
            "from ~/.erlang.cookie or default"
        }
    );

    // A registered process that echoes messages back to their sender.
    {
        let mut inbox = node.spawn_mailbox();
        inbox.register("rex_inbox")?;
        let n = node.clone();
        tokio::spawn(async move {
            while let Some(sig) = inbox.recv().await {
                match sig {
                    Signal::Message(Term::Tuple(items))
                        if items.len() == 2 && items[0].as_pid().is_some() =>
                    {
                        let from = items[0].as_pid().unwrap().clone();
                        let reply = Term::tuple(vec![Term::atom("echo"), items[1].clone()]);
                        if let Err(e) = n.send(&from, reply).await {
                            eprintln!("echo reply failed: {e}");
                        }
                    }
                    other => println!("rex_inbox got: {other:?}"),
                }
            }
        });
    }

    // Log node up/down events.
    {
        let mut events = node.events();
        tokio::spawn(async move {
            while let Ok(ev) = events.recv().await {
                println!("event: {ev:?}");
            }
        });
    }

    if gossip {
        node.start_strategy(gossip_cfg);
    }
    if !connect.is_empty() {
        node.start_strategy(Hosts::new(connect).interval(poll_interval));
    }
    if local_epmd {
        node.start_strategy(LocalEpmd::new().interval(poll_interval));
    }
    if let Some(path) = erlang_hosts {
        let mut s = ErlangHosts::new().interval(poll_interval);
        if let Some(p) = path {
            s = s.path(p);
        }
        node.start_strategy(s);
    }
    // A custom resolver for the DNS strategies, e.g. --nameserver 127.0.0.1:5353/tcp
    let resolver = if nameservers.is_empty() {
        None
    } else {
        let parsed: Vec<_> = nameservers
            .iter()
            .map(|n| Resolver::parse_server(n).expect("nameserver"))
            .collect();
        let tcp = parsed.iter().any(|(_, tcp)| *tcp);
        Some(
            Resolver::with_nameservers(&parsed.iter().map(|(a, _)| *a).collect::<Vec<_>>(), tcp)
                .expect("resolver"),
        )
    };
    if let Some((query, base)) = dns_poll {
        let mut s = DnsPoll::new(query, base).interval(poll_interval);
        if let Some(r) = &resolver {
            s = s.resolver(r.clone());
        }
        node.start_strategy(s);
    }
    if let Some((service, base)) = k8s_dns {
        let mut s = KubernetesDns::new(service, base).interval(poll_interval);
        if let Some(ns) = &k8s_ns {
            s = s.namespace(ns);
        }
        if let Some(r) = &resolver {
            s = s.resolver(r.clone());
        }
        node.start_strategy(s);
    }
    if let Some((service, ns, base)) = k8s_dns_srv {
        let mut s = KubernetesDnsSrv::new(service, ns, base).interval(poll_interval);
        if let Some(r) = &resolver {
            s = s.resolver(r.clone());
        }
        node.start_strategy(s);
    }
    if let Some((base, selector)) = k8s {
        let mut s = Kubernetes::new(base, selector)
            .mode(k8s_mode)
            .ip_lookup_mode(k8s_lookup)
            .insecure_skip_tls_verify(k8s_insecure)
            .interval(poll_interval);
        if let Some(ns) = k8s_ns {
            s = s.namespace(ns);
        }
        if let Some(api) = k8s_api {
            s = s.api_url(api);
        }
        if let Some(svc) = k8s_service {
            s = s.service_name(svc);
        }
        if let Some(t) = k8s_token {
            s = s.token(t);
        }
        node.start_strategy(s);
    }
    if let Some(base) = rancher {
        let mut s = Rancher::new(base).interval(poll_interval);
        if let Some(u) = rancher_url {
            s = s.metadata_url(u);
        }
        node.start_strategy(s);
    }
    if let Some((discovery, base)) = nebula {
        let mut s = Nebula::new(base, discovery)
            .naming(nebula_naming)
            .interval(poll_interval);
        for c in &cidrs {
            s = s.cidr(c).expect("cidr");
        }
        node.start_strategy(s);
    }
    if !poll_static.is_empty() {
        let names = poll_static.clone();
        node.start_strategy(Poll::new(poll_interval, move |_node| {
            let names = names.clone();
            async move { Ok(names) }
        }));
    }

    tokio::signal::ctrl_c().await.ok();
    node.shutdown();
    Ok(())
}

fn read_cookie() -> Option<String> {
    let home = std::env::var_os("HOME")?;
    std::fs::read_to_string(std::path::Path::new(&home).join(".erlang.cookie"))
        .ok()
        .map(|s| s.trim().to_string())
}
