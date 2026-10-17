//! Benchmark peer/driver on the Rust side.
//!
//! ```text
//! cargo run --release --example bench_node -- rexbench@host --cookie C [--target base@host]
//! ```
//! Serves `Rex.Bench.echo/1`, `Rex.Bench.sum/1` and the registered processes
//! `rex_bench_echo` / `rex_bench_sink`, which `elixir/bench.exs` drives. With
//! `--target` it also measures Rust -> Elixir calls against `elixir/bench_peer.exs`.

use rex::{Dest, Exception, FnModule, Node, Signal, Term};
use std::time::{Duration, Instant};

fn pct(sorted: &[f64], p: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    let idx = ((sorted.len() as f64 - 1.0) * p).round() as usize;
    sorted[idx]
}

fn fmt_bytes(n: usize) -> String {
    if n >= 1 << 20 {
        format!("{} MiB", n >> 20)
    } else if n >= 1 << 10 {
        format!("{} KiB", n >> 10)
    } else {
        format!("{n} B")
    }
}

#[tokio::main]
async fn main() -> rex::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "rex=warn".parse().unwrap()))
        .init();
    let mut args = std::env::args().skip(1);
    let mut name = "rexbench@localhost".to_string();
    let mut cookie = "rexcookie".to_string();
    let mut target: Option<String> = None;
    while let Some(a) = args.next() {
        match a.as_str() {
            "--cookie" => cookie = args.next().expect("cookie"),
            "--target" => target = Some(args.next().expect("target node")),
            other => name = other.to_string(),
        }
    }

    let bench = FnModule::elixir("Rex.Bench")
        .function("echo", 1, |mut args, _| Ok(args.pop().unwrap()))
        .function("sum", 1, |args, _| {
            let list = args[0].as_list().ok_or_else(Exception::badarg)?;
            Ok(Term::Int(list.iter().filter_map(|t| t.as_int()).sum()))
        })
        .function("size", 1, |args, _| Ok(Term::from(args[0].as_binary().map(|b| b.len()).unwrap_or(0))));

    let node = Node::builder(&name).cookie(&cookie).module(bench).start().await?;

    // Echo process: {From, Ref, Payload} -> From ! {Ref, Payload}; also answers gen_calls.
    {
        let mut mb = node.spawn_mailbox();
        mb.register("rex_bench_echo")?;
        let n = node.clone();
        tokio::spawn(async move {
            while let Some(sig) = mb.recv().await {
                if let Signal::Message(Term::Tuple(mut items)) = sig {
                    if items.len() == 3 && items[0].is_atom("$gen_call") {
                        let req = items.pop().unwrap();
                        let from = items.pop().unwrap();
                        n.gen_reply(&from, req);
                    } else if items.len() == 3 {
                        let payload = items.pop().unwrap();
                        let r = items.pop().unwrap();
                        if let Some(from) = items[0].as_pid() {
                            let _ = n.try_send(from, Term::tuple(vec![r, payload]));
                        }
                    }
                }
            }
        });
    }
    // Sink process: counts {msg, _}; {From, Ref, flush} -> From ! {Ref, Count} (or gen_call flush).
    {
        let mut mb = node.spawn_mailbox();
        mb.register("rex_bench_sink")?;
        let n = node.clone();
        tokio::spawn(async move {
            let mut count: i64 = 0;
            let mut bytes: i64 = 0;
            while let Some(sig) = mb.recv().await {
                if let Signal::Message(Term::Tuple(items)) = sig {
                    if items.len() == 2 && items[0].is_atom("msg") {
                        count += 1;
                        bytes += items[1].as_binary().map(|b| b.len() as i64).unwrap_or(0);
                    } else if items.len() == 3 && items[2].is_atom("flush") {
                        if let Some(from) = items[0].as_pid() {
                            let _ = n.try_send(from, Term::tuple(vec![items[1].clone(), Term::Int(count), Term::Int(bytes)]));
                        }
                        count = 0;
                        bytes = 0;
                    } else if items.len() == 3 && items[0].is_atom("$gen_call") {
                        n.gen_reply(&items[1], Term::tuple(vec![Term::Int(count), Term::Int(bytes)]));
                        count = 0;
                        bytes = 0;
                    }
                }
            }
        });
    }
    println!("bench node {} ready (port {})", node.name(), node.listen_port());

    if let Some(target) = target {
        node.connect(&target).await?;
        println!("\n## Rust -> Elixir ({} -> {target})\n", node.name());
        println!("| benchmark | size | n | calls/s | MB/s | p50 µs | p99 µs |");
        println!("|---|---|---|---|---|---|---|");

        // rpc_call latency
        {
            let n = 2000;
            let mut lat = Vec::with_capacity(n);
            let t0 = Instant::now();
            for _ in 0..n {
                let t = Instant::now();
                let r = node.rpc_call(&target, "erlang", "node", vec![], Duration::from_secs(5)).await?;
                assert!(r.as_atom().is_some());
                lat.push(t.elapsed().as_secs_f64() * 1e6);
            }
            let el = t0.elapsed().as_secs_f64();
            lat.sort_by(|a, b| a.partial_cmp(b).unwrap());
            println!("| rpc_call erlang:node/0 | - | {n} | {:.0} | - | {:.0} | {:.0} |", n as f64 / el, pct(&lat, 0.5), pct(&lat, 0.99));
        }
        // erpc_call (spawn_request) latency and echo at several sizes
        {
            let n = 2000;
            let mut lat = Vec::with_capacity(n);
            let t0 = Instant::now();
            for _ in 0..n {
                let t = Instant::now();
                let r = node.erpc_call(&target, "erlang", "node", vec![], Duration::from_secs(5)).await?;
                assert!(r.as_atom().is_some());
                lat.push(t.elapsed().as_secs_f64() * 1e6);
            }
            let el = t0.elapsed().as_secs_f64();
            lat.sort_by(|a, b| a.partial_cmp(b).unwrap());
            println!("| erpc_call erlang:node/0 | - | {n} | {:.0} | - | {:.0} | {:.0} |", n as f64 / el, pct(&lat, 0.5), pct(&lat, 0.99));
        }
        for (size, n) in [(1usize << 10, 2000usize), (64 << 10, 500), (1 << 20, 50)] {
            let payload = Term::Binary(vec![0xABu8; size]);
            let mut lat = Vec::with_capacity(n);
            let t0 = Instant::now();
            for _ in 0..n {
                let t = Instant::now();
                let r = node.erpc_call(&target, "Elixir.Rex.Bench", "echo", vec![payload.clone()], Duration::from_secs(30)).await?;
                assert_eq!(r.as_binary().map(|b| b.len()), Some(size));
                lat.push(t.elapsed().as_secs_f64() * 1e6);
            }
            let el = t0.elapsed().as_secs_f64();
            lat.sort_by(|a, b| a.partial_cmp(b).unwrap());
            println!(
                "| erpc_call Rex.Bench.echo/1 | {} | {n} | {:.0} | {:.1} | {:.0} | {:.0} |",
                fmt_bytes(size),
                n as f64 / el,
                (2 * size * n) as f64 / el / 1e6,
                pct(&lat, 0.5),
                pct(&lat, 0.99)
            );
        }
        // gen call echo at several sizes
        for (size, n) in [(0usize, 2000usize), (1 << 10, 2000), (64 << 10, 500), (1 << 20, 50)] {
            let payload = Term::Binary(vec![0xABu8; size]);
            let mut lat = Vec::with_capacity(n);
            let t0 = Instant::now();
            for _ in 0..n {
                let t = Instant::now();
                let r = node.call(Dest::named("rex_bench_echo", target.as_str()), payload.clone(), Duration::from_secs(30)).await?;
                assert_eq!(r.as_binary().map(|b| b.len()), Some(size));
                lat.push(t.elapsed().as_secs_f64() * 1e6);
            }
            let el = t0.elapsed().as_secs_f64();
            lat.sort_by(|a, b| a.partial_cmp(b).unwrap());
            println!(
                "| call echo (round trip) | {} | {n} | {:.0} | {:.1} | {:.0} | {:.0} |",
                fmt_bytes(size),
                n as f64 / el,
                (2 * size * n) as f64 / el / 1e6,
                pct(&lat, 0.5),
                pct(&lat, 0.99)
            );
        }
        // one-way flood
        for (size, n) in [(0usize, 200_000usize), (1 << 10, 100_000), (64 << 10, 5_000)] {
            let payload = Term::tuple(vec![Term::atom("msg"), Term::Binary(vec![0xCDu8; size])]);
            let t0 = Instant::now();
            for _ in 0..n {
                node.send_to(Dest::named("rex_bench_sink", target.as_str()), payload.clone()).await?;
            }
            let r = node.call(Dest::named("rex_bench_sink", target.as_str()), Term::atom("flush"), Duration::from_secs(120)).await?;
            let el = t0.elapsed().as_secs_f64();
            let got = r.as_tuple().and_then(|t| t[0].as_int()).unwrap_or(0);
            assert_eq!(got, n as i64, "sink counted {got}");
            println!("| send flood (one way) | {} | {n} | {:.0} | {:.1} | - | - |", fmt_bytes(size), n as f64 / el, (size * n) as f64 / el / 1e6);
        }
        println!();
    }

    tokio::signal::ctrl_c().await.ok();
    node.shutdown();
    Ok(())
}
