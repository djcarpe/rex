//! A Rust compute node for the rex demo.
//!
//! It joins the BEAM cluster as `compute<N>@host` and serves `Rex.Compute`,
//! a native module that renders Mandelbrot tiles to PNG. The Phoenix
//! dashboard (`demo/dashboard`) fans a render out over every compute node it
//! can see, shows the tiles as they land, and re-queues tiles when a node
//! dies mid-batch.
//!
//! ```text
//! cargo run --release -- compute1@$(hostname) --cookie rexcookie
//! ```
//!
//! What this shows: CPU-bound work in plain Rust (no NIF, no port), run on a
//! blocking thread pool so one node serves many concurrent callers; progress
//! pushed *back* to the BEAM while a tile is being rendered; and a node that
//! can be killed at any time without taking anything else down.

use rex::{CallContext, Exception, FnModule, Node, Term};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::mpsc;

/// What the dashboard asks for: a window of the complex plane and a pixel size.
struct TileReq {
    id: i64,
    x0: f64,
    y0: f64,
    x1: f64,
    y1: f64,
    size: usize,
    max_iter: u32,
}

impl TileReq {
    fn from_term(t: &Term) -> Result<TileReq, Exception> {
        let get = |k: &str| t.map_get_atom(k).ok_or_else(|| Exception::badarg_msg(format!("missing key {k}")));
        let num = |k: &str| get(k)?.as_number().ok_or_else(|| Exception::badarg_msg(format!("{k} must be a number")));
        let int = |k: &str| get(k)?.as_int().ok_or_else(|| Exception::badarg_msg(format!("{k} must be an integer")));
        let size = int("size")?;
        let max_iter = int("max_iter")?;
        if !(8..=1024).contains(&size) || !(1..=100_000).contains(&max_iter) {
            return Err(Exception::badarg_msg("size must be 8..1024 and max_iter 1..100000"));
        }
        Ok(TileReq {
            id: int("id")?,
            x0: num("x0")?,
            y0: num("y0")?,
            x1: num("x1")?,
            y1: num("y1")?,
            size: size as usize,
            max_iter: max_iter as u32,
        })
    }
}

/// Escape-time Mandelbrot with smooth colouring. `progress` is told after
/// every row so the caller can watch the tile fill in.
fn render(req: &TileReq, mut progress: impl FnMut(usize)) -> (Vec<u8>, u64) {
    let n = req.size;
    let mut rgb = vec![0u8; n * n * 3];
    let mut iterations: u64 = 0;
    for py in 0..n {
        let ci = req.y0 + (req.y1 - req.y0) * (py as f64 + 0.5) / n as f64;
        for px in 0..n {
            let cr = req.x0 + (req.x1 - req.x0) * (px as f64 + 0.5) / n as f64;
            let (mut zr, mut zi) = (0.0f64, 0.0f64);
            let mut i = 0u32;
            while i < req.max_iter && zr * zr + zi * zi <= 4.0 {
                let t = zr * zr - zi * zi + cr;
                zi = 2.0 * zr * zi + ci;
                zr = t;
                i += 1;
            }
            iterations += i as u64;
            let o = (py * n + px) * 3;
            if i < req.max_iter {
                // Smooth iteration count -> hue.
                let mag = (zr * zr + zi * zi).sqrt().max(1.0000001);
                let nu = i as f64 + 1.0 - mag.ln().ln() / std::f64::consts::LN_2;
                let t = (nu / 64.0).fract();
                let (r, g, b) = palette(t);
                rgb[o] = r;
                rgb[o + 1] = g;
                rgb[o + 2] = b;
            }
        }
        progress(py + 1);
    }
    (encode_png(&rgb, n), iterations)
}

fn palette(t: f64) -> (u8, u8, u8) {
    // A cosine palette (Inigo Quilez style).
    let f = |a: f64, b: f64, c: f64, d: f64| ((a + b * (6.283_185 * (c * t + d)).cos()) * 255.0).clamp(0.0, 255.0) as u8;
    (f(0.5, 0.5, 1.0, 0.0), f(0.5, 0.5, 1.0, 0.33), f(0.5, 0.5, 1.0, 0.67))
}

fn encode_png(rgb: &[u8], n: usize) -> Vec<u8> {
    let mut out = Vec::with_capacity(n * n);
    {
        let mut enc = png::Encoder::new(&mut out, n as u32, n as u32);
        enc.set_color(png::ColorType::Rgb);
        enc.set_depth(png::BitDepth::Eight);
        enc.set_compression(png::Compression::Fast);
        let mut w = enc.write_header().expect("png header");
        w.write_image_data(rgb).expect("png data");
    }
    out
}

#[derive(Default)]
struct Stats {
    tiles: AtomicU64,
    busy: AtomicU64,
    iterations: AtomicU64,
}

#[tokio::main]
async fn main() -> rex::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "rex=info,compute=info".parse().unwrap()))
        .init();
    let mut args = std::env::args().skip(1);
    let mut name = format!("compute@{}", hostname());
    let mut cookie = "rexcookie".to_string();
    while let Some(a) = args.next() {
        match a.as_str() {
            "--cookie" => cookie = args.next().expect("--cookie needs a value"),
            other => name = other.to_string(),
        }
    }

    let stats = Arc::new(Stats::default());
    let s = stats.clone();
    let compute = FnModule::elixir("Rex.Compute")
        .function("ping", 0, |_, _| Ok(Term::atom("pong")))
        .function("info", 0, move |_, ctx| {
            Ok(Term::map(vec![
                (Term::atom("node"), Term::Atom(ctx.node.name().clone())),
                (Term::atom("cores"), Term::from(std::thread::available_parallelism().map(|n| n.get()).unwrap_or(1))),
                (Term::atom("tiles"), Term::from(s.tiles.load(Ordering::Relaxed) as i64)),
                (Term::atom("busy"), Term::from(s.busy.load(Ordering::Relaxed) as i64)),
                (Term::atom("iterations"), Term::from(s.iterations.load(Ordering::Relaxed) as i64)),
                (Term::atom("version"), Term::string(env!("CARGO_PKG_VERSION"))),
            ]))
        })
        .async_function("tile", 1, move |args, ctx: CallContext| {
            let stats = stats.clone();
            async move {
                let req = TileReq::from_term(&args[0])?;
                stats.busy.fetch_add(1, Ordering::Relaxed);
                let started = Instant::now();
                // Progress goes to {:render_progress, CallerNode} while we work:
                // the blocking thread reports rows, this task forwards them.
                let (tx, mut rx) = mpsc::unbounded_channel::<usize>();
                let size = req.size;
                let id = req.id;
                let worker = tokio::task::spawn_blocking(move || {
                    let mut last = 0usize;
                    let r = render(&req, |row| {
                        // Report in ~10% steps, not every row.
                        if row * 10 / size > last * 10 / size || row == size {
                            let _ = tx.send(row);
                        }
                        last = row;
                    });
                    r
                });
                let node = ctx.node.clone();
                let me = node.name().clone();
                let peer = ctx.peer.clone();
                while let Some(row) = rx.recv().await {
                    let msg = Term::tuple(vec![
                        Term::atom("tile_progress"),
                        Term::Int(id),
                        Term::Atom(me.clone()),
                        Term::Int((row * 100 / size) as i64),
                    ]);
                    let _ = node.send_named("render_progress", peer.as_str(), msg).await;
                }
                stats.busy.fetch_sub(1, Ordering::Relaxed);
                let (png, iterations) = worker.await.map_err(|e| Exception::error(Term::string(format!("render panicked: {e}"))))?;
                stats.tiles.fetch_add(1, Ordering::Relaxed);
                stats.iterations.fetch_add(iterations, Ordering::Relaxed);
                let ms = started.elapsed().as_secs_f64() * 1000.0;
                tracing::info!(target: "compute", id, size, iterations, ms = format_args!("{ms:.1}"), "tile rendered");
                Ok(Term::map(vec![
                    (Term::atom("id"), Term::Int(id)),
                    (Term::atom("node"), Term::Atom(me)),
                    (Term::atom("png"), Term::Binary(png)),
                    (Term::atom("ms"), Term::Float(ms)),
                    (Term::atom("iterations"), Term::from(iterations as i64)),
                ]))
            }
        });

    let node = Node::builder(&name).cookie(&cookie).module(compute).start().await?;
    // Find the dashboard (and other BEAM nodes) through the local EPMD, the
    // way libcluster's LocalEpmd strategy does.
    node.start_strategy(rex::cluster::LocalEpmd::new());
    tracing::info!(target: "compute", node = %node.name(), port = node.listen_port(), "compute node ready");

    let mut events = node.events();
    tokio::spawn(async move {
        while let Ok(ev) = events.recv().await {
            tracing::info!(target: "compute", "{ev:?}");
        }
    });

    tokio::signal::ctrl_c().await.ok();
    node.shutdown();
    Ok(())
}

fn hostname() -> String {
    std::process::Command::new("hostname")
        .output()
        .ok()
        .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string())
        .filter(|h| !h.is_empty())
        .unwrap_or_else(|| "localhost".into())
}
