//! Micro-benchmark of the term layer alone: encode, decode, clone and drop of
//! the payloads `elixir/bench.exs` sends, with no network in the way.
//!
//! ```text
//! cargo run --release --example etf_bench
//! ```
use rex::etf;
use rex::Term;
use std::time::Instant;

fn time<T>(label: &str, iters: usize, mut f: impl FnMut() -> T) -> T {
    let mut out = None;
    let t0 = Instant::now();
    for _ in 0..iters {
        out = Some(f());
    }
    let per = t0.elapsed().as_secs_f64() / iters as f64;
    println!("{label:<40} {:>9.1} µs", per * 1e6);
    out.unwrap()
}

fn main() {
    let structured = Term::list(
        (1..=10_000)
            .map(|i| {
                Term::tuple(vec![
                    Term::Int(i),
                    Term::string(format!("item-{i}")),
                    Term::map(vec![(Term::atom("id"), Term::Int(i)), (Term::atom("ok"), Term::from(true))]),
                ])
            })
            .collect(),
    );
    let ints = Term::list((1..=100_000).map(Term::Int).collect());
    let big = Term::Binary(vec![0xAB; 1 << 20]);
    for (name, term) in [("10k tuples+maps", &structured), ("100k ints", &ints), ("1 MiB binary", &big)] {
        println!("== {name} ({} B encoded)", etf::encoded_len(term));
        let bytes = time("encode", 50, || etf::encode(term));
        let decoded = time("decode", 50, || etf::decode(&bytes).unwrap());
        time("clone", 50, || decoded.clone());
        time("clone+drop", 50, || drop(decoded.clone()));
        time("encoded_len", 50, || etf::encoded_len(term));
        let _ = time("decode+encode (echo)", 50, || etf::encode(&etf::decode(&bytes).unwrap()));
    }
}
