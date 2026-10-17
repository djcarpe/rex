//! # rex
//!
//! A native Erlang distribution node in Rust. It registers with EPMD, speaks
//! the OTP 23+ distribution handshake and protocol, and so can join a cluster
//! of Erlang/Elixir nodes without being an Erlang VM itself.
//!
//! What you get:
//!
//! * **Cluster membership** – connect to Elixir nodes or let them connect to
//!   you; `Node.connect/1`, `Node.ping/1`, `Node.list(:hidden)` all work.
//! * **libcluster discovery** – every core libcluster strategy has a
//!   counterpart in [`cluster`]: Gossip (same multicast protocol and
//!   encryption), Epmd/LocalEpmd/ErlangHosts, DNSPoll, Kubernetes (API, DNS
//!   and DNSSRV), Rancher, plus libcluster_nebula's Nebula strategy and a
//!   generic [`cluster::Poll`] for anything else. Elixir nodes using any
//!   libcluster strategy can also simply connect to us, since we are
//!   registered with EPMD.
//! * **Native modules over erpc** – register a [`NativeModule`] (or build one
//!   from closures with [`FnModule`]) and call it from Elixir with
//!   `:erpc.call(:"rex@host", MyMod, :fun, args)`, `:rpc.call/4`,
//!   `Node.spawn/4` or `:rpc.block_call/4`. No NIFs, no BEAM on the Rust side.
//! * **Messaging the other way** – [`Node::send`], [`Node::call`] (GenServer
//!   call), [`Node::cast`], [`Node::rpc_call`], plus [`Mailbox`]es that remote
//!   processes can send to, link to and monitor.
//!
//! ```no_run
//! use rex::{FnModule, Exception, Node, Term};
//!
//! # async fn demo() -> rex::Result<()> {
//! let node = Node::builder("rex@desktop")
//!     .cookie("secret")
//!     .module(FnModule::elixir("Rex.Math").function("add", 2, |args, _ctx| {
//!         let a = args[0].as_int().ok_or_else(Exception::badarg)?;
//!         let b = args[1].as_int().ok_or_else(Exception::badarg)?;
//!         Ok(Term::Int(a + b))
//!     }))
//!     .start()
//!     .await?;
//! node.start_strategy(rex::cluster::Gossip::new());
//! # Ok(()) }
//! ```
//!
//! Then, in `iex --sname app --cookie secret`:
//! `:erpc.call(:"rex@desktop", Rex.Math, :add, [1, 2])` returns `3`.

pub mod cluster;
pub mod dist;
pub mod epmd;
pub mod etf;
pub mod handshake;
pub mod term;

mod builtins;
mod connection;
mod error;
mod native;
mod node;
mod process;

pub use error::{Error, Result};
pub use native::{BoxFuture, CallContext, CallResult, Exception, FnModule, NativeModule};
pub use node::{Dest, Node, NodeBuilder, NodeEvent, SpawnOpts};
pub use process::{Mailbox, Signal};
pub use term::{Atom, BigInt, Pid, Port, Reference, Term};
