//! Cluster formation strategies compatible with
//! [libcluster](https://hexdocs.pm/libcluster) and friends.
//!
//! | libcluster                                   | rex                                   |
//! |----------------------------------------------|---------------------------------------|
//! | `Cluster.Strategy.Gossip`                    | [`Gossip`]                            |
//! | `Cluster.Strategy.Epmd` (`hosts:`)           | [`Hosts`]                             |
//! | `Cluster.Strategy.LocalEpmd`                 | [`LocalEpmd`]                         |
//! | `Cluster.Strategy.ErlangHosts`               | [`ErlangHosts`]                       |
//! | `Cluster.Strategy.DNSPoll`                   | [`DnsPoll`]                           |
//! | `Cluster.Strategy.Kubernetes`                | [`Kubernetes`] (API, endpoints/pods)  |
//! | `Cluster.Strategy.Kubernetes.DNS`            | [`KubernetesDns`]                     |
//! | `Cluster.Strategy.Kubernetes.DNSSRV`         | [`KubernetesDnsSrv`]                  |
//! | `Cluster.Strategy.Rancher`                   | [`Rancher`]                           |
//! | `Cluster.Strategy.Nebula` (libcluster_nebula)| [`Nebula`]                            |
//! | anything else                                | [`Poll`] with your own discovery fn   |
//!
//! Every strategy is a long-running task started with
//! [`Node::start_strategy`]. Several can run at once. Elixir nodes running any
//! libcluster strategy can always connect to us too, since a started node is
//! registered with EPMD.

mod gossip;
mod hosts;
mod local;
mod poll;

#[cfg(feature = "dns")]
mod dns;
#[cfg(feature = "http")]
mod kubernetes;
#[cfg(feature = "dns")]
mod nebula;
#[cfg(feature = "http")]
mod rancher;
#[cfg(feature = "dns")]
mod resolver;

pub use gossip::Gossip;
pub use hosts::Hosts;
pub use local::{ErlangHosts, LocalEpmd};
pub use poll::Poll;

#[cfg(feature = "dns")]
pub use dns::{DnsPoll, KubernetesDns, KubernetesDnsSrv};
#[cfg(feature = "http")]
pub use kubernetes::{IpLookupMode, Kubernetes, NodeNameMode};
#[cfg(feature = "dns")]
pub use nebula::{Cidr, Nebula, NebulaDiscovery, NebulaNaming};
#[cfg(feature = "http")]
pub use rancher::Rancher;
#[cfg(feature = "dns")]
pub use resolver::Resolver;

use crate::native::BoxFuture;
use crate::node::Node;
use crate::term::Atom;
use std::collections::HashSet;
use std::future::Future;
use std::time::Duration;

/// A long-running task that connects the node to peers.
pub trait Strategy: Send + 'static {
    fn run(self: Box<Self>, node: Node) -> BoxFuture<'static, ()>;
}

impl Node {
    /// Run a strategy in the background for the lifetime of the node.
    pub fn start_strategy<S: Strategy>(&self, strategy: S) -> tokio::task::JoinHandle<()> {
        let node = self.clone();
        tokio::spawn(Box::new(strategy).run(node))
    }

    /// The host part of our own node name.
    pub(crate) fn host_part(&self) -> String {
        self.name()
            .as_str()
            .split_once('@')
            .map(|(_, h)| h.to_string())
            .unwrap_or_default()
    }
}

/// Connect, logging failure at debug level; strategies retry anyway.
pub(crate) async fn try_connect(node: &Node, name: &str) {
    if name == node.name().as_str() || node.is_connected(&Atom::new(name)) {
        return;
    }
    match node.connect(name).await {
        Ok(()) => tracing::info!(node = name, "connected via cluster strategy"),
        Err(e) => tracing::debug!(node = name, "connect failed: {e}"),
    }
}

/// The loop shared by every polling strategy: discover a list of node names,
/// connect to the new ones (concurrently), optionally disconnect from the ones
/// that disappeared, sleep, repeat. `discover` errors are logged and retried.
pub(crate) async fn poll_loop<F, Fut>(
    node: Node,
    label: &'static str,
    interval: Duration,
    disconnect_missing: bool,
    mut discover: F,
) where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<Vec<String>, String>>,
{
    let mut known: HashSet<String> = HashSet::new();
    loop {
        match discover().await {
            Ok(names) => {
                let current: HashSet<String> = names
                    .into_iter()
                    .filter(|n| n != node.name().as_str())
                    .collect();
                tracing::debug!(strategy = label, nodes = ?current, "discovered");
                if disconnect_missing {
                    for gone in known.difference(&current) {
                        if node.disconnect(&Atom::new(gone)) {
                            tracing::info!(strategy = label, node = %gone, "disconnected: no longer in topology");
                        }
                    }
                }
                let mut set = tokio::task::JoinSet::new();
                for name in current.clone() {
                    let n = node.clone();
                    set.spawn(async move { try_connect(&n, &name).await });
                }
                while set.join_next().await.is_some() {}
                known = current;
            }
            Err(e) => tracing::warn!(strategy = label, "discovery failed: {e}"),
        }
        tokio::time::sleep(interval).await;
    }
}
