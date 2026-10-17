use super::{try_connect, Strategy};
use crate::native::BoxFuture;
use crate::node::Node;
use std::time::Duration;

/// Connect to a fixed list of nodes and keep retrying; the equivalent of
/// `Cluster.Strategy.Epmd` with `hosts: [...]`.
pub struct Hosts {
    pub hosts: Vec<String>,
    pub interval: Duration,
}

impl Hosts {
    pub fn new<I, S>(hosts: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        Hosts {
            hosts: hosts.into_iter().map(Into::into).collect(),
            interval: Duration::from_secs(5),
        }
    }

    pub fn interval(mut self, interval: Duration) -> Self {
        self.interval = interval;
        self
    }
}

impl Strategy for Hosts {
    fn run(self: Box<Self>, node: Node) -> BoxFuture<'static, ()> {
        Box::pin(async move {
            loop {
                for h in &self.hosts {
                    try_connect(&node, h).await;
                }
                tokio::time::sleep(self.interval).await;
            }
        })
    }
}
