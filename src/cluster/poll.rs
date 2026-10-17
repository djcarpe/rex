use super::{poll_loop, Strategy};
use crate::native::BoxFuture;
use crate::node::Node;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

type Source = dyn Fn(Node) -> BoxFuture<'static, Result<Vec<String>, String>> + Send + Sync;

/// A strategy from your own discovery function: anything that can produce a
/// list of node names (`name@host`) every `interval`. This is how to plug in a
/// topology this crate does not know about (Consul, etcd, a database table...).
///
/// ```no_run
/// # use rex::cluster::Poll; use std::time::Duration;
/// # async fn demo(node: rex::Node) {
/// node.start_strategy(Poll::new(Duration::from_secs(10), |_node| async {
///     Ok(vec!["app@10.0.0.5".to_string(), "app@10.0.0.6".to_string()])
/// }));
/// # }
/// ```
pub struct Poll {
    interval: Duration,
    disconnect_missing: bool,
    source: Arc<Source>,
}

impl Poll {
    pub fn new<F, Fut>(interval: Duration, source: F) -> Self
    where
        F: Fn(Node) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Vec<String>, String>> + Send + 'static,
    {
        Poll {
            interval,
            disconnect_missing: false,
            source: Arc::new(move |n| Box::pin(source(n))),
        }
    }

    /// Also disconnect from nodes that drop out of the discovered list
    /// (libcluster's Kubernetes strategies do this; Epmd/Gossip do not).
    pub fn disconnect_missing(mut self, yes: bool) -> Self {
        self.disconnect_missing = yes;
        self
    }
}

impl Strategy for Poll {
    fn run(self: Box<Self>, node: Node) -> BoxFuture<'static, ()> {
        Box::pin(async move {
            let src = self.source.clone();
            let n = node.clone();
            poll_loop(
                node,
                "poll",
                self.interval,
                self.disconnect_missing,
                move || src(n.clone()),
            )
            .await
        })
    }
}
