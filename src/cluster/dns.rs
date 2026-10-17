use super::resolver::{cluster_domain, Resolver};
use super::{poll_loop, Strategy};
use crate::native::BoxFuture;
use crate::node::Node;
use std::time::Duration;

fn resolver_or_system(r: &Option<Resolver>) -> Result<Resolver, String> {
    match r {
        Some(r) => Ok(r.clone()),
        None => Resolver::system(),
    }
}

/// Resolve `query` to A/AAAA records and connect to `node_basename@<ip>` for
/// each; the equivalent of `Cluster.Strategy.DNSPoll`.
pub struct DnsPoll {
    pub query: String,
    pub node_basename: String,
    pub interval: Duration,
    pub resolver: Option<Resolver>,
}

impl DnsPoll {
    pub fn new(query: impl Into<String>, node_basename: impl Into<String>) -> Self {
        DnsPoll {
            query: query.into(),
            node_basename: node_basename.into(),
            interval: Duration::from_secs(5),
            resolver: None,
        }
    }
    pub fn interval(mut self, interval: Duration) -> Self {
        self.interval = interval;
        self
    }
    pub fn resolver(mut self, resolver: Resolver) -> Self {
        self.resolver = Some(resolver);
        self
    }
}

impl Strategy for DnsPoll {
    fn run(self: Box<Self>, node: Node) -> BoxFuture<'static, ()> {
        Box::pin(async move {
            let resolver = resolver_or_system(&self.resolver);
            let (query, base) = (self.query.clone(), self.node_basename.clone());
            poll_loop(node, "dns_poll", self.interval, false, move || {
                let (resolver, query, base) = (resolver.clone(), query.clone(), base.clone());
                async move {
                    let ips = resolver?.lookup_ips(&query).await?;
                    Ok(ips.into_iter().map(|ip| format!("{base}@{ip}")).collect())
                }
            })
            .await
        })
    }
}

/// `Cluster.Strategy.Kubernetes.DNS`: A records of a headless service give the
/// pod IPs; nodes are `application_name@<pod ip>`. Peers must be started with
/// `--name app@<pod ip>` (`RELEASE_DISTRIBUTION=name`, `RELEASE_NODE=app@$(POD_IP)`).
pub struct KubernetesDns {
    /// Headless service name. A bare name is resolved through the pod's search
    /// domains like libcluster does; give `namespace` to build the FQDN instead.
    pub service: String,
    pub application_name: String,
    pub namespace: Option<String>,
    pub interval: Duration,
    pub resolver: Option<Resolver>,
    /// Disconnect from nodes that leave the service (libcluster does).
    pub disconnect_missing: bool,
}

impl KubernetesDns {
    pub fn new(service: impl Into<String>, application_name: impl Into<String>) -> Self {
        KubernetesDns {
            service: service.into(),
            application_name: application_name.into(),
            namespace: None,
            interval: Duration::from_secs(5),
            resolver: None,
            disconnect_missing: true,
        }
    }
    pub fn namespace(mut self, ns: impl Into<String>) -> Self {
        self.namespace = Some(ns.into());
        self
    }
    pub fn interval(mut self, interval: Duration) -> Self {
        self.interval = interval;
        self
    }
    pub fn resolver(mut self, resolver: Resolver) -> Self {
        self.resolver = Some(resolver);
        self
    }
    pub fn disconnect_missing(mut self, yes: bool) -> Self {
        self.disconnect_missing = yes;
        self
    }

    /// The name actually queried.
    pub fn query(&self) -> String {
        match &self.namespace {
            Some(ns) if !self.service.contains('.') => format!(
                "{}.{}.svc.{}.",
                self.service,
                ns,
                cluster_domain().trim_end_matches('.')
            ),
            _ => self.service.clone(),
        }
    }
}

impl Strategy for KubernetesDns {
    fn run(self: Box<Self>, node: Node) -> BoxFuture<'static, ()> {
        Box::pin(async move {
            let resolver = resolver_or_system(&self.resolver);
            let (query, base) = (self.query(), self.application_name.clone());
            poll_loop(
                node,
                "kubernetes_dns",
                self.interval,
                self.disconnect_missing,
                move || {
                    let (resolver, query, base) = (resolver.clone(), query.clone(), base.clone());
                    async move {
                        let ips = resolver?.lookup_ips(&query).await?;
                        Ok(ips.into_iter().map(|ip| format!("{base}@{ip}")).collect())
                    }
                },
            )
            .await
        })
    }
}

/// `Cluster.Strategy.Kubernetes.DNSSRV`: SRV records of a headless service
/// give pod hostnames; nodes are
/// `application_name@<pod>.<service>.<namespace>.svc.<cluster domain>`.
/// Peers must be a StatefulSet started with `--name app@$(hostname -f)`.
pub struct KubernetesDnsSrv {
    pub service: String,
    pub namespace: String,
    pub application_name: String,
    pub interval: Duration,
    pub resolver: Option<Resolver>,
    pub disconnect_missing: bool,
}

impl KubernetesDnsSrv {
    pub fn new(
        service: impl Into<String>,
        namespace: impl Into<String>,
        application_name: impl Into<String>,
    ) -> Self {
        KubernetesDnsSrv {
            service: service.into(),
            namespace: namespace.into(),
            application_name: application_name.into(),
            interval: Duration::from_secs(5),
            resolver: None,
            disconnect_missing: true,
        }
    }
    pub fn interval(mut self, interval: Duration) -> Self {
        self.interval = interval;
        self
    }
    pub fn resolver(mut self, resolver: Resolver) -> Self {
        self.resolver = Some(resolver);
        self
    }
    pub fn disconnect_missing(mut self, yes: bool) -> Self {
        self.disconnect_missing = yes;
        self
    }

    /// `service.namespace.svc.cluster.local.`
    pub fn query(&self) -> String {
        format!(
            "{}.{}.svc.{}.",
            self.service,
            self.namespace,
            cluster_domain().trim_end_matches('.')
        )
    }
}

impl Strategy for KubernetesDnsSrv {
    fn run(self: Box<Self>, node: Node) -> BoxFuture<'static, ()> {
        Box::pin(async move {
            let resolver = resolver_or_system(&self.resolver);
            let (query, base) = (self.query(), self.application_name.clone());
            poll_loop(
                node,
                "kubernetes_dns_srv",
                self.interval,
                self.disconnect_missing,
                move || {
                    let (resolver, query, base) = (resolver.clone(), query.clone(), base.clone());
                    async move {
                        let targets = resolver?.lookup_srv(&query).await?;
                        Ok(targets
                            .into_iter()
                            .map(|(host, _port)| format!("{base}@{host}"))
                            .collect())
                    }
                },
            )
            .await
        })
    }
}
