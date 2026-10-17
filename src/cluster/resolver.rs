//! DNS lookups for the DNS-based strategies, on top of hickory-resolver.

use hickory_resolver::config::{NameServerConfig, ResolverConfig};
use hickory_resolver::name_server::TokioConnectionProvider;
use hickory_resolver::proto::xfer::Protocol;
use hickory_resolver::TokioResolver;
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;

/// A DNS resolver: either the system one (`/etc/resolv.conf`, including
/// search domains and `ndots`, which matters inside Kubernetes) or a fixed set
/// of nameservers.
#[derive(Clone)]
pub struct Resolver {
    inner: Arc<TokioResolver>,
}

impl Resolver {
    /// The system resolver.
    pub fn system() -> Result<Self, String> {
        let r = TokioResolver::builder_tokio()
            .map_err(|e| e.to_string())?
            .build();
        Ok(Resolver { inner: Arc::new(r) })
    }

    /// Query only these servers. `tcp` forces DNS over TCP (useful through a
    /// `kubectl port-forward`, which cannot carry UDP).
    pub fn with_nameservers(servers: &[SocketAddr], tcp: bool) -> Result<Self, String> {
        if servers.is_empty() {
            return Err("no nameservers given".into());
        }
        let mut cfg = ResolverConfig::new();
        for s in servers {
            cfg.add_name_server(NameServerConfig::new(
                *s,
                if tcp { Protocol::Tcp } else { Protocol::Udp },
            ));
        }
        let mut b = TokioResolver::builder_with_config(cfg, TokioConnectionProvider::default());
        b.options_mut().ndots = 0;
        Ok(Resolver {
            inner: Arc::new(b.build()),
        })
    }

    /// Parse `host:port` or `host` (port 53), optionally suffixed `/tcp`.
    pub fn parse_server(spec: &str) -> Result<(SocketAddr, bool), String> {
        let (spec, tcp) = match spec.strip_suffix("/tcp") {
            Some(s) => (s, true),
            None => (spec, false),
        };
        if let Ok(sa) = spec.parse::<SocketAddr>() {
            return Ok((sa, tcp));
        }
        if let Ok(ip) = spec.parse::<IpAddr>() {
            return Ok((SocketAddr::new(ip, 53), tcp));
        }
        Err(format!(
            "cannot parse nameserver {spec:?} (expected ip or ip:port)"
        ))
    }

    /// A and AAAA records.
    pub async fn lookup_ips(&self, name: &str) -> Result<Vec<IpAddr>, String> {
        let r = self
            .inner
            .lookup_ip(name)
            .await
            .map_err(|e| format!("lookup {name}: {e}"))?;
        Ok(r.iter().collect())
    }

    /// SRV records as `(target, port)`, targets without the trailing dot.
    pub async fn lookup_srv(&self, name: &str) -> Result<Vec<(String, u16)>, String> {
        let r = self
            .inner
            .srv_lookup(name)
            .await
            .map_err(|e| format!("srv lookup {name}: {e}"))?;
        Ok(r.iter()
            .map(|srv| {
                (
                    srv.target().to_utf8().trim_end_matches('.').to_string(),
                    srv.port(),
                )
            })
            .collect())
    }
}

/// `CLUSTER_DOMAIN` or `cluster.local`, as libcluster reads it.
pub(crate) fn cluster_domain() -> String {
    std::env::var("CLUSTER_DOMAIN")
        .ok()
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "cluster.local".to_string())
}
