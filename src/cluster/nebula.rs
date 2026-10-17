//! `Cluster.Strategy.Nebula` (the `libcluster_nebula` package): discover peers
//! on a Nebula overlay and refuse any address outside the overlay range.

use super::resolver::Resolver;
use super::{poll_loop, Strategy};
use crate::native::BoxFuture;
use crate::node::Node;
use std::net::{IpAddr, SocketAddr};
use std::path::PathBuf;
use std::time::Duration;

/// An IPv4 or IPv6 range in CIDR notation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Cidr {
    addr: IpAddr,
    prefix: u8,
}

impl Cidr {
    pub fn parse(s: &str) -> Result<Cidr, String> {
        let (a, p) = s
            .split_once('/')
            .ok_or_else(|| format!("{s:?}: expected addr/prefix"))?;
        let addr: IpAddr = a.parse().map_err(|_| format!("{s:?}: bad address"))?;
        let prefix: u8 = p.parse().map_err(|_| format!("{s:?}: bad prefix"))?;
        let max = if addr.is_ipv4() { 32 } else { 128 };
        if prefix > max {
            return Err(format!("{s:?}: prefix > {max}"));
        }
        Ok(Cidr { addr, prefix })
    }

    pub fn contains(&self, ip: &IpAddr) -> bool {
        match (self.addr, ip) {
            (IpAddr::V4(net), IpAddr::V4(ip)) => {
                let mask = if self.prefix == 0 {
                    0
                } else {
                    u32::MAX << (32 - self.prefix)
                };
                (u32::from(net) & mask) == (u32::from(*ip) & mask)
            }
            (IpAddr::V6(net), IpAddr::V6(ip)) => {
                let mask = if self.prefix == 0 {
                    0
                } else {
                    u128::MAX << (128 - self.prefix)
                };
                (u128::from(net) & mask) == (u128::from(*ip) & mask)
            }
            _ => false,
        }
    }
}

/// Where to learn who is on the mesh (`discovery` / `discovery_opts`).
#[derive(Debug, Clone)]
pub enum NebulaDiscovery {
    /// Ask a lighthouse's built-in DNS (`serve_dns: true`) for each certificate
    /// name; only hosts that have checked in resolve. Lighthouses are tried in
    /// order, the first that answers wins. `NXDOMAIN` means "not here now".
    LighthouseDns {
        lighthouses: Vec<SocketAddr>,
        hosts: Vec<String>,
    },
    /// Your own DNS zone: every A/AAAA record behind each name is a peer.
    Dns {
        hosts: Vec<String>,
        nameservers: Vec<SocketAddr>,
    },
    /// The keys of `static_host_map` (and `lighthouse.hosts`) in the local
    /// Nebula `config.yml`.
    StaticHostMap {
        path: PathBuf,
        include_lighthouses: bool,
        exclude: Vec<String>,
    },
}

/// How to name peers (`node_naming`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NebulaNaming {
    /// `basename@<overlay ip>` (default).
    Ip,
    /// `basename@<certificate name>`; only meaningful with `LighthouseDns`.
    Name,
}

pub struct Nebula {
    pub node_basename: String,
    /// Overlay ranges. Discovered addresses outside every range are dropped.
    pub cidrs: Vec<Cidr>,
    pub discovery: NebulaDiscovery,
    pub naming: NebulaNaming,
    pub interval: Duration,
    pub disconnect_missing: bool,
}

impl Nebula {
    pub fn new(node_basename: impl Into<String>, discovery: NebulaDiscovery) -> Self {
        Nebula {
            node_basename: node_basename.into(),
            cidrs: Vec::new(),
            discovery,
            naming: NebulaNaming::Ip,
            interval: Duration::from_secs(5),
            disconnect_missing: false,
        }
    }

    /// Restrict peers to this overlay range (repeatable).
    pub fn cidr(mut self, cidr: &str) -> Result<Self, String> {
        self.cidrs.push(Cidr::parse(cidr)?);
        Ok(self)
    }
    pub fn naming(mut self, naming: NebulaNaming) -> Self {
        self.naming = naming;
        self
    }
    pub fn interval(mut self, interval: Duration) -> Self {
        self.interval = interval;
        self
    }
    pub fn disconnect_missing(mut self, yes: bool) -> Self {
        self.disconnect_missing = yes;
        self
    }

    fn in_range(&self, ip: &IpAddr) -> bool {
        self.cidrs.is_empty() || self.cidrs.iter().any(|c| c.contains(ip))
    }

    /// `(certificate name or dns name, ip)` pairs to node names, applying the
    /// CIDR check and naming mode.
    pub(crate) fn format(&self, hosts: Vec<(String, IpAddr)>) -> Vec<String> {
        let mut out = Vec::new();
        for (name, ip) in hosts {
            if !self.in_range(&ip) {
                tracing::warn!(strategy = "nebula", %ip, host = %name, "address outside the overlay range, dropped");
                continue;
            }
            let node = match self.naming {
                NebulaNaming::Ip => format!("{}@{}", self.node_basename, ip),
                NebulaNaming::Name => format!("{}@{}", self.node_basename, name),
            };
            if !out.contains(&node) {
                out.push(node);
            }
        }
        out
    }

    async fn discover(&self) -> Result<Vec<(String, IpAddr)>, String> {
        match &self.discovery {
            NebulaDiscovery::LighthouseDns { lighthouses, hosts } => {
                let mut found = Vec::new();
                for host in hosts {
                    let mut answered = false;
                    for lh in lighthouses {
                        let resolver = Resolver::with_nameservers(&[*lh], false)?;
                        match tokio::time::timeout(
                            Duration::from_secs(3),
                            resolver.lookup_ips(&format!("{host}.")),
                        )
                        .await
                        {
                            Ok(Ok(ips)) => {
                                for ip in ips {
                                    found.push((host.clone(), ip));
                                }
                                answered = true;
                                break;
                            }
                            Ok(Err(e))
                                if e.contains("no record")
                                    || e.contains("NXDomain")
                                    || e.contains("no records") =>
                            {
                                // Not checked in with this lighthouse: absent, not an error.
                                answered = true;
                                break;
                            }
                            Ok(Err(e)) => {
                                tracing::debug!(strategy = "nebula", lighthouse = %lh, host, "lookup failed: {e}")
                            }
                            Err(_) => {
                                tracing::debug!(strategy = "nebula", lighthouse = %lh, host, "lookup timed out")
                            }
                        }
                    }
                    if !answered {
                        tracing::warn!(strategy = "nebula", host, "no lighthouse answered");
                    }
                }
                Ok(found)
            }
            NebulaDiscovery::Dns { hosts, nameservers } => {
                let resolver = if nameservers.is_empty() {
                    Resolver::system()?
                } else {
                    Resolver::with_nameservers(nameservers, false)?
                };
                let mut found = Vec::new();
                for host in hosts {
                    match resolver.lookup_ips(host).await {
                        Ok(ips) => found.extend(ips.into_iter().map(|ip| (host.clone(), ip))),
                        Err(e) => tracing::debug!(strategy = "nebula", host, "lookup failed: {e}"),
                    }
                }
                Ok(found)
            }
            NebulaDiscovery::StaticHostMap {
                path,
                include_lighthouses,
                exclude,
            } => {
                let contents = std::fs::read_to_string(path)
                    .map_err(|e| format!("{}: {e}", path.display()))?;
                Ok(parse_static_host_map(&contents, *include_lighthouses)
                    .into_iter()
                    .filter(|ip| !exclude.iter().any(|x| x == &ip.to_string()))
                    .map(|ip| (ip.to_string(), ip))
                    .collect())
            }
        }
    }
}

/// Pull overlay addresses out of a Nebula `config.yml` without a YAML parser:
/// the keys under `static_host_map:` and the list items under
/// `lighthouse: hosts:`. Values (underlay `host:port`) are ignored on purpose.
pub(crate) fn parse_static_host_map(yaml: &str, include_lighthouses: bool) -> Vec<IpAddr> {
    #[derive(PartialEq)]
    enum Section {
        None,
        StaticHostMap(usize),
        Lighthouse(usize),
        LighthouseHosts(usize),
    }
    let mut section = Section::None;
    let mut out = Vec::new();
    for raw in yaml.lines() {
        let line = raw.split('#').next().unwrap_or("");
        if line.trim().is_empty() {
            continue;
        }
        let indent = line.len() - line.trim_start().len();
        let t = line.trim();
        // Leaving a section when indentation returns to its level or less.
        match section {
            Section::StaticHostMap(i) | Section::Lighthouse(i)
                if indent <= i && !t.starts_with('-') =>
            {
                section = Section::None
            }
            Section::LighthouseHosts(i) if indent <= i && !t.starts_with('-') => {
                section = Section::Lighthouse(i.saturating_sub(2))
            }
            _ => {}
        }
        match section {
            Section::None => {
                if t.starts_with("static_host_map:") {
                    section = Section::StaticHostMap(indent);
                } else if t.starts_with("lighthouse:") {
                    section = Section::Lighthouse(indent);
                }
            }
            Section::StaticHostMap(_) => {
                if let Some((key, _)) = t.split_once(':') {
                    let key = key.trim().trim_matches(|c| c == '"' || c == '\'');
                    if let Ok(ip) = key.parse::<IpAddr>() {
                        out.push(ip);
                    }
                }
            }
            Section::Lighthouse(_) => {
                if t.starts_with("hosts:") {
                    section = Section::LighthouseHosts(indent);
                    // inline list: hosts: ["a", "b"]
                    if let Some(rest) = t.strip_prefix("hosts:") {
                        for item in rest
                            .trim()
                            .trim_matches(|c| c == '[' || c == ']')
                            .split(',')
                        {
                            if let Ok(ip) = item
                                .trim()
                                .trim_matches(|c| c == '"' || c == '\'')
                                .parse::<IpAddr>()
                            {
                                if include_lighthouses {
                                    out.push(ip);
                                }
                            }
                        }
                    }
                }
            }
            Section::LighthouseHosts(_) => {
                if let Some(item) = t.strip_prefix('-') {
                    if let Ok(ip) = item
                        .trim()
                        .trim_matches(|c| c == '"' || c == '\'')
                        .parse::<IpAddr>()
                    {
                        if include_lighthouses {
                            out.push(ip);
                        }
                    }
                }
            }
        }
    }
    out.dedup();
    out
}

impl Strategy for Nebula {
    fn run(self: Box<Self>, node: Node) -> BoxFuture<'static, ()> {
        Box::pin(async move {
            let cfg = std::sync::Arc::new(*self);
            if let Ok(ip) = node.host_part().parse::<IpAddr>() {
                if !cfg.in_range(&ip) {
                    tracing::warn!(strategy = "nebula", node = %node.name(), "this node's own address is outside the overlay range; peers will not be able to connect back");
                }
            }
            let interval = cfg.interval;
            let disconnect = cfg.disconnect_missing;
            poll_loop(node, "nebula", interval, disconnect, move || {
                let cfg = cfg.clone();
                async move { Ok(cfg.format(cfg.discover().await?)) }
            })
            .await
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cidr_contains() {
        let c = Cidr::parse("192.168.100.0/24").unwrap();
        assert!(c.contains(&"192.168.100.7".parse().unwrap()));
        assert!(!c.contains(&"192.168.101.7".parse().unwrap()));
        assert!(!c.contains(&"::1".parse().unwrap()));
        let c6 = Cidr::parse("fd00::/8").unwrap();
        assert!(c6.contains(&"fd00::1".parse().unwrap()));
        assert!(Cidr::parse("0.0.0.0/0")
            .unwrap()
            .contains(&"8.8.8.8".parse().unwrap()));
        assert!(Cidr::parse("1.2.3.4/33").is_err());
    }

    #[test]
    fn static_host_map_parsing() {
        let yaml = r#"
pki:
  ca: /etc/nebula/ca.crt
static_host_map:
  "192.168.100.1": ["203.0.113.1:4242"]
  '192.168.100.2':
    - "203.0.113.2:4242"
lighthouse:
  am_lighthouse: false
  hosts:
    - "192.168.100.1"
    - 192.168.100.3
listen:
  host: 0.0.0.0
"#;
        let ips = parse_static_host_map(yaml, true);
        let s: Vec<String> = ips.iter().map(|i| i.to_string()).collect();
        assert_eq!(
            s,
            vec![
                "192.168.100.1",
                "192.168.100.2",
                "192.168.100.1",
                "192.168.100.3"
            ]
        );
        let ips = parse_static_host_map(yaml, false);
        assert_eq!(ips.len(), 2);
    }

    #[test]
    fn cidr_filter_and_naming() {
        let n = Nebula::new(
            "app",
            NebulaDiscovery::Dns {
                hosts: vec![],
                nameservers: vec![],
            },
        )
        .cidr("192.168.100.0/24")
        .unwrap();
        let hosts = vec![
            ("web1".to_string(), "192.168.100.5".parse().unwrap()),
            ("evil".to_string(), "10.0.0.1".parse().unwrap()),
        ];
        assert_eq!(n.format(hosts.clone()), vec!["app@192.168.100.5"]);
        let n = n.naming(NebulaNaming::Name);
        assert_eq!(n.format(hosts), vec!["app@web1"]);
    }
}
