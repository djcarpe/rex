use super::{poll_loop, Strategy};
use crate::epmd;
use crate::native::BoxFuture;
use crate::node::Node;
use std::net::IpAddr;
use std::path::PathBuf;
use std::time::Duration;

/// Connect to every node registered with the local EPMD; the equivalent of
/// `Cluster.Strategy.LocalEpmd`. Node names are `<name>@<our host>`.
pub struct LocalEpmd {
    pub interval: Duration,
}

impl Default for LocalEpmd {
    fn default() -> Self {
        LocalEpmd {
            interval: Duration::from_secs(5),
        }
    }
}

impl LocalEpmd {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn interval(mut self, interval: Duration) -> Self {
        self.interval = interval;
        self
    }
}

impl Strategy for LocalEpmd {
    fn run(self: Box<Self>, node: Node) -> BoxFuture<'static, ()> {
        Box::pin(async move {
            let host = node.host_part();
            let port = epmd::epmd_port();
            let n = node.clone();
            poll_loop(node, "local_epmd", self.interval, false, move || {
                let host = host.clone();
                let _ = &n;
                async move {
                    let names = epmd::names(IpAddr::from([127, 0, 0, 1]), port)
                        .await
                        .map_err(|e| e.to_string())?;
                    Ok(names
                        .into_iter()
                        .map(|(name, _)| format!("{name}@{host}"))
                        .collect())
                }
            })
            .await
        })
    }
}

/// Connect to every node on every host listed in a `.hosts.erlang` file, by
/// asking each host's EPMD; the equivalent of `Cluster.Strategy.ErlangHosts`
/// and of `net_adm:world/0`.
///
/// The file holds one host per line as an Erlang term: `'host.example.com'.`
/// It is looked for in the current directory, then `$HOME`, unless a path is
/// given.
pub struct ErlangHosts {
    pub path: Option<PathBuf>,
    pub interval: Duration,
}

impl Default for ErlangHosts {
    fn default() -> Self {
        ErlangHosts {
            path: None,
            interval: Duration::from_secs(5),
        }
    }
}

impl ErlangHosts {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn path(mut self, path: impl Into<PathBuf>) -> Self {
        self.path = Some(path.into());
        self
    }
    pub fn interval(mut self, interval: Duration) -> Self {
        self.interval = interval;
        self
    }

    fn locate(&self) -> Option<PathBuf> {
        if let Some(p) = &self.path {
            return Some(p.clone());
        }
        let cwd = PathBuf::from(".hosts.erlang");
        if cwd.exists() {
            return Some(cwd);
        }
        let home = std::env::var_os("HOME")?;
        let p = PathBuf::from(home).join(".hosts.erlang");
        p.exists().then_some(p)
    }
}

/// Parse `.hosts.erlang` contents: `'host'.`, `"host".` or `host.` per term,
/// `%` comments. Quoted names may contain dots.
pub(crate) fn parse_hosts_file(contents: &str) -> Vec<String> {
    let mut hosts = Vec::new();
    let mut chars = contents.chars().peekable();
    let mut word = String::new();
    let flush = |word: &mut String, hosts: &mut Vec<String>| {
        if !word.is_empty() {
            hosts.push(std::mem::take(word));
        }
    };
    while let Some(c) = chars.next() {
        match c {
            '%' => {
                flush(&mut word, &mut hosts);
                for c in chars.by_ref() {
                    if c == '\n' {
                        break;
                    }
                }
            }
            '\'' | '"' => {
                flush(&mut word, &mut hosts);
                let mut quoted = String::new();
                for q in chars.by_ref() {
                    if q == c {
                        break;
                    }
                    quoted.push(q);
                }
                if !quoted.is_empty() {
                    hosts.push(quoted);
                }
            }
            '.' | ',' | '[' | ']' => flush(&mut word, &mut hosts),
            c if c.is_whitespace() => flush(&mut word, &mut hosts),
            c => word.push(c),
        }
    }
    flush(&mut word, &mut hosts);
    hosts
}

async fn names_on_host(host: &str, port: u16) -> Result<Vec<String>, String> {
    let ip = crate::node::resolve_host(host)
        .await
        .map_err(|e| e.to_string())?;
    let names = tokio::time::timeout(Duration::from_secs(5), epmd::names(ip, port))
        .await
        .map_err(|_| format!("EPMD on {host} timed out"))?
        .map_err(|e| e.to_string())?;
    Ok(names
        .into_iter()
        .map(|(n, _)| format!("{n}@{host}"))
        .collect())
}

impl Strategy for ErlangHosts {
    fn run(self: Box<Self>, node: Node) -> BoxFuture<'static, ()> {
        Box::pin(async move {
            let port = epmd::epmd_port();
            let me = self.clone_cfg();
            poll_loop(node, "erlang_hosts", self.interval, false, move || {
                let me = me.clone_cfg();
                async move {
                    let Some(path) = me.locate() else {
                        return Err("no .hosts.erlang file found".to_string());
                    };
                    let contents = std::fs::read_to_string(&path)
                        .map_err(|e| format!("{}: {e}", path.display()))?;
                    let mut all = Vec::new();
                    for host in parse_hosts_file(&contents) {
                        match names_on_host(&host, port).await {
                            Ok(names) => all.extend(names),
                            Err(e) => tracing::debug!(host, "EPMD names failed: {e}"),
                        }
                    }
                    Ok(all)
                }
            })
            .await
        })
    }
}

impl ErlangHosts {
    fn clone_cfg(&self) -> ErlangHosts {
        ErlangHosts {
            path: self.path.clone(),
            interval: self.interval,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_hosts_file() {
        let f = "'host1.example.com'.\n\"host2\". % comment\n% whole line comment\nhost3.\n\n'a'. 'b'.\n";
        assert_eq!(
            parse_hosts_file(f),
            vec!["host1.example.com", "host2", "host3", "a", "b"]
        );
    }
}
