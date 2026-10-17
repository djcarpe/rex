//! `Cluster.Strategy.Kubernetes`: ask the Kubernetes API for the endpoints or
//! pods matching a label selector and connect to them.

use super::{poll_loop, Strategy};
use crate::native::BoxFuture;
use crate::node::Node;
use serde_json::Value;
use std::path::PathBuf;
use std::time::Duration;

/// Which API resource to list (`kubernetes_ip_lookup_mode`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IpLookupMode {
    /// `endpoints` with the label selector (labels of the *Service*). Default.
    Endpoints,
    /// `pods` with the label selector (labels of the pods).
    Pods,
}

/// How to build the host part of the node name (`mode`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeNameMode {
    /// `app@10.42.0.7`. Default; peers run with `--name app@$(POD_IP)`.
    Ip,
    /// `app@10-42-0-7.<namespace>.pod.cluster.local` (pod A record).
    Dns,
    /// `app@<pod hostname>.<service_name>.<namespace>.svc.cluster.local`
    /// (StatefulSets; peers run with `--name app@$(hostname -f)`).
    Hostname,
}

pub struct Kubernetes {
    /// `kubernetes_node_basename`
    pub node_basename: String,
    /// `kubernetes_selector`, e.g. `app=myapp`
    pub selector: String,
    /// `kubernetes_namespace`; defaults to the service account's namespace.
    pub namespace: Option<String>,
    pub ip_lookup_mode: IpLookupMode,
    pub mode: NodeNameMode,
    /// `kubernetes_service_name`, needed for [`NodeNameMode::Hostname`].
    pub service_name: Option<String>,
    /// `kubernetes_cluster_name` (default `cluster`, giving `cluster.local`).
    pub cluster_name: String,
    /// `kubernetes_master` (default `kubernetes.default.svc`).
    pub master: String,
    /// Full API base URL, overriding `master` and the scheme. Handy for
    /// `kubectl proxy` (`http://127.0.0.1:8001`) during development.
    pub api_url: Option<String>,
    /// `kubernetes_service_account_path`
    pub service_account_path: PathBuf,
    /// Bearer token; defaults to the service account token file.
    pub token: Option<String>,
    /// CA certificate PEM path; defaults to the service account `ca.crt`.
    pub ca_cert_path: Option<PathBuf>,
    pub insecure_skip_tls_verify: bool,
    /// `polling_interval`
    pub interval: Duration,
    pub disconnect_missing: bool,
}

impl Kubernetes {
    pub fn new(node_basename: impl Into<String>, selector: impl Into<String>) -> Self {
        Kubernetes {
            node_basename: node_basename.into(),
            selector: selector.into(),
            namespace: None,
            ip_lookup_mode: IpLookupMode::Endpoints,
            mode: NodeNameMode::Ip,
            service_name: None,
            cluster_name: "cluster".into(),
            master: "kubernetes.default.svc".into(),
            api_url: None,
            service_account_path: PathBuf::from("/var/run/secrets/kubernetes.io/serviceaccount"),
            token: None,
            ca_cert_path: None,
            insecure_skip_tls_verify: false,
            interval: Duration::from_secs(5),
            disconnect_missing: true,
        }
    }
    pub fn namespace(mut self, ns: impl Into<String>) -> Self {
        self.namespace = Some(ns.into());
        self
    }
    pub fn ip_lookup_mode(mut self, m: IpLookupMode) -> Self {
        self.ip_lookup_mode = m;
        self
    }
    pub fn mode(mut self, m: NodeNameMode) -> Self {
        self.mode = m;
        self
    }
    pub fn service_name(mut self, s: impl Into<String>) -> Self {
        self.service_name = Some(s.into());
        self
    }
    pub fn cluster_name(mut self, s: impl Into<String>) -> Self {
        self.cluster_name = s.into();
        self
    }
    pub fn master(mut self, s: impl Into<String>) -> Self {
        self.master = s.into();
        self
    }
    pub fn api_url(mut self, s: impl Into<String>) -> Self {
        self.api_url = Some(s.into());
        self
    }
    pub fn service_account_path(mut self, p: impl Into<PathBuf>) -> Self {
        self.service_account_path = p.into();
        self
    }
    pub fn token(mut self, t: impl Into<String>) -> Self {
        self.token = Some(t.into());
        self
    }
    pub fn ca_cert_path(mut self, p: impl Into<PathBuf>) -> Self {
        self.ca_cert_path = Some(p.into());
        self
    }
    pub fn insecure_skip_tls_verify(mut self, yes: bool) -> Self {
        self.insecure_skip_tls_verify = yes;
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

    fn cluster_domain(&self) -> String {
        std::env::var("CLUSTER_DOMAIN")
            .ok()
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| format!("{}.local", self.cluster_name))
    }

    /// `https://<master>` with libcluster's cluster-domain completion.
    fn base_url(&self) -> String {
        if let Some(u) = &self.api_url {
            return u.trim_end_matches('/').to_string();
        }
        let domain = self.cluster_domain();
        let master = if self.master.ends_with(&domain) || self.master.ends_with('.') {
            self.master.clone()
        } else {
            format!("{}.{}", self.master, domain)
        };
        format!("https://{master}")
    }

    fn read_sa_file(&self, name: &str) -> Option<String> {
        std::fs::read_to_string(self.service_account_path.join(name))
            .ok()
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
    }

    fn resolve_namespace(&self) -> Result<String, String> {
        self.namespace
            .clone()
            .or_else(|| self.read_sa_file("namespace"))
            .ok_or_else(|| "no namespace given and no service account namespace file".to_string())
    }

    fn build_client(&self) -> Result<reqwest::Client, String> {
        let mut b = reqwest::Client::builder().timeout(Duration::from_secs(15));
        if self.insecure_skip_tls_verify {
            b = b.danger_accept_invalid_certs(true);
        } else {
            let ca_path = self
                .ca_cert_path
                .clone()
                .unwrap_or_else(|| self.service_account_path.join("ca.crt"));
            if let Ok(pem) = std::fs::read(&ca_path) {
                let cert = reqwest::Certificate::from_pem(&pem)
                    .map_err(|e| format!("{}: {e}", ca_path.display()))?;
                b = b.add_root_certificate(cert);
            }
        }
        b.build().map_err(|e| e.to_string())
    }

    fn url(&self, namespace: &str) -> String {
        let resource = match self.ip_lookup_mode {
            IpLookupMode::Endpoints => "endpoints",
            IpLookupMode::Pods => "pods",
        };
        format!(
            "{}/api/v1/namespaces/{namespace}/{resource}?labelSelector={}",
            self.base_url(),
            urlencode(&self.selector)
        )
    }

    /// Turn an API response into node names.
    pub(crate) fn nodes_from_response(&self, body: &Value, namespace: &str) -> Vec<String> {
        parse_addresses(self.ip_lookup_mode, body, namespace)
            .into_iter()
            .filter_map(|a| self.format_node(&a))
            .collect()
    }

    fn format_node(&self, a: &Address) -> Option<String> {
        let domain = format!("{}.local", self.cluster_name);
        Some(match self.mode {
            NodeNameMode::Ip => format!("{}@{}", self.node_basename, a.ip),
            NodeNameMode::Dns => format!(
                "{}@{}.{}.pod.{}",
                self.node_basename,
                a.ip.replace(['.', ':'], "-"),
                a.namespace,
                domain
            ),
            NodeNameMode::Hostname => {
                let hostname = a.hostname.as_ref()?;
                let service = self.service_name.as_ref()?;
                format!(
                    "{}@{}.{}.{}.svc.{}",
                    self.node_basename, hostname, service, a.namespace, domain
                )
            }
        })
    }
}

#[derive(Debug, PartialEq)]
pub(crate) struct Address {
    pub ip: String,
    pub namespace: String,
    pub hostname: Option<String>,
}

pub(crate) fn parse_addresses(mode: IpLookupMode, body: &Value, default_ns: &str) -> Vec<Address> {
    let items = body
        .get("items")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let mut out = Vec::new();
    for item in items {
        match mode {
            IpLookupMode::Endpoints => {
                for subset in item
                    .get("subsets")
                    .and_then(Value::as_array)
                    .into_iter()
                    .flatten()
                {
                    for addr in subset
                        .get("addresses")
                        .and_then(Value::as_array)
                        .into_iter()
                        .flatten()
                    {
                        let Some(ip) = addr.get("ip").and_then(Value::as_str) else {
                            continue;
                        };
                        let ns = addr
                            .pointer("/targetRef/namespace")
                            .and_then(Value::as_str)
                            .unwrap_or(default_ns)
                            .to_string();
                        let hostname = addr
                            .get("hostname")
                            .and_then(Value::as_str)
                            .map(str::to_string);
                        out.push(Address {
                            ip: ip.to_string(),
                            namespace: ns,
                            hostname,
                        });
                    }
                }
            }
            IpLookupMode::Pods => {
                let Some(ip) = item.pointer("/status/podIP").and_then(Value::as_str) else {
                    continue;
                };
                let ns = item
                    .pointer("/metadata/namespace")
                    .and_then(Value::as_str)
                    .unwrap_or(default_ns)
                    .to_string();
                let hostname = item
                    .pointer("/spec/hostname")
                    .and_then(Value::as_str)
                    .map(str::to_string);
                out.push(Address {
                    ip: ip.to_string(),
                    namespace: ns,
                    hostname,
                });
            }
        }
    }
    out
}

fn urlencode(s: &str) -> String {
    let mut out = String::new();
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(b as char)
            }
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

impl Strategy for Kubernetes {
    fn run(self: Box<Self>, node: Node) -> BoxFuture<'static, ()> {
        Box::pin(async move {
            let cfg = std::sync::Arc::new(*self);
            let interval = cfg.interval;
            let disconnect = cfg.disconnect_missing;
            poll_loop(node, "kubernetes", interval, disconnect, move || {
                let cfg = cfg.clone();
                async move {
                    let namespace = cfg.resolve_namespace()?;
                    let client = cfg.build_client()?;
                    let mut req = client
                        .get(cfg.url(&namespace))
                        .header("accept", "application/json");
                    if let Some(token) = cfg.token.clone().or_else(|| cfg.read_sa_file("token")) {
                        req = req.bearer_auth(token);
                    }
                    let resp = req
                        .send()
                        .await
                        .map_err(|e| format!("kubernetes api: {e}"))?;
                    let status = resp.status();
                    if !status.is_success() {
                        let body = resp.text().await.unwrap_or_default();
                        return Err(format!(
                            "kubernetes api returned {status}: {}",
                            body.chars().take(200).collect::<String>()
                        ));
                    }
                    let body: Value = resp
                        .json()
                        .await
                        .map_err(|e| format!("kubernetes api json: {e}"))?;
                    Ok(cfg.nodes_from_response(&body, &namespace))
                }
            })
            .await
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn endpoints() -> Value {
        serde_json::json!({"items": [{"subsets": [{"addresses": [
            {"ip": "10.42.0.7", "hostname": "app-0", "targetRef": {"namespace": "demo", "name": "app-0"}},
            {"ip": "10.42.1.9", "targetRef": {"namespace": "demo", "name": "app-1"}}
        ]}]}]})
    }

    fn pods() -> Value {
        serde_json::json!({"items": [
            {"status": {"podIP": "10.42.0.7"}, "metadata": {"namespace": "demo"}, "spec": {"hostname": "app-0"}},
            {"status": {}, "metadata": {"namespace": "demo"}, "spec": {}}
        ]})
    }

    #[test]
    fn ip_mode() {
        let k = Kubernetes::new("app", "app=demo");
        assert_eq!(
            k.nodes_from_response(&endpoints(), "demo"),
            vec!["app@10.42.0.7", "app@10.42.1.9"]
        );
        assert_eq!(
            k.ip_lookup_mode(IpLookupMode::Pods)
                .nodes_from_response(&pods(), "x"),
            vec!["app@10.42.0.7"]
        );
    }

    #[test]
    fn dns_mode() {
        let k = Kubernetes::new("app", "app=demo")
            .mode(NodeNameMode::Dns)
            .ip_lookup_mode(IpLookupMode::Pods);
        assert_eq!(
            k.nodes_from_response(&pods(), "demo"),
            vec!["app@10-42-0-7.demo.pod.cluster.local"]
        );
    }

    #[test]
    fn hostname_mode_needs_hostname_and_service() {
        let k = Kubernetes::new("app", "app=demo")
            .mode(NodeNameMode::Hostname)
            .service_name("app");
        assert_eq!(
            k.nodes_from_response(&endpoints(), "demo"),
            vec!["app@app-0.app.demo.svc.cluster.local"]
        );
    }

    #[test]
    fn urls() {
        let k = Kubernetes::new("app", "app=demo,tier in (a,b)");
        assert_eq!(k.url("demo"), "https://kubernetes.default.svc.cluster.local/api/v1/namespaces/demo/endpoints?labelSelector=app%3Ddemo%2Ctier%20in%20%28a%2Cb%29");
        let k = Kubernetes::new("app", "a=b")
            .api_url("http://127.0.0.1:8001/")
            .ip_lookup_mode(IpLookupMode::Pods);
        assert_eq!(
            k.url("demo"),
            "http://127.0.0.1:8001/api/v1/namespaces/demo/pods?labelSelector=a%3Db"
        );
        let k = Kubernetes::new("app", "a=b").master("10.0.0.1.");
        assert!(k.url("demo").starts_with("https://10.0.0.1./api"));
    }
}
