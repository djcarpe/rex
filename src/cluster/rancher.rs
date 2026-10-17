use super::{poll_loop, Strategy};
use crate::native::BoxFuture;
use crate::node::Node;
use serde_json::Value;
use std::time::Duration;

/// `Cluster.Strategy.Rancher`: ask the Rancher metadata service for the
/// containers of our own service and connect to `node_basename@<ip>`.
pub struct Rancher {
    pub node_basename: String,
    /// Default `http://rancher-metadata`.
    pub metadata_url: String,
    pub interval: Duration,
    pub disconnect_missing: bool,
}

impl Rancher {
    pub fn new(node_basename: impl Into<String>) -> Self {
        Rancher {
            node_basename: node_basename.into(),
            metadata_url: "http://rancher-metadata".into(),
            interval: Duration::from_secs(5),
            disconnect_missing: true,
        }
    }
    pub fn metadata_url(mut self, url: impl Into<String>) -> Self {
        self.metadata_url = url.into();
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
}

pub(crate) fn nodes_from_service(body: &Value, base: &str) -> Vec<String> {
    body.get("containers")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|c| {
            c.get("ips")
                .and_then(Value::as_array)
                .and_then(|ips| ips.first())
                .and_then(Value::as_str)
        })
        .map(|ip| format!("{base}@{ip}"))
        .collect()
}

impl Strategy for Rancher {
    fn run(self: Box<Self>, node: Node) -> BoxFuture<'static, ()> {
        Box::pin(async move {
            let url = format!(
                "{}/latest/self/service",
                self.metadata_url.trim_end_matches('/')
            );
            let base = self.node_basename.clone();
            let client = reqwest::Client::builder()
                .timeout(Duration::from_secs(15))
                .build()
                .ok();
            poll_loop(
                node,
                "rancher",
                self.interval,
                self.disconnect_missing,
                move || {
                    let (url, base, client) = (url.clone(), base.clone(), client.clone());
                    async move {
                        let client = client.ok_or("http client unavailable")?;
                        let resp = client
                            .get(&url)
                            .header("accept", "application/json")
                            .send()
                            .await
                            .map_err(|e| e.to_string())?;
                        if !resp.status().is_success() {
                            return Err(format!("rancher metadata returned {}", resp.status()));
                        }
                        let body: Value = resp.json().await.map_err(|e| e.to_string())?;
                        Ok(nodes_from_service(&body, &base))
                    }
                },
            )
            .await
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_containers() {
        let v = serde_json::json!({"containers": [{"ips": ["10.1.0.2", "10.1.0.3"]}, {"ips": []}, {"ips": ["10.1.0.4"]}]});
        assert_eq!(
            nodes_from_service(&v, "app"),
            vec!["app@10.1.0.2", "app@10.1.0.4"]
        );
    }
}
