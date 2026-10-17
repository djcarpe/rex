//! A minimal EPMD (Erlang Port Mapper Daemon) client.
//!
//! EPMD maps node names to listening ports on each host. We register our own
//! listener (`ALIVE2_REQ`) and look peers up (`PORT_PLEASE2_REQ`).

use crate::error::{Error, Result};
use std::net::{IpAddr, SocketAddr};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

pub const DEFAULT_PORT: u16 = 4369;

const ALIVE2_REQ: u8 = 120;
const ALIVE2_RESP: u8 = 121;
const ALIVE2_X_RESP: u8 = 118;
const PORT_PLEASE2_REQ: u8 = 122;
const PORT2_RESP: u8 = 119;
const NAMES_REQ: u8 = 110;

pub const NODE_TYPE_NORMAL: u8 = 77;
pub const NODE_TYPE_HIDDEN: u8 = 72;
const PROTOCOL_TCP_IPV4: u8 = 0;
const DIST_VERSION: u16 = 6;

/// The port EPMD listens on, honouring `ERL_EPMD_PORT` like Erlang does.
pub fn epmd_port() -> u16 {
    std::env::var("ERL_EPMD_PORT")
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(DEFAULT_PORT)
}

/// A live registration. EPMD forgets us as soon as this socket closes, so keep
/// it alive for the lifetime of the node.
pub struct Registration {
    _stream: TcpStream,
    pub creation: u32,
}

/// Information EPMD holds about a registered node.
#[derive(Debug, Clone)]
pub struct NodeInfo {
    pub port: u16,
    pub node_type: u8,
    pub protocol: u8,
    pub highest_version: u16,
    pub lowest_version: u16,
    pub name: String,
    pub extra: Vec<u8>,
}

async fn send_req(stream: &mut TcpStream, body: &[u8]) -> Result<()> {
    let mut frame = Vec::with_capacity(body.len() + 2);
    frame.extend_from_slice(&(body.len() as u16).to_be_bytes());
    frame.extend_from_slice(body);
    stream.write_all(&frame).await?;
    Ok(())
}

/// Register `alive_name` (the part before `@`) as listening on `port`.
pub async fn register(
    epmd: SocketAddr,
    alive_name: &str,
    port: u16,
    hidden: bool,
) -> Result<Registration> {
    let mut stream = TcpStream::connect(epmd)
        .await
        .map_err(|e| Error::Epmd(format!("cannot connect to EPMD at {epmd}: {e}")))?;
    let mut body = Vec::new();
    body.push(ALIVE2_REQ);
    body.extend_from_slice(&port.to_be_bytes());
    body.push(if hidden {
        NODE_TYPE_HIDDEN
    } else {
        NODE_TYPE_NORMAL
    });
    body.push(PROTOCOL_TCP_IPV4);
    body.extend_from_slice(&DIST_VERSION.to_be_bytes());
    body.extend_from_slice(&DIST_VERSION.to_be_bytes());
    body.extend_from_slice(&(alive_name.len() as u16).to_be_bytes());
    body.extend_from_slice(alive_name.as_bytes());
    body.extend_from_slice(&0u16.to_be_bytes());
    send_req(&mut stream, &body).await?;

    let tag = stream.read_u8().await?;
    let result = stream.read_u8().await?;
    let creation = match tag {
        ALIVE2_X_RESP => stream.read_u32().await?,
        ALIVE2_RESP => stream.read_u16().await? as u32,
        other => {
            return Err(Error::Epmd(format!(
                "unexpected ALIVE2 response tag {other}"
            )))
        }
    };
    if result != 0 {
        return Err(Error::Epmd(format!(
            "EPMD refused to register {alive_name:?} (result {result}); is another node using that name?"
        )));
    }
    Ok(Registration {
        _stream: stream,
        creation,
    })
}

/// Ask the EPMD on `host` where `alive_name` listens.
pub async fn lookup(host: IpAddr, epmd_port: u16, alive_name: &str) -> Result<Option<NodeInfo>> {
    let addr = SocketAddr::new(host, epmd_port);
    let mut stream = tokio::time::timeout(Duration::from_secs(5), TcpStream::connect(addr))
        .await
        .map_err(|_| Error::Epmd(format!("timeout connecting to EPMD at {addr}")))?
        .map_err(|e| Error::Epmd(format!("cannot connect to EPMD at {addr}: {e}")))?;
    let mut body = vec![PORT_PLEASE2_REQ];
    body.extend_from_slice(alive_name.as_bytes());
    send_req(&mut stream, &body).await?;

    let mut resp = Vec::new();
    stream.read_to_end(&mut resp).await?;
    if resp.len() < 2 || resp[0] != PORT2_RESP {
        return Err(Error::Epmd("malformed PORT2_RESP".into()));
    }
    if resp[1] != 0 {
        return Ok(None);
    }
    if resp.len() < 12 {
        return Err(Error::Epmd("short PORT2_RESP".into()));
    }
    let port = u16::from_be_bytes([resp[2], resp[3]]);
    let node_type = resp[4];
    let protocol = resp[5];
    let highest_version = u16::from_be_bytes([resp[6], resp[7]]);
    let lowest_version = u16::from_be_bytes([resp[8], resp[9]]);
    let nlen = u16::from_be_bytes([resp[10], resp[11]]) as usize;
    let name = String::from_utf8_lossy(resp.get(12..12 + nlen).unwrap_or(&[])).into_owned();
    let extra = resp
        .get(14 + nlen..)
        .map(|e| e.to_vec())
        .unwrap_or_default();
    Ok(Some(NodeInfo {
        port,
        node_type,
        protocol,
        highest_version,
        lowest_version,
        name,
        extra,
    }))
}

/// List `(name, port)` pairs registered with the EPMD on `host`.
pub async fn names(host: IpAddr, epmd_port: u16) -> Result<Vec<(String, u16)>> {
    let addr = SocketAddr::new(host, epmd_port);
    let mut stream = TcpStream::connect(addr).await?;
    send_req(&mut stream, &[NAMES_REQ]).await?;
    let mut resp = Vec::new();
    stream.read_to_end(&mut resp).await?;
    if resp.len() < 4 {
        return Ok(Vec::new());
    }
    let text = String::from_utf8_lossy(&resp[4..]);
    Ok(text
        .lines()
        .filter_map(|l| {
            // "name foo at port 12345"
            let mut it = l.split_whitespace();
            let (Some("name"), Some(n), Some("at"), Some("port"), Some(p)) =
                (it.next(), it.next(), it.next(), it.next(), it.next())
            else {
                return None;
            };
            Some((n.to_string(), p.parse().ok()?))
        })
        .collect())
}

/// Is something listening on the local EPMD port?
pub async fn is_running(epmd_port: u16) -> bool {
    let addr = SocketAddr::new(IpAddr::from([127, 0, 0, 1]), epmd_port);
    tokio::time::timeout(Duration::from_secs(2), TcpStream::connect(addr))
        .await
        .map(|r| r.is_ok())
        .unwrap_or(false)
}

/// Start `epmd -daemon` if nothing answers on the EPMD port, the way `erl`
/// does. Returns true if EPMD is reachable afterwards.
pub async fn ensure_running(epmd_port: u16) -> bool {
    let addr = SocketAddr::new(IpAddr::from([127, 0, 0, 1]), epmd_port);
    if is_running(epmd_port).await {
        return true;
    }
    tracing::info!("EPMD not running on {addr}, trying `epmd -daemon`");
    let mut cmd = tokio::process::Command::new("epmd");
    cmd.arg("-daemon");
    if epmd_port != DEFAULT_PORT {
        cmd.arg("-port").arg(epmd_port.to_string());
    }
    if let Err(e) = cmd.status().await {
        tracing::info!("no epmd binary available ({e})");
        return false;
    }
    for _ in 0..20 {
        if TcpStream::connect(addr).await.is_ok() {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    false
}

// ---------------------------------------------------------------------------
// Embedded EPMD server
// ---------------------------------------------------------------------------

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tokio::net::TcpListener;

#[derive(Debug, Clone)]
struct Entry {
    port: u16,
    node_type: u8,
    protocol: u8,
    highest_version: u16,
    lowest_version: u16,
    extra: Vec<u8>,
}

/// A minimal EPMD, for hosts and containers that have no Erlang installation.
/// It answers `ALIVE2`, `PORT_PLEASE2`, `NAMES` and `DUMP`, which is all a
/// peer needs to find us. Registrations live as long as the registering
/// connection, like the real EPMD. Dropping the server stops it.
pub struct EpmdServer {
    handle: tokio::task::JoinHandle<()>,
    pub port: u16,
}

impl Drop for EpmdServer {
    fn drop(&mut self) {
        self.handle.abort();
    }
}

/// Serve EPMD on `0.0.0.0:port` (and `[::]` is left to the OS).
pub async fn serve(port: u16) -> Result<EpmdServer> {
    let listener = TcpListener::bind(SocketAddr::new(
        IpAddr::V4(std::net::Ipv4Addr::UNSPECIFIED),
        port,
    ))
    .await
    .map_err(|e| Error::Epmd(format!("cannot bind embedded EPMD on port {port}: {e}")))?;
    let port = listener.local_addr()?.port();
    let table: Arc<Mutex<HashMap<String, Entry>>> = Arc::new(Mutex::new(HashMap::new()));
    tracing::info!(port, "serving embedded EPMD");
    let handle = tokio::spawn(async move {
        loop {
            let Ok((stream, _)) = listener.accept().await else {
                tokio::time::sleep(Duration::from_millis(100)).await;
                continue;
            };
            let table = table.clone();
            tokio::spawn(async move {
                if let Err(e) = serve_client(stream, table, port).await {
                    tracing::debug!("epmd client: {e}");
                }
            });
        }
    });
    Ok(EpmdServer { handle, port })
}

async fn serve_client(
    mut stream: TcpStream,
    table: Arc<Mutex<HashMap<String, Entry>>>,
    epmd_port: u16,
) -> Result<()> {
    let len = stream.read_u16().await? as usize;
    let mut body = vec![0u8; len];
    stream.read_exact(&mut body).await?;
    let Some((&tag, rest)) = body.split_first() else {
        return Ok(());
    };
    match tag {
        ALIVE2_REQ => {
            if rest.len() < 12 {
                return Ok(());
            }
            let port = u16::from_be_bytes([rest[0], rest[1]]);
            let node_type = rest[2];
            let protocol = rest[3];
            let highest_version = u16::from_be_bytes([rest[4], rest[5]]);
            let lowest_version = u16::from_be_bytes([rest[6], rest[7]]);
            let nlen = u16::from_be_bytes([rest[8], rest[9]]) as usize;
            let name = String::from_utf8_lossy(rest.get(10..10 + nlen).unwrap_or(&[])).into_owned();
            let extra = rest
                .get(12 + nlen..)
                .map(|e| e.to_vec())
                .unwrap_or_default();
            let registered = {
                let mut t = table.lock().unwrap();
                if t.contains_key(&name) {
                    false
                } else {
                    t.insert(
                        name.clone(),
                        Entry {
                            port,
                            node_type,
                            protocol,
                            highest_version,
                            lowest_version,
                            extra,
                        },
                    );
                    true
                }
            };
            let mut resp = vec![ALIVE2_X_RESP, if registered { 0 } else { 1 }];
            let creation: u32 = rand::random::<u32>().max(4);
            resp.extend_from_slice(&creation.to_be_bytes());
            stream.write_all(&resp).await?;
            if !registered {
                tracing::info!(name, "embedded EPMD refused duplicate registration");
                return Ok(());
            }
            tracing::info!(name, port, "embedded EPMD: node registered");
            // Keep the registration until the client goes away.
            let mut sink = [0u8; 64];
            loop {
                match stream.read(&mut sink).await {
                    Ok(0) | Err(_) => break,
                    Ok(_) => {}
                }
            }
            table.lock().unwrap().remove(&name);
            tracing::info!(name, "embedded EPMD: node unregistered");
        }
        PORT_PLEASE2_REQ => {
            let name = String::from_utf8_lossy(rest).into_owned();
            let entry = table.lock().unwrap().get(&name).cloned();
            let mut resp = vec![PORT2_RESP];
            match entry {
                None => resp.push(1),
                Some(e) => {
                    resp.push(0);
                    resp.extend_from_slice(&e.port.to_be_bytes());
                    resp.push(e.node_type);
                    resp.push(e.protocol);
                    resp.extend_from_slice(&e.highest_version.to_be_bytes());
                    resp.extend_from_slice(&e.lowest_version.to_be_bytes());
                    resp.extend_from_slice(&(name.len() as u16).to_be_bytes());
                    resp.extend_from_slice(name.as_bytes());
                    resp.extend_from_slice(&(e.extra.len() as u16).to_be_bytes());
                    resp.extend_from_slice(&e.extra);
                }
            }
            stream.write_all(&resp).await?;
        }
        NAMES_REQ | DUMP_REQ => {
            let mut resp = (epmd_port as u32).to_be_bytes().to_vec();
            let snapshot: Vec<(String, u16)> = table
                .lock()
                .unwrap()
                .iter()
                .map(|(n, e)| (n.clone(), e.port))
                .collect();
            for (name, port) in snapshot {
                if tag == NAMES_REQ {
                    resp.extend_from_slice(format!("name {name} at port {port}\n").as_bytes());
                } else {
                    resp.extend_from_slice(
                        format!("active name\t<{name}> at port {port}, fd = 0\n").as_bytes(),
                    );
                }
            }
            stream.write_all(&resp).await?;
        }
        KILL_REQ => {
            // Never let a peer kill us; pretend like a "-relaxed_command_check" epmd would not.
            stream.write_all(b"NO").await?;
        }
        _ => {}
    }
    let _ = stream.shutdown().await;
    Ok(())
}

const DUMP_REQ: u8 = 100;
const KILL_REQ: u8 = 107;

#[cfg(test)]
mod server_tests {
    use super::*;

    #[tokio::test]
    async fn register_lookup_names() {
        let server = serve(0).await.unwrap();
        let addr = SocketAddr::new(IpAddr::V4(std::net::Ipv4Addr::LOCALHOST), server.port);
        let reg = register(addr, "alpha", 4242, true).await.unwrap();
        assert!(reg.creation >= 4);
        let info = lookup(addr.ip(), server.port, "alpha")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(info.port, 4242);
        assert_eq!(info.node_type, NODE_TYPE_HIDDEN);
        assert_eq!(info.highest_version, 6);
        assert!(lookup(addr.ip(), server.port, "beta")
            .await
            .unwrap()
            .is_none());
        assert!(
            register(addr, "alpha", 1, true).await.is_err(),
            "duplicate must be refused"
        );
        assert_eq!(
            names(addr.ip(), server.port).await.unwrap(),
            vec![("alpha".to_string(), 4242)]
        );
        drop(reg);
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(
            lookup(addr.ip(), server.port, "alpha")
                .await
                .unwrap()
                .is_none(),
            "registration dies with its socket"
        );
    }
}
