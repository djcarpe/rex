use super::{try_connect, Strategy};
use crate::etf;
use crate::native::BoxFuture;
use crate::node::Node;
use crate::term::Term;
use aes::cipher::{block_padding::Pkcs7, BlockDecryptMut, BlockEncryptMut, KeyIvInit};
use sha2::{Digest, Sha256};
use std::net::{Ipv4Addr, SocketAddrV4};
use std::time::Duration;
use tokio::net::UdpSocket;

type Aes256CbcEnc = cbc::Encryptor<aes::Aes256>;
type Aes256CbcDec = cbc::Decryptor<aes::Aes256>;

const PRELUDE: &[u8] = b"heartbeat::";

/// `Cluster.Strategy.Gossip` compatible multicast discovery. Defaults match
/// libcluster 3.x: `233.252.1.32:45892`, TTL 1, no secret.
pub struct Gossip {
    pub port: u16,
    pub if_addr: Ipv4Addr,
    pub multicast_addr: Ipv4Addr,
    pub multicast_if: Option<Ipv4Addr>,
    pub multicast_ttl: u32,
    pub broadcast_only: bool,
    pub secret: Option<String>,
    /// Upper bound of the random delay between heartbeats (libcluster uses 5s).
    pub heartbeat_interval: Duration,
}

impl Default for Gossip {
    fn default() -> Self {
        Gossip {
            port: 45892,
            if_addr: Ipv4Addr::UNSPECIFIED,
            multicast_addr: Ipv4Addr::new(233, 252, 1, 32),
            multicast_if: None,
            multicast_ttl: 1,
            broadcast_only: false,
            secret: None,
            heartbeat_interval: Duration::from_secs(5),
        }
    }
}

impl Gossip {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn port(mut self, port: u16) -> Self {
        self.port = port;
        self
    }
    pub fn multicast_addr(mut self, addr: Ipv4Addr) -> Self {
        self.multicast_addr = addr;
        self
    }
    pub fn multicast_if(mut self, addr: Ipv4Addr) -> Self {
        self.multicast_if = Some(addr);
        self
    }
    pub fn if_addr(mut self, addr: Ipv4Addr) -> Self {
        self.if_addr = addr;
        self
    }
    pub fn multicast_ttl(mut self, ttl: u32) -> Self {
        self.multicast_ttl = ttl;
        self
    }
    pub fn broadcast_only(mut self, yes: bool) -> Self {
        self.broadcast_only = yes;
        self
    }
    /// Shared secret; heartbeats are then AES-256-CBC encrypted like libcluster's.
    pub fn secret(mut self, secret: impl Into<String>) -> Self {
        self.secret = Some(secret.into());
        self
    }

    fn open_socket(&self) -> std::io::Result<UdpSocket> {
        use socket2::{Domain, Protocol, Socket, Type};
        let sock = Socket::new(Domain::IPV4, Type::DGRAM, Some(Protocol::UDP))?;
        sock.set_reuse_address(true)?;
        #[cfg(all(unix, not(target_os = "solaris"), not(target_os = "illumos")))]
        sock.set_reuse_port(true)?;
        sock.set_broadcast(true)?;
        if !self.broadcast_only {
            sock.set_multicast_ttl_v4(self.multicast_ttl)?;
            sock.set_multicast_loop_v4(true)?;
            let iface = self.multicast_if.unwrap_or(Ipv4Addr::UNSPECIFIED);
            sock.join_multicast_v4(&self.multicast_addr, &iface)?;
            if let Some(mif) = self.multicast_if {
                sock.set_multicast_if_v4(&mif)?;
            }
        }
        sock.bind(&SocketAddrV4::new(self.if_addr, self.port).into())?;
        sock.set_nonblocking(true)?;
        UdpSocket::from_std(sock.into())
    }

    fn heartbeat_packet(&self, node: &Node) -> Vec<u8> {
        let term = Term::map(vec![(Term::atom("node"), Term::Atom(node.name().clone()))]);
        let mut plain = PRELUDE.to_vec();
        plain.extend(etf::encode(&term));
        match &self.secret {
            None => plain,
            Some(secret) => {
                let key = Sha256::digest(secret.as_bytes());
                let iv: [u8; 16] = rand::random();
                let ct =
                    Aes256CbcEnc::new(&key, &iv.into()).encrypt_padded_vec_mut::<Pkcs7>(&plain);
                let mut out = iv.to_vec();
                out.extend(ct);
                out
            }
        }
    }

    fn parse_heartbeat(&self, packet: &[u8]) -> Option<String> {
        let plain: Vec<u8> = match &self.secret {
            None => packet.to_vec(),
            Some(secret) => {
                if packet.len() < 32 {
                    return None;
                }
                let key = Sha256::digest(secret.as_bytes());
                let (iv, ct) = packet.split_at(16);
                Aes256CbcDec::new(&key, iv.into())
                    .decrypt_padded_vec_mut::<Pkcs7>(ct)
                    .ok()?
            }
        };
        let body = plain.strip_prefix(PRELUDE)?;
        let term = etf::decode(body).ok()?;
        let name = term.map_get_atom("node")?.as_atom()?;
        Some(name.as_str().to_string())
    }
}

impl Strategy for Gossip {
    fn run(self: Box<Self>, node: Node) -> BoxFuture<'static, ()> {
        Box::pin(async move {
            let socket = match self.open_socket() {
                Ok(s) => s,
                Err(e) => {
                    tracing::error!(
                        "gossip: cannot open multicast socket on {}:{}: {e}",
                        self.if_addr,
                        self.port
                    );
                    return;
                }
            };
            tracing::info!(
                "gossip: listening on {}:{} multicast {} (secret: {})",
                self.if_addr,
                self.port,
                self.multicast_addr,
                self.secret.is_some()
            );
            let target = SocketAddrV4::new(self.multicast_addr, self.port);
            let mut buf = vec![0u8; 65536];
            let mut next_beat = tokio::time::Instant::now();
            loop {
                tokio::select! {
                    _ = tokio::time::sleep_until(next_beat) => {
                        let pkt = self.heartbeat_packet(&node);
                        if let Err(e) = socket.send_to(&pkt, target).await {
                            tracing::debug!("gossip: heartbeat send failed: {e}");
                        }
                        let max = self.heartbeat_interval.as_millis().max(1) as u64;
                        let jitter = rand::random::<u64>() % max + 1;
                        next_beat = tokio::time::Instant::now() + Duration::from_millis(jitter);
                    }
                    r = socket.recv_from(&mut buf) => {
                        let Ok((n, _from)) = r else { continue };
                        if let Some(name) = self.parse_heartbeat(&buf[..n]) {
                            if name != node.name().as_str() {
                                tracing::trace!("gossip: heartbeat from {name}");
                                let node = node.clone();
                                tokio::spawn(async move { try_connect(&node, &name).await });
                            }
                        }
                    }
                }
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encrypted_roundtrip() {
        let g = Gossip::new().secret("pw");
        let key = Sha256::digest(b"pw");
        let iv = [7u8; 16];
        let mut plain = PRELUDE.to_vec();
        plain.extend(etf::encode(&Term::map(vec![(
            Term::atom("node"),
            Term::atom("x@y"),
        )])));
        let ct = Aes256CbcEnc::new(&key, &iv.into()).encrypt_padded_vec_mut::<Pkcs7>(&plain);
        let mut pkt = iv.to_vec();
        pkt.extend(ct);
        assert_eq!(g.parse_heartbeat(&pkt), Some("x@y".to_string()));
        assert_eq!(
            Gossip::new().parse_heartbeat(&plain),
            Some("x@y".to_string())
        );
        assert_eq!(Gossip::new().parse_heartbeat(b"garbage"), None);
    }
}
