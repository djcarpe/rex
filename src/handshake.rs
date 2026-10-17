//! The distribution handshake (OTP 23+ "version 6" flavour).
//!
//! Both roles are implemented: [`initiate`] when we connect out, [`accept`]
//! when a peer connects to our listener. Messages are framed with a 2-byte
//! length during the handshake; afterwards the connection switches to 4-byte
//! frames (see `connection.rs`).

use crate::error::{Error, Result};
use md5::{Digest, Md5};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

/// Distribution flags (`dist.hrl`).
pub mod dflags {
    pub const PUBLISHED: u64 = 0x01;
    pub const ATOM_CACHE: u64 = 0x02;
    pub const EXTENDED_REFERENCES: u64 = 0x04;
    pub const DIST_MONITOR: u64 = 0x08;
    pub const FUN_TAGS: u64 = 0x10;
    pub const DIST_MONITOR_NAME: u64 = 0x20;
    pub const HIDDEN_ATOM_CACHE: u64 = 0x40;
    pub const NEW_FUN_TAGS: u64 = 0x80;
    pub const EXTENDED_PIDS_PORTS: u64 = 0x100;
    pub const EXPORT_PTR_TAG: u64 = 0x200;
    pub const BIT_BINARIES: u64 = 0x400;
    pub const NEW_FLOATS: u64 = 0x800;
    pub const UNICODE_IO: u64 = 0x1000;
    pub const DIST_HDR_ATOM_CACHE: u64 = 0x2000;
    pub const SMALL_ATOM_TAGS: u64 = 0x4000;
    pub const UTF8_ATOMS: u64 = 0x10000;
    pub const MAP_TAG: u64 = 0x20000;
    pub const BIG_CREATION: u64 = 0x40000;
    pub const SEND_SENDER: u64 = 0x80000;
    pub const BIG_SEQTRACE_LABELS: u64 = 0x100000;
    pub const EXIT_PAYLOAD: u64 = 0x400000;
    pub const FRAGMENTS: u64 = 0x00800000;
    pub const HANDSHAKE_23: u64 = 0x01000000;
    pub const UNLINK_ID: u64 = 0x02000000;
    pub const MANDATORY_25_DIGEST: u64 = 0x04000000;
    pub const SPAWN: u64 = 0x01 << 32;
    pub const NAME_ME: u64 = 0x02 << 32;
    pub const V4_NC: u64 = 0x04 << 32;
    pub const ALIAS: u64 = 0x08 << 32;

    /// The flags `MANDATORY_25_DIGEST` stands for.
    pub const MANDATORY_25: u64 = EXTENDED_REFERENCES
        | FUN_TAGS
        | EXTENDED_PIDS_PORTS
        | NEW_FUN_TAGS
        | EXPORT_PTR_TAG
        | BIT_BINARIES
        | NEW_FLOATS
        | UTF8_ATOMS
        | MAP_TAG
        | BIG_CREATION
        | HANDSHAKE_23;

    /// What this node advertises. `DIST_HDR_ATOM_CACHE` and `FRAGMENTS` let
    /// peers use their atom cache (atoms arrive as one-byte references) and
    /// split large messages into interleavable fragments, both handled by
    /// `dist::Inbound`; we ourselves send headers with an empty atom table
    /// and never fragment. `EXIT_PAYLOAD` is on: large exit reasons (erpc
    /// results are exit reasons) travel as payload terms, which is much
    /// cheaper for the BEAM to receive.
    pub const OURS: u64 = MANDATORY_25
        | DIST_MONITOR
        | DIST_MONITOR_NAME
        | UNICODE_IO
        | SMALL_ATOM_TAGS
        | DIST_HDR_ATOM_CACHE
        | FRAGMENTS
        | UNLINK_ID
        | MANDATORY_25_DIGEST
        | EXIT_PAYLOAD
        | SPAWN
        | V4_NC
        | ALIAS;
}

/// Who the peer turned out to be.
#[derive(Debug, Clone)]
pub struct Peer {
    pub name: String,
    pub flags: u64,
    pub creation: u32,
}

/// What `accept` should answer to a connecting node's `send_name`, decided by
/// the node from its connection table.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Status {
    /// Proceed normally.
    Ok,
    /// Proceed; we had our own pending attempt towards the peer and drop it.
    OkSimultaneous,
    /// Refuse: the peer should keep its own pending attempt instead.
    Nok,
    /// Refuse for policy reasons.
    NotAllowed,
    /// We believe we are already connected; ask the peer to confirm.
    Alive,
}

async fn send(stream: &mut TcpStream, body: &[u8]) -> Result<()> {
    let mut frame = Vec::with_capacity(body.len() + 2);
    frame.extend_from_slice(&(body.len() as u16).to_be_bytes());
    frame.extend_from_slice(body);
    stream.write_all(&frame).await?;
    Ok(())
}

async fn recv(stream: &mut TcpStream) -> Result<Vec<u8>> {
    let len = stream.read_u16().await? as usize;
    let mut buf = vec![0u8; len];
    stream.read_exact(&mut buf).await?;
    Ok(buf)
}

fn digest(cookie: &str, challenge: u32) -> [u8; 16] {
    let mut h = Md5::new();
    h.update(cookie.as_bytes());
    h.update(challenge.to_string().as_bytes());
    h.finalize().into()
}

fn fail(node: &str, reason: impl Into<String>) -> Error {
    Error::Handshake {
        node: node.to_string(),
        reason: reason.into(),
    }
}

fn name_frame(tag: u8, flags: u64, extra: &[u8], creation: u32, name: &str) -> Vec<u8> {
    let mut b = vec![tag];
    b.extend_from_slice(&flags.to_be_bytes());
    b.extend_from_slice(extra);
    b.extend_from_slice(&creation.to_be_bytes());
    b.extend_from_slice(&(name.len() as u16).to_be_bytes());
    b.extend_from_slice(name.as_bytes());
    b
}

fn normalise_flags(flags: u64) -> u64 {
    if flags & dflags::MANDATORY_25_DIGEST != 0 {
        flags | dflags::MANDATORY_25
    } else {
        flags
    }
}

/// Parse an `N` frame shared by `send_name` and `send_challenge`.
fn parse_n(
    frame: &[u8],
    with_challenge: bool,
    node: &str,
) -> Result<(u64, Option<u32>, u32, String)> {
    let fixed = if with_challenge {
        1 + 8 + 4 + 4 + 2
    } else {
        1 + 8 + 4 + 2
    };
    if frame.len() < fixed {
        return Err(fail(node, "short N frame"));
    }
    let flags = u64::from_be_bytes(frame[1..9].try_into().unwrap());
    let mut pos = 9;
    let challenge = if with_challenge {
        let c = u32::from_be_bytes(frame[pos..pos + 4].try_into().unwrap());
        pos += 4;
        Some(c)
    } else {
        None
    };
    let creation = u32::from_be_bytes(frame[pos..pos + 4].try_into().unwrap());
    pos += 4;
    let nlen = u16::from_be_bytes(frame[pos..pos + 2].try_into().unwrap()) as usize;
    pos += 2;
    let name = frame
        .get(pos..pos + nlen)
        .ok_or_else(|| fail(node, "short name"))?;
    let name = std::str::from_utf8(name)
        .map_err(|_| fail(node, "node name is not utf-8"))?
        .to_string();
    Ok((normalise_flags(flags), challenge, creation, name))
}

/// Connect-side handshake: we have opened `stream` to a peer found via EPMD.
pub async fn initiate(
    stream: &mut TcpStream,
    this_name: &str,
    this_flags: u64,
    this_creation: u32,
    cookie: &str,
    expected_peer: &str,
) -> Result<Peer> {
    send(
        stream,
        &name_frame(b'N', this_flags, &[], this_creation, this_name),
    )
    .await?;

    // status
    let status = recv(stream).await?;
    if status.first() != Some(&b's') {
        return Err(fail(expected_peer, "expected status message"));
    }
    match &status[1..] {
        b"ok" | b"ok_simultaneous" => {}
        b"alive" => {
            // The peer thinks it is already connected to us. Our table says
            // otherwise (we are initiating), so tell it to continue.
            send(stream, b"strue").await?;
        }
        b"nok" => {
            return Err(fail(
                expected_peer,
                "peer is connecting to us simultaneously (nok)",
            ))
        }
        b"not_allowed" => {
            return Err(fail(
                expected_peer,
                "peer refused the connection (not_allowed)",
            ))
        }
        other => {
            return Err(fail(
                expected_peer,
                format!("unexpected status {:?}", String::from_utf8_lossy(other)),
            ))
        }
    }

    // challenge
    let frame = recv(stream).await?;
    if frame.first() != Some(&b'N') {
        return Err(fail(
            expected_peer,
            "expected N challenge (old 'n' handshake is not supported)",
        ));
    }
    let (peer_flags, challenge, peer_creation, peer_name) = parse_n(&frame, true, expected_peer)?;
    let their_challenge = challenge.unwrap();

    // challenge reply
    let our_challenge: u32 = rand::random();
    let mut reply = vec![b'r'];
    reply.extend_from_slice(&our_challenge.to_be_bytes());
    reply.extend_from_slice(&digest(cookie, their_challenge));
    send(stream, &reply).await?;

    // challenge ack
    let ack = recv(stream).await.map_err(|_| {
        fail(
            &peer_name,
            "peer closed before challenge ack (cookie mismatch?)",
        )
    })?;
    if ack.len() != 17 || ack[0] != b'a' {
        return Err(fail(&peer_name, "expected challenge ack"));
    }
    if ack[1..] != digest(cookie, our_challenge) {
        return Err(fail(&peer_name, "challenge ack mismatch: cookies differ"));
    }
    Ok(Peer {
        name: peer_name,
        flags: peer_flags,
        creation: peer_creation,
    })
}

/// Accept-side handshake. `decide` is consulted with the peer's name once it
/// is known so the node can detect simultaneous connects.
pub async fn accept<F>(
    stream: &mut TcpStream,
    this_name: &str,
    this_flags: u64,
    this_creation: u32,
    cookie: &str,
    decide: F,
) -> Result<Peer>
where
    F: FnOnce(&str) -> Status,
{
    let frame = recv(stream).await?;
    match frame.first() {
        Some(&b'N') => {}
        Some(&b'n') => {
            return Err(fail(
                "?",
                "peer used the pre-OTP-23 'n' handshake, which is not supported",
            ))
        }
        _ => return Err(fail("?", "expected send_name")),
    }
    let (peer_flags, _, peer_creation, peer_name) = parse_n(&frame, false, "?")?;
    if peer_flags & dflags::NAME_ME != 0 {
        send(stream, b"snot_allowed").await?;
        return Err(fail(
            &peer_name,
            "dynamic node names (NAME_ME) are not supported",
        ));
    }

    let status = decide(&peer_name);
    let status_bytes: &[u8] = match status {
        Status::Ok => b"sok",
        Status::OkSimultaneous => b"sok_simultaneous",
        Status::Nok => b"snok",
        Status::NotAllowed => b"snot_allowed",
        Status::Alive => b"salive",
    };
    send(stream, status_bytes).await?;
    match status {
        Status::Nok => return Err(fail(&peer_name, "rejected simultaneous connect (nok)")),
        Status::NotAllowed => return Err(fail(&peer_name, "not allowed")),
        Status::Alive => {
            let reply = recv(stream).await?;
            if reply != b"strue" {
                return Err(fail(
                    &peer_name,
                    "peer declined to replace the existing connection",
                ));
            }
        }
        _ => {}
    }

    let our_challenge: u32 = rand::random();
    send(
        stream,
        &name_frame(
            b'N',
            this_flags,
            &our_challenge.to_be_bytes(),
            this_creation,
            this_name,
        ),
    )
    .await?;

    let reply = recv(stream).await?;
    if reply.len() != 21 || reply[0] != b'r' {
        return Err(fail(&peer_name, "expected challenge reply"));
    }
    let their_challenge = u32::from_be_bytes(reply[1..5].try_into().unwrap());
    if reply[5..] != digest(cookie, our_challenge) {
        // Erlang just closes the socket here; do the same after logging.
        return Err(fail(&peer_name, "challenge reply mismatch: cookies differ"));
    }
    let mut ack = vec![b'a'];
    ack.extend_from_slice(&digest(cookie, their_challenge));
    send(stream, &ack).await?;
    Ok(Peer {
        name: peer_name,
        flags: peer_flags,
        creation: peer_creation,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn md5_digest_matches_erlang() {
        // erlang:md5("cookie" ++ integer_to_list(12345))
        let d = digest("cookie", 12345);
        assert_eq!(
            d.iter().map(|b| format!("{b:02x}")).collect::<String>(),
            format!("{:x}", Md5::digest(b"cookie12345"))
        );
    }
}
