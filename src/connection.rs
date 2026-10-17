//! One established distribution connection: framing, ticks and dispatch.

use crate::dist::Inbound;
use crate::node::Node;
use crate::term::Atom;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::mpsc;

/// Largest frame we accept, to bound memory on a misbehaving peer.
const MAX_FRAME: usize = 256 * 1024 * 1024;

pub(crate) async fn run(
    node: Node,
    peer: Atom,
    stream: TcpStream,
    mut rx: mpsc::UnboundedReceiver<Vec<u8>>,
    ticktime: Duration,
    generation: u64,
) {
    let _ = stream.set_nodelay(true);
    let (mut rd, mut wr) = stream.into_split();
    let tick_every = ticktime / 4;
    let idle_limit = ticktime + ticktime / 4;

    let writer = async move {
        let mut ticker = tokio::time::interval(tick_every);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        ticker.tick().await; // first tick fires immediately; skip it
        loop {
            tokio::select! {
                frame = rx.recv() => {
                    let Some(mut frame) = frame else { break };
                    // Control::encode left 4 bytes for the length prefix.
                    let len = (frame.len() - 4) as u32;
                    frame[..4].copy_from_slice(&len.to_be_bytes());
                    if wr.write_all(&frame).await.is_err() {
                        break;
                    }
                }
                _ = ticker.tick() => {
                    if wr.write_all(&[0, 0, 0, 0]).await.is_err() {
                        break;
                    }
                }
            }
        }
        let _ = wr.shutdown().await;
    };

    let reader_node = node.clone();
    let reader_peer = peer.clone();
    let reader = async move {
        let mut inbound = Inbound::new(MAX_FRAME);
        loop {
            let len = match tokio::time::timeout(idle_limit, rd.read_u32()).await {
                Ok(Ok(len)) => len as usize,
                Ok(Err(e)) => {
                    tracing::debug!(peer = %reader_peer, "connection closed: {e}");
                    break;
                }
                Err(_) => {
                    tracing::warn!(peer = %reader_peer, "no traffic for {:?}, dropping connection", idle_limit);
                    break;
                }
            };
            if len == 0 {
                continue; // tick
            }
            if len > MAX_FRAME {
                tracing::warn!(peer = %reader_peer, "frame of {len} bytes exceeds limit");
                break;
            }
            let mut body = vec![0u8; len];
            if let Err(e) = rd.read_exact(&mut body).await {
                tracing::debug!(peer = %reader_peer, "connection closed mid-frame: {e}");
                break;
            }
            match inbound.frame(&body) {
                Ok(Some(ctrl)) => {
                    tracing::trace!(peer = %reader_peer, "<- {ctrl:?}");
                    reader_node.handle_control(&reader_peer, ctrl);
                }
                Ok(None) => {} // fragment of a larger message
                Err(e) => tracing::warn!(peer = %reader_peer, "undecodable frame: {e}"),
            }
        }
    };

    tokio::select! {
        _ = writer => {}
        _ = reader => {}
    }
    node.connection_closed(&peer, generation);
}
