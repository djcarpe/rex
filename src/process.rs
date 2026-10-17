//! Lightweight local "processes": each is just a pid plus a channel. Remote
//! nodes can send to them, link to them and monitor them like any Erlang
//! process.

use crate::dist::ProcRef;
use crate::node::Node;
use crate::term::{Pid, Reference, Term};
use std::collections::{HashMap, HashSet};
use std::time::Duration;
use tokio::sync::mpsc;

/// Something that arrived for a local process.
#[derive(Debug, Clone, PartialEq)]
pub enum Signal {
    /// An ordinary message (`Pid ! Msg`, `send/2`, `GenServer.cast`...).
    Message(Term),
    /// An exit signal from a linked process, or `Process.exit(pid, reason)`.
    Exit { from: Pid, reason: Term },
    /// A monitored process went down (`{'DOWN', Ref, process, Object, Reason}`).
    Down {
        reference: Reference,
        object: Term,
        reason: Term,
    },
}

pub(crate) struct Monitor {
    /// The remote process watching us.
    pub monitoring: Pid,
    pub reference: Reference,
    /// How the remote referred to us (pid or registered name); echoed back in
    /// the `MONITOR_P_EXIT`.
    pub object: ProcRef,
}

pub(crate) struct ProcessEntry {
    pub tx: mpsc::UnboundedSender<Signal>,
    pub links: HashSet<Pid>,
    pub monitors: Vec<Monitor>,
}

#[derive(Default)]
pub(crate) struct ProcessTable {
    pub entries: HashMap<u32, ProcessEntry>,
    pub names: HashMap<crate::term::Atom, u32>,
    /// Monitors *we* hold on remote processes: reference -> (local pid, remote node).
    pub outgoing_monitors: HashMap<Reference, (Pid, crate::term::Atom)>,
}

/// The receiving end of a local process. Dropping it makes the process exit
/// with reason `normal`, notifying linked and monitoring remote processes.
pub struct Mailbox {
    pid: Pid,
    rx: mpsc::UnboundedReceiver<Signal>,
    node: Node,
    exited: bool,
}

impl Mailbox {
    pub(crate) fn new(pid: Pid, rx: mpsc::UnboundedReceiver<Signal>, node: Node) -> Self {
        Mailbox {
            pid,
            rx,
            node,
            exited: false,
        }
    }

    pub fn pid(&self) -> &Pid {
        &self.pid
    }

    /// Wait for the next signal. `None` once the node has shut down.
    pub async fn recv(&mut self) -> Option<Signal> {
        self.rx.recv().await
    }

    /// Wait for the next plain message, skipping exit and down signals.
    pub async fn recv_message(&mut self) -> Option<Term> {
        loop {
            match self.rx.recv().await? {
                Signal::Message(m) => return Some(m),
                _ => continue,
            }
        }
    }

    /// Like [`recv`](Self::recv) with a deadline. `Ok(None)` means the node
    /// shut down, `Err(Timeout)` that nothing arrived in time.
    pub async fn recv_timeout(&mut self, timeout: Duration) -> crate::Result<Option<Signal>> {
        tokio::time::timeout(timeout, self.rx.recv())
            .await
            .map_err(|_| crate::Error::Timeout)
    }

    /// Non-blocking receive.
    pub fn try_recv(&mut self) -> Option<Signal> {
        self.rx.try_recv().ok()
    }

    /// Register a name for this process so remote nodes can reach it as
    /// `{name, 'rex@host'}`.
    pub fn register(&self, name: &str) -> crate::Result<()> {
        self.node.register_name(name, &self.pid)
    }

    /// End the process with an explicit exit reason.
    pub fn exit(mut self, reason: Term) {
        self.exited = true;
        self.node.process_exited(&self.pid, reason);
    }
}

impl Drop for Mailbox {
    fn drop(&mut self) {
        if !self.exited {
            self.exited = true;
            self.node.process_exited(&self.pid, Term::atom("normal"));
        }
    }
}
