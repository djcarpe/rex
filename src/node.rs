//! The node: EPMD registration, listener, connection table, local processes
//! and the public API for talking to the cluster.

use crate::builtins;
use crate::connection;
use crate::dist::{Control, ProcRef};
use crate::epmd;
use crate::error::{Error, Result};
use crate::handshake::{self, dflags, Status};
use crate::native::{CallContext, CallResult, Exception, NativeModule};
use crate::process::{Mailbox, Monitor, ProcessEntry, ProcessTable, Signal};
use crate::term::{Atom, Pid, Reference, Term};
use std::collections::{HashMap, HashSet};
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{broadcast, mpsc};
use tokio::task::JoinHandle;

/// Connection lifecycle notifications, like `:net_kernel.monitor_nodes/1`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NodeEvent {
    NodeUp(Atom),
    NodeDown(Atom),
}

/// Options for [`Node::spawn_request`].
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SpawnOpts {
    /// Link the new process to the requesting mailbox.
    pub link: bool,
    /// Monitor the new process; the request id is the monitor reference.
    pub monitor: bool,
}

/// Where a message or call goes: a pid, or a registered name on a node.
#[derive(Debug, Clone, PartialEq)]
pub enum Dest {
    Pid(Pid),
    Name { name: Atom, node: Atom },
}

impl Dest {
    pub fn named(name: impl Into<Atom>, node: impl Into<Atom>) -> Dest {
        Dest::Name {
            name: name.into(),
            node: node.into(),
        }
    }
    fn node(&self) -> &Atom {
        match self {
            Dest::Pid(p) => &p.node,
            Dest::Name { node, .. } => node,
        }
    }
    fn proc_ref(&self) -> ProcRef {
        match self {
            Dest::Pid(p) => ProcRef::Pid(p.clone()),
            Dest::Name { name, .. } => ProcRef::Name(name.clone()),
        }
    }
}

impl From<Pid> for Dest {
    fn from(p: Pid) -> Self {
        Dest::Pid(p)
    }
}
impl From<&Pid> for Dest {
    fn from(p: &Pid) -> Self {
        Dest::Pid(p.clone())
    }
}
impl From<(&str, &str)> for Dest {
    fn from((name, node): (&str, &str)) -> Self {
        Dest::named(name, node)
    }
}
impl From<(Atom, Atom)> for Dest {
    fn from((name, node): (Atom, Atom)) -> Self {
        Dest::Name { name, node }
    }
}

struct ConnHandle {
    tx: mpsc::UnboundedSender<Vec<u8>>,
    generation: u64,
    /// The peer's distribution flags.
    flags: u64,
}

pub(crate) struct Inner {
    name: Atom,
    alive_name: String,
    cookie: String,
    flags: u64,
    creation: u32,
    net_ticktime: Duration,
    epmd_port: u16,
    listen_port: u16,
    connections: Mutex<HashMap<Atom, ConnHandle>>,
    pending: Mutex<HashSet<Atom>>,
    processes: Mutex<ProcessTable>,
    modules: RwLock<HashMap<String, Arc<dyn NativeModule>>>,
    events: broadcast::Sender<NodeEvent>,
    next_pid: AtomicU32,
    next_ref: AtomicU64,
    next_generation: AtomicU64,
    system_pid: Pid,
    epmd: Mutex<Option<epmd::Registration>>,
    epmd_server: Mutex<Option<epmd::EpmdServer>>,
    listener: Mutex<Option<JoinHandle<()>>>,
    shutdown: std::sync::atomic::AtomicBool,
}

/// Configuration for a node. Start it with [`NodeBuilder::start`].
pub struct NodeBuilder {
    name: String,
    cookie: Option<String>,
    hidden: bool,
    listen_addr: IpAddr,
    listen_port: u16,
    epmd_port: u16,
    start_epmd: bool,
    embedded_epmd: bool,
    net_ticktime: Duration,
    modules: Vec<Arc<dyn NativeModule>>,
}

impl NodeBuilder {
    /// `name` is the full node name, `alive@host`. Match the peer's naming
    /// mode: a short host (`rex@desktop`) for `--sname` nodes, a fully
    /// qualified host or IP (`rex@10.0.0.5`) for `--name` nodes.
    pub fn new(name: impl Into<String>) -> Self {
        NodeBuilder {
            name: name.into(),
            cookie: None,
            hidden: true,
            listen_addr: IpAddr::V4(Ipv4Addr::UNSPECIFIED),
            listen_port: 0,
            epmd_port: epmd::epmd_port(),
            start_epmd: true,
            embedded_epmd: true,
            net_ticktime: Duration::from_secs(60),
            modules: Vec::new(),
        }
    }

    /// The magic cookie. Defaults to the contents of `~/.erlang.cookie`.
    pub fn cookie(mut self, cookie: impl Into<String>) -> Self {
        self.cookie = Some(cookie.into());
        self
    }

    /// Register as a hidden node (the default, like C and Java nodes). Hidden
    /// nodes show up in `Node.list(:hidden)` and are not gossiped to other
    /// nodes by `global`.
    pub fn hidden(mut self, hidden: bool) -> Self {
        self.hidden = hidden;
        self
    }

    /// Register as a visible node: appears in `Node.list()`. Note that `global`
    /// on peers will then try to talk to a `global_name_server` here, which
    /// this crate does not implement; most clusters are fine with that but
    /// `:global.sync/0` will stall.
    pub fn visible(self) -> Self {
        self.hidden(false)
    }

    pub fn listen_addr(mut self, addr: IpAddr) -> Self {
        self.listen_addr = addr;
        self
    }

    /// Port for incoming distribution connections (0 = ephemeral).
    pub fn listen_port(mut self, port: u16) -> Self {
        self.listen_port = port;
        self
    }

    pub fn epmd_port(mut self, port: u16) -> Self {
        self.epmd_port = port;
        self
    }

    /// Run `epmd -daemon` if no EPMD is reachable (default true).
    pub fn start_epmd(mut self, yes: bool) -> Self {
        self.start_epmd = yes;
        self
    }

    /// If no EPMD is reachable and none can be started, serve one from this
    /// process (default true). This is what makes a rex container work without
    /// any Erlang installed in it.
    pub fn embedded_epmd(mut self, yes: bool) -> Self {
        self.embedded_epmd = yes;
        self
    }

    /// Must match the peers' `net_ticktime` (default 60s).
    pub fn net_ticktime(mut self, ticktime: Duration) -> Self {
        self.net_ticktime = ticktime;
        self
    }

    /// Register a native module before the node starts.
    pub fn module(mut self, module: impl NativeModule) -> Self {
        self.modules.push(Arc::new(module));
        self
    }

    pub async fn start(self) -> Result<Node> {
        let (alive, host) = self
            .name
            .split_once('@')
            .filter(|(a, h)| !a.is_empty() && !h.is_empty())
            .ok_or_else(|| Error::InvalidNodeName(self.name.clone()))?;
        let _ = host;
        let cookie = match self.cookie {
            Some(c) => c,
            None => read_cookie_file().ok_or(Error::NoCookie)?,
        };
        let mut epmd_server = None;
        let epmd_up = if self.start_epmd {
            epmd::ensure_running(self.epmd_port).await
        } else {
            epmd::is_running(self.epmd_port).await
        };
        if !epmd_up && self.embedded_epmd {
            epmd_server = Some(epmd::serve(self.epmd_port).await?);
        }
        let listener =
            TcpListener::bind(SocketAddr::new(self.listen_addr, self.listen_port)).await?;
        let port = listener.local_addr()?.port();
        let registration = epmd::register(
            SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), self.epmd_port),
            alive,
            port,
            self.hidden,
        )
        .await?;
        let creation = registration.creation;
        let name = Atom::new(self.name.clone());
        let mut flags = dflags::OURS;
        if !self.hidden {
            flags |= dflags::PUBLISHED;
        }
        let system_pid = Pid {
            node: name.clone(),
            id: 0,
            serial: 0,
            creation,
        };
        let (events, _) = broadcast::channel(256);
        let inner = Arc::new(Inner {
            name,
            alive_name: alive.to_string(),
            cookie,
            flags,
            creation,
            net_ticktime: self.net_ticktime,
            epmd_port: self.epmd_port,
            listen_port: port,
            connections: Mutex::new(HashMap::new()),
            pending: Mutex::new(HashSet::new()),
            processes: Mutex::new(ProcessTable::default()),
            modules: RwLock::new(HashMap::new()),
            events,
            next_pid: AtomicU32::new(1),
            next_ref: AtomicU64::new(rand::random::<u32>() as u64),
            next_generation: AtomicU64::new(1),
            system_pid,
            epmd: Mutex::new(Some(registration)),
            epmd_server: Mutex::new(epmd_server),
            listener: Mutex::new(None),
            shutdown: std::sync::atomic::AtomicBool::new(false),
        });
        let node = Node { inner };

        // The system process owns the builtin names so peers can monitor them.
        {
            let (tx, rx) = mpsc::unbounded_channel();
            std::mem::forget(rx); // never read; sends succeed and are dropped
            let mut t = node.inner.processes.lock().unwrap();
            t.entries.insert(
                0,
                ProcessEntry {
                    tx,
                    links: HashSet::new(),
                    monitors: Vec::new(),
                },
            );
            for n in ["rex", "net_kernel"] {
                t.names.insert(Atom::new(n), 0);
            }
        }
        node.register_module(builtins::rex_module());
        node.register_module(builtins::erlang_module());
        for m in self.modules {
            node.register_module_arc(m);
        }

        let accept_node = node.clone();
        let handle = tokio::spawn(async move {
            loop {
                match listener.accept().await {
                    Ok((stream, addr)) => {
                        let n = accept_node.clone();
                        tokio::spawn(async move {
                            if let Err(e) = n.accept_connection(stream).await {
                                tracing::info!("incoming connection from {addr} failed: {e}");
                            }
                        });
                    }
                    Err(e) => {
                        tracing::error!("accept failed: {e}");
                        tokio::time::sleep(Duration::from_millis(100)).await;
                    }
                }
            }
        });
        *node.inner.listener.lock().unwrap() = Some(handle);
        tracing::info!(node = %node.inner.name, port, creation, hidden = self.hidden, "node started");
        Ok(node)
    }
}

fn read_cookie_file() -> Option<String> {
    let home = std::env::var_os("HOME")?;
    let path = std::path::Path::new(&home).join(".erlang.cookie");
    let s = std::fs::read_to_string(path).ok()?;
    let s = s.trim().to_string();
    if s.is_empty() {
        None
    } else {
        Some(s)
    }
}

/// A running node. Cheap to clone; all clones share the same node.
#[derive(Clone)]
pub struct Node {
    pub(crate) inner: Arc<Inner>,
}

impl Node {
    pub fn builder(name: impl Into<String>) -> NodeBuilder {
        NodeBuilder::new(name)
    }

    /// Our full node name.
    pub fn name(&self) -> &Atom {
        &self.inner.name
    }

    pub fn creation(&self) -> u32 {
        self.inner.creation
    }

    /// The port our distribution listener is bound to.
    pub fn listen_port(&self) -> u16 {
        self.inner.listen_port
    }

    pub fn cookie(&self) -> &str {
        &self.inner.cookie
    }

    /// A pid standing for the node itself (used as sender for system replies).
    pub fn system_pid(&self) -> &Pid {
        &self.inner.system_pid
    }

    /// Subscribe to node up/down events.
    pub fn events(&self) -> broadcast::Receiver<NodeEvent> {
        self.inner.events.subscribe()
    }

    /// Names of currently connected peers.
    pub fn nodes(&self) -> Vec<Atom> {
        let mut v: Vec<_> = self
            .inner
            .connections
            .lock()
            .unwrap()
            .keys()
            .cloned()
            .collect();
        v.sort();
        v
    }

    pub fn is_connected(&self, node: &Atom) -> bool {
        self.inner.connections.lock().unwrap().contains_key(node)
    }

    // ---- native modules ----------------------------------------------------

    pub fn register_module(&self, module: impl NativeModule) {
        self.register_module_arc(Arc::new(module));
    }

    pub fn register_module_arc(&self, module: Arc<dyn NativeModule>) {
        let name = module.name().to_string();
        tracing::debug!(module = %name, "registered native module");
        self.inner.modules.write().unwrap().insert(name, module);
    }

    pub fn unregister_module(&self, name: &str) -> bool {
        self.inner.modules.write().unwrap().remove(name).is_some()
    }

    pub fn module_names(&self) -> Vec<String> {
        let mut v: Vec<_> = self.inner.modules.read().unwrap().keys().cloned().collect();
        v.sort();
        v
    }

    pub fn module_exports(&self, name: &str) -> Option<Vec<(String, Option<usize>)>> {
        self.inner
            .modules
            .read()
            .unwrap()
            .get(name)
            .map(|m| m.exports())
    }

    /// Invoke `module:function(args)` on a native module, as a remote caller would.
    pub async fn apply(
        &self,
        module: &Atom,
        function: &Atom,
        args: Vec<Term>,
        ctx: CallContext,
    ) -> CallResult {
        let m = self
            .inner
            .modules
            .read()
            .unwrap()
            .get(module.as_str())
            .cloned();
        match m {
            Some(m) => m.call(function.as_str(), args, ctx).await,
            None => Err(Exception::undef()),
        }
    }

    // ---- connections -------------------------------------------------------

    /// Connect to `name@host` through its EPMD. Succeeds immediately if already
    /// connected.
    pub async fn connect(&self, node: &str) -> Result<()> {
        let node_atom = Atom::new(node);
        if node_atom == self.inner.name {
            return Ok(());
        }
        if self.is_connected(&node_atom) {
            return Ok(());
        }
        let (alive, host) = node
            .split_once('@')
            .ok_or_else(|| Error::InvalidNodeName(node.to_string()))?;
        if !self.inner.pending.lock().unwrap().insert(node_atom.clone()) {
            // Another attempt is in flight; wait for it.
            for _ in 0..100 {
                tokio::time::sleep(Duration::from_millis(50)).await;
                if self.is_connected(&node_atom) {
                    return Ok(());
                }
                if !self.inner.pending.lock().unwrap().contains(&node_atom) {
                    break;
                }
            }
            return if self.is_connected(&node_atom) {
                Ok(())
            } else {
                Err(Error::NotConnected(node_atom))
            };
        }
        let result = self.connect_inner(&node_atom, alive, host).await;
        self.inner.pending.lock().unwrap().remove(&node_atom);
        result
    }

    async fn connect_inner(&self, node: &Atom, alive: &str, host: &str) -> Result<()> {
        let ip = resolve_host(host).await?;
        let info = epmd::lookup(ip, self.inner.epmd_port, alive)
            .await?
            .ok_or_else(|| Error::NodeNotFound(node.clone()))?;
        let addr = SocketAddr::new(ip, info.port);
        tracing::debug!(%node, %addr, "connecting");
        let mut stream = tokio::time::timeout(Duration::from_secs(10), TcpStream::connect(addr))
            .await
            .map_err(|_| Error::Timeout)??;
        let peer = tokio::time::timeout(
            Duration::from_secs(10),
            handshake::initiate(
                &mut stream,
                self.inner.name.as_str(),
                self.inner.flags,
                self.inner.creation,
                &self.inner.cookie,
                node.as_str(),
            ),
        )
        .await
        .map_err(|_| Error::Timeout)??;
        if peer.name != node.as_str() {
            return Err(Error::Handshake {
                node: node.as_str().to_string(),
                reason: format!("peer calls itself {}", peer.name),
            });
        }
        if self.is_connected(node) {
            // The peer connected to us while we were connecting to it and won.
            return Ok(());
        }
        self.install_connection(stream, peer);
        Ok(())
    }

    async fn accept_connection(&self, mut stream: TcpStream) -> Result<()> {
        let this = self.clone();
        let decide = move |peer: &str| -> Status {
            let peer = Atom::new(peer);
            if this.inner.connections.lock().unwrap().contains_key(&peer) {
                Status::Alive
            } else if this.inner.pending.lock().unwrap().contains(&peer) {
                // Simultaneous connect: the node with the greater name wins
                // as initiator. We are the acceptor here.
                if peer.as_str() > this.inner.name.as_str() {
                    Status::OkSimultaneous
                } else {
                    Status::Nok
                }
            } else {
                Status::Ok
            }
        };
        let peer = tokio::time::timeout(
            Duration::from_secs(10),
            handshake::accept(
                &mut stream,
                self.inner.name.as_str(),
                self.inner.flags,
                self.inner.creation,
                &self.inner.cookie,
                decide,
            ),
        )
        .await
        .map_err(|_| Error::Timeout)??;
        self.install_connection(stream, peer);
        Ok(())
    }

    fn install_connection(&self, stream: TcpStream, peer: handshake::Peer) {
        let name = Atom::new(peer.name.clone());
        let (tx, rx) = mpsc::unbounded_channel();
        let generation = self.inner.next_generation.fetch_add(1, Ordering::Relaxed);
        let old = self.inner.connections.lock().unwrap().insert(
            name.clone(),
            ConnHandle {
                tx,
                generation,
                flags: peer.flags,
            },
        );
        if old.is_some() {
            tracing::info!(node = %name, "replacing existing connection");
        }
        tracing::info!(node = %name, flags = format_args!("{:#x}", peer.flags), "connected");
        tokio::spawn(connection::run(
            self.clone(),
            name.clone(),
            stream,
            rx,
            self.inner.net_ticktime,
            generation,
        ));
        if old.is_none() {
            let _ = self.inner.events.send(NodeEvent::NodeUp(name));
        }
    }

    pub(crate) fn connection_closed(&self, peer: &Atom, generation: u64) {
        {
            let mut conns = self.inner.connections.lock().unwrap();
            match conns.get(peer) {
                Some(h) if h.generation == generation => {
                    conns.remove(peer);
                }
                _ => return, // superseded by a newer connection
            }
        }
        tracing::info!(node = %peer, "disconnected");
        // Tell local processes about lost links/monitors.
        let mut notify = Vec::new();
        {
            let mut t = self.inner.processes.lock().unwrap();
            let lost: Vec<Reference> = t
                .outgoing_monitors
                .iter()
                .filter(|(_, (_, n))| n == peer)
                .map(|(r, _)| r.clone())
                .collect();
            for r in lost {
                if let Some((pid, _)) = t.outgoing_monitors.remove(&r) {
                    notify.push((
                        pid,
                        Signal::Down {
                            reference: r,
                            object: Term::atom("noproc"),
                            reason: Term::atom("noconnection"),
                        },
                    ));
                }
            }
            for (id, entry) in t.entries.iter_mut() {
                entry.monitors.retain(|m| &m.monitoring.node != peer);
                let dead: Vec<Pid> = entry
                    .links
                    .iter()
                    .filter(|p| &p.node == peer)
                    .cloned()
                    .collect();
                for p in dead {
                    entry.links.remove(&p);
                    let _ = id;
                    let _ = entry.tx.send(Signal::Exit {
                        from: p,
                        reason: Term::atom("noconnection"),
                    });
                }
            }
            for (pid, sig) in notify {
                if let Some(e) = t.entries.get(&pid.id) {
                    let _ = e.tx.send(sig);
                }
            }
        }
        let _ = self.inner.events.send(NodeEvent::NodeDown(peer.clone()));
    }

    /// Drop the connection to a peer.
    pub fn disconnect(&self, node: &Atom) -> bool {
        // Dropping the sender ends the writer, which closes the socket; the
        // connection task then calls connection_closed.
        self.inner
            .connections
            .lock()
            .unwrap()
            .remove(node)
            .is_some()
    }

    /// Unregister from EPMD and close every connection. The node is unusable
    /// afterwards.
    pub fn shutdown(&self) {
        self.inner.shutdown.store(true, Ordering::SeqCst);
        if let Some(h) = self.inner.listener.lock().unwrap().take() {
            h.abort();
        }
        self.inner.epmd.lock().unwrap().take();
        self.inner.connections.lock().unwrap().clear();
        self.inner.epmd_server.lock().unwrap().take();
    }

    pub(crate) fn send_control(&self, node: &Atom, ctrl: Control) -> Result<()> {
        tracing::trace!(peer = %node, "-> {ctrl:?}");
        let conns = self.inner.connections.lock().unwrap();
        let h = conns
            .get(node)
            .ok_or_else(|| Error::NotConnected(node.clone()))?;
        h.tx.send(ctrl.encode(
            h.flags & dflags::EXIT_PAYLOAD != 0,
            h.flags & dflags::DIST_HDR_ATOM_CACHE != 0,
        ))
        .map_err(|_| Error::NotConnected(node.clone()))
    }

    async fn ensure_connected(&self, node: &Atom) -> Result<()> {
        if node == &self.inner.name || self.is_connected(node) {
            return Ok(());
        }
        self.connect(node.as_str()).await
    }

    // ---- processes ---------------------------------------------------------

    pub(crate) fn spawn_raw(&self) -> (Pid, mpsc::UnboundedReceiver<Signal>) {
        let id = self.inner.next_pid.fetch_add(1, Ordering::Relaxed);
        let pid = Pid {
            node: self.inner.name.clone(),
            id,
            serial: 0,
            creation: self.inner.creation,
        };
        let (tx, rx) = mpsc::unbounded_channel();
        self.inner.processes.lock().unwrap().entries.insert(
            id,
            ProcessEntry {
                tx,
                links: HashSet::new(),
                monitors: Vec::new(),
            },
        );
        (pid, rx)
    }

    /// Create a local process. Remote nodes can send to its pid (or a name
    /// registered for it); you read what arrives from the returned mailbox.
    pub fn spawn_mailbox(&self) -> Mailbox {
        let (pid, rx) = self.spawn_raw();
        Mailbox::new(pid, rx, self.clone())
    }

    /// Make `pid` reachable as `{name, 'rex@host'}` from remote nodes.
    pub fn register_name(&self, name: &str, pid: &Pid) -> Result<()> {
        let mut t = self.inner.processes.lock().unwrap();
        let name = Atom::new(name);
        if t.names.contains_key(&name) {
            return Err(Error::NameTaken(name));
        }
        if !t.entries.contains_key(&pid.id) {
            return Err(Error::NoProc);
        }
        t.names.insert(name, pid.id);
        Ok(())
    }

    pub fn unregister_name(&self, name: &str) -> bool {
        self.inner
            .processes
            .lock()
            .unwrap()
            .names
            .remove(&Atom::new(name))
            .is_some()
    }

    /// All registered names on this node.
    pub fn registered_names(&self) -> Vec<Atom> {
        let mut v: Vec<_> = self
            .inner
            .processes
            .lock()
            .unwrap()
            .names
            .keys()
            .cloned()
            .collect();
        v.sort();
        v
    }

    pub fn whereis(&self, name: &str) -> Option<Pid> {
        let t = self.inner.processes.lock().unwrap();
        let id = *t.names.get(&Atom::new(name))?;
        Some(Pid {
            node: self.inner.name.clone(),
            id,
            serial: 0,
            creation: self.inner.creation,
        })
    }

    fn is_local(&self, pid: &Pid) -> bool {
        pid.node == self.inner.name
    }

    fn deliver_local(&self, pid: &Pid, signal: Signal) -> bool {
        let t = self.inner.processes.lock().unwrap();
        match t.entries.get(&pid.id) {
            Some(e) => e.tx.send(signal).is_ok(),
            None => false,
        }
    }

    /// Called when a local process ends; notifies linked and monitoring peers.
    pub(crate) fn process_exited(&self, pid: &Pid, reason: Term) {
        let entry = {
            let mut t = self.inner.processes.lock().unwrap();
            t.names.retain(|_, id| *id != pid.id);
            t.entries.remove(&pid.id)
        };
        let Some(entry) = entry else { return };
        // The reason is cloned for all recipients but the last, which gets
        // the original: an erpc result has exactly one monitor, so it is
        // never copied.
        let total = entry.links.len() + entry.monitors.len();
        let mut reasons: Vec<Term> = Vec::with_capacity(total);
        for _ in 1..total {
            reasons.push(reason.clone());
        }
        reasons.push(reason);
        let mut reasons = reasons.into_iter();
        for linked in entry.links {
            let reason = reasons.next().unwrap();
            if self.is_local(&linked) {
                self.deliver_local(
                    &linked,
                    Signal::Exit {
                        from: pid.clone(),
                        reason,
                    },
                );
            } else {
                let node = linked.node.clone();
                let _ = self.send_control(
                    &node,
                    Control::Exit {
                        from: pid.clone(),
                        to: linked,
                        reason,
                    },
                );
            }
        }
        for m in entry.monitors {
            let reason = reasons.next().unwrap();
            let node = m.monitoring.node.clone();
            let _ = self.send_control(
                &node,
                Control::MonitorPExit {
                    from: m.object,
                    to: m.monitoring,
                    reference: m.reference,
                    reason,
                },
            );
        }
    }

    fn resolve(&self, r: &ProcRef) -> Option<u32> {
        let t = self.inner.processes.lock().unwrap();
        match r {
            ProcRef::Pid(p) if self.is_local(p) && t.entries.contains_key(&p.id) => Some(p.id),
            ProcRef::Name(n) => t.names.get(n).copied(),
            _ => None,
        }
    }

    /// A fresh reference.
    pub fn make_ref(&self) -> Reference {
        let n = self.inner.next_ref.fetch_add(1, Ordering::Relaxed);
        Reference {
            node: self.inner.name.clone(),
            creation: self.inner.creation,
            ids: vec![(n & 0x3ffff) as u32, (n >> 18) as u32, rand::random()],
        }
    }

    // ---- messaging ---------------------------------------------------------

    /// Send a message to a pid, connecting to its node first if needed.
    pub async fn send(&self, to: &Pid, msg: Term) -> Result<()> {
        if self.is_local(to) {
            return if self.deliver_local(to, Signal::Message(msg)) {
                Ok(())
            } else {
                Err(Error::NoProc)
            };
        }
        self.ensure_connected(&to.node).await?;
        self.send_control(
            &to.node,
            Control::Send {
                to: to.clone(),
                msg,
            },
        )
    }

    /// Send without auto-connecting; fails if the node is not connected.
    pub fn try_send(&self, to: &Pid, msg: Term) -> Result<()> {
        if self.is_local(to) {
            return if self.deliver_local(to, Signal::Message(msg)) {
                Ok(())
            } else {
                Err(Error::NoProc)
            };
        }
        self.send_control(
            &to.node,
            Control::Send {
                to: to.clone(),
                msg,
            },
        )
    }

    /// Send to a registered name on a node (`send({name, node}, msg)`).
    pub async fn send_named(&self, name: &str, node: &str, msg: Term) -> Result<()> {
        let node = Atom::new(node);
        if node == self.inner.name {
            let pid = self.whereis(name).ok_or(Error::NoProc)?;
            return self.try_send(&pid, msg);
        }
        self.ensure_connected(&node).await?;
        self.send_control(
            &node,
            Control::RegSend {
                from: self.inner.system_pid.clone(),
                to_name: Atom::new(name),
                msg,
            },
        )
    }

    /// Send to a [`Dest`].
    pub async fn send_to(&self, dest: impl Into<Dest>, msg: Term) -> Result<()> {
        match dest.into() {
            Dest::Pid(p) => self.send(&p, msg).await,
            Dest::Name { name, node } => self.send_named(name.as_str(), node.as_str(), msg).await,
        }
    }

    /// `GenServer.call`: send `{'$gen_call', From, Request}` and await the reply.
    pub async fn call(
        &self,
        dest: impl Into<Dest>,
        request: Term,
        timeout: Duration,
    ) -> Result<Term> {
        let dest = dest.into();
        let node = dest.node().clone();
        if node == self.inner.name {
            return Err(Error::NoProc); // calling ourselves is not supported
        }
        self.ensure_connected(&node).await?;
        let mut mb = self.spawn_mailbox();
        let mref = self.make_ref();
        self.inner
            .processes
            .lock()
            .unwrap()
            .outgoing_monitors
            .insert(mref.clone(), (mb.pid().clone(), node.clone()));
        self.send_control(
            &node,
            Control::MonitorP {
                from: mb.pid().clone(),
                to: dest.proc_ref(),
                reference: mref.clone(),
            },
        )?;
        let msg = Term::tuple(vec![
            Term::atom("$gen_call"),
            Term::tuple(vec![
                Term::Pid(mb.pid().clone()),
                Term::Reference(mref.clone()),
            ]),
            request,
        ]);
        match &dest {
            Dest::Pid(p) => self.send_control(&node, Control::Send { to: p.clone(), msg })?,
            Dest::Name { name, .. } => self.send_control(
                &node,
                Control::RegSend {
                    from: mb.pid().clone(),
                    to_name: name.clone(),
                    msg,
                },
            )?,
        }
        let deadline = tokio::time::Instant::now() + timeout;
        let result = loop {
            match tokio::time::timeout_at(deadline, mb.recv()).await {
                Err(_) => break Err(Error::Timeout),
                Ok(None) => break Err(Error::Shutdown),
                Ok(Some(Signal::Message(Term::Tuple(items))))
                    if items.len() == 2 && items[0].as_reference() == Some(&mref) =>
                {
                    break Ok(items.into_iter().nth(1).unwrap());
                }
                Ok(Some(Signal::Down {
                    reference, reason, ..
                })) if reference == mref => {
                    break Err(if reason.is_atom("noproc") {
                        Error::NoProc
                    } else {
                        Error::Down(reason)
                    });
                }
                Ok(Some(_)) => continue,
            }
        };
        self.inner
            .processes
            .lock()
            .unwrap()
            .outgoing_monitors
            .remove(&mref);
        let _ = self.send_control(
            &node,
            Control::DemonitorP {
                from: mb.pid().clone(),
                to: dest.proc_ref(),
                reference: mref,
            },
        );
        result
    }

    /// `GenServer.cast`: send `{'$gen_cast', Request}`.
    pub async fn cast(&self, dest: impl Into<Dest>, request: Term) -> Result<()> {
        self.send_to(dest, Term::tuple(vec![Term::atom("$gen_cast"), request]))
            .await
    }

    /// Call `module:function(args)` on a remote Erlang/Elixir node, through
    /// its `rex` server (the classic `rpc:call`).
    pub async fn rpc_call(
        &self,
        node: &str,
        module: &str,
        function: &str,
        args: Vec<Term>,
        timeout: Duration,
    ) -> Result<Term> {
        let req = Term::tuple(vec![
            Term::atom("call"),
            Term::atom(module),
            Term::atom(function),
            Term::list(args),
            Term::Pid(self.inner.system_pid.clone()),
        ]);
        let reply = self.call(Dest::named("rex", node), req, timeout).await?;
        if let Some(t) = reply.as_tuple() {
            if t.len() == 2 && t[0].is_atom("badrpc") {
                return Err(Error::BadRpc(t[1].clone()));
            }
        }
        Ok(reply)
    }

    /// `erlang:spawn_request/5` towards a peer: ask `node` to spawn
    /// `module:function(args...)` on behalf of `from`. The request id is
    /// returned; the peer answers with a `{spawn_reply, ReqId, ok, Pid}` (or
    /// `{spawn_reply, ReqId, error, Reason}`) message in `from`'s mailbox,
    /// and with `monitor` the new process is monitored with the request id as
    /// the monitor reference, so its exit arrives as a [`Signal::Down`].
    /// `link` links it to `from`.
    pub async fn spawn_request(
        &self,
        from: &Mailbox,
        node: &str,
        module: &str,
        function: &str,
        args: Vec<Term>,
        opts: SpawnOpts,
    ) -> Result<Reference> {
        let node = Atom::new(node);
        self.ensure_connected(&node).await?;
        let req_id = self.make_ref();
        let mut opt_terms = Vec::new();
        if opts.link {
            // The link itself is recorded when the reply tells us the pid.
            opt_terms.push(Term::atom("link"));
        }
        if opts.monitor {
            opt_terms.push(Term::atom("monitor"));
            self.inner
                .processes
                .lock()
                .unwrap()
                .outgoing_monitors
                .insert(req_id.clone(), (from.pid().clone(), node.clone()));
        }
        let arity = args.len() as i64;
        let r = self.send_control(
            &node,
            Control::SpawnRequest {
                req_id: req_id.clone(),
                from: from.pid().clone(),
                group_leader: self.inner.system_pid.clone(),
                module: Atom::new(module),
                function: Atom::new(function),
                arity,
                opts: opt_terms,
                args: Term::list(args),
            },
        );
        if r.is_err() {
            self.inner.processes.lock().unwrap().outgoing_monitors.remove(&req_id);
        }
        r.map(|_| req_id)
    }

    /// Call `module:function(args)` on a remote node the way `erpc:call/5`
    /// does: spawn `erpc:execute_call/4` there with [`spawn_request`]
    /// (monitored), and read the result from the exit reason. Exceptions
    /// raised by the call come back as [`Error::Exception`]; unlike
    /// [`rpc_call`] this needs no `rex` server on the peer.
    ///
    /// [`spawn_request`]: Self::spawn_request
    /// [`rpc_call`]: Self::rpc_call
    pub async fn erpc_call(
        &self,
        node: &str,
        module: &str,
        function: &str,
        args: Vec<Term>,
        timeout: Duration,
    ) -> Result<Term> {
        let mut mb = self.spawn_mailbox();
        let res_ref = self.make_ref();
        let req_id = self
            .spawn_request(
                &mb,
                node,
                "erpc",
                "execute_call",
                vec![Term::Reference(res_ref.clone()), Term::atom(module), Term::atom(function), Term::list(args)],
                SpawnOpts { monitor: true, link: false },
            )
            .await?;
        let deadline = tokio::time::Instant::now() + timeout;
        let result = loop {
            match tokio::time::timeout_at(deadline, mb.recv()).await {
                Err(_) => break Err(Error::Timeout),
                Ok(None) => break Err(Error::Shutdown),
                Ok(Some(Signal::Message(Term::Tuple(items))))
                    if items.len() == 4 && items[0].is_atom("spawn_reply") && items[1].as_reference() == Some(&req_id) =>
                {
                    if items[2].is_atom("error") {
                        break Err(Error::BadRpc(items.into_iter().nth(3).unwrap()));
                    }
                }
                Ok(Some(Signal::Down { reference, reason, .. })) if reference == req_id => {
                    break Ok(reason);
                }
                Ok(Some(_)) => continue,
            }
        };
        self.inner.processes.lock().unwrap().outgoing_monitors.remove(&req_id);
        let reason = result?;
        // execute_call/4 exits with {Ref, return, V} | {Ref, throw, T} |
        // {Ref, exit, R} | {Ref, error, R, Stack}; anything else killed it.
        let Term::Tuple(mut items) = reason else {
            return Err(Error::Down(reason));
        };
        if items.len() < 3 || items[0].as_reference() != Some(&res_ref) {
            return Err(Error::Down(Term::Tuple(items)));
        }
        let class = items[1].clone();
        let value = items.swap_remove(2);
        match class.as_atom().map(|a| a.as_str()) {
            Some("return") => Ok(value),
            Some("throw") => Err(Error::Exception(Exception::Throw(value))),
            Some("exit") => Err(Error::Exception(Exception::Exit(value))),
            Some("error") => Err(Error::Exception(Exception::Error(value))),
            _ => Err(Error::Down(Term::Tuple(items))),
        }
    }

    /// `:net_adm.ping/1` equivalent; true if the node answers.
    pub async fn ping(&self, node: &str) -> bool {
        let req = Term::tuple(vec![
            Term::atom("is_auth"),
            Term::Atom(self.inner.name.clone()),
        ]);
        matches!(self.call(Dest::named("net_kernel", node), req, Duration::from_secs(10)).await, Ok(t) if t.is_atom("yes"))
    }

    // ---- inbound dispatch --------------------------------------------------

    pub(crate) fn handle_control(&self, peer: &Atom, ctrl: Control) {
        match ctrl {
            Control::Send { to, msg } | Control::SendSender { to, msg, .. } => {
                if !self.deliver_local(&to, Signal::Message(msg)) {
                    tracing::debug!(%peer, "message for unknown local pid {to}");
                }
            }
            Control::AliasSend { alias, msg, .. } => {
                // We never hand out aliases, but tolerate replies keyed by our refs.
                tracing::debug!(%peer, "alias send to {alias} dropped: {msg}");
            }
            Control::RegSend { from, to_name, msg } => {
                let Some(msg) = builtins::handle_reg_send(self, peer, &from, &to_name, msg) else {
                    return;
                };
                let id = self
                    .inner
                    .processes
                    .lock()
                    .unwrap()
                    .names
                    .get(&to_name)
                    .copied();
                match id {
                    Some(id) => {
                        let pid = Pid {
                            node: self.inner.name.clone(),
                            id,
                            serial: 0,
                            creation: self.inner.creation,
                        };
                        self.deliver_local(&pid, Signal::Message(msg));
                    }
                    None => {
                        tracing::debug!(%peer, "message for unregistered name {to_name}: {msg}")
                    }
                }
            }
            Control::Link { from, to } => {
                let linked = {
                    let mut t = self.inner.processes.lock().unwrap();
                    match t.entries.get_mut(&to.id) {
                        Some(e) if self.is_local(&to) => {
                            e.links.insert(from.clone());
                            true
                        }
                        _ => false,
                    }
                };
                if !linked {
                    let _ = self.send_control(
                        peer,
                        Control::Exit {
                            from: to,
                            to: from,
                            reason: Term::atom("noproc"),
                        },
                    );
                }
            }
            Control::Unlink { from, to } => {
                if let Some(e) = self.inner.processes.lock().unwrap().entries.get_mut(&to.id) {
                    e.links.remove(&from);
                }
            }
            Control::UnlinkId { id, from, to } => {
                if let Some(e) = self.inner.processes.lock().unwrap().entries.get_mut(&to.id) {
                    e.links.remove(&from);
                }
                let _ = self.send_control(
                    peer,
                    Control::UnlinkIdAck {
                        id,
                        from: to,
                        to: from,
                    },
                );
            }
            Control::UnlinkIdAck { .. } => {}
            Control::MonitorP {
                from,
                to,
                reference,
            } => {
                let target = self.resolve(&to);
                match target {
                    Some(id) => {
                        if let Some(e) = self.inner.processes.lock().unwrap().entries.get_mut(&id) {
                            e.monitors.push(Monitor {
                                monitoring: from,
                                reference,
                                object: to,
                            });
                        }
                    }
                    None => {
                        let _ = self.send_control(
                            peer,
                            Control::MonitorPExit {
                                from: to,
                                to: from,
                                reference,
                                reason: Term::atom("noproc"),
                            },
                        );
                    }
                }
            }
            Control::DemonitorP { to, reference, .. } => {
                if let Some(id) = self.resolve(&to) {
                    if let Some(e) = self.inner.processes.lock().unwrap().entries.get_mut(&id) {
                        e.monitors.retain(|m| m.reference != reference);
                    }
                }
            }
            Control::MonitorPExit {
                from,
                to,
                reference,
                reason,
            } => {
                self.inner
                    .processes
                    .lock()
                    .unwrap()
                    .outgoing_monitors
                    .remove(&reference);
                let object = match from {
                    ProcRef::Pid(p) => Term::Pid(p),
                    ProcRef::Name(n) => Term::tuple(vec![Term::Atom(n), Term::Atom(peer.clone())]),
                };
                self.deliver_local(
                    &to,
                    Signal::Down {
                        reference,
                        object,
                        reason,
                    },
                );
            }
            Control::Exit { from, to, reason } | Control::Exit2 { from, to, reason } => {
                if let Some(e) = self.inner.processes.lock().unwrap().entries.get_mut(&to.id) {
                    e.links.remove(&from);
                }
                self.deliver_local(&to, Signal::Exit { from, reason });
            }
            Control::SpawnRequest {
                req_id,
                from,
                group_leader,
                module,
                function,
                arity,
                opts,
                args,
            } => {
                self.handle_spawn_request(
                    peer,
                    req_id,
                    from,
                    group_leader,
                    module,
                    function,
                    arity,
                    opts,
                    args,
                );
            }
            Control::SpawnReply { req_id, to, flags, result } => {
                // Shaped like the message erlang:spawn_request/5 delivers.
                let ok = result.as_pid().is_some();
                {
                    let mut t = self.inner.processes.lock().unwrap();
                    if !ok {
                        t.outgoing_monitors.remove(&req_id);
                    } else if flags & crate::dist::SPAWN_REPLY_FLAG_LINK != 0 {
                        if let (Some(e), Some(p)) = (t.entries.get_mut(&to.id), result.as_pid()) {
                            e.links.insert(p.clone());
                        }
                    }
                }
                let msg = Term::tuple(vec![
                    Term::atom("spawn_reply"),
                    Term::Reference(req_id),
                    Term::atom(if ok { "ok" } else { "error" }),
                    result,
                ]);
                if !self.deliver_local(&to, Signal::Message(msg)) {
                    tracing::debug!(%peer, "spawn reply for unknown local pid {to}");
                }
            }
            Control::NodeLink | Control::GroupLeader { .. } => {}
            Control::Unknown { control, .. } => {
                tracing::debug!(%peer, "ignoring control message {control}");
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn handle_spawn_request(
        &self,
        peer: &Atom,
        req_id: Reference,
        from: Pid,
        group_leader: Pid,
        module: Atom,
        function: Atom,
        _arity: i64,
        opts: Vec<Term>,
        args: Term,
    ) {
        let Term::List(args) = args else {
            let _ = self.send_control(
                peer,
                Control::SpawnReply {
                    req_id,
                    to: from,
                    flags: 0,
                    result: Term::atom("badarg"),
                },
            );
            return;
        };
        let link = opts.iter().any(|o| o.is_atom("link"));
        let monitor = opts.iter().any(|o| o.is_atom("monitor"));
        let (pid, rx) = self.spawn_raw();
        {
            let mut t = self.inner.processes.lock().unwrap();
            if let Some(e) = t.entries.get_mut(&pid.id) {
                if link {
                    e.links.insert(from.clone());
                }
                if monitor {
                    e.monitors.push(Monitor {
                        monitoring: from.clone(),
                        reference: req_id.clone(),
                        object: ProcRef::Pid(pid.clone()),
                    });
                }
            }
        }
        let mut flags = 0;
        if link {
            flags |= crate::dist::SPAWN_REPLY_FLAG_LINK;
        }
        if monitor {
            flags |= crate::dist::SPAWN_REPLY_FLAG_MONITOR;
        }
        if let Err(e) = self.send_control(
            peer,
            Control::SpawnReply {
                req_id,
                to: from.clone(),
                flags,
                result: Term::Pid(pid.clone()),
            },
        ) {
            tracing::debug!("spawn reply failed: {e}");
            self.process_exited(&pid, Term::atom("noconnection"));
            return;
        }
        let node = self.clone();
        let peer = peer.clone();
        tokio::spawn(async move {
            let ctx = CallContext::new(
                node.clone(),
                pid.clone(),
                Some(from),
                Some(group_leader),
                peer,
                rx,
            );
            tracing::debug!(%module, %function, arity = args.len(), "spawned call");
            let reason = builtins::run_spawned(&node, ctx, module, function, args).await;
            node.process_exited(&pid, reason);
        });
    }
}

impl std::fmt::Debug for Node {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Node")
            .field("name", &self.inner.name)
            .field("alive", &self.inner.alive_name)
            .finish()
    }
}

/// Resolve a node's host part to an address (IP literal or DNS name).
pub(crate) async fn resolve_host(host: &str) -> Result<IpAddr> {
    if let Ok(ip) = host.parse::<IpAddr>() {
        return Ok(ip);
    }
    let mut addrs = tokio::net::lookup_host((host, 0)).await?;
    // Prefer IPv4 since EPMD is registered for TCP/IPv4.
    let all: Vec<_> = addrs.by_ref().map(|a| a.ip()).collect();
    all.iter()
        .find(|a| a.is_ipv4())
        .or(all.first())
        .copied()
        .ok_or_else(|| Error::Epmd(format!("cannot resolve host {host}")))
}
