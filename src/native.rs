//! Native modules: Rust code that Erlang/Elixir nodes call with `erpc` /
//! `rpc`, as if it were an Erlang module on this node.
//!
//! ```ignore
//! let math = FnModule::elixir("Rex.Math")
//!     .function("add", 2, |args, _ctx| {
//!         let a = args[0].as_int().ok_or_else(Exception::badarg)?;
//!         let b = args[1].as_int().ok_or_else(Exception::badarg)?;
//!         Ok(Term::Int(a + b))
//!     });
//! ```
//! Then from Elixir: `:erpc.call(:"rex@host", Rex.Math, :add, [1, 2])`.

use crate::node::Node;
use crate::process::Signal;
use crate::term::{Atom, Pid, Term};
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, Mutex};

pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// How a native function fails, mirroring Erlang's three exception classes.
/// `erpc.call` on the Elixir side re-raises them accordingly.
#[derive(Debug, Clone, PartialEq)]
pub enum Exception {
    Error(Term),
    Exit(Term),
    Throw(Term),
}

impl Exception {
    pub fn error(reason: impl Into<Term>) -> Self {
        Exception::Error(reason.into())
    }
    pub fn exit(reason: impl Into<Term>) -> Self {
        Exception::Exit(reason.into())
    }
    pub fn throw(value: impl Into<Term>) -> Self {
        Exception::Throw(value.into())
    }
    pub fn badarg() -> Self {
        Exception::Error(Term::atom("badarg"))
    }
    pub fn undef() -> Self {
        Exception::Error(Term::atom("undef"))
    }
    pub fn function_clause() -> Self {
        Exception::Error(Term::atom("function_clause"))
    }
    /// `{badarg, Message}` style error carrying an explanatory string.
    pub fn badarg_msg(msg: impl AsRef<str>) -> Self {
        Exception::Error(Term::tuple(vec![Term::atom("badarg"), Term::string(msg)]))
    }
}

impl std::fmt::Display for Exception {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Exception::Error(t) => write!(f, "error: {t}"),
            Exception::Exit(t) => write!(f, "exit: {t}"),
            Exception::Throw(t) => write!(f, "throw: {t}"),
        }
    }
}

pub type CallResult = Result<Term, Exception>;

/// Everything a native function may want to know about its invocation. It is
/// cheap to clone and can be moved into spawned tasks.
#[derive(Clone)]
pub struct CallContext {
    /// The local node; use it to send messages or call back into the cluster.
    pub node: Node,
    pid: Pid,
    /// The remote process that asked for this call, when known.
    pub caller: Option<Pid>,
    /// The caller's group leader; `puts` writes there.
    pub group_leader: Option<Pid>,
    /// The node the request came from.
    pub peer: Atom,
    inbox: Arc<Mutex<mpsc::UnboundedReceiver<Signal>>>,
}

impl CallContext {
    pub(crate) fn new(
        node: Node,
        pid: Pid,
        caller: Option<Pid>,
        group_leader: Option<Pid>,
        peer: Atom,
        rx: mpsc::UnboundedReceiver<Signal>,
    ) -> Self {
        CallContext {
            node,
            pid,
            caller,
            group_leader,
            peer,
            inbox: Arc::new(Mutex::new(rx)),
        }
    }

    /// The pid of the process executing this call. Messages sent to it land
    /// in [`recv`](Self::recv).
    pub fn pid(&self) -> &Pid {
        &self.pid
    }

    /// Receive the next signal addressed to this call's process.
    pub async fn recv(&self) -> Option<Signal> {
        self.inbox.lock().await.recv().await
    }

    /// Receive with a deadline.
    pub async fn recv_timeout(&self, timeout: Duration) -> crate::Result<Option<Signal>> {
        tokio::time::timeout(timeout, self.recv())
            .await
            .map_err(|_| crate::Error::Timeout)
    }

    /// Print on the caller's terminal by sending an `io_request` to its group
    /// leader. Fire and forget.
    pub fn puts(&self, text: impl AsRef<str>) -> crate::Result<()> {
        let Some(gl) = &self.group_leader else {
            return Ok(());
        };
        let mut s = text.as_ref().to_string();
        s.push('\n');
        let req = Term::tuple(vec![
            Term::atom("io_request"),
            Term::Pid(self.pid.clone()),
            Term::Pid(self.pid.clone()),
            Term::tuple(vec![
                Term::atom("put_chars"),
                Term::atom("unicode"),
                Term::string(s),
            ]),
        ]);
        self.node.try_send(gl, req)
    }
}

/// A module callable from the cluster. Implement this directly for full
/// control, or build one with [`FnModule`].
pub trait NativeModule: Send + Sync + 'static {
    /// The module atom, e.g. `Elixir.MyApp.Math` or `my_module`.
    fn name(&self) -> &str;

    /// Run `function(args...)`. Unknown functions should return
    /// `Err(Exception::undef())`.
    fn call<'a>(
        &'a self,
        function: &'a str,
        args: Vec<Term>,
        ctx: CallContext,
    ) -> BoxFuture<'a, CallResult>;

    /// `(name, arity)` pairs, used by `rex:functions/1`. `None` arity means variadic.
    fn exports(&self) -> Vec<(String, Option<usize>)> {
        Vec::new()
    }
}

type Handler = dyn Fn(Vec<Term>, CallContext) -> BoxFuture<'static, CallResult> + Send + Sync;
/// Handlers for one function name, each with an arity (`None` = variadic).
type Overloads = Vec<(Option<usize>, Arc<Handler>)>;

/// A module assembled from closures.
pub struct FnModule {
    name: String,
    handlers: HashMap<String, Overloads>,
}

impl FnModule {
    /// A module with an exact atom name (`my_mod`).
    pub fn new(name: impl Into<String>) -> Self {
        FnModule {
            name: name.into(),
            handlers: HashMap::new(),
        }
    }

    /// A module reachable from Elixir as `Name` (atom `Elixir.Name`).
    pub fn elixir(name: &str) -> Self {
        Self::new(Atom::elixir(name).into_string())
    }

    fn add(mut self, name: &str, arity: Option<usize>, h: Arc<Handler>) -> Self {
        self.handlers
            .entry(name.to_string())
            .or_default()
            .push((arity, h));
        self
    }

    /// A synchronous function of fixed arity.
    pub fn function<F>(self, name: &str, arity: usize, f: F) -> Self
    where
        F: Fn(Vec<Term>, &CallContext) -> CallResult + Send + Sync + 'static,
    {
        let f = Arc::new(f);
        self.add(
            name,
            Some(arity),
            Arc::new(move |args, ctx| {
                let r = f(args, &ctx);
                Box::pin(std::future::ready(r))
            }),
        )
    }

    /// An asynchronous function of fixed arity. The future may hold the
    /// context, e.g. to await replies with `ctx.recv()`.
    pub fn async_function<F, Fut>(self, name: &str, arity: usize, f: F) -> Self
    where
        F: Fn(Vec<Term>, CallContext) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = CallResult> + Send + 'static,
    {
        let f = Arc::new(f);
        self.add(
            name,
            Some(arity),
            Arc::new(move |args, ctx| Box::pin(f(args, ctx))),
        )
    }

    /// A synchronous function accepting any number of arguments.
    pub fn variadic<F>(self, name: &str, f: F) -> Self
    where
        F: Fn(Vec<Term>, &CallContext) -> CallResult + Send + Sync + 'static,
    {
        let f = Arc::new(f);
        self.add(
            name,
            None,
            Arc::new(move |args, ctx| {
                let r = f(args, &ctx);
                Box::pin(std::future::ready(r))
            }),
        )
    }

    /// An asynchronous function accepting any number of arguments.
    pub fn async_variadic<F, Fut>(self, name: &str, f: F) -> Self
    where
        F: Fn(Vec<Term>, CallContext) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = CallResult> + Send + 'static,
    {
        let f = Arc::new(f);
        self.add(
            name,
            None,
            Arc::new(move |args, ctx| Box::pin(f(args, ctx))),
        )
    }
}

impl NativeModule for FnModule {
    fn name(&self) -> &str {
        &self.name
    }

    fn call<'a>(
        &'a self,
        function: &'a str,
        args: Vec<Term>,
        ctx: CallContext,
    ) -> BoxFuture<'a, CallResult> {
        let handler = self.handlers.get(function).and_then(|hs| {
            hs.iter()
                .find(|(a, _)| *a == Some(args.len()))
                .or_else(|| hs.iter().find(|(a, _)| a.is_none()))
                .map(|(_, h)| h.clone())
        });
        match handler {
            Some(h) => h(args, ctx),
            None => Box::pin(std::future::ready(Err(Exception::undef()))),
        }
    }

    fn exports(&self) -> Vec<(String, Option<usize>)> {
        let mut v: Vec<_> = self
            .handlers
            .iter()
            .flat_map(|(n, hs)| hs.iter().map(move |(a, _)| (n.clone(), *a)))
            .collect();
        v.sort();
        v
    }
}
