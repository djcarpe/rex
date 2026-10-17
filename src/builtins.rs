//! The handful of Erlang system processes and modules a peer expects to find
//! on any node: `net_kernel` (so `Node.ping` works), `rex` (legacy
//! `rpc:block_call`/`rpc:cast`), the `erpc` entry points used by `erpc:call`
//! and `rpc:call`, and a small introspection module named `rex`.

use crate::dist::Control;
use crate::native::{CallContext, Exception, FnModule};
use crate::node::Node;
use crate::term::{Atom, Pid, Term};

/// Handle a `REG_SEND` to a well-known name. Returns false if the name is not
/// a builtin so the caller can route it to a user process.
pub(crate) fn handle_reg_send(
    node: &Node,
    peer: &Atom,
    from: &Pid,
    to_name: &Atom,
    msg: Term,
) -> Option<Term> {
    match to_name.as_str() {
        "net_kernel" => {
            if let Some((from_tag, req)) = gen_call_parts(&msg) {
                if req
                    .as_tuple()
                    .is_some_and(|t| t.first().is_some_and(|x| x.is_atom("is_auth")))
                {
                    node.gen_reply(from_tag, Term::atom("yes"));
                }
            }
            None
        }
        "rex" => {
            handle_rex(node, peer, from, msg);
            None
        }
        _ => Some(msg),
    }
}

/// Split `{'$gen_call', From, Request}` into `(From, Request)`.
pub(crate) fn gen_call_parts(msg: &Term) -> Option<(&Term, &Term)> {
    let t = msg.as_tuple()?;
    if t.len() == 3 && t[0].is_atom("$gen_call") {
        Some((&t[1], &t[2]))
    } else {
        None
    }
}

fn handle_rex(node: &Node, peer: &Atom, from: &Pid, msg: Term) {
    if gen_call_parts(&msg).is_some() {
        // {'$gen_call', From, {call | block_call, M, F, A, GroupLeader}};
        // taken apart by value so the argument list is moved, not copied.
        let Term::Tuple(mut outer) = msg else { return };
        let req = outer.pop().unwrap();
        let from_tag = outer.pop().unwrap();
        let Term::Tuple(r) = req else {
            node.gen_reply(&from_tag, badrpc(Term::atom("badarg")));
            return;
        };
        if r.len() == 5 && (r[0].is_atom("call") || r[0].is_atom("block_call")) {
            let [_, m, f, a, gl]: [Term; 5] = r.try_into().unwrap();
            let (Term::Atom(m), Term::Atom(f), Term::List(a)) = (m, f, a) else {
                node.gen_reply(&from_tag, badrpc(Term::atom("badarg")));
                return;
            };
            let gl = gl.as_pid().cloned();
            let node = node.clone();
            let peer = peer.clone();
            let caller = from.clone();
            tokio::spawn(async move {
                let (pid, rx) = node.spawn_raw();
                let ctx = CallContext::new(node.clone(), pid.clone(), Some(caller), gl, peer, rx);
                let arity = a.len();
                let reply = match node.apply(&m, &f, a, ctx).await {
                    Ok(v) => v,
                    Err(e) => badrpc(Term::tuple(vec![
                        Term::atom("EXIT"),
                        exit_reason(e, &m, &f, arity),
                    ])),
                };
                node.process_exited(&pid, Term::atom("normal"));
                node.gen_reply(&from_tag, reply);
            });
        } else {
            node.gen_reply(&from_tag, badrpc(Term::atom("badarg")));
        }
        return;
    }
    // {'$gen_cast', {cast, M, F, A, GroupLeader}}
    let Some(t) = msg.as_tuple() else { return };
    if t.len() == 2 && t[0].is_atom("$gen_cast") {
        if let Some(r) = t[1].as_tuple() {
            if r.len() >= 4 && r[0].is_atom("cast") {
                let (Some(m), Some(f), Some(a)) = (r[1].as_atom(), r[2].as_atom(), r[3].as_list())
                else {
                    return;
                };
                let (m, f, a) = (m.clone(), f.clone(), a.to_vec());
                let gl = r.get(4).and_then(|g| g.as_pid().cloned());
                let node = node.clone();
                let peer = peer.clone();
                let caller = from.clone();
                tokio::spawn(async move {
                    let (pid, rx) = node.spawn_raw();
                    let ctx =
                        CallContext::new(node.clone(), pid.clone(), Some(caller), gl, peer, rx);
                    let _ = node.apply(&m, &f, a, ctx).await;
                    node.process_exited(&pid, Term::atom("normal"));
                });
            }
        }
    }
}

fn badrpc(reason: Term) -> Term {
    Term::tuple(vec![Term::atom("badrpc"), reason])
}

/// The exit reason an Erlang process would die with for an uncaught exception.
/// The stack frame carries the arity rather than the arguments, which avoids
/// copying (possibly huge) argument terms into every error.
pub(crate) fn exit_reason(e: Exception, m: &Atom, f: &Atom, arity: usize) -> Term {
    let frame = stack_frame(m, f, arity);
    match e {
        Exception::Error(r) => Term::tuple(vec![r, frame]),
        Exception::Exit(r) => r,
        Exception::Throw(t) => {
            Term::tuple(vec![Term::tuple(vec![Term::atom("nocatch"), t]), frame])
        }
    }
}

fn stack_frame(m: &Atom, f: &Atom, arity: usize) -> Term {
    Term::list(vec![Term::tuple(vec![Term::Atom(m.clone()), Term::Atom(f.clone()), Term::from(arity), Term::nil()])])
}

/// Run the body of a spawned process (`spawn_request` from a peer) and return
/// its exit reason. `erpc:execute_call/4` is what `erpc:call` and `rpc:call`
/// spawn; everything else is dispatched to native modules directly.
pub(crate) async fn run_spawned(node: &Node, ctx: CallContext, module: Atom, function: Atom, args: Vec<Term>) -> Term {
    if module.as_str() == "erpc" {
        match (function.as_str(), args.len()) {
            ("execute_call", 4) => {
                let [reference, m, f, a]: [Term; 4] = args.try_into().unwrap();
                let (Term::Atom(m), Term::Atom(f), Term::List(a)) = (m, f, a) else {
                    return Term::tuple(vec![
                        reference,
                        Term::atom("error"),
                        Term::tuple(vec![Term::atom("erpc"), Term::atom("badarg")]),
                    ]);
                };
                let arity = a.len();
                return match node.apply(&m, &f, a, ctx).await {
                    Ok(v) => Term::tuple(vec![reference, Term::atom("return"), v]),
                    Err(Exception::Throw(t)) => Term::tuple(vec![reference, Term::atom("throw"), t]),
                    Err(Exception::Exit(r)) => Term::tuple(vec![reference, Term::atom("exit"), r]),
                    Err(Exception::Error(r)) => {
                        Term::tuple(vec![reference, Term::atom("error"), r, stack_frame(&m, &f, arity)])
                    }
                };
            }
            ("execute_call", 3) => {
                let [m, f, a]: [Term; 3] = args.try_into().unwrap();
                let (Term::Atom(m), Term::Atom(f), Term::List(a)) = (m, f, a) else {
                    return Term::tuple(vec![Term::atom("badarg"), Term::nil()]);
                };
                let arity = a.len();
                return match node.apply(&m, &f, a, ctx).await {
                    Ok(v) => Term::tuple(vec![Term::atom("return"), v]),
                    Err(e) => exit_reason(e, &m, &f, arity),
                };
            }
            ("execute_cast", 3) => {
                let [m, f, a]: [Term; 3] = args.try_into().unwrap();
                if let (Term::Atom(m), Term::Atom(f), Term::List(a)) = (m, f, a) {
                    let arity = a.len();
                    if let Err(e) = node.apply(&m, &f, a, ctx).await {
                        return exit_reason(e, &m, &f, arity);
                    }
                }
                return Term::atom("normal");
            }
            _ => {}
        }
    }
    let arity = args.len();
    match node.apply(&module, &function, args, ctx).await {
        Ok(_) => Term::atom("normal"),
        Err(e) => exit_reason(e, &module, &function, arity),
    }
}

/// A sliver of the `erlang` module, enough for the usual smoke tests
/// (`:rpc.call(node, :erlang, :node, [])`).
pub(crate) fn erlang_module() -> FnModule {
    FnModule::new("erlang")
        .function("node", 0, |_, ctx| Ok(Term::Atom(ctx.node.name().clone())))
        .function("nodes", 0, |_, ctx| {
            Ok(Term::list(
                ctx.node.nodes().into_iter().map(Term::Atom).collect(),
            ))
        })
        .function("nodes", 1, |_, ctx| {
            Ok(Term::list(
                ctx.node.nodes().into_iter().map(Term::Atom).collect(),
            ))
        })
        .function("is_alive", 0, |_, _| Ok(Term::from(true)))
        .function("self", 0, |_, ctx| Ok(Term::Pid(ctx.pid().clone())))
        .function("whereis", 1, |args, ctx| {
            let name = args[0].as_atom().ok_or_else(Exception::badarg)?;
            Ok(ctx
                .node
                .whereis(name.as_str())
                .map(Term::Pid)
                .unwrap_or_else(|| Term::atom("undefined")))
        })
        .function("registered", 0, |_, ctx| {
            Ok(Term::list(
                ctx.node
                    .registered_names()
                    .into_iter()
                    .map(Term::Atom)
                    .collect(),
            ))
        })
        .function("system_info", 1, |args, _| {
            match args[0].as_atom().map(|a| a.as_str()) {
                Some("otp_release") => Ok(Term::charlist("rex")),
                Some("system_architecture") => Ok(Term::charlist(std::env::consts::ARCH)),
                Some("version") => Ok(Term::charlist(env!("CARGO_PKG_VERSION"))),
                _ => Err(Exception::badarg()),
            }
        })
}

/// `rex` module: introspection of what this node offers.
pub(crate) fn rex_module() -> FnModule {
    FnModule::new("rex")
        .function("ping", 0, |_, _| Ok(Term::atom("pong")))
        .function("node", 0, |_, ctx| Ok(Term::Atom(ctx.node.name().clone())))
        .function("version", 0, |_, _| {
            Ok(Term::string(env!("CARGO_PKG_VERSION")))
        })
        .function("modules", 0, |_, ctx| {
            Ok(Term::list(
                ctx.node
                    .module_names()
                    .into_iter()
                    .map(Term::atom)
                    .collect(),
            ))
        })
        .function("functions", 1, |args, ctx| {
            let m = args[0].as_atom().ok_or_else(Exception::badarg)?;
            let Some(exports) = ctx.node.module_exports(m.as_str()) else {
                return Err(Exception::error(Term::atom("undef")));
            };
            Ok(Term::list(
                exports
                    .into_iter()
                    .map(|(n, a)| {
                        Term::tuple(vec![
                            Term::atom(n),
                            a.map(Term::from).unwrap_or_else(|| Term::atom("any")),
                        ])
                    })
                    .collect(),
            ))
        })
}

/// Send `ok` back for a `{Tag, Reply}` style gen reply. Lives on `Node`.
impl Node {
    /// Reply to a `gen_server:call` style `From` (`{Pid, Tag}`), honouring
    /// OTP 24+ alias tags (`[alias | Ref]`).
    pub fn gen_reply(&self, from: &Term, reply: Term) {
        let Some(parts) = from.as_tuple() else { return };
        if parts.len() != 2 {
            return;
        }
        let Some(to) = parts[0].as_pid() else { return };
        let tag = &parts[1];
        let msg = Term::tuple(vec![tag.clone(), reply]);
        let ctrl = match alias_of(tag) {
            Some(alias) => Control::AliasSend {
                from: self.system_pid().clone(),
                alias,
                msg,
            },
            None => Control::Send {
                to: to.clone(),
                msg,
            },
        };
        if let Err(e) = self.send_control(&to.node, ctrl) {
            tracing::debug!("gen_reply to {to} failed: {e}");
        }
    }
}

/// `[alias | Ref]` or `[[alias | Ref] | _]` -> `Ref`.
fn alias_of(tag: &Term) -> Option<crate::term::Reference> {
    match tag {
        Term::ImproperList(items, tail) if items.len() == 1 && items[0].is_atom("alias") => {
            tail.as_reference().cloned()
        }
        Term::ImproperList(items, _) | Term::List(items) => {
            items.first().and_then(|first| match first {
                Term::ImproperList(inner, tail)
                    if inner.len() == 1 && inner[0].is_atom("alias") =>
                {
                    tail.as_reference().cloned()
                }
                _ => None,
            })
        }
        _ => None,
    }
}
