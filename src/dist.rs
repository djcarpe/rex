//! Distribution control messages ("DOP_*" in `dist.h`) and the framing
//! around them.
//!
//! A frame on an established connection is a 4-byte length followed by
//! either the pass-through byte `112`, a version-prefixed control tuple and
//! (for some operations) a version-prefixed payload term, or — once
//! `DFLAG_DIST_HDR_ATOM_CACHE` is negotiated — a distribution header
//! (`131 68`, or `131 69`/`131 70` for fragmented messages) carrying an atom
//! cache table, followed by the same terms without version bytes.
//! [`Inbound`] keeps the per-connection state those headers need.

use crate::error::{Error, Result};
use crate::etf::{self, EtfError};
use crate::term::{Atom, Pid, Reference, Term};
use std::collections::HashMap;

pub const PASS_THROUGH: u8 = 112;
const DIST_HEADER: u8 = 68;
const DIST_FRAG_HEADER: u8 = 69;
const DIST_FRAG_CONT: u8 = 70;
/// Entries in a peer's atom cache: 8 segments of 256.
const ATOM_CACHE_SIZE: usize = 2048;

const LINK: i64 = 1;
const SEND: i64 = 2;
const EXIT: i64 = 3;
const UNLINK: i64 = 4;
const NODE_LINK: i64 = 5;
const REG_SEND: i64 = 6;
const GROUP_LEADER: i64 = 7;
const EXIT2: i64 = 8;
const SEND_TT: i64 = 12;
const EXIT_TT: i64 = 13;
const REG_SEND_TT: i64 = 16;
const EXIT2_TT: i64 = 18;
const MONITOR_P: i64 = 19;
const DEMONITOR_P: i64 = 20;
const MONITOR_P_EXIT: i64 = 21;
const SEND_SENDER: i64 = 22;
const SEND_SENDER_TT: i64 = 23;
const PAYLOAD_EXIT: i64 = 24;
const PAYLOAD_EXIT_TT: i64 = 25;
const PAYLOAD_EXIT2: i64 = 26;
const PAYLOAD_EXIT2_TT: i64 = 27;
const PAYLOAD_MONITOR_P_EXIT: i64 = 28;
const SPAWN_REQUEST: i64 = 29;
const SPAWN_REQUEST_TT: i64 = 30;
const SPAWN_REPLY: i64 = 31;
const SPAWN_REPLY_TT: i64 = 32;
const ALIAS_SEND: i64 = 33;
const ALIAS_SEND_TT: i64 = 34;
const UNLINK_ID: i64 = 35;
const UNLINK_ID_ACK: i64 = 36;

/// Spawn reply flag: a link was set up.
pub const SPAWN_REPLY_FLAG_LINK: i64 = 1;
/// Spawn reply flag: a monitor was set up.
pub const SPAWN_REPLY_FLAG_MONITOR: i64 = 2;

/// Either a pid or a registered name on the other node.
#[derive(Debug, Clone, PartialEq)]
pub enum ProcRef {
    Pid(Pid),
    Name(Atom),
}

impl ProcRef {
    fn from_term(t: &Term) -> Result<ProcRef> {
        match t {
            Term::Pid(p) => Ok(ProcRef::Pid(p.clone())),
            Term::Atom(a) => Ok(ProcRef::Name(a.clone())),
            _ => Err(malformed("process reference")),
        }
    }
    fn to_term(&self) -> Term {
        match self {
            ProcRef::Pid(p) => Term::Pid(p.clone()),
            ProcRef::Name(a) => Term::Atom(a.clone()),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum Control {
    Link {
        from: Pid,
        to: Pid,
    },
    Send {
        to: Pid,
        msg: Term,
    },
    SendSender {
        from: Pid,
        to: Pid,
        msg: Term,
    },
    RegSend {
        from: Pid,
        to_name: Atom,
        msg: Term,
    },
    AliasSend {
        from: Pid,
        alias: Reference,
        msg: Term,
    },
    Exit {
        from: Pid,
        to: Pid,
        reason: Term,
    },
    Exit2 {
        from: Pid,
        to: Pid,
        reason: Term,
    },
    Unlink {
        from: Pid,
        to: Pid,
    },
    UnlinkId {
        id: i64,
        from: Pid,
        to: Pid,
    },
    UnlinkIdAck {
        id: i64,
        from: Pid,
        to: Pid,
    },
    NodeLink,
    GroupLeader {
        from: Pid,
        to: Pid,
    },
    MonitorP {
        from: Pid,
        to: ProcRef,
        reference: Reference,
    },
    DemonitorP {
        from: Pid,
        to: ProcRef,
        reference: Reference,
    },
    MonitorPExit {
        from: ProcRef,
        to: Pid,
        reference: Reference,
        reason: Term,
    },
    SpawnRequest {
        req_id: Reference,
        from: Pid,
        group_leader: Pid,
        module: Atom,
        function: Atom,
        arity: i64,
        opts: Vec<Term>,
        args: Term,
    },
    SpawnReply {
        req_id: Reference,
        to: Pid,
        flags: i64,
        result: Term,
    },
    Unknown {
        control: Term,
        payload: Option<Term>,
    },
}

fn malformed(what: &'static str) -> Error {
    Error::Etf(EtfError::Malformed(what))
}

fn pid(t: &Term) -> Result<Pid> {
    t.as_pid().cloned().ok_or_else(|| malformed("expected pid"))
}
fn atom(t: &Term) -> Result<Atom> {
    t.as_atom()
        .cloned()
        .ok_or_else(|| malformed("expected atom"))
}
fn reference(t: &Term) -> Result<Reference> {
    t.as_reference()
        .cloned()
        .ok_or_else(|| malformed("expected reference"))
}
fn int(t: &Term) -> Result<i64> {
    t.as_int().ok_or_else(|| malformed("expected integer"))
}

impl Control {
    /// Parse a pass-through frame body (everything after the 4-byte length).
    /// Frames with a distribution header go through [`Inbound::frame`].
    pub fn decode(frame: &[u8]) -> Result<Control> {
        if frame.first() != Some(&PASS_THROUGH) {
            return Err(malformed("expected pass-through byte"));
        }
        let (ctrl, used) = etf::decode_prefix(&frame[1..])?;
        let rest = &frame[1 + used..];
        let payload = if rest.is_empty() {
            None
        } else {
            Some(etf::decode(rest)?)
        };
        Self::from_terms(ctrl, payload)
    }

    /// Parse the control tuple and optional payload that follow a
    /// distribution header: no version bytes, atoms may be cache references.
    fn decode_cached(data: &[u8], atoms: &[Atom]) -> Result<Control> {
        let (ctrl, used) = etf::decode_cached(data, atoms)?;
        let rest = &data[used..];
        let payload = if rest.is_empty() {
            None
        } else {
            let (p, used) = etf::decode_cached(rest, atoms)?;
            if used != rest.len() {
                return Err(malformed("trailing bytes after payload"));
            }
            Some(p)
        };
        Self::from_terms(ctrl, payload)
    }

    fn from_terms(ctrl: Term, payload: Option<Term>) -> Result<Control> {
        let Some(items) = ctrl.as_tuple() else {
            return Err(malformed("control message is not a tuple"));
        };
        let op = items
            .first()
            .and_then(|t| t.as_int())
            .ok_or_else(|| malformed("control op"))?;
        let need = |n: usize| -> Result<()> {
            if items.len() < n {
                Err(malformed("control tuple too short"))
            } else {
                Ok(())
            }
        };
        // The payload is moved into the message, never copied: erpc arguments
        // and results can be megabytes.
        let mut payload = payload;
        let mut payload_or = |what: &'static str| payload.take().ok_or(malformed(what));
        Ok(match op {
            LINK => {
                need(3)?;
                Control::Link {
                    from: pid(&items[1])?,
                    to: pid(&items[2])?,
                }
            }
            SEND | SEND_TT => {
                need(3)?;
                Control::Send {
                    to: pid(&items[2])?,
                    msg: payload_or("send payload")?,
                }
            }
            SEND_SENDER | SEND_SENDER_TT => {
                need(3)?;
                Control::SendSender {
                    from: pid(&items[1])?,
                    to: pid(&items[2])?,
                    msg: payload_or("send payload")?,
                }
            }
            REG_SEND | REG_SEND_TT => {
                need(4)?;
                Control::RegSend {
                    from: pid(&items[1])?,
                    to_name: atom(&items[3])?,
                    msg: payload_or("reg_send payload")?,
                }
            }
            ALIAS_SEND | ALIAS_SEND_TT => {
                need(3)?;
                Control::AliasSend {
                    from: pid(&items[1])?,
                    alias: reference(&items[2])?,
                    msg: payload_or("alias_send payload")?,
                }
            }
            EXIT => {
                need(4)?;
                Control::Exit {
                    from: pid(&items[1])?,
                    to: pid(&items[2])?,
                    reason: items[3].clone(),
                }
            }
            EXIT_TT => {
                need(5)?;
                Control::Exit {
                    from: pid(&items[1])?,
                    to: pid(&items[2])?,
                    reason: items[4].clone(),
                }
            }
            EXIT2 => {
                need(4)?;
                Control::Exit2 {
                    from: pid(&items[1])?,
                    to: pid(&items[2])?,
                    reason: items[3].clone(),
                }
            }
            EXIT2_TT => {
                need(5)?;
                Control::Exit2 {
                    from: pid(&items[1])?,
                    to: pid(&items[2])?,
                    reason: items[4].clone(),
                }
            }
            PAYLOAD_EXIT | PAYLOAD_EXIT_TT => {
                need(3)?;
                Control::Exit {
                    from: pid(&items[1])?,
                    to: pid(&items[2])?,
                    reason: payload_or("exit reason")?,
                }
            }
            PAYLOAD_EXIT2 | PAYLOAD_EXIT2_TT => {
                need(3)?;
                Control::Exit2 {
                    from: pid(&items[1])?,
                    to: pid(&items[2])?,
                    reason: payload_or("exit reason")?,
                }
            }
            UNLINK => {
                need(3)?;
                Control::Unlink {
                    from: pid(&items[1])?,
                    to: pid(&items[2])?,
                }
            }
            UNLINK_ID => {
                need(4)?;
                Control::UnlinkId {
                    id: int(&items[1])?,
                    from: pid(&items[2])?,
                    to: pid(&items[3])?,
                }
            }
            UNLINK_ID_ACK => {
                need(4)?;
                Control::UnlinkIdAck {
                    id: int(&items[1])?,
                    from: pid(&items[2])?,
                    to: pid(&items[3])?,
                }
            }
            NODE_LINK => Control::NodeLink,
            GROUP_LEADER => {
                need(3)?;
                Control::GroupLeader {
                    from: pid(&items[1])?,
                    to: pid(&items[2])?,
                }
            }
            MONITOR_P => {
                need(4)?;
                Control::MonitorP {
                    from: pid(&items[1])?,
                    to: ProcRef::from_term(&items[2])?,
                    reference: reference(&items[3])?,
                }
            }
            DEMONITOR_P => {
                need(4)?;
                Control::DemonitorP {
                    from: pid(&items[1])?,
                    to: ProcRef::from_term(&items[2])?,
                    reference: reference(&items[3])?,
                }
            }
            MONITOR_P_EXIT => {
                need(5)?;
                Control::MonitorPExit {
                    from: ProcRef::from_term(&items[1])?,
                    to: pid(&items[2])?,
                    reference: reference(&items[3])?,
                    reason: items[4].clone(),
                }
            }
            PAYLOAD_MONITOR_P_EXIT => {
                need(4)?;
                Control::MonitorPExit {
                    from: ProcRef::from_term(&items[1])?,
                    to: pid(&items[2])?,
                    reference: reference(&items[3])?,
                    reason: payload_or("monitor exit reason")?,
                }
            }
            SPAWN_REQUEST | SPAWN_REQUEST_TT => {
                need(6)?;
                let mfa = items[4].as_tuple().ok_or_else(|| malformed("spawn mfa"))?;
                if mfa.len() != 3 {
                    return Err(malformed("spawn mfa arity"));
                }
                Control::SpawnRequest {
                    req_id: reference(&items[1])?,
                    from: pid(&items[2])?,
                    group_leader: pid(&items[3])?,
                    module: atom(&mfa[0])?,
                    function: atom(&mfa[1])?,
                    arity: int(&mfa[2])?,
                    opts: items[5].as_list().map(|l| l.to_vec()).unwrap_or_default(),
                    args: payload_or("spawn arguments")?,
                }
            }
            SPAWN_REPLY | SPAWN_REPLY_TT => {
                need(5)?;
                Control::SpawnReply {
                    req_id: reference(&items[1])?,
                    to: pid(&items[2])?,
                    flags: int(&items[3])?,
                    result: items[4].clone(),
                }
            }
            _ => Control::Unknown {
                control: ctrl.clone(),
                payload: payload_or("").ok(),
            },
        })
    }

    /// Encode as a frame: 4 bytes reserved for the length prefix (filled in by
    /// the connection writer), then the pass-through byte and the terms, or,
    /// with `dist_header` (peer negotiated `DFLAG_DIST_HDR_ATOM_CACHE`), a
    /// distribution header with an empty atom table and the terms without
    /// version bytes. Consumes the message so large payloads are never copied.
    /// With `exit_payload` (peer negotiated `DFLAG_EXIT_PAYLOAD`) exit reasons
    /// go in the payload term instead of the control tuple, which the BEAM
    /// handles far more efficiently for large reasons such as erpc results.
    pub fn encode(self, exit_payload: bool, dist_header: bool) -> Vec<u8> {
        let (ctrl, payload): (Term, Option<Term>) = match self {
            Control::Exit { from, to, reason } if exit_payload => {
                (tuple(vec![Term::Int(PAYLOAD_EXIT), Term::Pid(from), Term::Pid(to)]), Some(reason))
            }
            Control::Exit2 { from, to, reason } if exit_payload => {
                (tuple(vec![Term::Int(PAYLOAD_EXIT2), Term::Pid(from), Term::Pid(to)]), Some(reason))
            }
            Control::MonitorPExit { from, to, reference, reason } if exit_payload => (
                tuple(vec![Term::Int(PAYLOAD_MONITOR_P_EXIT), from.to_term(), Term::Pid(to), Term::Reference(reference)]),
                Some(reason),
            ),
            Control::Link { from, to } => (tuple(vec![Term::Int(LINK), Term::Pid(from), Term::Pid(to)]), None),
            Control::Send { to, msg } => (tuple(vec![Term::Int(SEND), Term::atom(""), Term::Pid(to)]), Some(msg)),
            Control::SendSender { from, to, msg } => (tuple(vec![Term::Int(SEND_SENDER), Term::Pid(from), Term::Pid(to)]), Some(msg)),
            Control::RegSend { from, to_name, msg } => {
                (tuple(vec![Term::Int(REG_SEND), Term::Pid(from), Term::atom(""), Term::Atom(to_name)]), Some(msg))
            }
            Control::AliasSend { from, alias, msg } => {
                (tuple(vec![Term::Int(ALIAS_SEND), Term::Pid(from), Term::Reference(alias)]), Some(msg))
            }
            Control::Exit { from, to, reason } => (tuple(vec![Term::Int(EXIT), Term::Pid(from), Term::Pid(to), reason]), None),
            Control::Exit2 { from, to, reason } => (tuple(vec![Term::Int(EXIT2), Term::Pid(from), Term::Pid(to), reason]), None),
            Control::Unlink { from, to } => (tuple(vec![Term::Int(UNLINK), Term::Pid(from), Term::Pid(to)]), None),
            Control::UnlinkId { id, from, to } => (tuple(vec![Term::Int(UNLINK_ID), Term::Int(id), Term::Pid(from), Term::Pid(to)]), None),
            Control::UnlinkIdAck { id, from, to } => {
                (tuple(vec![Term::Int(UNLINK_ID_ACK), Term::Int(id), Term::Pid(from), Term::Pid(to)]), None)
            }
            Control::NodeLink => (tuple(vec![Term::Int(NODE_LINK)]), None),
            Control::GroupLeader { from, to } => (tuple(vec![Term::Int(GROUP_LEADER), Term::Pid(from), Term::Pid(to)]), None),
            Control::MonitorP { from, to, reference } => {
                (tuple(vec![Term::Int(MONITOR_P), Term::Pid(from), to.to_term(), Term::Reference(reference)]), None)
            }
            Control::DemonitorP { from, to, reference } => {
                (tuple(vec![Term::Int(DEMONITOR_P), Term::Pid(from), to.to_term(), Term::Reference(reference)]), None)
            }
            Control::MonitorPExit { from, to, reference, reason } => (
                tuple(vec![Term::Int(MONITOR_P_EXIT), from.to_term(), Term::Pid(to), Term::Reference(reference), reason]),
                None,
            ),
            Control::SpawnRequest { req_id, from, group_leader, module, function, arity, opts, args } => (
                tuple(vec![
                    Term::Int(SPAWN_REQUEST),
                    Term::Reference(req_id),
                    Term::Pid(from),
                    Term::Pid(group_leader),
                    tuple(vec![Term::Atom(module), Term::Atom(function), Term::Int(arity)]),
                    Term::List(opts),
                ]),
                Some(args),
            ),
            Control::SpawnReply { req_id, to, flags, result } => (
                tuple(vec![Term::Int(SPAWN_REPLY), Term::Reference(req_id), Term::Pid(to), Term::Int(flags), result]),
                None,
            ),
            Control::Unknown { control, payload } => (control, payload),
        };
        let size = 8 + etf::encoded_len(&ctrl) + payload.as_ref().map(|p| 1 + etf::encoded_len(p)).unwrap_or(0);
        let mut buf = Vec::with_capacity(size);
        buf.extend_from_slice(&[0, 0, 0, 0]);
        if dist_header {
            buf.extend_from_slice(&[etf::VERSION, DIST_HEADER, 0]);
        } else {
            buf.extend_from_slice(&[PASS_THROUGH, etf::VERSION]);
        }
        etf::encode_into(&ctrl, &mut buf);
        if let Some(p) = payload {
            if !dist_header {
                buf.push(etf::VERSION);
            }
            etf::encode_into(&p, &mut buf);
        }
        buf
    }
}

/// A fragmented message being reassembled (`DIST_FRAG_HEADER` + `_CONT`s).
struct Fragmented {
    /// The fragment id we expect next (they count down to 1).
    next: u64,
    atoms: Vec<Atom>,
    data: Vec<u8>,
}

/// Inbound framing state for one connection: the peer's atom cache (filled
/// by distribution headers, referenced by `ATOM_CACHE_REF`s) and fragmented
/// messages in flight.
pub struct Inbound {
    cache: Vec<Option<Atom>>,
    fragments: HashMap<u64, Fragmented>,
    /// Upper bound on a reassembled message, so a misbehaving peer cannot
    /// make us buffer without end.
    max_message: usize,
}

impl Inbound {
    pub fn new(max_message: usize) -> Self {
        Inbound {
            cache: vec![None; ATOM_CACHE_SIZE],
            fragments: HashMap::new(),
            max_message,
        }
    }

    /// Feed one frame body (everything after the 4-byte length). Returns the
    /// control message once one is complete; `None` while a fragmented
    /// message is still arriving.
    pub fn frame(&mut self, body: &[u8]) -> Result<Option<Control>> {
        match body.first() {
            Some(&PASS_THROUGH) => return Control::decode(body).map(Some),
            Some(&etf::VERSION) => {}
            _ => return Err(malformed("frame starts with neither pass-through nor version byte")),
        }
        let Some(&tag) = body.get(1) else {
            return Err(malformed("truncated distribution header"));
        };
        let mut c = Cursor { buf: body, pos: 2 };
        match tag {
            DIST_HEADER => {
                let atoms = self.atom_refs(&mut c)?;
                Control::decode_cached(&body[c.pos..], &atoms).map(Some)
            }
            DIST_FRAG_HEADER => {
                let seq = c.u64()?;
                let frag = c.u64()?;
                let atoms = self.atom_refs(&mut c)?;
                let data = &body[c.pos..];
                if frag <= 1 {
                    return Control::decode_cached(data, &atoms).map(Some);
                }
                if self.fragments.len() >= 1024 {
                    return Err(malformed("too many fragmented messages in flight"));
                }
                let mut buf = Vec::with_capacity((data.len() * frag as usize).min(self.max_message));
                buf.extend_from_slice(data);
                self.fragments.insert(seq, Fragmented { next: frag - 1, atoms, data: buf });
                Ok(None)
            }
            DIST_FRAG_CONT => {
                let seq = c.u64()?;
                let frag = c.u64()?;
                let Some(entry) = self.fragments.get_mut(&seq) else {
                    return Err(malformed("fragment continuation for unknown sequence"));
                };
                if frag != entry.next {
                    self.fragments.remove(&seq);
                    return Err(malformed("fragment out of order"));
                }
                let data = &body[c.pos..];
                if entry.data.len() + data.len() > self.max_message {
                    self.fragments.remove(&seq);
                    return Err(malformed("fragmented message exceeds size limit"));
                }
                entry.data.extend_from_slice(data);
                entry.next -= 1;
                if frag == 1 {
                    let done = self.fragments.remove(&seq).unwrap();
                    tracing::debug!(seq, bytes = done.data.len(), "reassembled fragmented message");
                    Control::decode_cached(&done.data, &done.atoms).map(Some)
                } else {
                    Ok(None)
                }
            }
            other => Err(Error::Etf(EtfError::UnsupportedTag(other))),
        }
    }

    /// Parse `NumberOfAtomCacheRefs | Flags | AtomCacheRefs`, updating the
    /// cache with new entries, and return this message's atom table.
    fn atom_refs(&mut self, c: &mut Cursor) -> Result<Vec<Atom>> {
        let n = c.u8()? as usize;
        if n == 0 {
            return Ok(Vec::new());
        }
        let flags = c.take(n / 2 + 1)?;
        // Nibble i (low nibble first) is the flag of reference i; the nibble
        // after the last reference carries the LongAtoms bit.
        let nibble = |i: usize| if i % 2 == 0 { flags[i / 2] & 0x0f } else { flags[i / 2] >> 4 };
        let long_atoms = nibble(n) & 1 != 0;
        let mut atoms = Vec::with_capacity(n);
        for i in 0..n {
            let f = nibble(i);
            let new_entry = f & 0x08 != 0;
            let idx = ((f & 0x07) as usize) << 8 | c.u8()? as usize;
            if new_entry {
                let len = if long_atoms { c.u16()? as usize } else { c.u8()? as usize };
                let text = std::str::from_utf8(c.take(len)?).map_err(|_| Error::Etf(EtfError::BadAtom))?;
                let atom = Atom::new(text);
                tracing::trace!(idx, %atom, "new atom cache entry");
                self.cache[idx] = Some(atom.clone());
                atoms.push(atom);
            } else {
                let atom = self.cache[idx].clone().ok_or_else(|| malformed("reference to an empty atom cache slot"))?;
                atoms.push(atom);
            }
        }
        Ok(atoms)
    }
}

struct Cursor<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Cursor<'a> {
    fn take(&mut self, n: usize) -> Result<&'a [u8]> {
        if self.pos + n > self.buf.len() {
            return Err(Error::Etf(EtfError::Eof));
        }
        let s = &self.buf[self.pos..self.pos + n];
        self.pos += n;
        Ok(s)
    }
    fn u8(&mut self) -> Result<u8> {
        Ok(self.take(1)?[0])
    }
    fn u16(&mut self) -> Result<u16> {
        Ok(u16::from_be_bytes(self.take(2)?.try_into().unwrap()))
    }
    fn u64(&mut self) -> Result<u64> {
        Ok(u64::from_be_bytes(self.take(8)?.try_into().unwrap()))
    }
}

fn tuple(items: Vec<Term>) -> Term {
    Term::Tuple(items)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pid(id: u32) -> Pid {
        Pid {
            node: Atom::new("a@b"),
            id,
            serial: 0,
            creation: 1,
        }
    }

    #[test]
    fn control_roundtrip() {
        let cases = vec![
            Control::Send {
                to: pid(1),
                msg: Term::atom("hi"),
            },
            Control::RegSend {
                from: pid(1),
                to_name: Atom::new("rex"),
                msg: Term::Int(3),
            },
            Control::Exit {
                from: pid(1),
                to: pid(2),
                reason: Term::atom("normal"),
            },
            Control::MonitorP {
                from: pid(1),
                to: ProcRef::Name(Atom::new("x")),
                reference: Reference {
                    node: Atom::new("a@b"),
                    creation: 1,
                    ids: vec![1, 2, 3],
                },
            },
            Control::SpawnRequest {
                req_id: Reference {
                    node: Atom::new("a@b"),
                    creation: 1,
                    ids: vec![1],
                },
                from: pid(1),
                group_leader: pid(2),
                module: Atom::new("erpc"),
                function: Atom::new("execute_call"),
                arity: 4,
                opts: vec![Term::atom("monitor")],
                args: Term::list(vec![Term::Int(1)]),
            },
            Control::SpawnReply {
                req_id: Reference {
                    node: Atom::new("a@b"),
                    creation: 1,
                    ids: vec![1],
                },
                to: pid(1),
                flags: 2,
                result: Term::Pid(pid(9)),
            },
            Control::UnlinkIdAck {
                id: 7,
                from: pid(1),
                to: pid(2),
            },
        ];
        for c in cases {
            for exit_payload in [false, true] {
                let bytes = c.clone().encode(exit_payload, false);
                let back = Control::decode(&bytes[4..]).unwrap();
                assert_eq!(c, back);
                let bytes = c.clone().encode(exit_payload, true);
                assert_eq!(&bytes[4..7], &[131, 68, 0]);
                let back = Inbound::new(1 << 20).frame(&bytes[4..]).unwrap().unwrap();
                assert_eq!(c, back);
            }
        }
    }

    /// `{2, '', Pid}` + payload `{hello, world}` with the atoms sent through
    /// the atom cache the way the BEAM does: first message defines the
    /// entries, the second only references them.
    #[test]
    fn dist_header_atom_cache() {
        let mut inbound = Inbound::new(1 << 20);
        // refs: 0 = '' (new, segment 0, slot 1), 1 = 'a@b' (new, seg 1, slot 2),
        // 2 = hello (new, seg 0, slot 3), 3 = world (existing after msg 1).
        let mut first = vec![131, 68, 3];
        first.extend_from_slice(&[0x98, 0x08]); // nibbles: new/seg0, new/seg1, new/seg0, long_atoms=0
        first.extend_from_slice(&[1, 0]); // '' at 0:1
        first.extend_from_slice(&[2, 3, b'a', b'@', b'b']); // 'a@b' at 1:2
        first.extend_from_slice(&[3, 5, b'h', b'e', b'l', b'l', b'o']); // hello at 0:3
        // control {2, '', Pid}: SMALL_TUPLE 3, SMALL_INTEGER 2, ATOM_CACHE_REF 0, NEW_PID(node=ref 1)
        first.extend_from_slice(&[104, 3, 97, 2, 82, 0, 88, 82, 1, 0, 0, 0, 7, 0, 0, 0, 0, 0, 0, 0, 1]);
        // payload {hello, world}: world as a plain atom this time
        first.extend_from_slice(&[104, 2, 82, 2, 119, 5, b'w', b'o', b'r', b'l', b'd']);
        let got = inbound.frame(&first).unwrap().unwrap();
        assert_eq!(
            got,
            Control::Send { to: pid(7), msg: Term::tuple(vec![Term::atom("hello"), Term::atom("world")]) }
        );
        // Second message: every atom is an existing cache entry.
        let mut second = vec![131, 68, 3];
        second.extend_from_slice(&[0x10, 0x00]); // nibbles: existing seg0, existing seg1, existing seg0, long_atoms=0
        second.extend_from_slice(&[1, 2, 3]);
        second.extend_from_slice(&[104, 3, 97, 2, 82, 0, 88, 82, 1, 0, 0, 0, 7, 0, 0, 0, 0, 0, 0, 0, 1]);
        second.extend_from_slice(&[82, 2]);
        let got = inbound.frame(&second).unwrap().unwrap();
        assert_eq!(got, Control::Send { to: pid(7), msg: Term::atom("hello") });
        // A fresh connection does not know those slots.
        assert!(Inbound::new(1 << 20).frame(&second).is_err());
    }

    #[test]
    fn fragments_reassemble() {
        let mut inbound = Inbound::new(1 << 20);
        let payload = Term::Binary(vec![9u8; 1000]);
        let whole = Control::Send { to: pid(7), msg: payload.clone() }.encode(false, true);
        let terms = &whole[7..]; // after length + `131 68 0`
        let (a, rest) = terms.split_at(100);
        let (b, c) = rest.split_at(500);
        let seq = 42u64.to_be_bytes();
        let mut f1 = vec![131, 69];
        f1.extend_from_slice(&seq);
        f1.extend_from_slice(&3u64.to_be_bytes());
        f1.push(0);
        f1.extend_from_slice(a);
        let mut f2 = vec![131, 70];
        f2.extend_from_slice(&seq);
        f2.extend_from_slice(&2u64.to_be_bytes());
        f2.extend_from_slice(b);
        let mut f3 = vec![131, 70];
        f3.extend_from_slice(&seq);
        f3.extend_from_slice(&1u64.to_be_bytes());
        f3.extend_from_slice(c);
        assert!(inbound.frame(&f1).unwrap().is_none());
        // An unrelated pass-through frame may interleave.
        let other = Control::Link { from: pid(1), to: pid(2) }.encode(false, false);
        assert_eq!(inbound.frame(&other[4..]).unwrap(), Some(Control::Link { from: pid(1), to: pid(2) }));
        assert!(inbound.frame(&f2).unwrap().is_none());
        assert_eq!(inbound.frame(&f3).unwrap(), Some(Control::Send { to: pid(7), msg: payload }));
        // Out of order is an error and drops the sequence.
        assert!(inbound.frame(&f1).unwrap().is_none());
        assert!(inbound.frame(&f3).is_err());
        assert!(inbound.frame(&f2).is_err());
    }
}
