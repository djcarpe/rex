//! Erlang External Term Format (ETF) encoding and decoding.
//!
//! Everything a distribution peer can send us is decoded, including
//! `ATOM_CACHE_REF`s when the caller supplies the atom table from a
//! distribution header ([`decode_cached`]). Encoding always uses the modern
//! tags (UTF-8 atoms, NEW_PID, NEWER_REFERENCE, NEW_FLOAT) which every
//! OTP 23+ node accepts.

use crate::term::{Atom, BigInt, Fun, Pid, Port, Reference, Term};
use thiserror::Error;

/// Version byte that prefixes every stand-alone term.
pub const VERSION: u8 = 131;

const NEW_FLOAT_EXT: u8 = 70;
const BIT_BINARY_EXT: u8 = 77;
const ATOM_CACHE_REF: u8 = 82;
const NEW_PID_EXT: u8 = 88;
const NEW_PORT_EXT: u8 = 89;
const NEWER_REFERENCE_EXT: u8 = 90;
const SMALL_INTEGER_EXT: u8 = 97;
const INTEGER_EXT: u8 = 98;
const FLOAT_EXT: u8 = 99;
const ATOM_EXT: u8 = 100;
const REFERENCE_EXT: u8 = 101;
const PORT_EXT: u8 = 102;
const PID_EXT: u8 = 103;
const SMALL_TUPLE_EXT: u8 = 104;
const LARGE_TUPLE_EXT: u8 = 105;
const NIL_EXT: u8 = 106;
const STRING_EXT: u8 = 107;
const LIST_EXT: u8 = 108;
const BINARY_EXT: u8 = 109;
const SMALL_BIG_EXT: u8 = 110;
const LARGE_BIG_EXT: u8 = 111;
const NEW_FUN_EXT: u8 = 112;
const EXPORT_EXT: u8 = 113;
const NEW_REFERENCE_EXT: u8 = 114;
const SMALL_ATOM_EXT: u8 = 115;
const MAP_EXT: u8 = 116;
const FUN_EXT: u8 = 117;
const ATOM_UTF8_EXT: u8 = 118;
const SMALL_ATOM_UTF8_EXT: u8 = 119;
const V4_PORT_EXT: u8 = 120;
const COMPRESSED: u8 = 80;

#[derive(Debug, Error)]
pub enum EtfError {
    #[error("unexpected end of term data")]
    Eof,
    #[error("bad version byte {0}, expected 131")]
    BadVersion(u8),
    #[error("unsupported term tag {0}")]
    UnsupportedTag(u8),
    #[error("atom cache reference without a matching distribution header entry")]
    AtomCacheRef,
    #[error("compressed terms are not supported")]
    Compressed,
    #[error("invalid utf-8 in atom")]
    BadAtom,
    #[error("malformed term: {0}")]
    Malformed(&'static str),
}

// ---------------------------------------------------------------------------
// Encoding
// ---------------------------------------------------------------------------

/// Encode a term with the leading version byte.
pub fn encode(term: &Term) -> Vec<u8> {
    let mut buf = Vec::with_capacity(1 + encoded_len(term));
    buf.push(VERSION);
    encode_into(term, &mut buf);
    buf
}

/// The exact number of bytes [`encode_into`] appends for `term`. One cheap
/// pass over the term lets callers size the output buffer once instead of
/// growing it by doubling (which copies a multi-megabyte binary several times).
pub fn encoded_len(term: &Term) -> usize {
    match term {
        Term::Atom(a) => atom_len(a),
        Term::Int(i) => {
            if (0..=255).contains(i) {
                2
            } else if *i >= i32::MIN as i64 && *i <= i32::MAX as i64 {
                5
            } else {
                3 + trimmed_len(&i.unsigned_abs().to_le_bytes())
            }
        }
        Term::BigInt(b) => {
            let n = trimmed_len(&b.magnitude);
            (if n < 256 { 3 } else { 6 }) + n
        }
        Term::Float(_) => 9,
        Term::Tuple(items) => {
            (if items.len() < 256 { 2 } else { 5 }) + items.iter().map(encoded_len).sum::<usize>()
        }
        Term::List(items) => {
            if items.is_empty() {
                1
            } else if is_string(items) {
                3 + items.len()
            } else {
                6 + items.iter().map(encoded_len).sum::<usize>()
            }
        }
        Term::ImproperList(items, tail) => 5 + items.iter().map(encoded_len).sum::<usize>() + encoded_len(tail),
        Term::Binary(b) => 5 + b.len(),
        Term::BitBinary { bytes, .. } => 6 + bytes.len(),
        Term::Map(pairs) => 5 + pairs.iter().map(|(k, v)| encoded_len(k) + encoded_len(v)).sum::<usize>(),
        Term::Pid(p) => 13 + atom_len(&p.node),
        Term::Port(p) => (if p.id > u32::MAX as u64 { 13 } else { 9 }) + atom_len(&p.node),
        Term::Reference(r) => 7 + atom_len(&r.node) + 4 * r.ids.len(),
        Term::Export { module, function, arity } => 1 + atom_len(module) + atom_len(function) + encoded_len(&Term::Int(*arity as i64)),
        Term::Fun(fun) => {
            1 + 4 + 1 + 16 + 4 + 4
                + atom_len(&fun.module)
                + encoded_len(&Term::Int(fun.old_index))
                + encoded_len(&Term::Int(fun.old_uniq))
                + 13
                + atom_len(&fun.pid.node)
                + fun.free_vars.iter().map(encoded_len).sum::<usize>()
        }
    }
}

fn atom_len(a: &Atom) -> usize {
    let n = a.as_str().len();
    (if n < 256 { 2 } else { 3 }) + n
}

fn trimmed_len(magnitude: &[u8]) -> usize {
    let mut n = magnitude.len();
    while n > 0 && magnitude[n - 1] == 0 {
        n -= 1;
    }
    n
}

fn is_string(items: &[Term]) -> bool {
    items.len() <= 0xffff && items.iter().all(|t| matches!(t, Term::Int(i) if (0..=255).contains(i)))
}

/// Append the encoding of `term` (without version byte) to `buf`.
pub fn encode_into(term: &Term, buf: &mut Vec<u8>) {
    match term {
        Term::Atom(a) => encode_atom(a, buf),
        Term::Int(i) => encode_int(*i, buf),
        Term::BigInt(b) => encode_big(b.negative, &b.magnitude, buf),
        Term::Float(f) => {
            buf.push(NEW_FLOAT_EXT);
            buf.extend_from_slice(&f.to_bits().to_be_bytes());
        }
        Term::Tuple(items) => {
            if items.len() < 256 {
                buf.push(SMALL_TUPLE_EXT);
                buf.push(items.len() as u8);
            } else {
                buf.push(LARGE_TUPLE_EXT);
                buf.extend_from_slice(&(items.len() as u32).to_be_bytes());
            }
            for t in items {
                encode_into(t, buf);
            }
        }
        Term::List(items) => {
            if items.is_empty() {
                buf.push(NIL_EXT);
            } else if is_string(items) {
                buf.push(STRING_EXT);
                buf.extend_from_slice(&(items.len() as u16).to_be_bytes());
                for t in items {
                    if let Term::Int(i) = t {
                        buf.push(*i as u8);
                    }
                }
            } else {
                buf.push(LIST_EXT);
                buf.extend_from_slice(&(items.len() as u32).to_be_bytes());
                for t in items {
                    encode_into(t, buf);
                }
                buf.push(NIL_EXT);
            }
        }
        Term::ImproperList(items, tail) => {
            buf.push(LIST_EXT);
            buf.extend_from_slice(&(items.len() as u32).to_be_bytes());
            for t in items {
                encode_into(t, buf);
            }
            encode_into(tail, buf);
        }
        Term::Binary(b) => {
            buf.push(BINARY_EXT);
            buf.extend_from_slice(&(b.len() as u32).to_be_bytes());
            buf.extend_from_slice(b);
        }
        Term::BitBinary { bytes, bits } => {
            buf.push(BIT_BINARY_EXT);
            buf.extend_from_slice(&(bytes.len() as u32).to_be_bytes());
            buf.push(*bits);
            buf.extend_from_slice(bytes);
        }
        Term::Map(pairs) => {
            buf.push(MAP_EXT);
            buf.extend_from_slice(&(pairs.len() as u32).to_be_bytes());
            for (k, v) in pairs {
                encode_into(k, buf);
                encode_into(v, buf);
            }
        }
        Term::Pid(p) => encode_pid(p, buf),
        Term::Port(p) => {
            if p.id > u32::MAX as u64 {
                buf.push(V4_PORT_EXT);
                encode_atom(&p.node, buf);
                buf.extend_from_slice(&p.id.to_be_bytes());
            } else {
                buf.push(NEW_PORT_EXT);
                encode_atom(&p.node, buf);
                buf.extend_from_slice(&(p.id as u32).to_be_bytes());
            }
            buf.extend_from_slice(&p.creation.to_be_bytes());
        }
        Term::Reference(r) => {
            buf.push(NEWER_REFERENCE_EXT);
            buf.extend_from_slice(&(r.ids.len() as u16).to_be_bytes());
            encode_atom(&r.node, buf);
            buf.extend_from_slice(&r.creation.to_be_bytes());
            for id in &r.ids {
                buf.extend_from_slice(&id.to_be_bytes());
            }
        }
        Term::Export {
            module,
            function,
            arity,
        } => {
            buf.push(EXPORT_EXT);
            encode_atom(module, buf);
            encode_atom(function, buf);
            encode_int(*arity as i64, buf);
        }
        Term::Fun(fun) => {
            buf.push(NEW_FUN_EXT);
            let size_pos = buf.len();
            buf.extend_from_slice(&[0, 0, 0, 0]);
            buf.push(fun.arity);
            buf.extend_from_slice(&fun.uniq);
            buf.extend_from_slice(&fun.index.to_be_bytes());
            buf.extend_from_slice(&(fun.free_vars.len() as u32).to_be_bytes());
            encode_atom(&fun.module, buf);
            encode_int(fun.old_index, buf);
            encode_int(fun.old_uniq, buf);
            encode_pid(&fun.pid, buf);
            for v in &fun.free_vars {
                encode_into(v, buf);
            }
            let size = (buf.len() - size_pos) as u32;
            buf[size_pos..size_pos + 4].copy_from_slice(&size.to_be_bytes());
        }
    }
}

fn encode_atom(a: &Atom, buf: &mut Vec<u8>) {
    let bytes = a.as_str().as_bytes();
    if bytes.len() < 256 {
        buf.push(SMALL_ATOM_UTF8_EXT);
        buf.push(bytes.len() as u8);
    } else {
        buf.push(ATOM_UTF8_EXT);
        buf.extend_from_slice(&(bytes.len() as u16).to_be_bytes());
    }
    buf.extend_from_slice(bytes);
}

fn encode_int(i: i64, buf: &mut Vec<u8>) {
    if (0..=255).contains(&i) {
        buf.push(SMALL_INTEGER_EXT);
        buf.push(i as u8);
    } else if i >= i32::MIN as i64 && i <= i32::MAX as i64 {
        buf.push(INTEGER_EXT);
        buf.extend_from_slice(&(i as i32).to_be_bytes());
    } else {
        let mag = i.unsigned_abs().to_le_bytes();
        encode_big(i < 0, &mag, buf);
    }
}

fn encode_big(negative: bool, magnitude: &[u8], buf: &mut Vec<u8>) {
    let mut n = magnitude.len();
    while n > 0 && magnitude[n - 1] == 0 {
        n -= 1;
    }
    if n < 256 {
        buf.push(SMALL_BIG_EXT);
        buf.push(n as u8);
    } else {
        buf.push(LARGE_BIG_EXT);
        buf.extend_from_slice(&(n as u32).to_be_bytes());
    }
    buf.push(if negative { 1 } else { 0 });
    buf.extend_from_slice(&magnitude[..n]);
}

fn encode_pid(p: &Pid, buf: &mut Vec<u8>) {
    buf.push(NEW_PID_EXT);
    encode_atom(&p.node, buf);
    buf.extend_from_slice(&p.id.to_be_bytes());
    buf.extend_from_slice(&p.serial.to_be_bytes());
    buf.extend_from_slice(&p.creation.to_be_bytes());
}

// ---------------------------------------------------------------------------
// Decoding
// ---------------------------------------------------------------------------

/// Decode one term that starts with the version byte. Trailing bytes are an
/// error; use [`decode_prefix`] when several terms are concatenated.
pub fn decode(bytes: &[u8]) -> Result<Term, EtfError> {
    let (term, used) = decode_prefix(bytes)?;
    if used != bytes.len() {
        return Err(EtfError::Malformed("trailing bytes after term"));
    }
    Ok(term)
}

/// Decode one version-prefixed term and return it with the number of bytes
/// consumed.
pub fn decode_prefix(bytes: &[u8]) -> Result<(Term, usize), EtfError> {
    let mut d = Decoder { buf: bytes, pos: 0, atoms: &[] };
    let v = d.u8()?;
    if v != VERSION {
        return Err(EtfError::BadVersion(v));
    }
    let t = d.term()?;
    Ok((t, d.pos))
}

/// Decode one term as it appears after a distribution header: no version
/// byte, and `ATOM_CACHE_REF` tags index into `atoms`, the table the header
/// carried. Returns the term and the number of bytes consumed.
pub fn decode_cached(bytes: &[u8], atoms: &[Atom]) -> Result<(Term, usize), EtfError> {
    let mut d = Decoder { buf: bytes, pos: 0, atoms };
    let t = d.term()?;
    Ok((t, d.pos))
}

struct Decoder<'a> {
    buf: &'a [u8],
    pos: usize,
    /// Atom cache references in this message, from the distribution header.
    atoms: &'a [Atom],
}

impl<'a> Decoder<'a> {
    fn take(&mut self, n: usize) -> Result<&'a [u8], EtfError> {
        if self.pos + n > self.buf.len() {
            return Err(EtfError::Eof);
        }
        let s = &self.buf[self.pos..self.pos + n];
        self.pos += n;
        Ok(s)
    }
    fn u8(&mut self) -> Result<u8, EtfError> {
        Ok(self.take(1)?[0])
    }
    /// A safe pre-allocation for `n` elements: every element takes at least
    /// one byte, so a forged length cannot make us reserve more than the
    /// input holds.
    fn bounded(&self, n: usize) -> usize {
        n.min(self.buf.len() - self.pos)
    }
    fn u16(&mut self) -> Result<u16, EtfError> {
        Ok(u16::from_be_bytes(self.take(2)?.try_into().unwrap()))
    }
    fn u32(&mut self) -> Result<u32, EtfError> {
        Ok(u32::from_be_bytes(self.take(4)?.try_into().unwrap()))
    }
    fn u64(&mut self) -> Result<u64, EtfError> {
        Ok(u64::from_be_bytes(self.take(8)?.try_into().unwrap()))
    }
    fn i32(&mut self) -> Result<i32, EtfError> {
        Ok(i32::from_be_bytes(self.take(4)?.try_into().unwrap()))
    }

    fn atom(&mut self) -> Result<Atom, EtfError> {
        let tag = self.u8()?;
        self.atom_body(tag)
    }

    fn atom_body(&mut self, tag: u8) -> Result<Atom, EtfError> {
        match tag {
            SMALL_ATOM_UTF8_EXT => {
                let n = self.u8()? as usize;
                let s = std::str::from_utf8(self.take(n)?).map_err(|_| EtfError::BadAtom)?;
                Ok(Atom::new(s))
            }
            ATOM_UTF8_EXT => {
                let n = self.u16()? as usize;
                let s = std::str::from_utf8(self.take(n)?).map_err(|_| EtfError::BadAtom)?;
                Ok(Atom::new(s))
            }
            ATOM_EXT => {
                let n = self.u16()? as usize;
                Ok(Atom::new(latin1(self.take(n)?)))
            }
            SMALL_ATOM_EXT => {
                let n = self.u8()? as usize;
                Ok(Atom::new(latin1(self.take(n)?)))
            }
            ATOM_CACHE_REF => {
                let idx = self.u8()? as usize;
                self.atoms.get(idx).cloned().ok_or(EtfError::AtomCacheRef)
            }
            other => Err(EtfError::UnsupportedTag(other)),
        }
    }

    fn pid_body(&mut self, tag: u8) -> Result<Pid, EtfError> {
        let node = self.atom()?;
        let id = self.u32()?;
        let serial = self.u32()?;
        let creation = if tag == NEW_PID_EXT {
            self.u32()?
        } else {
            self.u8()? as u32
        };
        Ok(Pid {
            node,
            id,
            serial,
            creation,
        })
    }

    fn small_int_term(&mut self) -> Result<i64, EtfError> {
        match self.term()? {
            Term::Int(i) => Ok(i),
            _ => Err(EtfError::Malformed("expected integer")),
        }
    }

    fn term(&mut self) -> Result<Term, EtfError> {
        let tag = self.u8()?;
        match tag {
            SMALL_INTEGER_EXT => Ok(Term::Int(self.u8()? as i64)),
            INTEGER_EXT => Ok(Term::Int(self.i32()? as i64)),
            NEW_FLOAT_EXT => Ok(Term::Float(f64::from_bits(self.u64()?))),
            FLOAT_EXT => {
                let s = self.take(31)?;
                let s = std::str::from_utf8(s).map_err(|_| EtfError::Malformed("float"))?;
                let s = s.trim_end_matches('\0').trim();
                s.parse::<f64>()
                    .map(Term::Float)
                    .map_err(|_| EtfError::Malformed("float"))
            }
            SMALL_ATOM_UTF8_EXT | ATOM_UTF8_EXT | ATOM_EXT | SMALL_ATOM_EXT | ATOM_CACHE_REF => {
                Ok(Term::Atom(self.atom_body(tag)?))
            }
            SMALL_TUPLE_EXT | LARGE_TUPLE_EXT => {
                let n = if tag == SMALL_TUPLE_EXT {
                    self.u8()? as usize
                } else {
                    self.u32()? as usize
                };
                let mut items = Vec::with_capacity(self.bounded(n));
                for _ in 0..n {
                    items.push(self.term()?);
                }
                Ok(Term::Tuple(items))
            }
            NIL_EXT => Ok(Term::List(Vec::new())),
            STRING_EXT => {
                let n = self.u16()? as usize;
                let bytes = self.take(n)?;
                Ok(Term::List(
                    bytes.iter().map(|b| Term::Int(*b as i64)).collect(),
                ))
            }
            LIST_EXT => {
                let n = self.u32()? as usize;
                let mut items = Vec::with_capacity(self.bounded(n));
                for _ in 0..n {
                    items.push(self.term()?);
                }
                match self.term()? {
                    Term::List(tail) => {
                        items.extend(tail);
                        Ok(Term::List(items))
                    }
                    Term::ImproperList(more, tail) => {
                        items.extend(more);
                        Ok(Term::ImproperList(items, tail))
                    }
                    tail => Ok(Term::ImproperList(items, Box::new(tail))),
                }
            }
            BINARY_EXT => {
                let n = self.u32()? as usize;
                Ok(Term::Binary(self.take(n)?.to_vec()))
            }
            BIT_BINARY_EXT => {
                let n = self.u32()? as usize;
                let bits = self.u8()?;
                let bytes = self.take(n)?.to_vec();
                if bits == 8 || bytes.is_empty() {
                    Ok(Term::Binary(bytes))
                } else {
                    Ok(Term::BitBinary { bytes, bits })
                }
            }
            SMALL_BIG_EXT | LARGE_BIG_EXT => {
                let n = if tag == SMALL_BIG_EXT {
                    self.u8()? as usize
                } else {
                    self.u32()? as usize
                };
                let negative = self.u8()? != 0;
                let mag = self.take(n)?;
                let mut trimmed = mag.len();
                while trimmed > 0 && mag[trimmed - 1] == 0 {
                    trimmed -= 1;
                }
                if trimmed <= 8 {
                    let mut v: u64 = 0;
                    for (i, b) in mag[..trimmed].iter().enumerate() {
                        v |= (*b as u64) << (8 * i);
                    }
                    if !negative && v <= i64::MAX as u64 {
                        return Ok(Term::Int(v as i64));
                    }
                    if negative && v <= (i64::MAX as u64) + 1 {
                        return Ok(Term::Int((v as i64).wrapping_neg()));
                    }
                }
                Ok(Term::BigInt(BigInt {
                    negative,
                    magnitude: mag[..trimmed].to_vec(),
                }))
            }
            MAP_EXT => {
                let n = self.u32()? as usize;
                let mut pairs = Vec::with_capacity(self.bounded(n) / 2);
                for _ in 0..n {
                    let k = self.term()?;
                    let v = self.term()?;
                    pairs.push((k, v));
                }
                Ok(Term::Map(pairs))
            }
            NEW_PID_EXT | PID_EXT => Ok(Term::Pid(self.pid_body(tag)?)),
            NEW_PORT_EXT | PORT_EXT | V4_PORT_EXT => {
                let node = self.atom()?;
                let id = if tag == V4_PORT_EXT {
                    self.u64()?
                } else {
                    self.u32()? as u64
                };
                let creation = if tag == PORT_EXT {
                    self.u8()? as u32
                } else {
                    self.u32()?
                };
                Ok(Term::Port(Port { node, id, creation }))
            }
            NEWER_REFERENCE_EXT | NEW_REFERENCE_EXT => {
                let len = self.u16()? as usize;
                let node = self.atom()?;
                let creation = if tag == NEWER_REFERENCE_EXT {
                    self.u32()?
                } else {
                    self.u8()? as u32
                };
                let mut ids = Vec::with_capacity(len);
                for _ in 0..len {
                    ids.push(self.u32()?);
                }
                Ok(Term::Reference(Reference {
                    node,
                    creation,
                    ids,
                }))
            }
            REFERENCE_EXT => {
                let node = self.atom()?;
                let id = self.u32()?;
                let creation = self.u8()? as u32;
                Ok(Term::Reference(Reference {
                    node,
                    creation,
                    ids: vec![id],
                }))
            }
            EXPORT_EXT => {
                let module = self.atom()?;
                let function = self.atom()?;
                let arity = self.small_int_term()? as u8;
                Ok(Term::Export {
                    module,
                    function,
                    arity,
                })
            }
            NEW_FUN_EXT => {
                let _size = self.u32()?;
                let arity = self.u8()?;
                let uniq: [u8; 16] = self.take(16)?.try_into().unwrap();
                let index = self.u32()?;
                let num_free = self.u32()? as usize;
                let module = self.atom()?;
                let old_index = self.small_int_term()?;
                let old_uniq = self.small_int_term()?;
                let pid_tag = self.u8()?;
                if pid_tag != NEW_PID_EXT && pid_tag != PID_EXT {
                    return Err(EtfError::Malformed("fun pid"));
                }
                let pid = self.pid_body(pid_tag)?;
                let mut free_vars = Vec::with_capacity(num_free.min(1 << 10));
                for _ in 0..num_free {
                    free_vars.push(self.term()?);
                }
                Ok(Term::Fun(Box::new(Fun {
                    arity,
                    uniq,
                    index,
                    module,
                    old_index,
                    old_uniq,
                    pid,
                    free_vars,
                })))
            }
            FUN_EXT => Err(EtfError::UnsupportedTag(FUN_EXT)),
            COMPRESSED => Err(EtfError::Compressed),
            other => Err(EtfError::UnsupportedTag(other)),
        }
    }
}

fn latin1(bytes: &[u8]) -> String {
    bytes.iter().map(|b| *b as char).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn roundtrip(t: Term) {
        let bytes = encode(&t);
        assert_eq!(bytes.len(), 1 + encoded_len(&t), "encoded_len of {t}");
        let back = decode(&bytes).expect("decode");
        assert_eq!(t, back, "roundtrip of {t}");
    }

    #[test]
    fn atom_cache_refs_resolve_through_the_table() {
        // {82 0, 82 1} with table [ok, 'rex@host'] -- the form used after a
        // distribution header (no version byte).
        let table = [Atom::new("ok"), Atom::new("rex@host")];
        let bytes = [104, 2, 82, 0, 82, 1];
        let (t, used) = decode_cached(&bytes, &table).unwrap();
        assert_eq!(used, bytes.len());
        assert_eq!(t, Term::tuple(vec![Term::atom("ok"), Term::atom("rex@host")]));
        assert!(matches!(decode_cached(&[82, 5], &table), Err(EtfError::AtomCacheRef)));
        assert!(matches!(decode(&[131, 82, 0]), Err(EtfError::AtomCacheRef)));
    }

    #[test]
    fn roundtrips() {
        roundtrip(Term::atom("ok"));
        roundtrip(Term::atom("Elixir.Rex.Math"));
        roundtrip(Term::Int(0));
        roundtrip(Term::Int(255));
        roundtrip(Term::Int(-1));
        roundtrip(Term::Int(i32::MAX as i64 + 1));
        roundtrip(Term::Int(i64::MIN));
        roundtrip(Term::Int(i64::MAX));
        roundtrip(Term::Float(3.25));
        roundtrip(Term::tuple(vec![
            Term::atom("a"),
            Term::Int(1),
            Term::nil(),
        ]));
        roundtrip(Term::list(vec![Term::Int(1), Term::Int(2), Term::Int(300)]));
        roundtrip(Term::charlist("hello"));
        roundtrip(Term::string("hello"));
        roundtrip(Term::ImproperList(
            vec![Term::atom("alias")],
            Box::new(Term::Int(7)),
        ));
        roundtrip(Term::map(vec![(
            Term::atom("node"),
            Term::atom("rex@host"),
        )]));
        roundtrip(Term::BitBinary {
            bytes: vec![0xff, 0x80],
            bits: 1,
        });
        roundtrip(Term::Pid(Pid {
            node: Atom::new("a@b"),
            id: 1,
            serial: 2,
            creation: 3,
        }));
        roundtrip(Term::Reference(Reference {
            node: Atom::new("a@b"),
            creation: 9,
            ids: vec![1, 2, 3],
        }));
        roundtrip(Term::Port(Port {
            node: Atom::new("a@b"),
            id: 1,
            creation: 3,
        }));
        roundtrip(Term::Export {
            module: Atom::new("m"),
            function: Atom::new("f"),
            arity: 2,
        });
        roundtrip(Term::BigInt(BigInt {
            negative: false,
            magnitude: vec![1; 12],
        }));
    }

    #[test]
    fn decodes_known_bytes() {
        // term_to_binary({heartbeat, 'rex@host'})
        let bytes = [
            131, 104, 2, 119, 9, b'h', b'e', b'a', b'r', b't', b'b', b'e', b'a', b't', 119, 8,
            b'r', b'e', b'x', b'@', b'h', b'o', b's', b't',
        ];
        let t = decode(&bytes).unwrap();
        assert_eq!(
            t,
            Term::tuple(vec![Term::atom("heartbeat"), Term::atom("rex@host")])
        );
        // term_to_binary("abc") is a STRING_EXT charlist
        let t = decode(&[131, 107, 0, 3, 97, 98, 99]).unwrap();
        assert_eq!(t, Term::charlist("abc"));
    }

    #[test]
    fn bigint_display() {
        let b = BigInt {
            negative: false,
            magnitude: 1_000_000_000_000u64.to_le_bytes().to_vec(),
        };
        assert_eq!(b.to_string(), "1000000000000");
    }
}
