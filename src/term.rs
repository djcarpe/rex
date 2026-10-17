//! The Erlang term model.
//!
//! [`Term`] mirrors the shapes that travel over the External Term Format and
//! the distribution protocol. Construction helpers (`Term::atom`, `Term::tuple`,
//! ...) and accessors (`as_atom`, `as_tuple`, ...) keep call handlers short.

use std::fmt;
use std::sync::Arc;

/// An Erlang atom. Elixir module names are atoms with an `Elixir.` prefix,
/// e.g. `Elixir.MyApp.Math`.
///
/// The name is reference counted, so cloning an atom (which happens for
/// every pid's node name, every atom-cache reference a peer sends, and every
/// `Term::clone`) is a refcount bump rather than an allocation.
#[derive(Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Atom(Arc<str>);

impl Atom {
    pub fn new(s: impl AsRef<str>) -> Self {
        Atom(Arc::from(s.as_ref()))
    }

    /// Build the atom for an Elixir module name: `Atom::elixir("Foo.Bar")`
    /// is `'Elixir.Foo.Bar'`.
    pub fn elixir(module: &str) -> Self {
        if module.starts_with("Elixir.") {
            Atom::new(module)
        } else {
            Atom::new(format!("Elixir.{module}"))
        }
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn into_string(self) -> String {
        self.0.to_string()
    }
}

impl fmt::Debug for Atom {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self)
    }
}

impl fmt::Display for Atom {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s: &str = &self.0;
        let plain = !s.is_empty()
            && s.chars().next().is_some_and(|c| c.is_ascii_lowercase())
            && s.chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '@');
        if plain {
            f.write_str(s)
        } else {
            write!(f, "'{}'", s.replace('\\', "\\\\").replace('\'', "\\'"))
        }
    }
}

impl From<&str> for Atom {
    fn from(s: &str) -> Self {
        Atom::new(s)
    }
}

impl From<String> for Atom {
    fn from(s: String) -> Self {
        Atom(Arc::from(s))
    }
}

impl PartialEq<str> for Atom {
    fn eq(&self, other: &str) -> bool {
        &*self.0 == other
    }
}

impl PartialEq<&str> for Atom {
    fn eq(&self, other: &&str) -> bool {
        &*self.0 == *other
    }
}

/// A process identifier.
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct Pid {
    pub node: Atom,
    pub id: u32,
    pub serial: u32,
    pub creation: u32,
}

impl fmt::Display for Pid {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "<{}.{}.{}>", self.node, self.id, self.serial)
    }
}

/// A port identifier (opaque to us, but we can carry it around).
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct Port {
    pub node: Atom,
    pub id: u64,
    pub creation: u32,
}

impl fmt::Display for Port {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "#Port<{}.{}>", self.node, self.id)
    }
}

/// A reference (`make_ref()`).
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct Reference {
    pub node: Atom,
    pub creation: u32,
    pub ids: Vec<u32>,
}

impl fmt::Display for Reference {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "#Ref<{}", self.node)?;
        for id in self.ids.iter().rev() {
            write!(f, ".{id}")?;
        }
        write!(f, ">")
    }
}

/// An integer that does not fit in `i64`. Magnitude is little-endian bytes.
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct BigInt {
    pub negative: bool,
    pub magnitude: Vec<u8>,
}

impl fmt::Display for BigInt {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Decimal conversion by repeated division; big ints are rare so
        // simplicity beats speed here.
        let mut digits = self.magnitude.clone();
        while digits.last() == Some(&0) {
            digits.pop();
        }
        if digits.is_empty() {
            return f.write_str("0");
        }
        let mut out = Vec::new();
        while !digits.is_empty() {
            let mut rem: u32 = 0;
            for b in digits.iter_mut().rev() {
                let cur = (rem << 8) | *b as u32;
                *b = (cur / 10) as u8;
                rem = cur % 10;
            }
            out.push(b'0' + rem as u8);
            while digits.last() == Some(&0) {
                digits.pop();
            }
        }
        if self.negative {
            f.write_str("-")?;
        }
        out.reverse();
        f.write_str(std::str::from_utf8(&out).unwrap())
    }
}

/// An Erlang fun as it appears on the wire (`NEW_FUN_EXT`). We cannot run it,
/// but we can hold it and hand it back unchanged.
#[derive(Clone, PartialEq, Debug)]
pub struct Fun {
    pub arity: u8,
    pub uniq: [u8; 16],
    pub index: u32,
    pub module: Atom,
    pub old_index: i64,
    pub old_uniq: i64,
    pub pid: Pid,
    pub free_vars: Vec<Term>,
}

/// An Erlang term.
#[derive(Clone, PartialEq, Debug)]
pub enum Term {
    Atom(Atom),
    Int(i64),
    BigInt(BigInt),
    Float(f64),
    Tuple(Vec<Term>),
    /// A proper list. The empty list `[]` is `List(vec![])`.
    List(Vec<Term>),
    /// A list whose tail is not `[]`, e.g. `[alias | Ref]`.
    ImproperList(Vec<Term>, Box<Term>),
    Binary(Vec<u8>),
    /// A bitstring whose last byte holds only `bits` significant bits.
    BitBinary {
        bytes: Vec<u8>,
        bits: u8,
    },
    /// Key/value pairs in wire order. Duplicate keys never occur on the wire.
    Map(Vec<(Term, Term)>),
    Pid(Pid),
    Port(Port),
    Reference(Reference),
    Export {
        module: Atom,
        function: Atom,
        arity: u8,
    },
    Fun(Box<Fun>),
}

impl Term {
    // ---- constructors -----------------------------------------------------

    pub fn atom(name: impl AsRef<str>) -> Term {
        Term::Atom(Atom::new(name))
    }

    pub fn nil() -> Term {
        Term::List(Vec::new())
    }

    pub fn int(i: impl Into<i64>) -> Term {
        Term::Int(i.into())
    }

    pub fn tuple(items: Vec<Term>) -> Term {
        Term::Tuple(items)
    }

    pub fn list(items: Vec<Term>) -> Term {
        Term::List(items)
    }

    pub fn map(pairs: Vec<(Term, Term)>) -> Term {
        Term::Map(pairs)
    }

    pub fn binary(bytes: impl Into<Vec<u8>>) -> Term {
        Term::Binary(bytes.into())
    }

    /// An Elixir string, i.e. a UTF-8 binary.
    pub fn string(s: impl AsRef<str>) -> Term {
        Term::Binary(s.as_ref().as_bytes().to_vec())
    }

    /// An Erlang string, i.e. a list of code points.
    pub fn charlist(s: impl AsRef<str>) -> Term {
        Term::List(s.as_ref().chars().map(|c| Term::Int(c as i64)).collect())
    }

    /// `{ok, Value}`.
    pub fn ok(value: Term) -> Term {
        Term::Tuple(vec![Term::atom("ok"), value])
    }

    /// `{error, Reason}`.
    pub fn error(reason: Term) -> Term {
        Term::Tuple(vec![Term::atom("error"), reason])
    }

    // ---- accessors --------------------------------------------------------

    pub fn as_atom(&self) -> Option<&Atom> {
        match self {
            Term::Atom(a) => Some(a),
            _ => None,
        }
    }

    /// True when the term is exactly the atom `name`.
    pub fn is_atom(&self, name: &str) -> bool {
        matches!(self, Term::Atom(a) if a.as_str() == name)
    }

    pub fn as_int(&self) -> Option<i64> {
        match self {
            Term::Int(i) => Some(*i),
            _ => None,
        }
    }

    /// Integer or float as `f64`.
    pub fn as_number(&self) -> Option<f64> {
        match self {
            Term::Int(i) => Some(*i as f64),
            Term::Float(f) => Some(*f),
            _ => None,
        }
    }

    pub fn as_float(&self) -> Option<f64> {
        match self {
            Term::Float(f) => Some(*f),
            _ => None,
        }
    }

    pub fn as_bool(&self) -> Option<bool> {
        match self {
            Term::Atom(a) if a.as_str() == "true" => Some(true),
            Term::Atom(a) if a.as_str() == "false" => Some(false),
            _ => None,
        }
    }

    pub fn as_tuple(&self) -> Option<&[Term]> {
        match self {
            Term::Tuple(t) => Some(t),
            _ => None,
        }
    }

    /// Elements of a proper list.
    pub fn as_list(&self) -> Option<&[Term]> {
        match self {
            Term::List(l) => Some(l),
            _ => None,
        }
    }

    pub fn as_binary(&self) -> Option<&[u8]> {
        match self {
            Term::Binary(b) => Some(b),
            _ => None,
        }
    }

    /// A binary interpreted as UTF-8 (an Elixir string).
    pub fn as_str(&self) -> Option<&str> {
        match self {
            Term::Binary(b) => std::str::from_utf8(b).ok(),
            _ => None,
        }
    }

    /// Either an Elixir string (binary) or an Erlang charlist, as a `String`.
    pub fn to_text(&self) -> Option<String> {
        match self {
            Term::Binary(b) => std::str::from_utf8(b).ok().map(|s| s.to_string()),
            Term::List(items) => items
                .iter()
                .map(|t| {
                    t.as_int()
                        .and_then(|i| u32::try_from(i).ok())
                        .and_then(char::from_u32)
                })
                .collect::<Option<String>>(),
            _ => None,
        }
    }

    pub fn as_map(&self) -> Option<&[(Term, Term)]> {
        match self {
            Term::Map(m) => Some(m),
            _ => None,
        }
    }

    /// Look a key up in a map term.
    pub fn map_get(&self, key: &Term) -> Option<&Term> {
        self.as_map()?
            .iter()
            .find(|(k, _)| k == key)
            .map(|(_, v)| v)
    }

    /// Look an atom key up in a map term (`%{node: ...}`).
    pub fn map_get_atom(&self, key: &str) -> Option<&Term> {
        self.as_map()?
            .iter()
            .find(|(k, _)| k.is_atom(key))
            .map(|(_, v)| v)
    }

    pub fn as_pid(&self) -> Option<&Pid> {
        match self {
            Term::Pid(p) => Some(p),
            _ => None,
        }
    }

    pub fn as_reference(&self) -> Option<&Reference> {
        match self {
            Term::Reference(r) => Some(r),
            _ => None,
        }
    }

    /// Length of a proper list or tuple.
    #[allow(clippy::len_without_is_empty)]
    pub fn len(&self) -> Option<usize> {
        match self {
            Term::List(l) => Some(l.len()),
            Term::Tuple(t) => Some(t.len()),
            _ => None,
        }
    }

    /// `true` for the empty list `[]`.
    pub fn is_nil(&self) -> bool {
        matches!(self, Term::List(l) if l.is_empty())
    }
}

impl From<i64> for Term {
    fn from(i: i64) -> Self {
        Term::Int(i)
    }
}
impl From<i32> for Term {
    fn from(i: i32) -> Self {
        Term::Int(i as i64)
    }
}
impl From<u32> for Term {
    fn from(i: u32) -> Self {
        Term::Int(i as i64)
    }
}
impl From<usize> for Term {
    fn from(i: usize) -> Self {
        Term::Int(i as i64)
    }
}
impl From<f64> for Term {
    fn from(f: f64) -> Self {
        Term::Float(f)
    }
}
impl From<bool> for Term {
    fn from(b: bool) -> Self {
        Term::atom(if b { "true" } else { "false" })
    }
}
/// `&str` becomes an Elixir string (a binary). Use `Term::atom` for atoms.
impl From<&str> for Term {
    fn from(s: &str) -> Self {
        Term::string(s)
    }
}
impl From<String> for Term {
    fn from(s: String) -> Self {
        Term::Binary(s.into_bytes())
    }
}
impl From<Vec<u8>> for Term {
    fn from(b: Vec<u8>) -> Self {
        Term::Binary(b)
    }
}
impl From<Atom> for Term {
    fn from(a: Atom) -> Self {
        Term::Atom(a)
    }
}
impl From<Pid> for Term {
    fn from(p: Pid) -> Self {
        Term::Pid(p)
    }
}
impl From<Reference> for Term {
    fn from(r: Reference) -> Self {
        Term::Reference(r)
    }
}
impl From<Vec<Term>> for Term {
    fn from(l: Vec<Term>) -> Self {
        Term::List(l)
    }
}
impl<T: Into<Term>> From<Option<T>> for Term {
    /// `None` becomes the atom `nil`, matching Elixir.
    fn from(o: Option<T>) -> Self {
        match o {
            Some(v) => v.into(),
            None => Term::atom("nil"),
        }
    }
}

fn fmt_binary(f: &mut fmt::Formatter<'_>, bytes: &[u8]) -> fmt::Result {
    if !bytes.is_empty() && bytes.iter().all(|b| (0x20..0x7f).contains(b)) {
        write!(
            f,
            "<<\"{}\">>",
            String::from_utf8_lossy(bytes).replace('"', "\\\"")
        )
    } else {
        f.write_str("<<")?;
        for (i, b) in bytes.iter().enumerate() {
            if i > 0 {
                f.write_str(",")?;
            }
            write!(f, "{b}")?;
        }
        f.write_str(">>")
    }
}

impl fmt::Display for Term {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Term::Atom(a) => write!(f, "{a}"),
            Term::Int(i) => write!(f, "{i}"),
            Term::BigInt(b) => write!(f, "{b}"),
            Term::Float(x) => {
                if x.fract() == 0.0 && x.abs() < 1e15 {
                    write!(f, "{x:.1}")
                } else {
                    write!(f, "{x}")
                }
            }
            Term::Tuple(items) => {
                f.write_str("{")?;
                for (i, t) in items.iter().enumerate() {
                    if i > 0 {
                        f.write_str(",")?;
                    }
                    write!(f, "{t}")?;
                }
                f.write_str("}")
            }
            Term::List(items) => {
                f.write_str("[")?;
                for (i, t) in items.iter().enumerate() {
                    if i > 0 {
                        f.write_str(",")?;
                    }
                    write!(f, "{t}")?;
                }
                f.write_str("]")
            }
            Term::ImproperList(items, tail) => {
                f.write_str("[")?;
                for (i, t) in items.iter().enumerate() {
                    if i > 0 {
                        f.write_str(",")?;
                    }
                    write!(f, "{t}")?;
                }
                write!(f, "|{tail}]")
            }
            Term::Binary(b) => fmt_binary(f, b),
            Term::BitBinary { bytes, bits } => {
                f.write_str("<<")?;
                for (i, b) in bytes.iter().enumerate() {
                    if i > 0 {
                        f.write_str(",")?;
                    }
                    if i + 1 == bytes.len() {
                        write!(f, "{}:{}", b >> (8 - bits), bits)?;
                    } else {
                        write!(f, "{b}")?;
                    }
                }
                f.write_str(">>")
            }
            Term::Map(pairs) => {
                f.write_str("#{")?;
                for (i, (k, v)) in pairs.iter().enumerate() {
                    if i > 0 {
                        f.write_str(",")?;
                    }
                    write!(f, "{k} => {v}")?;
                }
                f.write_str("}")
            }
            Term::Pid(p) => write!(f, "{p}"),
            Term::Port(p) => write!(f, "{p}"),
            Term::Reference(r) => write!(f, "{r}"),
            Term::Export {
                module,
                function,
                arity,
            } => write!(f, "fun {module}:{function}/{arity}"),
            Term::Fun(fun) => write!(f, "#Fun<{}.{}.{}>", fun.module, fun.index, fun.old_uniq),
        }
    }
}
