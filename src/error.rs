use crate::etf::EtfError;
use crate::native::Exception;
use crate::term::{Atom, Term};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("term encoding error: {0}")]
    Etf(#[from] EtfError),
    #[error("EPMD error: {0}")]
    Epmd(String),
    #[error("node {0} is not registered with EPMD on its host")]
    NodeNotFound(Atom),
    #[error("handshake with {node} failed: {reason}")]
    Handshake { node: String, reason: String },
    #[error("not connected to node {0}")]
    NotConnected(Atom),
    #[error("invalid node name {0:?} (expected name@host)")]
    InvalidNodeName(String),
    #[error("no cookie given and ~/.erlang.cookie could not be read")]
    NoCookie,
    #[error("name {0} is already registered")]
    NameTaken(Atom),
    #[error("timeout")]
    Timeout,
    #[error("the called process does not exist")]
    NoProc,
    #[error("the called process exited: {0}")]
    Down(Term),
    #[error("remote call failed: {0}")]
    BadRpc(Term),
    #[error("remote call raised {0}")]
    Exception(Exception),
    #[error("the node has been shut down")]
    Shutdown,
}

pub type Result<T> = std::result::Result<T, Error>;
