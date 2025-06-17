use thiserror::Error;

/// Result type for Rukko operations
pub type Result<T> = std::result::Result<T, RukkoError>;

/// Errors that can occur in Rukko operations
#[derive(Error, Debug)]
pub enum RukkoError {
    #[error("Connection error: {0}")]
    Connection(#[from] std::io::Error),
    
    #[error("Protocol error: {0}")]
    Protocol(String),
    
    #[error("Actor not found: {0}")]
    ActorNotFound(String),
    
    #[error("Timeout waiting for response")]
    Timeout,
    
    #[error("Invalid actor path: {0}")]
    InvalidActorPath(String),
    
    #[error("Handshake failed: {0}")]
    HandshakeFailed(String),
    
    #[error("Invalid message format: {0}")]
    InvalidMessage(String),
}