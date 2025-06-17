//! # Rukko
//! 
//! A Rust library for communicating with Pekko actors using the Artery TCP protocol.
//! 
//! This library provides a Rust implementation of the Pekko Artery remoting protocol,
//! allowing Rust applications to send and receive (string) messages from JVM-based Pekko actors.
//! 
//! ## Features
//! 
//! - Artery TCP protocol implementation
//! - String-based message support
//! - Connection management and error handling
//! 
//! ## Logging
//! 
//! This library uses the `tracing` crate for logging. To see log output, 
//! initialize a tracing subscriber in your application:
//! 
//! ```rust
//! // For text output:
//! tracing_subscriber::fmt::init();
//! ```
//! 
//! For JSON output, enable the "json" feature in tracing-subscriber:
//! ```toml
//! tracing-subscriber = { version = "0.3", features = ["json"] }
//! ```
//! 
//! ## Quick Start
//! 
//! ```rust,no_run
//! use rukko::{ActorSystem, ActorSelection, Message};
//! 
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     // Initialize logging (optional, but recommended for debugging)
//!     tracing_subscriber::fmt::init();
//!     
//!     let system = ActorSystem::new("MySystem").await?;
//!     let remote_actor = system.actor_selection("pekko://RemoteSystem@127.0.0.1:25552/user/myActor").await?;
//!     
//!     let message = Message::text("Hello, anybody there?");
//!     let response = remote_actor.ask(message).await?;
//!     
//!     println!("Response: {:?}", response);
//!     Ok(())
//! }
//! ```

pub mod actor;
pub mod protocol;
pub mod pekko_protobuf;
pub mod protobuf_conversions;
pub mod transport;
pub mod error;

#[cfg(test)]
mod tests;

#[cfg(test)]
mod pekko_compatibility_tests;

pub use actor::{ActorSystem, ActorSelection};
pub use protocol::Message;
pub use error::{RukkoError, Result};

