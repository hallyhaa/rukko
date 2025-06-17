use std::sync::Arc;
use tokio::time::Duration;
use tokio::net::TcpListener;
use tokio::sync::Mutex;
use tracing::{debug, info, warn};
use rand::random;

use crate::error::Result;
use crate::protocol::{ActorPath, Message};
use crate::transport::ArteryTransport;

/// Actor system that manages connections and message routing
#[derive(Debug, Clone)]
pub struct ActorSystem {
    name: String,
    uid: u64,
    local_path: ActorPath,
    bound_port: u16,
    transport: Arc<ArteryTransport>,
    server_handle: Arc<Mutex<Option<tokio::task::JoinHandle<()>>>>,
}

impl ActorSystem {
    /// Get the deadLetters actor path for this system
    pub fn dead_letters(&self) -> ActorPath {
        ActorPath::new(
            self.name.clone(),
            self.local_path.host.clone(),
            self.local_path.port,
            "deadLetters".to_string(),
        )
    }
    
    /// Actually just returns the deadLetters ActorRef
    pub fn no_sender(&self) -> ActorPath {
        self.dead_letters()
    }
    
    /// Create a new actor system (shorthand for `new_with_address(name, "127.0.0.1", 0)`)
    pub async fn new(name: impl Into<String>) -> Result<Self> {
        Self::new_with_address(name, "127.0.0.1", 0).await
    }
    
    /// Create a new actor system with the given host and port
    /// 
    /// # Arguments
    /// * `name` - Name of the actor system
    /// * `host` - Host address to bind to (must be available on this machine)
    /// * `port` - Port to bind to (0 for OS-allocated port)
    /// 
    /// # Errors
    /// Returns `RukkoError::Connection` if the specified address cannot be bound to.
    /// No fallback is attempted.
    pub async fn new_with_address(name: impl Into<String>, host: &str, port: u16) -> Result<Self> {
        let name = name.into();
        
        // System UID
        let uid = random::<u64>();
        
        // Bind to the specified address - fail fast if not available
        let bind_address = format!("{}:{}", host, port);
        let listener = TcpListener::bind(&bind_address).await
            .map_err(|e| crate::error::RukkoError::Connection(
                std::io::Error::new(
                    e.kind(),
                    format!("Failed to bind ActorSystem to [{}]: {}", bind_address, e)
                )
            ))?;
            
        let bound_port = listener.local_addr()?.port();
        debug!("ActorSystem '{}' bound to {}:{}", name, host, bound_port);
        
        // Get the actual local address from the bound listener
        let local_host = listener.local_addr()?.ip().to_string();
        
        let local_path = ActorPath::new(
            name.clone(),
            local_host.clone(),
            bound_port,
            "system".to_string(),
        );
        
        let transport = Arc::new(ArteryTransport::new(uid, bound_port, local_host.clone(), name.clone()));
        
        let server_handle = tokio::spawn({
            let transport = transport.clone();
            async move {
                ArteryTransport::run_server(listener, transport).await;
            }
        });
        
        info!("Created actor system: {} with UID: {} on {}:{}", name, uid, local_host, bound_port);
        
        Ok(Self {
            name,
            uid,
            local_path,
            bound_port,
            transport,
            server_handle: Arc::new(Mutex::new(Some(server_handle))),
        })
    }
    
    /// Create an actor selection for a remote actor
    pub async fn actor_selection(&self, path: impl Into<String>) -> Result<ActorSelection> {
        let path_str = path.into();
        let actor_path = ActorPath::from_string(path_str)?;
        
        // ActorSelection is just a logical view
        // connections are established when messages are sent
        // Timeout for the ActorSelection's ask operations defaults to 30 seconds
        Ok(ActorSelection {
            path: actor_path,
            system: self.clone(),
            default_timeout: Duration::from_secs(30),
        })
    }
    
    /// Shutdown the actor system
    pub async fn shutdown(&self) {
        info!("Shutting down actor system {}", self.name);
        
        // Stop the server first
        if let Some(handle) = self.server_handle.lock().await.take() {
            handle.abort();
            debug!("Server task stopped for actor system {}", self.name);
        }
        
        // Shutdown the transport layer
        (*self.transport).shutdown().await;
        
        info!("Actor system {} shutdown complete", self.name);
    }
    
    /// Get the name of this actor system
    pub fn name(&self) -> &str {
        &self.name
    }
    
    /// Get the local actor path
    pub fn path(&self) -> &ActorPath {
        &self.local_path
    }
    
    /// Get the system UID
    pub fn uid(&self) -> u64 {
        self.uid
    }
    
    
    /// Get the bound port
    pub fn bound_port(&self) -> u16 {
        self.bound_port
    }
}


/// Actor selection for addressing actors by path
#[derive(Debug, Clone)]
pub struct ActorSelection {
    path: ActorPath,
    system: ActorSystem,
    default_timeout: Duration,
}

impl ActorSelection {

    /// Create a new ActorSelection with a different default timeout
    pub fn with_default_timeout(&self, timeout: Duration) -> Self {
        Self {
            path: self.path.clone(),
            system: self.system.clone(),
            default_timeout: timeout,
        }
    }

    /// Send a message and wait for a response (using default timeout)
    pub async fn ask(&self, message: Message) -> Result<Message> {
        self.ask_with_timeout(message, self.default_timeout).await
    }
    
    /// Send a message and wait for a response
    pub async fn ask_with_timeout(&self, message: Message, timeout: Duration) -> Result<Message> {
        debug!("Asking actor selection {} with timeout {:?}", self.path.to_string(), timeout);
        
        // Generate a unique temporary actor path
        let temp_path = self.system.transport.temp_path_for_selection(&self.path.to_string());
        let sender_path = ActorPath::new(
            self.system.name.clone(),
            self.system.local_path.host.clone(),
            self.system.bound_port,
            temp_path,
        );
        
        self.system.transport.ask(
            sender_path,
            self.path.clone(),
            message,
            timeout,
        ).await
    }
    
    /// Send a message without waiting for a response (fire-and-forget).
    /// Sender will be specified as no_sender(), i.e. deadLetters.
    pub fn tell(&self, message: Message) {
        self.tell_with_explicit_sender(message, self.system.no_sender())
    }
    
    /// Send a message without waiting for a response giving `sender` as sender.
    pub fn tell_with_explicit_sender(&self, message: Message, sender: ActorPath) {
        debug!("Telling actor selection {} from {}", self.path.to_string(), sender.to_string());
        
        let transport = self.system.transport.clone();
        let target_path = self.path.clone();
        let sender_for_log = sender.clone();
        let target_path_for_log = target_path.clone();

        tokio::spawn(async move {
            if let Err(e) = transport.tell(
                sender,
                target_path,
                message,
            ).await {
                warn!("Tell operation {} --> {} failed: {}", sender_for_log.to_string(), target_path_for_log.to_string(), e);
            }
        });
    }
    
    /// Returns the actor path
    pub fn path(&self) -> &ActorPath {
        &self.path
    }
    
    /// Returns the current default timeout
    pub fn default_timeout(&self) -> Duration {
        self.default_timeout
    }

}


#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::RukkoError;
    
    #[tokio::test]
    async fn test_actor_system_creation() {
        let system = ActorSystem::new("TestSystem").await.unwrap();
        assert_eq!(system.name(), "TestSystem");
    }
    
    #[tokio::test]
    async fn test_actor_selection_creation() {
        let system = ActorSystem::new("TestSystem").await.unwrap();
        let path = "pekko://RemoteSystem@127.0.0.1:25552/user/testActor";
        
        // This should parse the path correctly
        match system.actor_selection(path).await {
            Ok(selection) => {
                assert_eq!(selection.path().system, "RemoteSystem");
                assert_eq!(selection.path().host, "127.0.0.1");
                assert_eq!(selection.path().port, 25552);
                assert_eq!(selection.path().path, "user/testActor");
            }
            Err(e) => panic!("Unexpected error: {}", e),
        }
    }
    
    
    #[tokio::test]
    async fn test_dead_letters_path() {
        let system = ActorSystem::new("DeadLettersTest").await.unwrap();
        let dead_letters = system.dead_letters();
        
        assert_eq!(dead_letters.path, "deadLetters");
        assert_eq!(dead_letters.system, "DeadLettersTest");
        assert!(dead_letters.to_string().starts_with("pekko://"));
        
        // Should look like: pekko://DeadLettersTest@127.0.0.1:PORT/deadLetters
        let expected_prefix = format!("pekko://{}@127.0.0.1:{}/deadLetters", 
                                    dead_letters.system, dead_letters.port);
        assert_eq!(dead_letters.to_string(), expected_prefix);
        
        system.shutdown().await;
    }
    
    #[tokio::test]
    async fn test_no_sender_equivalent_to_dead_letters() {
        let system = ActorSystem::new("NoSenderTest").await.unwrap();
        
        // no_sender() should return the same as dead_letters()
        let no_sender = system.no_sender();
        let dead_letters = system.dead_letters();
        
        assert_eq!(no_sender.to_string(), dead_letters.to_string());
        assert_eq!(no_sender.path, "deadLetters");
        
        system.shutdown().await;
    }

    #[tokio::test]
    async fn test_new_with_address() {
        let system = ActorSystem::new_with_address("TestWithAddress", "127.0.0.1", 0).await.unwrap();
        assert_eq!(system.name(), "TestWithAddress");
        assert!(system.bound_port() > 0);
        system.shutdown().await;
    }

    #[tokio::test]
    async fn test_new_with_address_localhost() {
        let system = ActorSystem::new_with_address("TestWithAddress", "localhost", 0).await.unwrap();
        assert_eq!(system.name(), "TestWithAddress");
        assert!(system.bound_port() > 0);
        system.shutdown().await;
    }

    #[tokio::test]
    async fn test_new_with_address_specific_port() {
        let port = 40000 + (random::<u16>() % 10000);
        let system = ActorSystem::new_with_address("TestSpecificPort", "127.0.0.1", port).await.unwrap();

        assert_eq!(system.bound_port(), port);
        system.shutdown().await;
    }
    
    #[tokio::test]
    async fn test_new_with_address_invalid_host() {
        let result = ActorSystem::new_with_address("TestInvalidHost", "google.com", 12345).await;
        assert!(result.is_err());
        
        if let Err(RukkoError::Connection(io_err)) = result {
            assert!(io_err.to_string().contains("Failed to bind ActorSystem to [google.com:12345]"));
        } else {
            panic!("Expected Connection error with bind failure message");
        }
    }
    
    #[tokio::test]
    async fn test_new_with_address_port_in_use() {
        // Arrange
        let port = 40000 + (rand::random::<u16>() % 10000);
        let _system1 = ActorSystem::new_with_address("System1", "127.0.0.1", port).await.unwrap();
        
        // Act
        let result = ActorSystem::new_with_address("System2", "127.0.0.1", port).await;

        // Assert
        assert!(result.is_err());
        
        if let Err(RukkoError::Connection(io_err)) = result {
            let error_msg = io_err.to_string();
            assert!(error_msg.contains("Failed to bind ActorSystem to"));
            assert!(error_msg.contains(&format!("127.0.0.1:{}", port)));
            assert!(error_msg.contains("Address already in use"));
        } else {
            panic!("Expected Connection error with port in use");
        }
        
        _system1.shutdown().await;
    }
    
    #[tokio::test]
    async fn test_new_delegates_to_new_with_address() {
        let system1 = ActorSystem::new("DelegateTest").await.unwrap();
        let system2 = ActorSystem::new_with_address("DelegateTest2", "127.0.0.1", 0).await.unwrap();
        
        // Both should be bound to 127.0.0.1
        assert_eq!(system1.path().host, "127.0.0.1");
        assert_eq!(system2.path().host, "127.0.0.1");
        
        // Both should have valid ports
        assert!(system1.bound_port() > 0);
        assert!(system2.bound_port() > 0);
        
        // Ports should be different (OS-allocated)
        assert_ne!(system1.bound_port(), system2.bound_port());
        
        system1.shutdown().await;
        system2.shutdown().await;
    }
    
    #[tokio::test]
    async fn test_actor_selection_default_timeout() {
        // Arrange
        let system = ActorSystem::new("TimeoutTest").await.unwrap();
        let path = "pekko://RemoteSystem@127.0.0.1:25552/user/testActor";
        let selection = system.actor_selection(path).await.unwrap();
        
        // Assert
        assert_eq!(selection.default_timeout(), Duration::from_secs(30));
        
        // Arrange some more
        let custom_selection = selection.with_default_timeout(Duration::from_secs(10));
        // Assert some more
        assert_eq!(custom_selection.default_timeout(), Duration::from_secs(10));
        
        // ... and the original should still have 30 seconds:
        assert_eq!(selection.default_timeout(), Duration::from_secs(30));
        
        system.shutdown().await;
    }
    
    #[tokio::test]
    async fn test_actor_selection_tell_methods() {
        // Arrange
        //<editor-fold desc="Log capturing preparations">
        use regex::Regex;
        use std::sync::{Arc, Mutex};
        use std::sync::mpsc;
        
        // Create a channel to capture log messages
        let (log_sender, log_receiver) = mpsc::channel::<String>();
        let log_sender_arc = Arc::new(Mutex::new(log_sender));
        
        // Custom writer that captures logs
        struct LogCapture {
            sender: Arc<Mutex<mpsc::Sender<String>>>,
        }
        
        impl std::io::Write for LogCapture {
            fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
                let text = String::from_utf8_lossy(buf).to_string();
                if let Ok(sender) = self.sender.lock() {
                    let _ = sender.send(text);
                }
                Ok(buf.len())
            }
            
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }
        
        let log_sender_clone = log_sender_arc.clone();
        
        // Use set_default instead of try_init to avoid conflicts with other tests
        let subscriber = tracing_subscriber::fmt()
            .with_writer(move || LogCapture { sender: log_sender_clone.clone() })
            .with_max_level(tracing::Level::WARN)
            .with_ansi(false)
            .finish();
        
        let _guard = tracing::subscriber::set_default(subscriber);
        //</editor-fold>

        let port = 40000 + (random::<u16>() % 10000);
        let system = ActorSystem::new("TellTest").await.unwrap();
        let path = format!("pekko://RemoteSystem@127.0.0.1:{}/user/testActor", port);
        let selection = system.actor_selection(path).await.unwrap();
        let message = Message::text("test message");

        // Act (one default, the other with specific sender)
        selection.tell(message.clone());
        let custom_sender = ActorPath::new(
            "TellTest".to_string(),
            "127.0.0.1".to_string(),
            12345,
            "user/givenSender".to_string(),
        );
        selection.tell_with_explicit_sender(message, custom_sender);
        
        // Give time for async operations to complete and generate error logs
        tokio::time::sleep(Duration::from_millis(200)).await;
        
        // Collect all logged messages as a list of lines
        let mut all_logs = Vec::<String>::new();
        while let Ok(log_msg) = log_receiver.try_recv() {
            all_logs.push(log_msg);
        }
        
        // Assert
        assert_eq!(2, all_logs.len());

        // https://regex101.com/r/kLCl1S/1
        let expected_log_lines = Regex::new(r"^\S+\s+WARN rukko::actor: Tell operation pekko://TellTest@127\.0\.0\.1:\d+/[\w/]+ --> pekko://RemoteSystem@127\.0\.0\.1:\d+/user/testActor failed: Connection error: Connection refused.+").unwrap();
        assert!(all_logs.iter().all(|line| expected_log_lines.is_match(line)),
                "Expected to see 'Tell operation ... failed' warnings in logs. Log lines: {:?}", all_logs);
        
        system.shutdown().await;
    }
}
