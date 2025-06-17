use bytes::{Buf, Bytes, BytesMut};
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, atomic::{AtomicU64, AtomicBool, Ordering}};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{oneshot, Mutex};
use tokio::time::{timeout, Duration};
use tokio::task::JoinSet;
use tracing::{debug, error, info, trace, warn};
use prost::Message;

use crate::error::{RukkoError, Result};
use crate::protocol::{ActorPath, FrameHeader, Message as ProtocolMessage, MessageEnvelope, StreamId, UniqueAddress, InternalMessage};

/// TCP transport for Artery protocol
#[derive(Debug)]
pub struct HandshakeState {
    completed: bool,
    queued_messages: Vec<MessageEnvelope>,
}

/// Shutdown timeout configuration
#[derive(Debug, Clone)]
pub struct ShutdownTimeouts {
    pub flush_timeout: Duration,
    pub connection_timeout: Duration,
    pub total_timeout: Duration,
}

/// Default timeouts, following Pekko defaults
impl Default for ShutdownTimeouts {
    fn default() -> Self {
        Self {
            flush_timeout: Duration::from_secs(1),      // Pekko: shutdown-flush-timeout
            connection_timeout: Duration::from_secs(5), // Pekko: tcp.connection-timeout
            total_timeout: Duration::from_secs(10),     // Total hard limit
        }
    }
}

#[derive(Debug)]
pub struct ArteryTransport {
    system_uid: u64,
    local_port: u16,
    local_host: String,
    system_name: String,
    temp_counter: AtomicU64,
    connections: Arc<Mutex<HashMap<String, Connection>>>,
    pending_responses: Arc<Mutex<HashMap<String, oneshot::Sender<ProtocolMessage>>>>,
    handshake_states: Arc<Mutex<HashMap<String, HandshakeState>>>,
    shutdown_flag: AtomicBool,
    shutdown_timeouts: ShutdownTimeouts,
    expected_termination_acks: Arc<Mutex<HashSet<String>>>,
}

impl Clone for ArteryTransport {
    fn clone(&self) -> Self {
        Self {
            system_uid: self.system_uid,
            local_port: self.local_port,
            local_host: self.local_host.clone(),
            system_name: self.system_name.clone(),
            temp_counter: AtomicU64::new(self.temp_counter.load(Ordering::SeqCst)),
            connections: self.connections.clone(),
            pending_responses: self.pending_responses.clone(),
            handshake_states: self.handshake_states.clone(),
            shutdown_flag: AtomicBool::new(self.shutdown_flag.load(Ordering::SeqCst)),
            shutdown_timeouts: self.shutdown_timeouts.clone(),
            expected_termination_acks: self.expected_termination_acks.clone(),
        }
    }
}

impl ArteryTransport {
    /// Pekko-compatible base64 encoding for temporary actor paths
    fn pekko_base64(mut n: u64) -> String {
        const BASE64_CHARS: &[u8] = b"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789+~";
        let mut result = String::from("$");
        
        loop {
            result.push(BASE64_CHARS[(n & 63) as usize] as char);
            n >>= 6;
            if n == 0 {
                break;
            }
        }
        result
    }
    
    /// Generate a temporary actor path like Pekko does for ActorSelection ask pattern
    /// For ActorSelection, Pekko uses: temp/{encoded_target_path}$a
    /// Where encoded_target_path is the target actor path with "/" replaced by "_"
    pub fn temp_path_for_selection(&self, target_path: &str) -> String {
        let counter = self.temp_counter.fetch_add(1, Ordering::SeqCst);
        
        // Extract the path part from the target and encode like Pekko does
        // For "pekko://SomeSystem@127.0.0.1:25552/user/highway" we want "/user/highway"
        let path_part = if let Some(at_pos) = target_path.find("@") {
            // Look for the first "/" after the "@" symbol
            if let Some(path_start) = target_path[at_pos..].find("/") {
                &target_path[at_pos + path_start..]
            } else {
                // No path part found, use the whole string
                target_path
            }
        } else if target_path.starts_with('/') {
            // Already a path like "/user/highway"
            target_path
        } else {
            // Fallback: treat as relative path
            target_path
        };
        
        // Replace "/" with "_" and URL-encode (like Pekko does)
        // "/user/highway" becomes "_user_highway"
        let encoded_path = path_part.replace("/", "_");
        
        format!("temp/{}{}", encoded_path, Self::pekko_base64(counter))
    }
    
    /// Generate a simple temporary actor path (fallback for other uses)
    pub fn temp_path(&self) -> String {
        let counter = self.temp_counter.fetch_add(1, Ordering::SeqCst);
        format!("temp/{}", Self::pekko_base64(counter))
    }

    
    pub fn new(uid: u64, port: u16, host: String, system_name: String) -> Self {
        Self {
            system_uid: uid,
            local_port: port,
            local_host: host,
            system_name,
            temp_counter: AtomicU64::new(0),
            connections: Arc::new(Mutex::new(HashMap::new())),
            pending_responses: Arc::new(Mutex::new(HashMap::new())),
            handshake_states: Arc::new(Mutex::new(HashMap::new())),
            shutdown_flag: AtomicBool::new(false),
            shutdown_timeouts: ShutdownTimeouts::default(),
            expected_termination_acks: Arc::new(Mutex::new(HashSet::new())),
        }
    }
    
    /// Get a clone of the pending responses for use by the TCP server
    pub fn get_pending_responses(&self) -> Arc<Mutex<HashMap<String, oneshot::Sender<ProtocolMessage>>>> {
        self.pending_responses.clone()
    }
    
    /// Get a clone of the handshake states for use by the TCP server
    pub fn get_handshake_states(&self) -> Arc<Mutex<HashMap<String, HandshakeState>>> {
        self.handshake_states.clone()
    }
    
    
    /// Get the local actor path for this transport
    pub fn get_local_actor_path(&self) -> ActorPath {
        ActorPath::new(
            self.system_name.clone(),
            self.local_host.clone(),
            self.local_port,
            "system".to_string(),
        )
    }
    
    /// Connect to a remote actor system
    pub async fn connect(&self, host: &str, system_name: &str, port: u16) -> Result<()> {
        let address = format!("{}:{}", host, port);
        
        let mut connections = self.connections.lock().await;
        if connections.contains_key(&address) {
            return Ok(()); // Already connected
        }
        
        debug!("Connecting to {}", address);
        let stream = TcpStream::connect(&address).await?;
        
        // Initialize handshake state for this connection
        {
            let mut handshake_states = self.handshake_states.lock().await;
            handshake_states.insert(address.clone(), HandshakeState {
                completed: false,
                queued_messages: Vec::new(),
            });
        }
        
        let connection = Connection::new(stream, self.system_uid, self.local_port, &self.local_host, &self.system_name, host, port, system_name, self.pending_responses.clone(), self.clone()).await?;
        connections.insert(address.clone(), connection);
        
        debug!("[CONNECTION_LIFECYCLE] Connection established - HandshakeReq sent, waiting for HandshakeRsp");
        
        Ok(())
    }
    
    /// Run TCP server to accept incoming connections from Pekko or Rukko systems
    pub async fn run_server(
        listener: TcpListener, 
        transport: Arc<ArteryTransport>,
    ) {
        debug!("Starting TCP server for incoming connections");
        
        loop {
            match listener.accept().await {
                Ok((stream, addr)) => {
                    debug!("Accepted incoming connection from {}", addr);

                    // Handle each connection in a separate task
                    let transport_clone = transport.clone();
                    tokio::spawn(async move {
                        if let Err(e) = Self::handle_incoming_connection(stream, transport_clone).await {
                            warn!("Error handling incoming connection: {}", e);
                        }
                    });
                }
                Err(e) => {
                    error!("Failed to accept connection: {}", e);
                    break;
                }
            }
        }
    }
    
    /// Handle an incoming connection from a remote Pekko system
    async fn handle_incoming_connection(
        stream: TcpStream, 
        transport: Arc<ArteryTransport>,
    ) -> Result<()> {
        let peer_addr = stream.peer_addr().map(|a| a.to_string()).unwrap_or_else(|_| "unknown".to_string());
        debug!("[CONNECTION_LIFECYCLE] OPENED incoming connection from {}", peer_addr);
        
        let (reader, writer) = tokio::io::split(stream);
        let _writer = Arc::new(Mutex::new(writer)); // Keep writer alive to maintain connection
        
        // Start a reader task for this connection
        let peer_addr_clone = peer_addr.clone();
        let reader_handle = tokio::spawn(async move {
            debug!("[CONNECTION_LIFECYCLE] 🔄 Starting reader loop for {}", peer_addr_clone);
            Self::incoming_reader_loop(reader, transport).await;
            debug!("[CONNECTION_LIFECYCLE] Reader loop ended for {}", peer_addr_clone);
        });
        
        // Wait for the reader to complete
        let _ = reader_handle.await;
        debug!("[CONNECTION_LIFECYCLE] CLOSED incoming connection from {}", peer_addr);
        
        Ok(())
    }
    
    /// Reader loop for incoming connections
    async fn incoming_reader_loop(
        mut reader: tokio::io::ReadHalf<TcpStream>, 
        transport: Arc<ArteryTransport>,
    ) {
        debug!("[CONNECTION_LIFECYCLE] 🟢 Incoming reader loop STARTED");
        trace!("[HANDSHAKE_TRACE] Incoming reader loop is ready to receive HandshakeRsp");
        let mut buffer = BytesMut::with_capacity(8192);
        
        // First, read connection header (magic bytes + stream ID)
        if let Err(e) = ArteryTransport::read_connection_header(&mut reader, &mut buffer).await {
            warn!("[CONNECTION_LIFECYCLE] Failed to read connection header: {}", e);
            return;
        }
        debug!("[CONNECTION_LIFECYCLE] Connection header read successfully");
        trace!("[HANDSHAKE_TRACE] Connection header processing complete - ready for HandshakeRsp frames");
        
        // Then process frames
        loop {
            match reader.read_buf(&mut buffer).await {
                Ok(0) => {
                    debug!("[CONNECTION_LIFECYCLE] Incoming connection closed by remote (EOF)");
                    break;
                }
                Ok(n) => {
                    debug!("[CONNECTION_LIFECYCLE] Read {} bytes from incoming connection (buffer now {} bytes)", n, buffer.len());
                    debug!("[CONNECTION_LIFECYCLE] New data hex: {}",
                           buffer[buffer.len()-n..].iter().map(|b| format!("{:02x}", b)).collect::<Vec<_>>().join(" "));
                    
                    // Process any complete frames in the buffer
                    let mut frames_processed = 0;
                    while let Some(frame_result) = Connection::try_parse_frame(&mut buffer) {
                        match frame_result {
                            Ok((header, data)) => {
                                frames_processed += 1;
                                debug!("[CONNECTION_LIFECYCLE] Processing incoming frame #{}: {} bytes", frames_processed, header.size);
                                trace!("[HANDSHAKE_TRACE] Processing incoming frame - checking for HandshakeRsp");
                                transport.process_incoming_frame(data).await;
                            }
                            Err(e) => {
                                error!("[CONNECTION_LIFECYCLE] Error parsing incoming frame: {}", e);
                                break;
                            }
                        }
                    }
                    if frames_processed > 0 {
                        debug!("[CONNECTION_LIFECYCLE] Processed {} frames in this read cycle", frames_processed);
                    }
                }
                Err(e) => {
                    error!("[CONNECTION_LIFECYCLE] Error reading from incoming connection: {}", e);
                    break;
                }
            }
        }
        
        debug!("[CONNECTION_LIFECYCLE] 🔴 Incoming reader loop ENDED");
    }
    
    
    /// Read connection header (AKKA magic + stream ID)
    async fn read_connection_header(reader: &mut tokio::io::ReadHalf<TcpStream>, buffer: &mut BytesMut) -> Result<()> {
        // Read at least 5 bytes (4 for "AKKA" + 1 for stream ID)
        while buffer.len() < 5 {
            match reader.read_buf(buffer).await {
                Ok(0) => return Err(RukkoError::HandshakeFailed("Connection closed during handshake header read".to_string())),
                Ok(_) => {},
                Err(e) => return Err(RukkoError::Connection(e)),
            }
        }
        
        // Check magic bytes
        if &buffer[0..4] != b"AKKA" {
            return Err(RukkoError::HandshakeFailed(format!("Invalid magic bytes during handshake: {:?}", &buffer[0..4])));
        }
        
        let stream_id = buffer[4];
        debug!("Received connection header: magic=AKKA, stream_id={}", stream_id);
        
        // Consume the header bytes
        buffer.advance(5);
        
        Ok(())
    }
    
    /// Process frames received on incoming connections
    async fn process_incoming_frame(&self, data: Bytes) {
        trace!("Processing incoming frame with {} bytes", data.len());
        
        // CRITICAL: Check for HandshakeRsp at binary level BEFORE envelope decoding
        // HandshakeRsp uses serializer_id=17 with manifest="e" but payload is MessageWithAddress,
        // not RemoteEnvelope, so standard envelope decoding fails.
        if data.len() >= 16 {
            let serializer_id = u32::from_le_bytes([data[12], data[13], data[14], data[15]]);
            trace!("[HANDSHAKE_TRACE] 🔍 Checking incoming frame: serializer_id={}, data_len={}", serializer_id, data.len());
            
            if serializer_id == 17 {
                trace!("[HANDSHAKE_TRACE] Found serializer_id=17 frame - processing Artery message");
                
                // This might be HandshakeRsp - try to extract manifest to confirm
                match Self::try_parse_envelope_header(&data) {
                    Ok(envelope) => {
                        trace!("[HANDSHAKE_TRACE] Processing Artery message with manifest: '{}'", envelope.class_manifest);
                        
                        if envelope.class_manifest == "e" {
                            trace!("[HANDSHAKE_TRACE] FOUND HandshakeRsp on INCOMING connection!");
                            
                            // HandshakeRsp payload is MessageWithAddress protobuf
                            match crate::pekko_protobuf::MessageWithAddress::decode(envelope.message_payload.as_ref()) {
                                Ok(message_with_address) => {
                                    trace!("[HANDSHAKE_TRACE] Successfully decoded HandshakeRsp as MessageWithAddress");
                                    trace!("HandshakeRsp: system={}, hostname={}, port={}, uid={}", 
                                           message_with_address.address.address.system, 
                                           message_with_address.address.address.hostname,
                                           message_with_address.address.address.port, 
                                           message_with_address.address.uid);
                                    
                                    // Complete the handshake
                                    let server_address = format!("{}:{}", 
                                        message_with_address.address.address.hostname,
                                        message_with_address.address.address.port);
                                    if let Err(e) = self.complete_handshake(&server_address).await {
                                        error!("Failed to complete handshake for {}: {}", server_address, e);
                                    } else {
                                        trace!("[HANDSHAKE_TRACE] Handshake completed successfully for {}", server_address);
                                    }
                                }
                                Err(e) => {
                                    error!("Failed to decode HandshakeRsp MessageWithAddress: {}", e);
                                    let hex_dump: String = envelope.message_payload.iter()
                                        .take(64)
                                        .map(|b| format!("{:02x}", b))
                                        .collect::<Vec<String>>()
                                        .join(" ");
                                    error!("HandshakeRsp payload hex: {}", hex_dump);
                                }
                            }
                            return;
                        } else if envelope.class_manifest == "d" {
                            trace!("[HANDSHAKE_TRACE] Received additional HandshakeReq (manifest='d') after handshake completion");
                            trace!("[HANDSHAKE_TRACE] This is normal Pekko behavior - handshake already completed via auto-timer");
                            return;
                        } else if envelope.class_manifest == "c" {
                            trace!("[HANDSHAKE_TRACE] Received ActorSystemTerminatingAck (manifest='c')");
                            
                            // ActorSystemTerminatingAck payload is MessageWithAddress protobuf (like HandshakeRsp)
                            match crate::pekko_protobuf::MessageWithAddress::decode(envelope.message_payload.as_ref()) {
                                Ok(message_with_address) => {
                                    debug!("Successfully decoded ActorSystemTerminatingAck from {:?}", message_with_address.address);
                                    let sender_addr = format!("{}:{}", 
                                                            message_with_address.address.address.hostname,
                                                            message_with_address.address.address.port);
                                    self.handle_termination_ack(sender_addr).await;
                                }
                                Err(e) => {
                                    warn!("Failed to decode ActorSystemTerminatingAck MessageWithAddress: {}", e);
                                }
                            }
                            return;
                        } else if envelope.class_manifest == "b" {
                            trace!("[HANDSHAKE_TRACE] Received ActorSystemTerminating (manifest='b')");
                            
                            // ActorSystemTerminating payload is MessageWithAddress protobuf
                            match crate::pekko_protobuf::MessageWithAddress::decode(envelope.message_payload.as_ref()) {
                                Ok(message_with_address) => {
                                    debug!("Successfully decoded ActorSystemTerminating from {:?}", message_with_address.address);
                                    debug!("ActorSystemTerminating handled - ACK response is implemented in the main message handler");
                                }
                                Err(e) => {
                                    warn!("Failed to decode ActorSystemTerminating MessageWithAddress: {}", e);
                                }
                            }
                            return;
                        } else {
                            warn!("Unknown Artery message - manifest is '{}' (expecting 'd', 'e', 'b', or 'c')", envelope.class_manifest);
                        }
                    }
                    Err(e) => {
                        error!("Failed to parse handshake envelope: {}", e);
                        // Note: This is a warning, not a returned error since we continue processing
                    }
                }
            }
        }
        
        // Try to decode as regular message envelope
        let mut data_buf = data.clone();
        match MessageEnvelope::decode(&mut data_buf) {
            Ok(envelope) => {
                trace!("Decoded incoming message from {} with serializer_id={} manifest={}", 
                       envelope.sender.to_string(), envelope.serializer_id, envelope.class_manifest);
                
                // Route this to the ask pattern handler for regular messages
                Self::handle_incoming_message(envelope, self.pending_responses.clone(), Some(self)).await;
            }
            Err(e) => {
                warn!("Failed to decode incoming message: {}", e);
                let hex_dump: String = data.iter()
                    .take(64)
                    .map(|b| format!("{:02x}", b))
                    .collect::<Vec<String>>()
                    .join(" ");
                warn!("Raw frame data hex dump: {}", hex_dump);
            }
        }
    }
    
    /// Try to parse envelope header without full decoding (for HandshakeRsp detection)
    fn try_parse_envelope_header(data: &[u8]) -> Result<MessageEnvelope> {
        if data.len() < 28 {
            return Err(RukkoError::Protocol("Insufficient data for envelope header".to_string()));
        }
        
        // Parse fixed header (28 bytes) 
        let version = data[0];
        let flags = data[1];
        let actor_ref_compression_version = data[2];
        let class_manifest_compression_version = data[3];
        
        let uid = u64::from_le_bytes([
            data[4], data[5], data[6], data[7], data[8], data[9], data[10], data[11]
        ]);
        
        let serializer_id = u32::from_le_bytes([data[12], data[13], data[14], data[15]]);
        
        // Skip the tags and read literals for manifest
        let mut pos = 28;
        
        // Skip sender literal
        if pos + 2 > data.len() { return Err(RukkoError::Protocol("Insufficient data".to_string())); }
        let sender_len = u16::from_le_bytes([data[pos], data[pos + 1]]) as usize;
        pos += 2 + sender_len;
        
        // Skip recipient literal
        if pos + 2 > data.len() { return Err(RukkoError::Protocol("Insufficient data".to_string())); }
        let recipient_len = u16::from_le_bytes([data[pos], data[pos + 1]]) as usize;
        pos += 2 + recipient_len;
        
        // Read manifest literal
        if pos + 2 > data.len() { return Err(RukkoError::Protocol("Insufficient data".to_string())); }
        let manifest_len = u16::from_le_bytes([data[pos], data[pos + 1]]) as usize;
        pos += 2;
        
        if pos + manifest_len > data.len() { return Err(RukkoError::Protocol("Insufficient data".to_string())); }
        let class_manifest = String::from_utf8_lossy(&data[pos..pos + manifest_len]).to_string();
        pos += manifest_len;
        
        // Extract message payload (rest of the buffer)
        let message_payload = Bytes::from(data[pos..].to_vec());
        
        // Create minimal envelope for HandshakeRsp detection 
        Ok(MessageEnvelope {
            version,
            flags,
            actor_ref_compression_version,
            class_manifest_compression_version,
            uid,
            serializer_id,
            sender: ActorPath::new("unknown".to_string(), "127.0.0.1".to_string(), 25552, "system".to_string()),
            recipient: ActorPath::new("unknown".to_string(), "127.0.0.1".to_string(), 0, "system".to_string()),
            class_manifest,
            message_payload,
        })
    }
    
    
    /// Handle incoming decoded messages (responses, etc.)
    async fn handle_incoming_message(envelope: MessageEnvelope, pending_responses: Arc<Mutex<HashMap<String, oneshot::Sender<ProtocolMessage>>>>, transport: Option<&ArteryTransport>) {
        debug!("Handling incoming message with serializer ID {}", envelope.serializer_id);
        
        // Reconstruct the message from the envelope
        let message = match envelope.serializer_id {
            20 => {
                // Primitive string serializer
                let content = String::from_utf8_lossy(&envelope.message_payload);
                ProtocolMessage::text(content.to_string())
            }
            4 => {
                // Bytes serializer - not supported for user messages
                return; // Skip binary messages
            }
            17 => {
                // Artery message serializer (control messages: handshake, termination, acks)
                if !envelope.class_manifest.is_empty() {
                    let manifest = envelope.class_manifest.as_str();
                    match manifest {
                        "c" => {
                            // ActorSystemTerminatingAck message
                            if let Some(transport) = transport {
                                debug!("Received ActorSystemTerminatingAck from {:?}", envelope.sender);
                                let sender_addr = format!("{}:{}", envelope.sender.host, envelope.sender.port);
                                transport.handle_termination_ack(sender_addr).await;
                            }
                            return;
                        }
                        "b" => {
                            // ActorSystemTerminating message - respond with ACK
                            if let Some(transport) = transport {
                                debug!("Received ActorSystemTerminating from {:?}, sending ACK", envelope.sender);
                                let local_address = UniqueAddress {
                                    address: ActorPath {
                                        protocol: "pekko",
                                        system: transport.system_name.clone(),
                                        host: transport.local_host.clone(),
                                        port: transport.local_port,
                                        path: "".to_string(),
                                    },
                                    uid: transport.system_uid,
                                };
                                // Create and send ActorSystemTerminatingAck response
                                let ack_message = InternalMessage::actor_system_terminating_ack(local_address);
                                let ack_envelope = match MessageEnvelope::new(
                                    transport.get_local_actor_path(),
                                    envelope.sender.clone(),
                                    ack_message,
                                ) {
                                    Ok(env) => env,
                                    Err(e) => {
                                        error!("Failed to create ACK envelope: {}", e);
                                        return;
                                    }
                                };
                                
                                // Send the ACK back through the existing connection infrastructure
                                let connection_key = format!("{}:{}", envelope.sender.host, envelope.sender.port);
                                let connections = transport.connections.lock().await;
                                if let Some(connection) = connections.get(&connection_key) {
                                    if let Err(e) = connection.send_message(ack_envelope).await {
                                        error!("Failed to send ActorSystemTerminatingAck: {}", e);
                                    } else {
                                        debug!("Successfully sent ActorSystemTerminatingAck to {}", envelope.sender.to_string());
                                    }
                                } else {
                                    warn!("No active connection found to send ACK to {}", envelope.sender.to_string());
                                }
                            }
                            return;
                        }
                        "d" | "e" => {
                            // Handshake messages
                            trace!("Received handshake message");
                            return;
                        }
                        _ => {
                            debug!("Unknown control message manifest: {}", manifest);
                            return;
                        }
                    }
                } else {
                    trace!("Control message without manifest");
                    return;
                }
            }
            _ => {
                warn!("Unknown serializer ID: {}", envelope.serializer_id);
                return;
            }
        };
        
        trace!("Received message: {:?}", message);
        
        // Check if this is a response to a pending ask request by actor path
        // Use proper path-based correlation like Pekko does
        // Now handles both ActorRef (temp/highway$a) and ActorSelection (temp/_user_highway$a) formats
        if envelope.recipient.path.starts_with("temp/") {
            trace!("Received response to temporary actor path, looking for pending ask request");
            
            // Use path-based correlation - match exact temporary actor path
            let recipient_path = envelope.recipient.to_string();
            let response_tx = {
                let mut pending = pending_responses.lock().await;
                pending.remove(&recipient_path)
            };
            
            if let Some(response_tx) = response_tx {
                trace!("Found pending ask request for path: {}", recipient_path);
                if let Err(_) = response_tx.send(message) {
                    warn!("Failed to send response - receiver dropped");
                }
            } else {
                warn!("No pending ask request found for temporary response path: {}", recipient_path);
            }
        } else {
            debug!("Message not addressed to temporary actor path - treating as unsolicited message");
        }
    }
    
    
    /// Send a message and wait for a response
    pub async fn ask(
        &self,
        sender: ActorPath,
        recipient: ActorPath,
        message: ProtocolMessage,
        timeout_duration: Duration,
    ) -> Result<ProtocolMessage> {
        let (response_tx, response_rx) = oneshot::channel();
        
        // Register pending response by temporary actor path (Pekko-style)
        let temp_actor_path = sender.to_string();
        {
            let mut pending = self.pending_responses.lock().await;
            pending.insert(temp_actor_path.clone(), response_tx);
        }
        
        // Send message (Pekko uses actor path correlation, not sequence numbers)
        self.tell(sender, recipient, message).await?;
        
        // Wait for response
        match timeout(timeout_duration, response_rx).await {
            Ok(Ok(response)) => Ok(response),
            Ok(Err(_)) => Err(RukkoError::ActorNotFound("Actor did not respond - channel closed".to_string())),
            Err(_) => {
                // Clean up pending response on timeout
                let mut pending = self.pending_responses.lock().await;
                pending.remove(&temp_actor_path);
                Err(RukkoError::Timeout)
            }
        }
    }
    
    /// Send a fire-and-forget message
    pub async fn tell(
        &self,
        sender: ActorPath,
        recipient: ActorPath,
        message: ProtocolMessage,
    ) -> Result<()> {
        let address = format!("{}:{}", recipient.host, recipient.port);
        
        // Ensure connection exists
        self.connect(&recipient.host, &recipient.system, recipient.port).await?;

        let internal_message = InternalMessage::from_user_message(&message);
        let mut envelope = MessageEnvelope::new(sender, recipient, internal_message)?;
        envelope.uid = self.system_uid;
        
        // Check handshake completion status
        let mut handshake_states = self.handshake_states.lock().await;
        let handshake_state = handshake_states.entry(address.clone()).or_insert(HandshakeState {
            completed: false,
            queued_messages: Vec::new(),
        });
        
        if handshake_state.completed {
            // Handshake completed - send message immediately
            trace!("Handshake completed for {} - sending message immediately", address);
            drop(handshake_states); // Release lock before sending
            
            let connections = self.connections.lock().await;
            if let Some(connection) = connections.get(&address) {
                connection.send_message(envelope).await?;
            } else {
                return Err(RukkoError::Connection(std::io::Error::new(
                    std::io::ErrorKind::NotConnected,
                    "Connection not found",
                )));
            }
        } else {
            // Handshake not completed - queue message and trigger auto-completion
            debug!("Handshake not completed for {} - queuing message and starting connection timer", address);
            handshake_state.queued_messages.push(envelope);
            
            // Start auto-completion timer if this is the first queued message
            if handshake_state.queued_messages.len() == 1 {
                trace!("[HANDSHAKE_TRACE] Starting connection establishment timer for {}", address);
                let transport_clone = self.clone();
                let address_clone = address.clone();
                tokio::spawn(async move {
                    // Wait for established connections and settle
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    
                    // Check if transport is shutdown before completing
                    if transport_clone.is_shutdown() {
                        trace!("[HANDSHAKE_TRACE] Transport shutdown during connection establishment for {}", address_clone);
                        return;
                    }
                    
                    // Complete handshake automatically - connection has been established
                    if let Err(e) = transport_clone.complete_handshake(&address_clone).await {
                        error!("Failed to complete handshake for {}: {}", address_clone, e);
                    } else {
                        trace!("[HANDSHAKE_TRACE] Connection established and handshake completed for {}", address_clone);
                    }
                });
            }
        }
        
        Ok(())
    }
    
    /// Mark handshake as completed and process any queued messages
    async fn complete_handshake(&self, server_address: &str) -> Result<()> {
        debug!("Marking handshake as completed for {} and processing queued messages", server_address);
        
        let mut handshake_states = self.handshake_states.lock().await;
        if let Some(handshake_state) = handshake_states.get_mut(server_address) {
            if !handshake_state.completed {
                handshake_state.completed = true;
                
                // Process all queued messages
                let queued_count = handshake_state.queued_messages.len();
                if queued_count > 0 {
                    debug!("Processing {} queued messages for {}", queued_count, server_address);
                    
                    let queued_messages = std::mem::take(&mut handshake_state.queued_messages);
                    drop(handshake_states); // Release lock before sending messages
                    
                    // Send all queued messages
                    let connections = self.connections.lock().await;
                    if let Some(connection) = connections.get(server_address) {
                        for envelope in queued_messages {
                            trace!("Sending queued message from {} to {}", 
                                   envelope.sender.to_string(), envelope.recipient.to_string());
                            if let Err(e) = connection.send_message(envelope).await {
                                error!("Failed to send queued message: {}", e);
                            }
                        }
                        debug!("Successfully processed {} queued messages for {}", queued_count, server_address);
                    } else {
                        warn!("Connection not found for {} when processing queued messages", server_address);
                    }
                } else {
                    debug!("No queued messages to process for {}", server_address);
                }
            } else {
                debug!("Handshake already completed for {}", server_address);
            }
        } else {
            debug!("No handshake state found for {}", server_address);
        }
        
        Ok(())
    }
    
    /// Check if transport is shutdown
    pub fn is_shutdown(&self) -> bool {
        self.shutdown_flag.load(Ordering::SeqCst)
    }
    
    /// Shutdown the transport and cleanup all resources
    /// Follows Pekko's graceful shutdown pattern with comprehensive error recovery
    pub async fn shutdown(&self) {
        // Prevent double shutdown
        if self.shutdown_flag.compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst).is_err() {
            debug!("ArteryTransport already shutting down, ignoring duplicate shutdown call");
            return;
        }
        
        info!("Starting graceful shutdown of ArteryTransport for system '{}'", self.system_name);
        
        // Apply total timeout to entire shutdown sequence (like Pekko's JVM shutdown hook)
        let total_result = timeout(self.shutdown_timeouts.total_timeout, async {
            // Run internal shutdown with error recovery at each step
            self.internal_shutdown().await
        }).await;
        
        match total_result {
            Ok(_) => info!("ArteryTransport shutdown completed successfully for system '{}'", self.system_name),
            Err(_) => {
                warn!("ArteryTransport shutdown timed out after {:?}, forcing immediate termination for system '{}'", 
                      self.shutdown_timeouts.total_timeout, self.system_name);
                // Force immediate cleanup even if operations are hanging
                self.force_cleanup().await;
            }
        }
    }
    
    /// Internal shutdown sequence with error recovery (like Pekko's internalShutdown)
    async fn internal_shutdown(&self) {
        // Step 1: Stop accepting new connections (infrastructure for future server features)
        debug!("Phase 1: Stopping new connection acceptance");
        
        // Step 2: Send termination hints to connected peers
        debug!("Phase 2: Sending termination hints to connected peers");
        if let Err(e) = timeout(Duration::from_secs(1), self.send_termination_hints()).await {
            warn!("Termination hints timed out: {:?}", e);
        }
        
        // Step 3: Wait for pending responses with timeout (graceful flush like FlushOnShutdown)
        debug!("Phase 3: Waiting for pending responses to complete");
        if let Err(_) = timeout(self.shutdown_timeouts.flush_timeout, self.wait_for_pending_responses()).await {
            warn!("Graceful flush timeout reached after {:?}, proceeding with shutdown", self.shutdown_timeouts.flush_timeout);
        } else {
            debug!("All pending responses completed gracefully");
        }
        
        // Step 4: Close all connections gracefully with individual timeouts
        debug!("Phase 4: Closing all connections gracefully");
        if let Err(e) = timeout(self.shutdown_timeouts.connection_timeout, self.close_all_connections_with_recovery()).await {
            warn!("Connection shutdown timed out after {:?}: {:?}", self.shutdown_timeouts.connection_timeout, e);
        }
        
        // Step 5: Clear remaining state (always succeeds)
        debug!("Phase 5: Clearing remaining state");
        self.clear_all_state().await;
    }
    
    /// Force cleanup when all else fails (like Pekko's emergency shutdown)
    async fn force_cleanup(&self) {
        warn!("Executing force cleanup - aborting all operations immediately");
        
        // Clear all state synchronously without waiting
        if let Ok(mut connections) = self.connections.try_lock() {
            for (addr, connection) in connections.drain() {
                debug!("Force-aborting connection to {}", addr);
                connection._reader_handle.abort();
            }
        }
        
        if let Ok(mut pending) = self.pending_responses.try_lock() {
            let count = pending.len();
            pending.clear();
            if count > 0 {
                warn!("Force-dropped {} pending responses", count);
            }
        }
        
        if let Ok(mut handshakes) = self.handshake_states.try_lock() {
            handshakes.clear();
        }
        
        warn!("Force cleanup complete");
    }
    
    /// Send termination hints to all connected peers and wait for ACKs (like Pekko's FlushOnShutdown)
    async fn send_termination_hints(&self) {
        let connections = self.connections.lock().await;
        let peer_count = connections.len();
        
        if peer_count > 0 {
            debug!("Sending ActorSystemTerminating messages to {} connected peers", peer_count);
            
            // Create our local unique address for the termination message
            let local_address = UniqueAddress {
                address: ActorPath {
                    protocol: "pekko",
                    system: self.system_name.clone(),
                    host: self.local_host.clone(),
                    port: self.local_port,
                    path: "".to_string(),
                },
                uid: self.system_uid,
            };
            
            // Clear any previous expected ACKs
            {
                let mut expected_acks = self.expected_termination_acks.lock().await;
                expected_acks.clear();
            }
            
            // Use JoinSet to send messages concurrently to all peers
            let mut join_set = JoinSet::new();
            
            for (addr, connection) in connections.iter() {
                let addr_clone = addr.clone();
                let writer = connection.writer.clone();
                let local_addr_clone = local_address.clone();
                let system_uid = self.system_uid;
                let expected_acks_clone = self.expected_termination_acks.clone();
                
                join_set.spawn(async move {
                    let success = Self::send_termination_to_peer(addr_clone.clone(), writer, local_addr_clone, system_uid).await.unwrap_or(false);
                    if success {
                        // Add to expected ACKs if message was sent successfully
                        expected_acks_clone.lock().await.insert(addr_clone);
                    }
                    success
                });
            }
            
            // Wait for all termination messages to be sent (with error collection)
            let mut success_count = 0;
            let mut error_count = 0;
            
            while let Some(result) = join_set.join_next().await {
                match result {
                    Ok(true) => success_count += 1,
                    Ok(false) => error_count += 1,
                    Err(e) => {
                        error_count += 1;
                        debug!("Termination message task failed: {:?}", e);
                    }
                }
            }
            
            info!("Termination messages sent: {} succeeded, {} failed", success_count, error_count);
            
            // Wait for ACKs from peers that successfully received termination messages
            let expected_count = {
                let acks = self.expected_termination_acks.lock().await;
                acks.len()
            };
            
            if expected_count > 0 {
                debug!("Waiting for {} ActorSystemTerminatingAck messages", expected_count);
                self.wait_for_termination_acks(self.expected_termination_acks.clone(), Duration::from_secs(1)).await;
            }
        } else {
            debug!("No connected peers to notify about termination");
        }
    }
    
    /// Wait for ActorSystemTerminatingAck messages with timeout (like Pekko's FlushOnShutdown)
    async fn wait_for_termination_acks(&self, expected_acks: Arc<Mutex<HashSet<String>>>, timeout_duration: Duration) {
        debug!("Starting to wait for termination ACKs with {}ms timeout", timeout_duration.as_millis());
        
        let start_time = tokio::time::Instant::now();
        let mut poll_interval = tokio::time::interval(Duration::from_millis(50));
        
        loop {
            // Check if timeout has been reached
            if start_time.elapsed() >= timeout_duration {
                let remaining = {
                    let acks = expected_acks.lock().await;
                    acks.clone()
                };
                
                if !remaining.is_empty() {
                    warn!("Timeout waiting for {} ActorSystemTerminatingAck messages from peers: {:?}", 
                          remaining.len(), remaining);
                } else {
                    debug!("All termination ACKs received before timeout");
                }
                break;
            }
            
            // Check if all ACKs have been received
            let remaining_count = {
                let acks = expected_acks.lock().await;
                acks.len()
            };
            
            if remaining_count == 0 {
                debug!("All {} termination ACKs received successfully", expected_acks.lock().await.len());
                break;
            }
            
            // Wait for next poll interval
            poll_interval.tick().await;
        }
        
        debug!("Termination ACK waiting completed");
    }
    
    /// Process incoming ActorSystemTerminatingAck message (called from message handler)
    pub async fn handle_termination_ack(&self, from_address: String) {
        let removed = {
            let mut expected_acks = self.expected_termination_acks.lock().await;
            expected_acks.remove(&from_address)
        };
        
        if removed {
            debug!("Received ActorSystemTerminatingAck from expected peer: {}", from_address);
        } else {
            debug!("Received unexpected ActorSystemTerminatingAck from: {}", from_address);
        }
    }
    
    /// Send ActorSystemTerminating message to a single peer
    async fn send_termination_to_peer(
        addr: String, 
        writer: Arc<Mutex<tokio::io::WriteHalf<TcpStream>>>,
        local_address: UniqueAddress,
        system_uid: u64
    ) -> Result<bool> {
        debug!("Sending ActorSystemTerminating to {}", addr);
        
        // Create the termination message
        let termination_message = InternalMessage::actor_system_terminating(local_address.clone());
        
        // Create envelope for the termination message
        let sender_path = local_address.address;
        let recipient_path = ActorPath::from_string(format!("pekko://System@{}/", addr))
            .unwrap_or_else(|_| ActorPath {
                protocol: "pekko",
                system: "System".to_string(),
                host: addr.split(':').next().unwrap_or("127.0.0.1").to_string(),
                port: addr.split(':').nth(1).and_then(|p| p.parse().ok()).unwrap_or(25552),
                path: "".to_string(),
            });
        
        let mut envelope = MessageEnvelope::new(sender_path, recipient_path, termination_message)?;
        envelope.uid = system_uid;
        
        // Try to send the message with a short timeout
        let send_timeout = Duration::from_millis(500);
        let send_result = timeout(send_timeout, async {
            if let Ok(mut writer_guard) = writer.try_lock() {
                Self::send_envelope_to_connection(&mut writer_guard, &envelope).await
            } else {
                debug!("Could not acquire writer lock for {}, skipping termination message", addr);
                Ok(())
            }
        }).await;
        
        match send_result {
            Ok(Ok(_)) => {
                debug!("Successfully sent ActorSystemTerminating to {}", addr);
                Ok(true)
            }
            Ok(Err(e)) => {
                debug!("Failed to send ActorSystemTerminating to {}: {}", addr, e);
                Ok(false)
            }
            Err(_) => {
                warn!("Timeout sending ActorSystemTerminating to {} ({}ms)", addr, send_timeout.as_millis());
                Ok(false)
            }
        }
    }
    
    /// Send an envelope to a specific connection (helper method)
    async fn send_envelope_to_connection(
        writer: &mut tokio::io::WriteHalf<TcpStream>,
        envelope: &MessageEnvelope
    ) -> Result<()> {
        // Encode the envelope
        let frame_data = envelope.encode()?;
        
        // Create frame header - use Control stream for termination messages
        let frame_header = FrameHeader::new(frame_data.len() as u32);
        let header_bytes = frame_header.encode();
        
        // Send header and payload
        writer.write_all(&header_bytes).await?;
        writer.write_all(&frame_data).await?;
        writer.flush().await?;
        
        debug!("Envelope sent successfully (Control stream)");
        Ok(())
    }
    
    /// Wait for all pending responses to complete
    async fn wait_for_pending_responses(&self) {
        loop {
            let pending_count = {
                let pending = self.pending_responses.lock().await;
                pending.len()
            };
            
            if pending_count == 0 {
                debug!("No pending responses remaining");
                break;
            }
            
            debug!("Waiting for {} pending responses to complete", pending_count);
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
    
    /// Close all connections gracefully with error recovery (like Pekko's connection cleanup)
    async fn close_all_connections_with_recovery(&self) {
        let mut connections = self.connections.lock().await;
        let connection_count = connections.len();
        
        if connection_count > 0 {
            debug!("Closing {} connections gracefully with individual timeouts", connection_count);
            
            // Use JoinSet to handle multiple connections concurrently with timeouts
            let mut join_set = JoinSet::new();
            
            for (addr, connection) in connections.drain() {
                let addr_clone = addr.clone();
                join_set.spawn(async move {
                    Self::close_single_connection_with_timeout(addr_clone, connection).await
                });
            }
            
            // Wait for all connection closures with individual error recovery
            let mut success_count = 0;
            let mut error_count = 0;
            
            while let Some(result) = join_set.join_next().await {
                match result {
                    Ok(true) => success_count += 1,
                    Ok(false) => error_count += 1,
                    Err(e) => {
                        error_count += 1;
                        debug!("Connection closure task failed: {:?}", e);
                    }
                }
            }
            
            debug!("Connection closure complete: {} succeeded, {} failed", success_count, error_count);
        }
    }
    
    /// Close a single connection with timeout (like Pekko's individual connection shutdown)
    async fn close_single_connection_with_timeout(addr: String, connection: Connection) -> bool {
        debug!("Closing connection to {} with timeout", addr);
        
        // Timeout for individual connection shutdown (shorter than total)
        let connection_timeout = Duration::from_secs(2);
        
        let shutdown_result = timeout(connection_timeout, async {
            // Try to shutdown the connection gracefully
            if let Some(writer_arc) = connection.stream {
                if let Ok(mut writer) = writer_arc.try_lock() {
                    writer.shutdown().await
                } else {
                    debug!("Could not acquire writer lock for {}, skipping graceful shutdown", addr);
                    Ok(())
                }
            } else {
                Ok(())
            }
        }).await;
        
        // Always abort the reader task regardless of shutdown success
        connection._reader_handle.abort();
        
        match shutdown_result {
            Ok(Ok(_)) => {
                debug!("Successfully closed connection to {}", addr);
                true
            }
            Ok(Err(e)) => {
                debug!("Error during graceful shutdown to {}: {}", addr, e);
                false
            }
            Err(_) => {
                warn!("Connection shutdown to {} timed out after {:?}, connection forcefully closed", addr, connection_timeout);
                false
            }
        }
    }
    
    /// Clear all remaining state
    async fn clear_all_state(&self) {
        // Clear any remaining pending responses
        let mut pending = self.pending_responses.lock().await;
        let remaining_responses = pending.len();
        if remaining_responses > 0 {
            warn!("Dropping {} remaining pending responses", remaining_responses);
        }
        pending.clear();
        
        // Clear handshake states
        let mut handshakes = self.handshake_states.lock().await;
        let remaining_handshakes = handshakes.len();
        if remaining_handshakes > 0 {
            debug!("Clearing {} remaining handshake states", remaining_handshakes);
        }
        handshakes.clear();
        
        debug!("State cleanup complete");
    }
    
}

/// TCP connection wrapper
#[derive(Debug)]
struct Connection {
    writer: Arc<Mutex<tokio::io::WriteHalf<TcpStream>>>,
    _reader_handle: tokio::task::JoinHandle<()>,
    stream: Option<Arc<Mutex<tokio::io::WriteHalf<TcpStream>>>>,
}

impl Connection {
    async fn new(stream: TcpStream, system_uid: u64, local_port: u16, local_host: &str, system_name: &str, target_host: &str, target_port: u16, target_system: &str, pending_responses: Arc<Mutex<HashMap<String, oneshot::Sender<ProtocolMessage>>>>, transport: ArteryTransport) -> Result<Self> {
        let peer_addr = stream.peer_addr().map(|a| a.to_string()).unwrap_or_else(|_| "unknown".to_string());
        debug!("[CONNECTION_LIFECYCLE] OPENED outbound connection to {}", peer_addr);
        
        let (reader, writer) = tokio::io::split(stream);
        let writer = Arc::new(Mutex::new(writer));
        
        // Spawn reader task for handshake and regular message handling
        let peer_addr_clone = peer_addr.clone();
        let transport_clone = transport.clone();
        let reader_handle = tokio::spawn(async move {
            debug!("[CONNECTION_LIFECYCLE] 🔄 Starting outbound reader loop for {}", peer_addr_clone);
            Self::reader_loop_with_transport(reader, pending_responses, transport_clone).await;
            debug!("[CONNECTION_LIFECYCLE] Outbound reader loop ended for {}", peer_addr_clone);
        });
        
        // Perform handshake
        debug!("[CONNECTION_LIFECYCLE] 🤝 Starting handshake with {}", peer_addr);
        Self::handshake(&writer, system_uid, local_port, local_host, system_name, target_host, target_port, target_system).await?;
        debug!("[CONNECTION_LIFECYCLE] HandshakeReq sent to {}", peer_addr);
        
        // IMPORTANT: HandshakeRsp will come on incoming connections that a remote actor establishes
        // back to us. The handshake is NOT complete until we receive and process HandshakeRsp.
        
        debug!("[CONNECTION_LIFECYCLE] 🔄 Connection established - waiting for HandshakeRsp on incoming connection");
        Ok(Self {
            writer: writer.clone(),
            _reader_handle: reader_handle,
            stream: Some(writer),
        })
    }
    
    async fn handshake(
        writer: &Arc<Mutex<tokio::io::WriteHalf<TcpStream>>>,
        system_uid: u64,
        local_port: u16,
        local_host: &str,
        system_name: &str,
        target_host: &str,
        target_port: u16,
        target_system: &str,
    ) -> Result<()> {
        debug!("Starting handshake with UID: {} on port {}", system_uid, local_port);
        
        // Step 1: Send magic bytes 'AKKA' followed by Control stream ID
        let mut writer_guard = writer.lock().await;
        writer_guard.write_all(b"AKKA").await?;
        writer_guard.write_u8(StreamId::Control as u8).await?; // Use Control stream for handshake
        writer_guard.flush().await?;
        
        debug!("TCP magic bytes and stream ID sent");
        
        // Step 2: Send HandshakeReq message as pure protobuf (control message format)
        let from_addr = ActorPath::new(
            system_name.to_string(),
            local_host.to_string(),
            local_port,
            "system".to_string(), // handshakes are handled by the /system actor path
        );
        let to_addr = ActorPath::new(
            target_system.to_string(),
            target_host.to_string(),
            target_port,
            "system".to_string(),
        );
        
        let from_unique = UniqueAddress {
            address: from_addr.clone(),
            uid: system_uid,
        };
        
        trace!("🤝 Creating HandshakeReq:");
        trace!("  From: {}", from_addr.to_string());
        trace!("  To: {}", to_addr.to_string()); 
        trace!("  From UID: {}", system_uid);
        trace!("  From unique address: {:?}", from_unique);
        
        // HandshakeReq should be sent with proper envelope format (version 0), but with
        // ArteryMessageSerializer (ID 17) and manifest "d", NOT as pure protobuf.
        let handshake_msg = InternalMessage::handshake_req(from_unique.clone(), to_addr.clone());
        trace!("  HandshakeReq message: {:?}", handshake_msg);
        
        let mut envelope = MessageEnvelope::new(from_addr, to_addr, handshake_msg)?;
        envelope.uid = system_uid;
        envelope.version = 0; // Ensure version is 0 to avoid "Incompatible protocol version" error
        trace!("  HandshakeReq envelope: {:?}", envelope);
        
        // Encode as proper envelope with version 0, serializer 17, manifest "d"
        let frame_data = envelope.encode()?;
        trace!("  Encoded HandshakeReq frame size: {} bytes", frame_data.len());
        trace!("  HandshakeReq frame hex (first 64 bytes): {}", 
               frame_data.iter().take(64).map(|b| format!("{:02x}", b)).collect::<Vec<_>>().join(" "));
        
        let frame_header = FrameHeader::new(frame_data.len() as u32);
        let header_bytes = frame_header.encode();
        trace!("  Frame header: {:?}", frame_header);
        trace!("  Frame header bytes: {}", 
               header_bytes.iter().map(|b| format!("{:02x}", b)).collect::<Vec<_>>().join(" "));
        
        writer_guard.write_all(&header_bytes).await?;
        writer_guard.write_all(&frame_data).await?;
        writer_guard.flush().await?;
        drop(writer_guard); // Release the lock so the reader can work
        
        debug!("HandshakeReq message sent with protobuf encoding");
        
        debug!("HandshakeReq sent - waiting for HandshakeRsp on incoming connection");
        
        Ok(())
    }
    
    async fn reader_loop_with_transport(mut reader: tokio::io::ReadHalf<TcpStream>, pending_responses: Arc<Mutex<HashMap<String, oneshot::Sender<ProtocolMessage>>>>, transport: ArteryTransport) {
        debug!("[CONNECTION_LIFECYCLE] 🟢 Outbound reader loop STARTED");
        trace!("[HANDSHAKE_TRACE] Outbound reader loop is ready to receive responses INCLUDING HandshakeRsp");
        let mut buffer = BytesMut::with_capacity(8192);
        
        // CRITICAL: HandshakeRsp comes back on THIS outbound connection, not on incoming connections
        // This connection handles both HandshakeRsp AND regular ask/tell responses
        
        loop {
            match reader.read_buf(&mut buffer).await {
                Ok(0) => {
                    debug!("[CONNECTION_LIFECYCLE] Outbound connection closed by remote (EOF)");
                    break;
                }
                Ok(n) => {
                    debug!("[CONNECTION_LIFECYCLE] Read {} bytes from outbound connection (buffer now {} bytes)", n, buffer.len());
                    
                    // Log first few bytes to debug what we're receiving
                    if n > 0 {
                        let debug_bytes: String = buffer.iter()
                            .take(32)
                            .map(|b| format!("{:02x}", b))
                            .collect::<Vec<String>>()
                            .join(" ");
                        trace!("[HANDSHAKE_TRACE] Raw bytes received on outbound: {}", debug_bytes);
                    }
                    
                    // Process any complete frames in the buffer
                    while let Some(frame_result) = Connection::try_parse_frame(&mut buffer) {
                        match frame_result {
                            Ok((header, data)) => {
                                debug!("Parsed outbound frame: size={}", header.size);
                                trace!("[HANDSHAKE_TRACE] Processing outbound frame - checking for HandshakeRsp");
                                Self::process_outbound_frame(data, pending_responses.clone(), &transport).await;
                            }
                            Err(e) => {
                                error!("Error parsing outbound frame: {}", e);
                                break;
                            }
                        }
                    }
                }
                Err(e) => {
                    error!("[CONNECTION_LIFECYCLE] Error reading from outbound connection: {}", e);
                    break;
                }
            }
        }
        
        debug!("[CONNECTION_LIFECYCLE] Outbound reader loop ENDED");
    }

    
    /// Process frames received on outbound connections:
    /// 1. HandshakeRsp (pure protobuf MessageWithAddress)
    /// 2. Regular message envelopes (including ask responses)
    async fn process_outbound_frame(data: Bytes, pending_responses: Arc<Mutex<HashMap<String, oneshot::Sender<ProtocolMessage>>>>, transport: &ArteryTransport) {
        trace!("[HANDSHAKE_TRACE] Processing outbound frame with {} bytes - checking for HandshakeRsp", data.len());
        
        // HandshakeRsp from Pekko is sent as pure protobuf MessageWithAddress, not wrapped in an envelope
        match crate::pekko_protobuf::MessageWithAddress::decode(data.as_ref()) {
            Ok(message_with_address) => {
                trace!("[HANDSHAKE_TRACE] Received HandshakeRsp as pure protobuf on outbound connection!");
                trace!("HandshakeRsp protobuf: address.system={}, address.hostname={}, address.port={}, uid={}", 
                       message_with_address.address.address.system, message_with_address.address.address.hostname, 
                       message_with_address.address.address.port, message_with_address.address.uid);
                
                // Complete the handshake
                let server_address = format!("{}:{}", 
                    message_with_address.address.address.hostname,
                    message_with_address.address.address.port);
                
                if let Err(e) = transport.complete_handshake(&server_address).await {
                    error!("Failed to complete handshake for {}: {}", server_address, e);
                } else {
                    trace!("[HANDSHAKE_TRACE] Handshake completed successfully for {}", server_address);
                }
                return;
            }
            Err(_) => {
                // Not a protobuf HandshakeRsp, try binary envelope format
                trace!("[HANDSHAKE_TRACE] Not pure protobuf HandshakeRsp, trying binary envelope format");
            }
        }
        
        // Try to decode the message envelope (for regular ask/tell responses)
        let mut data_buf = data.clone();
        match MessageEnvelope::decode(&mut data_buf) {
            Ok(envelope) => {
                trace!("Decoded outbound message from {} with serializer_id={} manifest={}", 
                       envelope.sender.to_string(), envelope.serializer_id, envelope.class_manifest);
                
                // Check if this is a HandshakeRsp in binary envelope format (fallback)
                if envelope.serializer_id == 17 && envelope.class_manifest == "e" {
                    trace!("[HANDSHAKE_TRACE] Received HandshakeRsp in binary envelope format on outbound connection!");
                    
                    // Extract server address from envelope sender
                    let server_address = format!("{}:{}", envelope.sender.host, envelope.sender.port);
                    
                    // Complete the handshake and process queued messages
                    if let Err(e) = transport.complete_handshake(&server_address).await {
                        error!("Failed to complete handshake for {}: {}", server_address, e);
                    } else {
                        trace!("[HANDSHAKE_TRACE] Handshake completed successfully for {}", server_address);
                    }
                } else {
                    // This is a regular response to an ask request
                    debug!("Received outbound response with serializer ID {}", envelope.serializer_id);
                    ArteryTransport::handle_incoming_message(envelope, pending_responses, None).await;
                }
            }
            Err(e) => {
                warn!("Failed to decode outbound message: {}", e);
                let hex_dump: String = data.iter()
                    .take(64)
                    .map(|b| format!("{:02x}", b))
                    .collect::<Vec<String>>()
                    .join(" ");
                debug!("Raw outbound frame data: {}", hex_dump);
            }
        }
    }
    
    fn try_parse_frame(buffer: &mut BytesMut) -> Option<Result<(FrameHeader, Bytes)>> {
        if buffer.len() < 4 {
            return None; // Need at least 4 bytes for frame header
        }
        
        // Parse frame header (little-endian)
        let size = u32::from_le_bytes([buffer[0], buffer[1], buffer[2], buffer[3]]);
        
        if buffer.len() < 4 + size as usize {
            return None; // Don't have complete frame yet
        }
        
        // Extract frame header and data
        let _header_bytes = buffer.split_to(4);
        let frame_data = buffer.split_to(size as usize);
        
        let header = FrameHeader {
            size,
        };
        
        Some(Ok((header, frame_data.freeze())))
    }
    
    
    async fn send_message(&self, envelope: MessageEnvelope) -> Result<()> {
        debug!("Sending message: {:?}", envelope);
        
        let envelope_data = envelope.encode()?;
        debug!("Encoded envelope data length: {}", envelope_data.len());
        
        // Log first 32 bytes of envelope for debugging
        let debug_bytes: String = envelope_data.iter()
            .take(32)
            .map(|b| format!("{:02x}", b))
            .collect::<Vec<String>>()
            .join(" ");
        debug!("First 32 bytes of envelope: {}", debug_bytes);
        
        // Specifically log the version byte (offset 0) to debug the protocol version issue
        if envelope_data.len() > 0 {
            debug!("Version byte at offset 0: {}", envelope_data[0]);
        }
        if envelope_data.len() > 12 {
            let serializer_id = u32::from_le_bytes([
                envelope_data[12], envelope_data[13], envelope_data[14], envelope_data[15]
            ]);
            debug!("Serializer ID at offset 12 (back to original format): {}", serializer_id);
        }
        
        let frame_header = FrameHeader::new(envelope_data.len() as u32);
        let header_data = frame_header.encode();
        
        debug!("Frame header: {} bytes, stream ID: {:?}", envelope_data.len(), StreamId::Ordinary);
        let frame_debug: String = header_data.iter()
            .map(|b| format!("{:02x}", b))
            .collect::<Vec<String>>()
            .join(" ");
        debug!("Frame header bytes: {}", frame_debug);
        
        let mut writer_guard = self.writer.lock().await;
        writer_guard.write_all(&header_data).await?;
        writer_guard.write_all(&envelope_data).await?;
        writer_guard.flush().await?;
        
        debug!("Message sent successfully");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::net::TcpListener;
    
    #[tokio::test]
    async fn test_connection_creation() {
        // Start a simple echo server for testing
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        
        tokio::spawn(async move {
            if let Ok((mut stream, _)) = listener.accept().await {
                let mut buf = [0u8; 1024];
                while let Ok(n) = stream.read(&mut buf).await {
                    if n == 0 { break; }
                    let _ = stream.write_all(&buf[..n]).await;
                }
            }
        });
        
        let transport = ArteryTransport::new(111111, 0, "127.0.0.1".to_string(), "ConnectionTest".to_string());
        assert!(transport.connect("127.0.0.1", "TestSystem", addr.port()).await.is_ok());
    }
    
    #[tokio::test]
    async fn test_graceful_shutdown() {
        let transport = ArteryTransport::new(222222, 0, "127.0.0.1".to_string(), "ShutdownTest".to_string());
        
        // Test initial state
        assert!(!transport.is_shutdown());
        
        // Test graceful shutdown
        transport.shutdown().await;
        assert!(transport.is_shutdown());
        
        // Test double shutdown (should be idempotent)
        transport.shutdown().await;
        assert!(transport.is_shutdown());
    }
    
    #[tokio::test]
    async fn test_shutdown_with_pending_responses() {
        let transport = ArteryTransport::new(333333, 0, "127.0.0.1".to_string(), "PendingTest".to_string());
        
        // Add a mock pending response
        {
            let mut pending = transport.pending_responses.lock().await;
            let (tx, _rx) = oneshot::channel();
            pending.insert("test_key".to_string(), tx);
        }
        
        // Verify we have pending responses
        {
            let pending = transport.pending_responses.lock().await;
            assert_eq!(pending.len(), 1);
        }
        
        // Shutdown should complete even with pending responses (after timeout)
        let start = std::time::Instant::now();
        transport.shutdown().await;
        let elapsed = start.elapsed();
        
        // Should complete within total timeout (10 seconds) but faster due to flush timeout (1 second)
        assert!(elapsed < Duration::from_secs(3)); // Should be much faster than total timeout
        assert!(transport.is_shutdown());
        
        // Pending responses should be cleared
        {
            let pending = transport.pending_responses.lock().await;
            assert_eq!(pending.len(), 0);
        }
    }
    
    #[tokio::test]
    async fn test_robust_shutdown_timeouts() {
        let mut transport = ArteryTransport::new(444444, 0, "127.0.0.1".to_string(), "TimeoutTest".to_string());
        // Set very short timeouts for testing
        transport.shutdown_timeouts = ShutdownTimeouts {
            flush_timeout: Duration::from_millis(100),
            connection_timeout: Duration::from_millis(200),
            total_timeout: Duration::from_millis(500),
        };
        
        // Add multiple pending responses that won't be resolved
        {
            let mut pending = transport.pending_responses.lock().await;
            for i in 0..5 {
                let (tx, _rx) = oneshot::channel();
                pending.insert(format!("test_key_{}", i), tx);
            }
        }
        
        let start = std::time::Instant::now();
        transport.shutdown().await;
        let elapsed = start.elapsed();
        
        // Should complete within total timeout, not hang indefinitely
        assert!(elapsed < Duration::from_secs(1));
        assert!(transport.is_shutdown());
        
        // All state should be cleared even with timeout
        {
            let pending = transport.pending_responses.lock().await;
            assert_eq!(pending.len(), 0);
        }
    }
    
    #[tokio::test]
    async fn test_double_shutdown_idempotency() {
        let transport = ArteryTransport::new(555555, 0, "127.0.0.1".to_string(), "IdempotencyTest".to_string());
        
        // First shutdown
        let start1 = std::time::Instant::now();
        transport.shutdown().await;
        let elapsed1 = start1.elapsed();
        assert!(transport.is_shutdown());
        
        // Second shutdown should return immediately
        let start2 = std::time::Instant::now();
        transport.shutdown().await;
        let elapsed2 = start2.elapsed();
        
        // Second shutdown should be much faster (just returns early)
        assert!(elapsed2 < Duration::from_millis(10));
        assert!(elapsed2 < elapsed1); // Should be significantly faster
        assert!(transport.is_shutdown());
    }
    
    #[tokio::test]
    async fn test_force_cleanup_mechanism() {
        let transport = ArteryTransport::new(666666, 0, "127.0.0.1".to_string(), "ForceCleanupTest".to_string());
        
        // Add some state that would normally be cleaned up
        {
            let mut pending = transport.pending_responses.lock().await;
            let (tx, _rx) = oneshot::channel();
            pending.insert("test_key".to_string(), tx);
        }
        
        {
            let mut handshakes = transport.handshake_states.lock().await;
            handshakes.insert("test_addr".to_string(), HandshakeState {
                completed: false,
                queued_messages: Vec::new(),
            });
        }
        
        // Call force cleanup directly
        transport.force_cleanup().await;
        
        // Verify all state is cleared
        {
            let pending = transport.pending_responses.lock().await;
            assert_eq!(pending.len(), 0);
        }
        
        {
            let handshakes = transport.handshake_states.lock().await;
            assert_eq!(handshakes.len(), 0);
        }
    }
    
    #[tokio::test]
    async fn test_termination_message_creation() {
        // Test that we can create Pekko-compatible termination messages
        let local_address = UniqueAddress {
            address: ActorPath::new(
                "TestSystem".to_string(),
                "127.0.0.1".to_string(),
                25552,
                "system".to_string(),
            ),
            uid: 12345,
        };
        
        // Test ActorSystemTerminating message
        let terminating_msg = InternalMessage::actor_system_terminating(local_address.clone());
        assert_eq!(terminating_msg.serializer_id, 17);
        
        // Test ActorSystemTerminatingAck message  
        let ack_msg = InternalMessage::actor_system_terminating_ack(local_address);
        assert_eq!(ack_msg.serializer_id, 17);
        
        // Test that messages can be encoded without errors
        let terminating_encoded = terminating_msg.encode();
        assert!(terminating_encoded.is_ok());
        
        let ack_encoded = ack_msg.encode();
        assert!(ack_encoded.is_ok());
        
        // Verify encoded data is not empty
        assert!(!terminating_encoded.unwrap().is_empty());
        assert!(!ack_encoded.unwrap().is_empty());
    }
    
    #[tokio::test]
    async fn test_termination_hints_no_connections() {
        let transport = ArteryTransport::new(777777, 0, "127.0.0.1".to_string(), "TerminationTest".to_string());
        
        // Test that sending termination hints with no connections doesn't error
        transport.send_termination_hints().await;
        
        // Should complete without errors even with no connections
        assert!(true);
    }
}
