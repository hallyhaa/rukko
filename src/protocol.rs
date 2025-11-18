use bytes::{Buf, BufMut, Bytes, BytesMut};
use tracing::debug;
use crate::error::{RukkoError, Result};

/// Pekko protocol constant
pub const PEKKO: &str = "pekko";

/// Stream IDs for different message types in Artery protocol
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum StreamId {
    Control = 0x01,
    Ordinary = 0x02,
    Large = 0x03,
}

impl StreamId {
    pub fn from_u8(value: u8) -> Result<Self> {
        match value {
            0x01 => Ok(StreamId::Control),
            0x02 => Ok(StreamId::Ordinary),
            0x03 => Ok(StreamId::Large),
            _ => Err(RukkoError::InvalidMessage(format!("Invalid stream ID: {}", value))),
        }
    }
}

/// Artery protocol frame header
#[derive(Debug, Clone)]
pub(crate) struct FrameHeader {
    pub size: u32,
}

impl FrameHeader {
    pub fn new(size: u32) -> Self {
        Self { size }
    }
    
    pub fn encode(&self) -> Bytes {
        let mut buf = BytesMut::with_capacity(4);
        buf.put_u32_le(self.size); // Little endian for Pekko Artery
        buf.freeze()
    }
    
    pub fn decode(buf: &mut Bytes) -> Result<Self> {
        if buf.len() < 4 {
            return Err(RukkoError::Protocol("Insufficient data for frame header".to_string()));
        }
        
        let size = buf.get_u32_le(); // Little endian for Pekko Artery
        Ok(FrameHeader { size })
    }
}

/// Message envelope for Artery protocol - should match Pekko's EnvelopeBuffer format
#[derive(Debug, Clone)]
pub(crate) struct MessageEnvelope {
    pub version: u8,                     // Offset 0 (1 byte)
    pub flags: u8,                       // Offset 1 (1 byte)
    pub actor_ref_compression_version: u8, // Offset 2 (1 byte)
    pub class_manifest_compression_version: u8, // Offset 3 (1 byte)
    pub uid: u64,                        // Offset 4 (8 bytes)
    pub serializer_id: u32,              // Offset 12 (4 bytes)
    pub sender: ActorPath,               // Offset 16 (4 byte tag)
    pub recipient: ActorPath,            // Offset 20 (4 byte tag)
    pub class_manifest: String,          // Offset 24 (4 byte tag)
    pub message_payload: Bytes,          // After offset 28
}

impl MessageEnvelope {
    pub fn new(sender: ActorPath, recipient: ActorPath, message: InternalMessage) -> Result<Self> {
        let message_payload = message.encode()?;
        
        Ok(Self {
            version: 0,
            flags: 0,    // No flags set
            actor_ref_compression_version: 0,
            class_manifest_compression_version: 0,
            uid: 0,      // Will be set by the transport
            serializer_id: message.serializer_id,
            sender,
            recipient,
            class_manifest: match &message.payload {
                InternalPayload::User(_) => "".to_string(),
                InternalPayload::Control(control_msg) => match control_msg {
                    ControlMessage::HandshakeReq(_) => "d".to_string(),
                    ControlMessage::HandshakeRsp(_) => "e".to_string(),
                    ControlMessage::ActorSystemTerminating(_) => "b".to_string(),
                    ControlMessage::ActorSystemTerminatingAck(_) => "c".to_string(),
                }
            },
            message_payload,
        })
    }
    
    pub fn encode(&self) -> Result<Bytes> {
        // For now, always use binary format - protobuf approach needs more work
        self.encode_binary()
    }
    
    
    /// Encode using binary envelope format
    fn encode_binary(&self) -> Result<Bytes> {
        // Create a fixed-size header buffer (28 bytes as per original Pekko format)
        let mut buf = BytesMut::with_capacity(28 + 1024); // Header + space for literals
        
        // Fixed header fields at exact offsets matching Pekko's EnvelopeBuffer format
        buf.resize(28, 0); // Ensure we have space for the header
        
        // Offset 0: Version (1 byte)
        buf[0] = self.version;
        
        // Offset 1: Flags (1 byte)
        buf[1] = self.flags;
        
        // Offset 2: Actor ref compression table version (1 byte)
        buf[2] = self.actor_ref_compression_version;
        
        // Offset 3: Class manifest compression table version (1 byte)
        buf[3] = self.class_manifest_compression_version;
        
        // Offset 4: UID (8 bytes, little endian)
        let uid_bytes = self.uid.to_le_bytes();
        buf[4..12].copy_from_slice(&uid_bytes);
        
        // Offset 12: Serializer ID (4 bytes, little endian)
        let serializer_bytes = self.serializer_id.to_le_bytes();
        buf[12..16].copy_from_slice(&serializer_bytes);
        
        // Offset 16: Sender actor ref tag (4 bytes) - 0 means literal follows
        let sender_tag = 0u32.to_le_bytes();
        buf[16..20].copy_from_slice(&sender_tag);
        
        // Offset 20: Recipient actor ref tag (4 bytes) - 0 means literal follows  
        let recipient_tag = 0u32.to_le_bytes();
        buf[20..24].copy_from_slice(&recipient_tag);
        
        // Offset 24: Class manifest tag (4 bytes) - 0 means literal follows
        let manifest_tag = 0u32.to_le_bytes();
        buf[24..28].copy_from_slice(&manifest_tag);
        
        // After offset 28: Literals section
        // Add sender actor ref literal (format: length as u16 + string bytes)
        let sender_path = self.sender.to_string();
        buf.extend_from_slice(&(sender_path.len() as u16).to_le_bytes());
        buf.extend_from_slice(sender_path.as_bytes());
        
        // Add recipient actor ref literal (format: length as u16 + string bytes)
        let recipient_path = self.recipient.to_string();
        buf.extend_from_slice(&(recipient_path.len() as u16).to_le_bytes());
        buf.extend_from_slice(recipient_path.as_bytes());
        
        // Add class manifest literal (format: length as u16 + string bytes)
        buf.extend_from_slice(&(self.class_manifest.len() as u16).to_le_bytes());
        buf.extend_from_slice(self.class_manifest.as_bytes());
        
        // Add message payload
        buf.extend_from_slice(&self.message_payload);
        
        Ok(buf.freeze())
    }
    
    /// Decode a message envelope from binary EnvelopeBuffer format
    pub fn decode(buf: &mut Bytes) -> Result<Self> {
        Self::decode_binary(buf)
    }
    
    /// Decode a message envelope from binary EnvelopeBuffer format  
    fn decode_binary(buf: &mut Bytes) -> Result<Self> {
        if buf.len() < 28 {
            return Err(RukkoError::Protocol("Insufficient data for envelope header".to_string()));
        }

        if tracing::enabled!(tracing::Level::DEBUG) {
            // Debug: Log the binary format we're trying to decode
            debug!(
                hex_bytes = %buf.iter()
                  .take(32) // The 32 first bytes will have to do
                  .map(|b| format!("{:02x}", b))
                  .collect::<Vec<String>>()
                  .join(" "),
              "Attempting to decode binary envelope."
            );
        }
        
        // Parse fixed header (28 bytes)
        let version = buf[0];
        let flags = buf[1];
        let actor_ref_compression_version = buf[2];
        let class_manifest_compression_version = buf[3];
        
        let uid = u64::from_le_bytes([
            buf[4], buf[5], buf[6], buf[7], buf[8], buf[9], buf[10], buf[11]
        ]);
        
        let serializer_id = u32::from_le_bytes([buf[12], buf[13], buf[14], buf[15]]);

        debug!(
            version = version,
            flags = flags,
            uid = uid,
            serializer_id = serializer_id,
            "Binary decoding header."
        );

        // Skip the tags (sender, recipient, manifest) - we'll read the literals
        let mut pos = 28;
        
        // Read sender actor ref literal
        if pos + 2 > buf.len() {
            return Err(RukkoError::Protocol("Insufficient data for sender length".to_string()));
        }
        let sender_len = u16::from_le_bytes([buf[pos], buf[pos + 1]]) as usize;
        pos += 2;
        
        if pos + sender_len > buf.len() {
            return Err(RukkoError::Protocol("Insufficient data for sender path".to_string()));
        }
        let sender_path = String::from_utf8_lossy(&buf[pos..pos + sender_len]).to_string();
        pos += sender_len;
        
        // Read recipient actor ref literal
        if pos + 2 > buf.len() {
            return Err(RukkoError::Protocol("Insufficient data for recipient length".to_string()));
        }
        let recipient_len = u16::from_le_bytes([buf[pos], buf[pos + 1]]) as usize;
        pos += 2;
        
        if pos + recipient_len > buf.len() {
            return Err(RukkoError::Protocol("Insufficient data for recipient path".to_string()));
        }
        let recipient_path = String::from_utf8_lossy(&buf[pos..pos + recipient_len]).to_string();
        pos += recipient_len;
        
        // Read class manifest literal
        if pos + 2 > buf.len() {
            return Err(RukkoError::Protocol("Insufficient data for manifest length".to_string()));
        }
        let manifest_len = u16::from_le_bytes([buf[pos], buf[pos + 1]]) as usize;
        pos += 2;
        
        if pos + manifest_len > buf.len() {
            return Err(RukkoError::Protocol("Insufficient data for manifest".to_string()));
        }
        let class_manifest = String::from_utf8_lossy(&buf[pos..pos + manifest_len]).to_string();
        pos += manifest_len;
        
        // Read message payload (everything after the literals)
        let message_payload = buf.slice(pos..);
        
        // Parse actor paths
        let sender = ActorPath::from_string(sender_path)?;
        let recipient = ActorPath::from_string(recipient_path)?;
        
        Ok(MessageEnvelope {
            version,
            flags,
            actor_ref_compression_version,
            class_manifest_compression_version,
            uid,
            serializer_id,
            sender,
            recipient,
            class_manifest,
            message_payload,
        })
    }
}

/// Actor path representation
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActorPath {
    pub system: String,
    pub host: String,
    pub port: u16,
    pub path: String,
    pub protocol: &'static str,
}

impl ActorPath {
    pub fn new(system: String, host: String, port: u16, path: String) -> Self {
        Self {
            system,
            host,
            port,
            path,
            protocol: PEKKO,
        }
    }
    
    pub fn from_string(s: String) -> Result<Self> {
        // Parse pekko://system@host:port/path only
        let (protocol, without_protocol) = if s.starts_with("pekko://") {
            (PEKKO, &s[8..]) // Remove "pekko://"
        } else {
            return Err(RukkoError::InvalidActorPath(format!("Invalid protocol, expected pekko:// : {}", s)));
        };
        let parts: Vec<&str> = without_protocol.splitn(2, '/').collect();

        if parts.len() != 2 {
            return Err(RukkoError::InvalidActorPath(format!("Missing path component: {}", s)));
        }

        let path = parts[1].to_string();
        if path.is_empty() {
            return Err(RukkoError::InvalidActorPath(format!("Empty path component: {}", s)));
        }

        let system_and_address = parts[0];
        let address_parts: Vec<&str> = system_and_address.splitn(2, '@').collect();
        if address_parts.len() != 2 {
            return Err(RukkoError::InvalidActorPath(format!("Missing address component: {}", s)));
        }

        let system = address_parts[0].to_string();
        if system.is_empty() {
            return Err(RukkoError::InvalidActorPath(format!("Empty system name: {}", s)));
        }

        let host_and_port = address_parts[1];
        if host_and_port.is_empty() {
            return Err(RukkoError::InvalidActorPath(format!("Host and port must be specified: {}", s)));
        }

        let host_port_parts: Vec<&str> = host_and_port.splitn(2, ':').collect();
        if host_port_parts.len() != 2 {
            return Err(RukkoError::InvalidActorPath(format!("Port number not specified: {}", s)));
        }

        let host = host_port_parts[0].to_string();
        if host.is_empty() {
            return Err(RukkoError::InvalidActorPath(format!("Empty host name: {}", s)));
        }

        let port = host_port_parts[1].parse::<u16>()
            .map_err(|_| RukkoError::InvalidActorPath(format!("Invalid port: {}", host_port_parts[1])))?;

        Ok(ActorPath {
            system,
            host,
            port,
            path,
            protocol,
        })
    }
}

impl std::fmt::Display for ActorPath {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}://{}@{}:{}/{}", self.protocol, self.system, self.host, self.port, self.path)
    }
}

/// Control messages for protocol-level communication (handshakes, termination, etc.)
#[derive(Debug, Clone)]
pub(crate) enum ControlMessage {
    HandshakeReq(crate::pekko_protobuf::HandshakeReq),
    HandshakeRsp(crate::pekko_protobuf::MessageWithAddress),
    ActorSystemTerminating(UniqueAddress),
    ActorSystemTerminatingAck(UniqueAddress),
}

/// Unique address combining network address with system UID
#[derive(Debug, Clone)]
pub(crate) struct UniqueAddress {
    pub address: ActorPath,
    pub uid: u64,
}

/// Message that users send between actors.
/// Rukko only supports string contents.
#[derive(Debug, Clone)]
pub struct Message {
    content: String,
}

/// Internal message wrapper that includes either user content or control messages
#[derive(Debug, Clone)]
pub(crate) struct InternalMessage {
    pub(crate) payload: InternalPayload,
    pub(crate) serializer_id: u32,
}

/// Internal payload - either user content or protocol control message
#[derive(Debug, Clone)]
pub(crate) enum InternalPayload {
    User(String),
    Control(ControlMessage),
}

impl Message {
    /// Create a new text message
    pub fn text(content: impl Into<String>) -> Self {
        Self {
            content: content.into(),
        }
    }
    
    /// Get the text content of this message
    pub fn content(&self) -> &str {
        &self.content
    }
    
    /// Extract text content (for compatibility)
    pub fn as_text(&self) -> Option<&str> {
        Some(&self.content)
    }
}

impl std::fmt::Display for Message {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", self.content)
    }
}

impl InternalMessage {
    /// Create an internal message from user content
    pub(crate) fn from_user_message(message: &Message) -> Self {
        Self {
            payload: InternalPayload::User(message.content.clone()),
            serializer_id: 20, // Pekko primitive-string serializer ID
        }
    }
    
    /// Convenience method for tests - encode a user message directly
    #[cfg(test)]
    pub(crate) fn encode_user_message(message: &Message) -> Result<Bytes> {
        Self::from_user_message(message).encode()
    }
    
    pub(crate) fn handshake_req(from: UniqueAddress, to: ActorPath) -> Self {
        let protobuf_req = crate::pekko_protobuf::HandshakeReq {
            from: crate::pekko_protobuf::UniqueAddress::from(from),
            to: crate::pekko_protobuf::Address::from(to),
        };
        
        Self {
            payload: InternalPayload::Control(ControlMessage::HandshakeReq(protobuf_req)),
            serializer_id: 17, // Pekko ArteryMessageSerializer ID
        }
    }
    
    pub(crate) fn handshake_rsp(from: UniqueAddress) -> Self {
        let protobuf_rsp = crate::pekko_protobuf::MessageWithAddress {
            address: crate::pekko_protobuf::UniqueAddress::from(from),
        };
        
        Self {
            payload: InternalPayload::Control(ControlMessage::HandshakeRsp(protobuf_rsp)),
            serializer_id: 17, // Pekko ArteryMessageSerializer ID
        }
    }
    
    /// Create an ActorSystemTerminating control message (shutdown signal)
    pub(crate) fn actor_system_terminating(from: UniqueAddress) -> Self {
        Self {
            payload: InternalPayload::Control(ControlMessage::ActorSystemTerminating(from)),
            serializer_id: 17, // Pekko ArteryMessageSerializer ID for control messages
        }
    }
    
    /// Create an ActorSystemTerminatingAck control message (response to termination signal)
    pub(crate) fn actor_system_terminating_ack(from: UniqueAddress) -> Self {
        Self {
            payload: InternalPayload::Control(ControlMessage::ActorSystemTerminatingAck(from)),
            serializer_id: 17, // Pekko ArteryMessageSerializer ID for control messages
        }
    }

    pub fn encode(&self) -> Result<Bytes> {
        // For Artery, we just encode the raw message payload
        // The manifest and serializer info is handled at the envelope level
        let mut buf = BytesMut::new();
        
        // Encode based on payload type
        match &self.payload {
            InternalPayload::User(content) => {
                buf.extend_from_slice(content.as_bytes());
            }
            InternalPayload::Control(control_msg) => {
                match control_msg {
                    ControlMessage::HandshakeReq(req) => {
                        // Encode HandshakeReq directly using protobuf
                        use prost::Message;
                        let protobuf_bytes = req.encode_to_vec();
                        buf.extend_from_slice(&protobuf_bytes);
                    }
                    ControlMessage::HandshakeRsp(rsp) => {
                        // Encode HandshakeRsp directly using protobuf (MessageWithAddress)
                        use prost::Message;
                        let protobuf_bytes = rsp.encode_to_vec();
                        buf.extend_from_slice(&protobuf_bytes);
                    }
                    ControlMessage::ActorSystemTerminating(from) => {
                        // Encode ActorSystemTerminating as protobuf MessageWithAddress
                        use prost::Message;
                        let protobuf_msg = crate::pekko_protobuf::MessageWithAddress {
                            address: crate::pekko_protobuf::UniqueAddress::from(from.clone()),
                        };
                        let protobuf_bytes = protobuf_msg.encode_to_vec();
                        buf.extend_from_slice(&protobuf_bytes);
                    }
                    ControlMessage::ActorSystemTerminatingAck(from) => {
                        // Encode ActorSystemTerminatingAck as protobuf MessageWithAddress
                        use prost::Message;
                        let protobuf_msg = crate::pekko_protobuf::MessageWithAddress {
                            address: crate::pekko_protobuf::UniqueAddress::from(from.clone()),
                        };
                        let protobuf_bytes = protobuf_msg.encode_to_vec();
                        buf.extend_from_slice(&protobuf_bytes);
                    }
                }
            }
        }
        
        Ok(buf.freeze())
    }
    
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_actor_path_parsing() {
        let path_str = "pekko://TestSystem@127.0.0.1:25552/user/testActor";
        let path = ActorPath::from_string(path_str.to_string()).unwrap();
        
        assert_eq!(path.system, "TestSystem");
        assert_eq!(path.host, "127.0.0.1");
        assert_eq!(path.port, 25552);
        assert_eq!(path.path, "user/testActor");
        assert_eq!(path.to_string(), path_str);
    }

    #[test]
    fn test_actor_path_parsing_empty_path() {
        // Test that empty path (trailing slash) is rejected
        let path_str = "pekko://TestSystem@127.0.0.1:25552/";
        let result = ActorPath::from_string(path_str.to_string());

        assert!(result.is_err());
        if let Err(e) = result {
            assert!(e.to_string().contains("Empty path component"));
        }
    }

    #[test]
    fn test_actor_path_parsing_no_port_number_1() {
        // Test that empty path (trailing slash) is rejected
        let path_str = "pekko://TestSystem@127.0.0.1/user/some_path";
        let result = ActorPath::from_string(path_str.to_string());

        assert!(result.is_err());
        if let Err(e) = result {
            assert!(e.to_string().contains("Port number not specified"));
        }
    }

    #[test]
    fn test_actor_path_parsing_no_port_number_2() {
        // Test that empty path (trailing slash) is rejected
        let path_str = "pekko://TestSystem@127.0.0.1:/user/some_path";
        let result = ActorPath::from_string(path_str.to_string());

        assert!(result.is_err());
        if let Err(e) = result {
            assert!(e.to_string().contains("Invalid port"));
        }
    }

    #[test]
    fn test_actor_path_parsing_no_port_number_3() {
        // Test that empty path (trailing slash) is rejected
        let path_str = "pekko://TestSystem@127.0.0.1:fifty-eight/user/some_path";
        let result = ActorPath::from_string(path_str.to_string());

        assert!(result.is_err());
        if let Err(e) = result {
            assert!(e.to_string().contains("Invalid port"));
        }
    }

    #[test]
    fn test_message_encoding() {
        let original = Message::text("Hello, World!");
        let encoded = InternalMessage::encode_user_message(&original).unwrap();
        
        // For Artery protocol, Message::encode just returns the raw payload
        assert_eq!(encoded.as_ref(), b"Hello, World!");
        
        let json_content = serde_json::to_string(&serde_json::json!({"test": "value"})).unwrap();
        let json_msg = Message::text(json_content);
        let json_encoded = InternalMessage::encode_user_message(&json_msg).unwrap();
        assert_eq!(json_encoded.as_ref(), b"{\"test\":\"value\"}");
    }
    
    #[test]
    fn test_message_as_text() {
        let text_msg = Message::text("Hello, World!");
        assert_eq!(text_msg.as_text(), Some("Hello, World!"));
        
        // Test that we can extract text content from messages
        let json_content = serde_json::to_string(&serde_json::json!({"test": "value"})).unwrap();
        let json_msg = Message::text(json_content.clone());
        assert_eq!(json_msg.as_text(), Some(json_content.as_str()));
    }
    
    #[test]
    fn test_frame_header_encoding() {
        let header = FrameHeader::new(1024);
        let encoded = header.encode();
        let mut buf = encoded;
        let decoded = FrameHeader::decode(&mut buf).unwrap();
        
        assert_eq!(header.size, decoded.size);
    }
    
    #[test]
    fn test_envelope_encoding_decoding() {
        let sender = ActorPath::from_string("pekko://TestSystem@127.0.0.1:25551/user/sender".to_string()).unwrap();
        let recipient = ActorPath::from_string("pekko://TestSystem@127.0.0.1:25552/user/recipient".to_string()).unwrap();
        let message = Message::text("Test message");
        
        // Test basic envelope encoding/decoding
        let internal_message = InternalMessage::from_user_message(&message);
        let envelope = MessageEnvelope::new(sender.clone(), recipient.clone(), internal_message).unwrap();
        let encoded = envelope.encode().unwrap();
        let mut buf = encoded;
        let decoded = MessageEnvelope::decode(&mut buf).unwrap();
        
        assert_eq!(decoded.sender, sender);
        assert_eq!(decoded.recipient, recipient);
        assert_eq!(decoded.serializer_id, 20); // Text message serializer ID
        assert_eq!(decoded.class_manifest, ""); // Text messages have empty manifest
    }
}
