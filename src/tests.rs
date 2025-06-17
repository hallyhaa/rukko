//! Comprehensive test suite for Rukko
//! 
//! This module contains extensive tests to ensure the reliability of
//! the Pekko Artery protocol implementation.

use crate::actor::ActorSystem;
use crate::error::RukkoError;
use crate::protocol::{ActorPath, FrameHeader, Message, MessageEnvelope, StreamId, UniqueAddress, InternalMessage};
use crate::transport::ArteryTransport;
use std::time::Duration;
use tokio::time::timeout;

/// Test module for protocol edge cases and error handling
#[cfg(test)]
mod protocol_edge_cases {
    use super::*;
    
    #[test]
    fn test_invalid_actor_paths() {
        // Missing protocol
        assert!(ActorPath::from_string("invalid_path".to_string()).is_err());
        
        // Missing @
        assert!(ActorPath::from_string("pekko://system/user/actor".to_string()).is_err());
        
        // Missing port
        assert!(ActorPath::from_string("pekko://system@host/user/actor".to_string()).is_err());
        
        // Invalid port
        assert!(ActorPath::from_string("pekko://system@host:invalid/user/actor".to_string()).is_err());
        
        // Missing path
        assert!(ActorPath::from_string("pekko://system@host:1234".to_string()).is_err());
    }

    #[test]
    fn test_pekko_protocol_support() {
        let path_str = "pekko://TestSystem@127.0.0.1:25552/user/testActor";
        let path = ActorPath::from_string(path_str.to_string()).unwrap();
        
        assert_eq!(path.system, "TestSystem");
        assert_eq!(path.host, "127.0.0.1");
        assert_eq!(path.port, 25552);
        assert_eq!(path.path, "user/testActor");
    }

    #[test]
    fn test_message_types() {
        // Text message
        let text_msg = Message::text("Hello");
        // Note: serializer_id is now internal to InternalMessage
        if let Some(content) = text_msg.as_text() {
            assert_eq!(content, "Hello");
        } else {
            panic!("Expected text message");
        }
    }

    #[test]
    fn test_handshake_messages() {
        let from_addr = ActorPath::new(
            "TestSystem".to_string(),
            "127.0.0.1".to_string(),
            12345,
            "system".to_string(),
        );
        let to_addr = ActorPath::new(
            "TargetSystem".to_string(),
            "127.0.0.1".to_string(),
            25552,
            "system".to_string(),
        );
        let unique_addr = UniqueAddress {
            address: from_addr.clone(),
            uid: 123456789,
        };

        // Note: handshake messages are now internal and not exposed to users
        let handshake_req = InternalMessage::handshake_req(unique_addr.clone(), to_addr.clone());
        assert_eq!(handshake_req.serializer_id, 17);

        let handshake_rsp = InternalMessage::handshake_rsp(unique_addr);
        assert_eq!(handshake_rsp.serializer_id, 17);
    }

    #[test]
    fn test_stream_id_conversion() {
        assert_eq!(StreamId::from_u8(0x01).unwrap(), StreamId::Control);
        assert_eq!(StreamId::from_u8(0x02).unwrap(), StreamId::Ordinary);
        assert_eq!(StreamId::from_u8(0x03).unwrap(), StreamId::Large);
        
        assert!(StreamId::from_u8(0x04).is_err());
        assert!(StreamId::from_u8(0x00).is_err());
    }

    #[test]
    fn test_frame_header_encoding_decoding() {
        let header = FrameHeader::new(12345);
        let encoded = header.encode();
        
        // Should be 4 bytes, little endian
        assert_eq!(encoded.len(), 4);
        assert_eq!(encoded, &12345u32.to_le_bytes()[..]);
        
        let mut buf = encoded;
        let decoded = FrameHeader::decode(&mut buf).unwrap();
        assert_eq!(decoded.size, 12345);
    }

    #[test]
    fn test_message_envelope_creation() {
        let sender = ActorPath::new(
            "SenderSystem".to_string(),
            "127.0.0.1".to_string(),
            12345,
            "user/sender".to_string(),
        );
        let recipient = ActorPath::new(
            "RecipientSystem".to_string(),
            "127.0.0.1".to_string(),
            25552,
            "user/recipient".to_string(),
        );
        let message = Message::text("Test message");
        
        let internal_message = InternalMessage::from_user_message(&message);
        let envelope = MessageEnvelope::new(sender.clone(), recipient.clone(), internal_message).unwrap();
        
        assert_eq!(envelope.version, 0);
        assert_eq!(envelope.flags, 0);
        assert_eq!(envelope.serializer_id, 20);
        assert_eq!(envelope.sender, sender);
        assert_eq!(envelope.recipient, recipient);
        assert_eq!(envelope.class_manifest, "");
    }

    #[test]
    fn test_message_envelope_encoding() {
        let sender = ActorPath::new(
            "SenderSystem".to_string(),
            "127.0.0.1".to_string(),
            12345,
            "user/sender".to_string(),
        );
        let recipient = ActorPath::new(
            "RecipientSystem".to_string(),
            "127.0.0.1".to_string(),
            25552,
            "user/recipient".to_string(),
        );
        let message = Message::text("Test");
        
        let internal_message = InternalMessage::from_user_message(&message);
        let mut envelope = MessageEnvelope::new(sender, recipient, internal_message).unwrap();
        envelope.uid = 987654321;
        
        let encoded = envelope.encode().unwrap();
        
        // Should have at least the fixed header (28 bytes) plus literals
        assert!(encoded.len() > 28);
        
        // Check version at offset 0
        assert_eq!(encoded[0], 0);
        
        // Check UID at offset 4-11 (little endian)
        let uid_bytes = &encoded[4..12];
        let decoded_uid = u64::from_le_bytes([
            uid_bytes[0], uid_bytes[1], uid_bytes[2], uid_bytes[3],
            uid_bytes[4], uid_bytes[5], uid_bytes[6], uid_bytes[7],
        ]);
        assert_eq!(decoded_uid, 987654321);
    }

    #[test]
    fn test_message_envelope_decode_roundtrip() {
        let sender = ActorPath::new(
            "SenderSystem".to_string(),
            "127.0.0.1".to_string(),
            12345,
            "user/sender".to_string(),
        );
        let recipient = ActorPath::new(
            "RecipientSystem".to_string(),
            "127.0.0.1".to_string(),
            25552,
            "user/recipient".to_string(),
        );
        let message = Message::text("Test message");
        
        let internal_message = InternalMessage::from_user_message(&message);
        let mut original_envelope = MessageEnvelope::new(sender.clone(), recipient.clone(), internal_message).unwrap();
        original_envelope.uid = 123456789;
        
        let encoded = original_envelope.encode().unwrap();
        let mut buf = encoded;
        let decoded_envelope = MessageEnvelope::decode(&mut buf).unwrap();
        
        assert_eq!(decoded_envelope.version, original_envelope.version);
        assert_eq!(decoded_envelope.uid, original_envelope.uid);
        assert_eq!(decoded_envelope.serializer_id, original_envelope.serializer_id);
        assert_eq!(decoded_envelope.sender, original_envelope.sender);
        assert_eq!(decoded_envelope.recipient, original_envelope.recipient);
        assert_eq!(decoded_envelope.class_manifest, original_envelope.class_manifest);
    }
}

/// Test module for actor system functionality
#[cfg(test)]
mod actor_system_tests {
    use super::*;

    #[tokio::test]
    async fn test_actor_system_creation_with_unique_ports() {
        let system1 = ActorSystem::new("TestSystem1").await.unwrap();
        let system2 = ActorSystem::new("TestSystem2").await.unwrap();
        
        // Should have different UIDs
        assert_ne!(system1.uid(), system2.uid());
        
        // Should have different ports (OS-allocated)
        assert_ne!(system1.bound_port(), system2.bound_port());
        
        // Should have unique addresses
        assert_ne!(system1.uid(), system2.uid());
        assert_ne!(system1.path().port, system2.path().port);
        
        system1.shutdown().await;
        system2.shutdown().await;
    }

    #[tokio::test]
    async fn test_actor_selection_parsing() {
        let system = ActorSystem::new("TestSystem").await.unwrap();
        
        // Test various address formats
        let valid_addresses = vec![
            "pekko://RemoteSystem@127.0.0.1:25552/user/testActor",
            "pekko://RemoteSystem@127.0.0.1:25552/user/testActor",
            "pekko://System@localhost:8080/user/service/worker",
        ];
        
        for address in valid_addresses {
            // This will fail to connect but should parse correctly
            match system.actor_selection(address).await {
                Ok(selection) => {
                    assert!(!selection.path().system.is_empty());
                    assert!(!selection.path().host.is_empty());
                    assert!(selection.path().port > 0);
                }
                Err(RukkoError::Connection(_)) => {
                    // Expected - no server running
                }
                Err(e) => panic!("Unexpected error for {}: {}", address, e),
            }
        }
        
        system.shutdown().await;
    }

    #[tokio::test]
    async fn test_direct_system_methods() -> Result<(), Box<dyn std::error::Error>> {
        let system = ActorSystem::new("TestSystem").await.unwrap();
        
        let test_address = "pekko://NonExistent@127.0.0.1:65533/user/test";
        let message = Message::text("Test message");
        
        // Should fail to connect but not panic
        let selection = system.actor_selection(test_address).await?;
        
        // tell() is fire-and-forget, connection issues happen at transport level
        // We just verify it doesn't panic
        selection.tell(message);
        
        system.shutdown().await;
        Ok(())
    }

}

/// Test module for transport layer functionality
#[cfg(test)]
mod transport_tests {
    use super::*;
    use tokio::net::TcpListener;

    #[tokio::test]
    async fn test_transport_creation() {
        let _transport1 = ArteryTransport::new(123456, 8080, "127.0.0.1".to_string(), "TestSystem1".to_string());
        let _transport2 = ArteryTransport::new(789012, 9090, "192.168.1.100".to_string(), "TestSystem2".to_string());
        
        // Basic construction should work without panicking
        // Sequence counter is private, so we can't test it directly
    }

    #[tokio::test]
    async fn test_connection_to_nonexistent_server() -> Result<(), Box<dyn std::error::Error>> {
        let transport = ArteryTransport::new(456789, 25552, "127.0.0.1".to_string(), "ConnTestSystem".to_string());
        
        // Should fail to connect to non-existent server
        let result = transport.connect("127.0.0.1", "TestSystem", 65535).await;
        assert!(result.is_err());
        
        match result {
            Err(RukkoError::Connection(_)) => {
                // Expected
            }
            Err(e) => panic!("Unexpected error type: {}", e),
            Ok(_) => panic!("Should not succeed connecting to non-existent server"),
        }
        Ok(())
    }

    #[test]
    fn test_transport_basic_functionality() {
        let _transport = ArteryTransport::new(111222, 2552, "127.0.0.1".to_string(), "BasicTestSystem".to_string());
        // Basic test that transport can be created without errors
        // Sequence counter functionality is tested indirectly through integration tests
    }

    #[tokio::test]
    async fn test_tcp_server_startup() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        
        let transport = std::sync::Arc::new(ArteryTransport::new(123456, port, "127.0.0.1".to_string(), "TestTcpServer".to_string()));
        
        // Start server in background
        let transport_clone = transport.clone();
        let server_handle = tokio::spawn(async move {
            // Run server for a short time, then exit
            let server_future = ArteryTransport::run_server(listener, transport_clone);
            timeout(Duration::from_millis(100), server_future).await
        });
        
        // Give it a moment to start
        tokio::time::sleep(Duration::from_millis(10)).await;
        
        // Try to connect (will fail handshake but should accept connection)
        let connect_result = tokio::net::TcpStream::connect(format!("127.0.0.1:{}", port)).await;
        assert!(connect_result.is_ok());
        
        // Clean up
        server_handle.abort();
    }
}

/// Test module for error handling
#[cfg(test)]
mod error_tests {
    use super::*;

    #[test]
    fn test_error_types() {
        let io_error = std::io::Error::new(std::io::ErrorKind::ConnectionRefused, "test");
        let pekko_error = RukkoError::Connection(io_error);
        
        assert!(matches!(pekko_error, RukkoError::Connection(_)));
        assert!(pekko_error.to_string().contains("Connection error"));
        
        let protocol_error = RukkoError::Protocol("Invalid format".to_string());
        assert!(protocol_error.to_string().contains("Protocol error"));
        
        let timeout_error = RukkoError::Timeout;
        assert!(timeout_error.to_string().contains("Timeout"));
    }

    #[test]
    fn test_error_conversion() {
        let io_error = std::io::Error::new(std::io::ErrorKind::TimedOut, "timeout");
        let pekko_error: RukkoError = io_error.into();
        
        assert!(matches!(pekko_error, RukkoError::Connection(_)));
    }
}

/// Test module for serialization edge cases
#[cfg(test)]
mod serialization_tests {
    use super::*;

    #[test]
    fn test_text_edge_cases() {
        // Empty text
        let msg = Message::text("");
        let encoded = InternalMessage::encode_user_message(&msg).unwrap();
        assert_eq!(encoded.as_ref(), b"");
        
        // Text with special characters
        let special_chars = "Hello\nWorld\t\"";
        let msg = Message::text(special_chars);
        let encoded = InternalMessage::encode_user_message(&msg).unwrap();
        assert_eq!(encoded.as_ref(), special_chars.as_bytes());
        
        // Large text
        let large_text = "x".repeat(10000);
        let msg = Message::text(&large_text);
        let encoded = InternalMessage::encode_user_message(&msg).unwrap();
        assert_eq!(encoded.as_ref(), large_text.as_bytes());
    }

}

/// Integration tests with timeouts
#[cfg(test)]
mod integration_tests {
    use super::*;

    #[tokio::test]
    async fn test_ask_timeout_behavior() -> Result<(), Box<dyn std::error::Error>> {
        let system = ActorSystem::new("TestSystem").await.unwrap();
        
        // Try to ask a non-existent actor - should timeout
        let non_existent = "pekko://NonExistent@127.0.0.1:65534/user/test";
        let message = Message::text("Hello");
        
        let start_time = std::time::Instant::now();
        let selection = system.actor_selection(non_existent).await?;
        let result = timeout(Duration::from_millis(200), selection.ask(message)).await;
        let elapsed = start_time.elapsed();
        
        // Should timeout quickly (connection failure) or within our timeout
        assert!(elapsed < Duration::from_millis(300));
        
        match result {
            Err(_) => {
                // Timeout from our wrapper - acceptable
            }
            Ok(Err(RukkoError::Connection(_))) => {
                // Connection error - also acceptable
            }
            Ok(Err(RukkoError::Timeout)) => {
                // Pekko timeout - also acceptable
            }
            Ok(Ok(_)) => panic!("Should not succeed"),
            Ok(Err(e)) => panic!("Unexpected error: {}", e),
        }
        
        system.shutdown().await;
        Ok(())
    }

    #[tokio::test]
    async fn test_multiple_concurrent_connections() {
        let system = ActorSystem::new("TestSystem").await.unwrap();
        
        // Try multiple concurrent connections
        let mut handles = Vec::new();
        
        for i in 0..5 {
            let system_clone = system.clone();
            let handle = tokio::spawn(async move {
                let address = format!("pekko://Test{}@127.0.0.1:6553{}/user/test", i, i);
                let message = Message::text(&format!("Message {}", i));
                
                // This will fail but should handle multiple concurrent attempts gracefully
                if let Ok(selection) = system_clone.actor_selection(&address).await {
                    let _result = timeout(Duration::from_millis(50), async { selection.tell(message); Ok::<(), Box<dyn std::error::Error>>(()) }).await;
                }
            });
            handles.push(handle);
        }
        
        // Wait for all to complete
        for handle in handles {
            handle.await.unwrap();
        }
        
        system.shutdown().await;
    }
}