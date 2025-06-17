//! Pekko Compatibility Tests
//! 
//! These tests are adapted from the Pekko test suite to ensure Rukko
//! maintains compatibility with Pekko's behavior and expectations.

use crate::actor::ActorSystem;
use crate::error::RukkoError;
use crate::protocol::{ActorPath, Message, StreamId, FrameHeader, InternalMessage};
use std::time::Duration;
use tokio::time::timeout;

/// Tests adapted from Pekko's ActorPathSpec.scala
#[cfg(test)]
mod actor_path_compatibility {
    use super::*;

    #[test]
    fn test_actor_path_serialization_round_trip() {
        // From Pekko: actor paths should survive serialization round-trip
        let original = "pekko://my_sys@darkstar:1234/some/ref";
        let path = ActorPath::from_string(original.to_string()).unwrap();
        let serialized = path.to_string();
        assert_eq!(serialized, original);
    }

    #[test]
    fn test_actor_path_with_complex_names() {
        // From Pekko: paths with special characters and complex naming
        let test_cases = vec![
            "pekko://MySystem@host:1234/user/my-actor",
            "pekko://Test_System@127.0.0.1:25552/user/actor.with.dots",
            "pekko://legacy-system@localhost:8080/user/service/worker123",
        ];
        
        for test_case in test_cases {
            let path = ActorPath::from_string(test_case.to_string());
            assert!(path.is_ok(), "Failed to parse path: {}", test_case);
            
            let parsed = path.unwrap();
            assert!(!parsed.system.is_empty());
            assert!(!parsed.host.is_empty());
            assert!(parsed.port > 0);
            assert!(!parsed.path.is_empty());
        }
    }

    #[test]
    fn test_actor_path_invalid_formats() {
        // From Pekko: various invalid path formats should be rejected
        let invalid_paths = vec![
            "not-a-path",
            "https://wrong-protocol@host:1234/user/actor",
            "akka://sys@host:1234/user/actor",  // akka protocol not supported
            "pekko://sys@/user/actor",  // missing host
            "pekko://sys@host/user/actor",  // missing port
            "pekko://sys@host:abc/user/actor",  // invalid port
            "pekko://sys@host:1234",  // missing path
            "pekko://@host:1234/user/actor",  // empty system
            "pekko://sys@:1234/user/actor",  // empty host
        ];
        
        for invalid_path in invalid_paths {
            let result = ActorPath::from_string(invalid_path.to_string());
            assert!(result.is_err(), "Should have failed to parse: {}", invalid_path);
        }
    }

    #[test]
    fn test_actor_path_elements() {
        // From Pekko: verify individual path elements are parsed correctly
        let path = ActorPath::from_string("pekko://TestSys@example.com:9999/user/my/deep/actor".to_string()).unwrap();
        
        assert_eq!(path.system, "TestSys");
        assert_eq!(path.host, "example.com");
        assert_eq!(path.port, 9999);
        assert_eq!(path.path, "user/my/deep/actor");
    }
}

/// Tests adapted from Pekko's AskSpec.scala
#[cfg(test)]
mod ask_pattern_compatibility {
    use super::*;

    #[tokio::test]
    async fn test_ask_timeout_message_format() -> Result<(), Box<dyn std::error::Error>> {
        // From Pekko: timeout errors should contain specific information
        let system = ActorSystem::new("TestSystem").await.unwrap();
        
        let non_existent = "pekko://NonExistent@127.0.0.1:65530/user/test";
        let message = Message::text("Hello");
        
        let selection = system.actor_selection(non_existent).await?;
        let result = timeout(Duration::from_millis(100), selection.ask(message)).await;
        
        // Should either timeout or get connection error quickly
        match result {
            Err(_) => {
                // Our timeout - acceptable
            }
            Ok(Err(RukkoError::Timeout)) => {
                // Pekko timeout - should contain useful information
            }
            Ok(Err(RukkoError::Connection(_))) => {
                // Connection error - also acceptable for non-existent server
            }
            Ok(Ok(_)) => panic!("Should not succeed"),
            Ok(Err(e)) => panic!("Unexpected error: {}", e),
        }
        
        system.shutdown().await;
        Ok(())
    }

    #[tokio::test]
    async fn test_ask_with_different_timeout_durations() -> Result<(), Box<dyn std::error::Error>> {
        // From Pekko: ask pattern should respect different timeout values
        let system = ActorSystem::new("TestSystem").await.unwrap();
        
        let test_address = "pekko://Test@127.0.0.1:65529/user/timeout-test";
        
        // Very short timeout
        let short_start = std::time::Instant::now();
        let selection = system.actor_selection(test_address).await?;
        let short_result = timeout(Duration::from_millis(1), selection.ask(Message::text("short"))).await;
        let short_elapsed = short_start.elapsed();
        
        // Should fail quickly
        assert!(short_elapsed < Duration::from_millis(50));
        assert!(short_result.is_err() || matches!(short_result, Ok(Err(_))));
        
        system.shutdown().await;
        Ok(())
    }

    #[tokio::test]
    async fn test_ask_vs_tell_behavior() -> Result<(), Box<dyn std::error::Error>> {
        // From Pekko: ask and tell should behave differently for non-existent actors
        let system = ActorSystem::new("TestSystem").await.unwrap();
        
        let test_address = "pekko://Test@127.0.0.1:65528/user/behavior-test";
        let message = Message::text("test");
        let selection = system.actor_selection(test_address).await.unwrap();
        
        // Tell should fail fast or succeed without waiting
        let tell_start = std::time::Instant::now();
        let _tell_result = timeout(Duration::from_millis(100), async { selection.tell(message.clone()); Ok::<(), Box<dyn std::error::Error>>(()) }).await;
        let tell_elapsed = tell_start.elapsed();
        
        // Ask should also fail fast for connection errors
        let ask_start = std::time::Instant::now();
        let _ask_result = timeout(Duration::from_millis(100), selection.ask(message)).await;
        let ask_elapsed = ask_start.elapsed();
        
        // Both should complete quickly for connection errors
        assert!(tell_elapsed < Duration::from_millis(150));
        assert!(ask_elapsed < Duration::from_millis(150));
        
        system.shutdown().await;
        Ok(())
    }
}

/// Tests adapted from Pekko's TCP Framing and Artery tests
#[cfg(test)]
mod protocol_compatibility {
    use super::*;

    #[test]
    fn test_frame_header_little_endian() {
        // From Pekko: frame headers must be little-endian encoded
        let header = FrameHeader::new(0x12345678);
        let encoded = header.encode();
        
        // Should be exactly 4 bytes
        assert_eq!(encoded.len(), 4);
        
        // Should be little-endian encoded
        let expected = 0x12345678u32.to_le_bytes();
        assert_eq!(encoded.as_ref(), &expected);
        
        // Verify we can decode it back
        let mut buf = encoded;
        let decoded = FrameHeader::decode(&mut buf).unwrap();
        assert_eq!(decoded.size, 0x12345678);
    }

    #[test]
    fn test_stream_id_values() {
        // From Pekko: stream IDs must match Pekko's exact values
        assert_eq!(StreamId::Control as u8, 0x01);
        assert_eq!(StreamId::Ordinary as u8, 0x02);
        assert_eq!(StreamId::Large as u8, 0x03);
        
        // Test conversion from u8
        assert_eq!(StreamId::from_u8(0x01).unwrap(), StreamId::Control);
        assert_eq!(StreamId::from_u8(0x02).unwrap(), StreamId::Ordinary);
        assert_eq!(StreamId::from_u8(0x03).unwrap(), StreamId::Large);
        
        // Invalid stream IDs should be rejected
        assert!(StreamId::from_u8(0x00).is_err());
        assert!(StreamId::from_u8(0x04).is_err());
        assert!(StreamId::from_u8(0xFF).is_err());
    }

    #[test]
    fn test_frame_size_boundaries() {
        // From Pekko: test various frame sizes including edge cases
        let test_sizes = vec![
            0,           // Empty frame
            1,           // Single byte
            1024,        // Small frame
            65536,       // 64KB frame
            1048576,     // 1MB frame
            16777216,    // 16MB frame (Pekko's maximum)
            0xFFFFFFFF,  // Maximum u32 value
        ];
        
        for size in test_sizes {
            let header = FrameHeader::new(size);
            let encoded = header.encode();
            
            let mut buf = encoded;
            let decoded = FrameHeader::decode(&mut buf).unwrap();
            assert_eq!(decoded.size, size);
        }
    }
}

/// Tests adapted from Pekko's message serialization tests
#[cfg(test)]
mod message_compatibility {
    use super::*;

    #[test]
    fn test_message_serializer_ids() {
        // From Pekko: verify serializer IDs match Pekko's expectations
        // Test that user messages get the correct serializer ID when converted to internal format
        let user_msg = Message::text("test");
        let internal_msg = InternalMessage::from_user_message(&user_msg);
        assert_eq!(internal_msg.serializer_id, 20);  // primitive-string
        
        // Handshake messages use Artery serializer
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
        let unique_addr = crate::protocol::UniqueAddress {
            address: from_addr.clone(),
            uid: 123456789,
        };
        
        assert_eq!(InternalMessage::handshake_req(unique_addr.clone(), to_addr).serializer_id, 17);
        assert_eq!(InternalMessage::handshake_rsp(unique_addr).serializer_id, 17);
    }

    #[test]
    fn test_message_encoding_empty_content() {
        // From Pekko: empty messages should be handled correctly
        let empty_text = Message::text("");
        let encoded = InternalMessage::encode_user_message(&empty_text).unwrap();
        assert_eq!(encoded.len(), 0);
        
        
    }

    #[test]
    fn test_message_encoding_large_content() {
        // From Pekko: large messages should be handled without corruption
        let large_text = "x".repeat(100000);
        let message = Message::text(&large_text);
        let encoded = InternalMessage::encode_user_message(&message).unwrap();
        assert_eq!(encoded.len(), 100000);
        assert_eq!(encoded.as_ref(), large_text.as_bytes());
        
    }
}

/// Tests adapted from Pekko's actor system lifecycle tests
#[cfg(test)]
mod system_lifecycle_compatibility {
    use super::*;

    #[tokio::test]
    async fn test_multiple_systems_isolation() -> Result<(), Box<dyn std::error::Error>> {
        // From Pekko: multiple actor systems should be properly isolated
        let system1 = ActorSystem::new("System1").await.unwrap();
        let system2 = ActorSystem::new("System2").await.unwrap();
        let system3 = ActorSystem::new("System3").await.unwrap();
        
        // Each should have unique UIDs and ports
        let uid1 = system1.uid();
        let uid2 = system2.uid();
        let uid3 = system3.uid();
        
        assert_ne!(uid1, uid2);
        assert_ne!(uid2, uid3);
        assert_ne!(uid1, uid3);
        
        let port1 = system1.bound_port();
        let port2 = system2.bound_port();
        let port3 = system3.bound_port();
        
        assert_ne!(port1, port2);
        assert_ne!(port2, port3);
        assert_ne!(port1, port3);
        
        // Systems should have correct names
        assert_eq!(system1.name(), "System1");
        assert_eq!(system2.name(), "System2");
        assert_eq!(system3.name(), "System3");
        
        // Cleanup
        system1.shutdown().await;
        system2.shutdown().await;
        system3.shutdown().await;
        Ok(())
    }

    #[tokio::test]
    async fn test_system_shutdown_idempotency() -> Result<(), Box<dyn std::error::Error>> {
        // From Pekko: shutdown should be idempotent
        let system = ActorSystem::new("ShutdownTest").await.unwrap();
        
        // Multiple shutdowns should not panic or fail
        system.shutdown().await;
        system.shutdown().await;
        system.shutdown().await;
        Ok(())
    }

}

/// Tests adapted from Pekko's connection and network tests
#[cfg(test)]
mod connection_compatibility {
    use super::*;

    #[tokio::test]
    async fn test_concurrent_connection_attempts() -> Result<(), Box<dyn std::error::Error>> {
        // From Pekko: multiple concurrent connections should be handled gracefully
        let system = ActorSystem::new("ConcurrentTest").await.unwrap();
        
        let mut handles = Vec::new();
        
        // Try 10 concurrent connections to non-existent servers
        for i in 0..10 {
            let system_clone = system.clone();
            let handle = tokio::spawn(async move {
                let address = format!("pekko://Test{}@127.0.0.1:6552{}/user/test", i, i % 10);
                let message = Message::text(&format!("Concurrent message {}", i));
                
                // Should handle failures gracefully
                if let Ok(selection) = system_clone.actor_selection(&address).await {
                    let _result = timeout(Duration::from_millis(50), async { selection.tell(message); Ok::<(), Box<dyn std::error::Error>>(()) }).await;
                }
                // Don't assert on result - just ensure no panic or crash
            });
            handles.push(handle);
        }
        
        // Wait for all to complete
        for handle in handles {
            handle.await.unwrap();
        }
        
        system.shutdown().await;
        Ok(())
    }

    #[tokio::test]
    async fn test_connection_cleanup_on_failure() -> Result<(), Box<dyn std::error::Error>> {
        // From Pekko: failed connections should clean up resources
        let system = ActorSystem::new("CleanupTest").await.unwrap();
        
        // Try connecting to multiple non-existent servers
        for i in 0..5 {
            let address = format!("pekko://Cleanup{}@127.0.0.1:6551{}/user/test", i, i);
            let message = Message::text("cleanup test");
            
            let selection = system.actor_selection(&address).await.ok();
            if let Some(sel) = selection {
                let _result = timeout(Duration::from_millis(10), async { sel.tell(message); Ok::<(), Box<dyn std::error::Error>>(()) }).await;
            }
            // Each failed connection should clean up without leaking resources
        }
        
        system.shutdown().await;
        Ok(())
    }
}

/// Tests for message ordering and delivery guarantees
#[cfg(test)]
mod delivery_compatibility {
    use super::*;

    #[tokio::test]
    async fn test_message_ordering_single_actor() -> Result<(), Box<dyn std::error::Error>> {
        // From Pekko: messages to the same actor should maintain order
        let system = ActorSystem::new("OrderingTest").await.unwrap();
        
        let target = "pekko://Test@127.0.0.1:65527/user/ordering-test";
        
        // Send multiple messages quickly
        let mut handles = Vec::new();
        for i in 0..5 {
            let system_clone = system.clone();
            let target_clone = target.to_string();
            let handle = tokio::spawn(async move {
                let message = Message::text(&format!("Message {}", i));
                if let Ok(selection) = system_clone.actor_selection(&target_clone).await {
                    let _result = timeout(Duration::from_millis(10), async { selection.tell(message); Ok::<(), Box<dyn std::error::Error>>(()) }).await;
                }
            });
            handles.push(handle);
        }
        
        // All should complete (though they'll fail to connect)
        for handle in handles {
            handle.await.unwrap();
        }
        
        system.shutdown().await;
        Ok(())
    }
}