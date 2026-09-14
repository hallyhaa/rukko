//! Integration tests against a real JVM Pekko node.
//!
//! Start the node first: `./jvm-test-node/run.sh`, then run
//! `cargo test --test jvm_integration -- --ignored --test-threads=1`.

use rukko::{ActorSystem, Message, RukkoError};
use std::time::Duration;

fn node_port() -> u16 {
    std::env::var("RUKKO_JVM_NODE_PORT")
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(25552)
}

fn actor(name: &str) -> String {
    format!("pekko://PekkoNode@127.0.0.1:{}/user/{}", node_port(), name)
}

#[tokio::test]
#[ignore = "requires a running JVM test node (see jvm-test-node/README.md)"]
async fn ask_echo_returns_same_text() {
    let system = ActorSystem::new("RukkoIT").await.unwrap();
    let echo = system.actor_selection(actor("echo")).await.unwrap();

    let reply = echo.ask(Message::text("hello from rust")).await.unwrap();
    assert_eq!(reply.content(), "hello from rust");

    system.shutdown().await;
}

#[tokio::test]
#[ignore = "requires a running JVM test node (see jvm-test-node/README.md)"]
async fn ask_echo_round_trips_unicode_and_json() {
    let system = ActorSystem::new("RukkoIT").await.unwrap();
    let echo = system.actor_selection(actor("echo")).await.unwrap();

    let text = "æøå ünïcödé 🦀 {\"k\":[1,2,3],\"s\":\"v\"}";
    let reply = echo.ask(Message::text(text)).await.unwrap();
    assert_eq!(reply.content(), text);

    system.shutdown().await;
}

#[tokio::test]
#[ignore = "requires a running JVM test node (see jvm-test-node/README.md)"]
async fn ask_json_actor_reports_temp_sender_path() {
    let system = ActorSystem::new("RukkoIT").await.unwrap();
    let json = system.actor_selection(actor("json")).await.unwrap();

    let reply = json.ask(Message::text("ping")).await.unwrap();
    let value: serde_json::Value = serde_json::from_str(reply.content()).unwrap();
    assert_eq!(value["received"], "ping");
    assert_eq!(value["length"], 4);
    let sender = value["sender"].as_str().unwrap();
    assert!(
        sender.starts_with(&format!("pekko://RukkoIT@127.0.0.1:{}/temp/", system.bound_port())),
        "unexpected sender path seen by the JVM: {sender}"
    );

    system.shutdown().await;
}

#[tokio::test]
#[ignore = "requires a running JVM test node (see jvm-test-node/README.md)"]
async fn many_concurrent_asks_are_correlated_correctly() {
    let system = ActorSystem::new("RukkoIT").await.unwrap();
    let echo = system.actor_selection(actor("echo")).await.unwrap();

    let mut handles = Vec::new();
    for i in 0..50 {
        let echo = echo.clone();
        handles.push(tokio::spawn(async move {
            let text = format!("message-{i}");
            let reply = echo.ask(Message::text(&text)).await.unwrap();
            assert_eq!(reply.content(), text);
        }));
    }
    for h in handles {
        h.await.unwrap();
    }

    system.shutdown().await;
}

#[tokio::test]
#[ignore = "requires a running JVM test node (see jvm-test-node/README.md)"]
async fn sequential_asks_on_one_selection() {
    let system = ActorSystem::new("RukkoIT").await.unwrap();
    let counter = system.actor_selection(actor("counter")).await.unwrap();

    let first: u64 = counter.ask(Message::text("a")).await.unwrap().content().parse().unwrap();
    let second: u64 = counter.ask(Message::text("b")).await.unwrap().content().parse().unwrap();
    assert_eq!(second, first + 1);

    system.shutdown().await;
}

#[tokio::test]
#[ignore = "requires a running JVM test node (see jvm-test-node/README.md)"]
async fn tell_is_delivered() {
    let system = ActorSystem::new("RukkoIT").await.unwrap();
    let counter = system.actor_selection(actor("counter")).await.unwrap();

    let before: u64 = counter.ask(Message::text("probe")).await.unwrap().content().parse().unwrap();
    counter.tell(Message::text("fire-and-forget"));
    // Give the tell time to be flushed before the next ask
    tokio::time::sleep(Duration::from_millis(200)).await;
    let after: u64 = counter.ask(Message::text("probe")).await.unwrap().content().parse().unwrap();
    assert_eq!(after, before + 2, "the tell should have been counted");

    system.shutdown().await;
}

#[tokio::test]
#[ignore = "requires a running JVM test node (see jvm-test-node/README.md)"]
async fn ask_silent_actor_times_out() {
    let system = ActorSystem::new("RukkoIT").await.unwrap();
    let silent = system.actor_selection(actor("silent")).await.unwrap();

    let result = silent
        .ask_with_timeout(Message::text("anyone?"), Duration::from_millis(500))
        .await;
    assert!(matches!(result, Err(RukkoError::Timeout)), "got {result:?}");

    system.shutdown().await;
}

#[tokio::test]
#[ignore = "requires a running JVM test node (see jvm-test-node/README.md)"]
async fn ask_slow_actor_succeeds_with_long_enough_timeout() {
    let system = ActorSystem::new("RukkoIT").await.unwrap();
    let slow = system.actor_selection(actor("slow")).await.unwrap();

    let reply = slow
        .ask_with_timeout(Message::text("patience"), Duration::from_secs(5))
        .await
        .unwrap();
    assert_eq!(reply.content(), "patience");

    system.shutdown().await;
}

#[tokio::test]
#[ignore = "requires a running JVM test node (see jvm-test-node/README.md)"]
async fn ask_actor_replying_with_bytes_times_out_gracefully() {
    // Rukko only understands String replies (serializer 20). A byte[] reply (serializer 4)
    // must be ignored without panicking, so the ask ends in a timeout.
    let system = ActorSystem::new("RukkoIT").await.unwrap();
    let bytes = system.actor_selection(actor("bytes")).await.unwrap();

    let result = bytes
        .ask_with_timeout(Message::text("raw"), Duration::from_millis(500))
        .await;
    assert!(matches!(result, Err(RukkoError::Timeout)), "got {result:?}");

    // The connection must still be usable afterwards
    let echo = system.actor_selection(actor("echo")).await.unwrap();
    let reply = echo.ask(Message::text("still alive")).await.unwrap();
    assert_eq!(reply.content(), "still alive");

    system.shutdown().await;
}

#[tokio::test]
#[ignore = "requires a running JVM test node (see jvm-test-node/README.md)"]
async fn ask_nonexistent_actor_times_out() {
    let system = ActorSystem::new("RukkoIT").await.unwrap();
    let nobody = system.actor_selection(actor("does-not-exist")).await.unwrap();

    let result = nobody
        .ask_with_timeout(Message::text("hello?"), Duration::from_millis(500))
        .await;
    assert!(matches!(result, Err(RukkoError::Timeout)), "got {result:?}");

    system.shutdown().await;
}

#[tokio::test]
#[ignore = "requires a running JVM test node (see jvm-test-node/README.md)"]
async fn two_systems_can_talk_to_the_same_node() {
    let a = ActorSystem::new("RukkoA").await.unwrap();
    let b = ActorSystem::new("RukkoB").await.unwrap();
    let echo_a = a.actor_selection(actor("echo")).await.unwrap();
    let echo_b = b.actor_selection(actor("echo")).await.unwrap();

    let (ra, rb) = tokio::join!(echo_a.ask(Message::text("from A")), echo_b.ask(Message::text("from B")));
    assert_eq!(ra.unwrap().content(), "from A");
    assert_eq!(rb.unwrap().content(), "from B");

    a.shutdown().await;
    b.shutdown().await;
}

#[tokio::test]
#[ignore = "requires a running JVM test node (see jvm-test-node/README.md)"]
async fn large_message_round_trips() {
    let system = ActorSystem::new("RukkoIT").await.unwrap();
    let echo = system.actor_selection(actor("echo")).await.unwrap();

    // 200 KiB is below Pekko's default maximum-frame-size of 256 KiB
    let text: String = "x".repeat(200 * 1024);
    let reply = echo.ask(Message::text(text.clone())).await.unwrap();
    assert_eq!(reply.content().len(), text.len());
    assert_eq!(reply.content(), text);

    system.shutdown().await;
}
