Rukko
=============
[![crates.io](https://img.shields.io/crates/v/rukko)](https://crates.io/crates/rukko)
[![docs](https://img.shields.io/docsrs/rukko)](https://docs.rs/rukko)

A rust library for communicating with [Apache Pekko](https://pekko.apache.org) JVM actors. Allows rust applications to
send messages to and receive replies from Pekko actors.

https://docs.rs/rukko/latest/rukko/

## Quick Start

Add Rukko to your `Cargo.toml`:

```toml
[dependencies]
rukko = "0.1.0"
tokio = "1.47.0"
```

### Basic Example

```rust
use rukko::{ActorSystem, Message};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    
    // Create an actor system
    let system = ActorSystem::new("RustClient").await?;
    
    // Connect to a remote Pekko actor
    let remote_actor = system
        .actor_selection("pekko://MySystem@127.0.0.1:25552/user/myActor")
        .await?;

    // Send a fire-and-forget message (tell pattern)
    remote_actor.tell(Message::text("One-way message"));

    // Send a message and wait for its response (ask pattern)
    let response = remote_actor
        .ask(Message::text("Hello from Rust!"))
        .await?;
    
    println!("Response: {:?}", response);
    
    system.shutdown().await;
    Ok(())
}
```

## Configuration

## Timeouts

If your actor path is wrong or something else makes an _ask_ hang until timeout, you will be waiting for 30 seconds for
your error. If you can't wait half a minute, set your own timeout like this:

```rust
use tokio::time::Duration;                                                                                                                                                                                                                                                 
...

// Default timeout (30 seconds)
let response = actor.ask(message).await?;

// Custom timeout
let response = actor
    .ask_with_timeout(important_question, Duration::from_secs(1))
    .await?;
```

## License

This project is licensed under the [MIT License](LICENSE).

## Contributing

Contributions are welcome! Please feel free to submit issues and PRs.

