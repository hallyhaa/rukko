# JVM test node

A minimal Apache Pekko 1.7.x actor system used to verify that Rukko speaks the
Artery TCP protocol correctly. Nothing in the Rust crate depends on it at build time.

Requirements: Java 21 and Maven.

Start it (default port 25552, system name `PekkoNode`):

    ./jvm-test-node/run.sh          # or: ./jvm-test-node/run.sh 25553

Actors under `/user`:

| Actor     | Behaviour                                                          |
|-----------|--------------------------------------------------------------------|
| `echo`    | replies with the same `String`                                     |
| `json`    | replies with a JSON document containing the text and sender path   |
| `silent`  | never replies (timeout tests)                                      |
| `slow`    | replies after 2 seconds                                            |
| `bytes`   | replies with a `byte[]` (serializer 4, which Rukko does not accept) |
| `counter` | replies with the number of messages received so far               |

Then run the ignored integration tests from the repository root:

    cargo test --test jvm_integration -- --ignored --test-threads=1

Set `RUKKO_JVM_NODE_PORT` if the node runs on a different port.
