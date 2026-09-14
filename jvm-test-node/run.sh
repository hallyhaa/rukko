#!/usr/bin/env bash
# Starts the JVM Pekko test node. Optional first argument: port (default 25552).
# The node prints "RUKKO_TEST_NODE_READY pekko://PekkoNode@127.0.0.1:<port>" when it accepts connections.
set -euo pipefail
cd "$(dirname "$0")"
exec mvn -q clean compile exec:java -Dexec.args="${1:-25552}"
