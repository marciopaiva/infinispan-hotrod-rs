#!/usr/bin/env bash
# Stops the server started by setup.sh.

set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")"

docker rm -f infinispan-live-test >/dev/null 2>&1 || true
