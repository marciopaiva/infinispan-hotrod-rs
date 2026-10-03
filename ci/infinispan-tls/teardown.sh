#!/usr/bin/env bash
# Stops the server started by setup.sh. Leaves generated/ in place so a
# repeated run does not need to regenerate the CA and keystore; pass
# --clean to remove it too.

set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")"

docker rm -f infinispan-tls-test >/dev/null 2>&1 || true

if [ "${1:-}" = "--clean" ]; then
  rm -rf generated
fi
