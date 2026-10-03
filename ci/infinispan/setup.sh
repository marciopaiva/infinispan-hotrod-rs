#!/usr/bin/env bash
# Starts a single-node, plain-TCP Infinispan server for the general
# live-server tests in hotrod-protocol/tests/live_server.rs: everything
# except the cluster_* and tls_* ones, which need their own fixtures
# (ci/infinispan-cluster/, ci/infinispan-tls/). Runs as the live-test job
# in ci.yml on every push to main and every PR, and reused by
# release.yml for the same tests at release time; run it by hand the
# same way for local iteration, then run teardown.sh.
#
# Usage:
#   ci/infinispan/setup.sh
#   cargo test --test live_server -- --ignored --skip cluster_ --skip tls_
#   ci/infinispan/teardown.sh

set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")"

docker rm -f infinispan-live-test >/dev/null 2>&1 || true
docker run -d --name infinispan-live-test \
  -p 11222:11222 \
  -e USER=unused \
  -e PASS=unused-but-required \
  -v "$PWD/infinispan.xml:/opt/infinispan/server/conf/infinispan.xml:ro" \
  -v "$PWD/users.properties:/opt/infinispan/server/conf/users.properties:ro" \
  -v "$PWD/groups.properties:/opt/infinispan/server/conf/groups.properties:ro" \
  infinispan/server:15.1

for i in $(seq 1 30); do
  if docker logs infinispan-live-test 2>&1 | grep -q "ISPN080001"; then
    echo "Infinispan is up on 127.0.0.1:11222"
    exit 0
  fi
  sleep 2
done

echo "Infinispan did not come up in time"
docker logs infinispan-live-test
exit 1
