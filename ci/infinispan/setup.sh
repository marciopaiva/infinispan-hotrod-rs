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
source ../lib.sh

rm_containers infinispan-live-test
docker run -d --name infinispan-live-test \
  -p 11222:11222 \
  -e USER=unused \
  -e PASS=unused-but-required \
  -v "$PWD/infinispan.xml:/opt/infinispan/server/conf/infinispan.xml:ro" \
  -v "$PWD/users.properties:/opt/infinispan/server/conf/users.properties:ro" \
  -v "$PWD/groups.properties:/opt/infinispan/server/conf/groups.properties:ro" \
  infinispan/server:15.1

wait_for_infinispan infinispan-live-test
echo "Infinispan is up on 127.0.0.1:11222"
