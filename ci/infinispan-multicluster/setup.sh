#!/usr/bin/env bash
# Starts two genuinely independent, single-node Infinispan servers for
# the multicluster_* tests in hotrod-protocol/tests/live_server.rs
# (issue #56): no shared Docker network, no JGroups discovery between
# them, unlike ci/infinispan-cluster/'s two nodes of one logical
# cluster. A multi-cluster failover test needs a second deployment
# that does not already share the first one's data through
# clustering, or a test could pass by accident (both nodes of a real
# cluster would show the same distributed cache content regardless of
# whether failover actually switched anything).
#
# Usage:
#   ci/infinispan-multicluster/setup.sh
#   cargo test --test live_server multicluster_ -- --ignored
#   ci/infinispan-multicluster/teardown.sh

set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")"
source ../lib.sh

rm_containers infinispan-multicluster-primary infinispan-multicluster-dr

start_node() {
  local name=$1 host_port=$2
  docker run -d --name "$name" \
    -p "$host_port:11222" \
    -e USER=unused \
    -e PASS=unused-but-required \
    -v "$PWD/infinispan.xml:/opt/infinispan/server/conf/infinispan.xml:ro" \
    -v "$PWD/users.properties:/opt/infinispan/server/conf/users.properties:ro" \
    -v "$PWD/groups.properties:/opt/infinispan/server/conf/groups.properties:ro" \
    infinispan/server:15.1 \
    >/dev/null
}

start_node infinispan-multicluster-primary 11222
start_node infinispan-multicluster-dr 11233

wait_for_infinispan infinispan-multicluster-primary
wait_for_infinispan infinispan-multicluster-dr

echo "Primary cluster up: 127.0.0.1:11222"
echo "DR cluster up: 127.0.0.1:11233"
echo "Matches multicluster_dr_addr()'s default in tests/live_server.rs, so no INFINISPAN_DR_ADDR override is needed."
