#!/usr/bin/env bash
# Starts a real two-node Infinispan cluster for the cluster_* tests in
# hotrod-protocol/tests/live_server.rs (issue #78). Two plain containers
# on a dedicated Docker network, no Kubernetes/kind: this runs as a CI
# job (.github/workflows/ci.yml) on a runner that has `docker` directly,
# so the heavier orchestration an earlier, session-local validation of
# #77 used (a throwaway `kind` cluster, since that sandbox had no
# `docker`) is not needed here.
#
# JGroups discovery uses the "kubernetes" stack (really just TCP plus
# DNS_PING under the hood, despite the name) pointed at the Docker
# network alias both containers share: Docker's embedded DNS returns
# both containers' addresses for that one alias, the same way a
# Kubernetes headless Service's DNS record would.
#
# Usage:
#   ci/infinispan-cluster/setup.sh
#   cargo test --test live_server cluster_ -- --ignored
#   ci/infinispan-cluster/teardown.sh

set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")"

NETWORK=hotrod-cluster-test
ALIAS=infinispan-cluster

docker rm -f infinispan-cluster-node0 infinispan-cluster-node1 >/dev/null 2>&1 || true
docker network rm "$NETWORK" >/dev/null 2>&1 || true
docker network create "$NETWORK" >/dev/null

start_node() {
  local name=$1 host_port=$2 node_name=$3
  docker run -d --name "$name" \
    --network "$NETWORK" --network-alias "$ALIAS" \
    -p "$host_port:11222" \
    -e USER=unused \
    -e PASS=unused-but-required \
    -v "$PWD/infinispan.xml:/opt/infinispan/server/conf/infinispan.xml:ro" \
    -v "$PWD/users.properties:/opt/infinispan/server/conf/users.properties:ro" \
    -v "$PWD/groups.properties:/opt/infinispan/server/conf/groups.properties:ro" \
    infinispan/server:15.1 \
    -n "$node_name" -g "$NETWORK" -j kubernetes \
    -Djgroups.dns.query="$ALIAS" \
    -Dinfinispan.external.host=127.0.0.1 \
    -Dinfinispan.external.port="$host_port" \
    >/dev/null
}

start_node infinispan-cluster-node0 11222 node0
start_node infinispan-cluster-node1 11322 node1

wait_for_node() {
  local name=$1
  for i in $(seq 1 30); do
    if docker logs "$name" 2>&1 | grep -q "ISPN080001"; then
      return 0
    fi
    sleep 2
  done
  echo "$name did not come up in time"
  docker logs "$name" 2>&1
  return 1
}

wait_for_node infinispan-cluster-node0
wait_for_node infinispan-cluster-node1

echo "Infinispan cluster up: 127.0.0.1:11222 (node0), 127.0.0.1:11322 (node1)"
echo "Matches cluster_seed_addrs()'s default in tests/live_server.rs, so no INFINISPAN_CLUSTER_ADDRS override is needed."
