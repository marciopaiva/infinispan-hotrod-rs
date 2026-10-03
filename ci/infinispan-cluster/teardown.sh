#!/usr/bin/env bash
# Tears down the cluster started by setup.sh.
set -euo pipefail

docker rm -f infinispan-cluster-node0 infinispan-cluster-node1 >/dev/null 2>&1 || true
docker network rm hotrod-cluster-test >/dev/null 2>&1 || true
