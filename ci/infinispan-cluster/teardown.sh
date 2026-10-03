#!/usr/bin/env bash
# Tears down the cluster started by setup.sh.
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")"
source ../lib.sh

rm_containers infinispan-cluster-node0 infinispan-cluster-node1
docker network rm hotrod-cluster-test >/dev/null 2>&1 || true
