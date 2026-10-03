#!/usr/bin/env bash
# Starts a real two-node Infinispan cluster on a throwaway kind cluster,
# for the cluster_* tests in hotrod-protocol/tests/live_server.rs and for
# manual validation that HotRodClient/RemoteCache (#77,
# docs/adr/0005-connection-pooling-and-client-cache-split.md) still
# routes correctly and dispatches concurrently against a real server.
#
# Not part of any CI workflow, same as ci/infinispan-tls/: run it by
# hand, run the tests, then run teardown.sh.
#
# Needs `kind` and `kubectl`. No `helm`, no Infinispan Operator: this is
# two plain Pods, a headless Service for JGroups DNS_PING discovery, and
# a ConfigMap, applied directly.
#
# Usage:
#   ci/infinispan-kind/setup.sh
#   cargo test --test live_server cluster_ -- --ignored
#   ci/infinispan-kind/teardown.sh

set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")"

# This environment has podman, not docker; kind's podman provider is
# experimental but works fine for a disposable single-node cluster.
export KIND_EXPERIMENTAL_PROVIDER=podman

CLUSTER_NAME=hotrod-rs-cluster-test
KUBECTL_CONTEXT="kind-${CLUSTER_NAME}"

if kind get clusters 2>/dev/null | grep -qx "${CLUSTER_NAME}"; then
  kind delete cluster --name "${CLUSTER_NAME}"
fi

kind create cluster --config kind-config.yaml

kubectl --context "${KUBECTL_CONTEXT}" create configmap infinispan-config \
  --from-file=infinispan.xml \
  --from-file=users.properties \
  --from-file=groups.properties

# The `default` ServiceAccount in a brand new namespace is created
# asynchronously by a controller; applying a Pod before it exists fails
# with "serviceaccount default not found" (hit in practice right after
# `kind create cluster`), so wait for it first.
kubectl --context "${KUBECTL_CONTEXT}" wait --for=create serviceaccount/default --timeout=60s

kubectl --context "${KUBECTL_CONTEXT}" apply \
  -f service-headless.yaml \
  -f pod-node0.yaml \
  -f pod-node1.yaml

wait_for_node() {
  local pod=$1
  for i in $(seq 1 60); do
    if kubectl --context "${KUBECTL_CONTEXT}" logs "${pod}" 2>/dev/null | grep -q "ISPN080001"; then
      return 0
    fi
    sleep 2
  done
  echo "${pod} did not come up in time"
  kubectl --context "${KUBECTL_CONTEXT}" logs "${pod}" 2>&1 || true
  return 1
}

wait_for_node infinispan-node0
wait_for_node infinispan-node1

echo "Infinispan cluster up: 127.0.0.1:11222 (node0), 127.0.0.1:11322 (node1)"
echo "Matches cluster_seed_addrs()'s default in tests/live_server.rs, so no INFINISPAN_CLUSTER_ADDRS override is needed."
