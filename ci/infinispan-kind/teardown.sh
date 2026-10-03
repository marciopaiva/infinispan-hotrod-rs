#!/usr/bin/env bash
# Tears down the kind cluster started by setup.sh.
set -euo pipefail

export KIND_EXPERIMENTAL_PROVIDER=podman

kind delete cluster --name hotrod-rs-cluster-test
