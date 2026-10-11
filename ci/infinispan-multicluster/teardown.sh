#!/usr/bin/env bash
# Stops the two servers started by setup.sh.

set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")"
source ../lib.sh

rm_containers infinispan-multicluster-primary infinispan-multicluster-dr
