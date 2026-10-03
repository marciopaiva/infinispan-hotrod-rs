#!/usr/bin/env bash
# Stops the server started by setup.sh.

set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")"
source ../lib.sh

rm_containers infinispan-live-test
