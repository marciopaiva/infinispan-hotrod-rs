# Shared helpers for the ci/infinispan*/setup.sh and teardown.sh
# scripts. Source it, do not execute it directly:
#
#   source "$(dirname "${BASH_SOURCE[0]}")/../lib.sh"

# Removes one or more containers if they exist, quietly. Safe to call
# whether or not they are actually running: every setup.sh calls this
# first, so a repeated run (or one after a previous run crashed before
# its own teardown) is idempotent.
rm_containers() {
  docker rm -f "$@" >/dev/null 2>&1 || true
}

# Polls `docker logs $1` for the startup-complete line (ISPN080001),
# $2 times (default 30) two seconds apart, before giving up. The port
# accepts connections before the server has actually finished
# starting, so a raw TCP check is not enough.
wait_for_infinispan() {
  local name=$1
  local attempts=${2:-30}
  for _ in $(seq 1 "$attempts"); do
    if docker logs "$name" 2>&1 | grep -q "ISPN080001"; then
      return 0
    fi
    sleep 2
  done
  echo "$name did not come up in time"
  docker logs "$name" 2>&1
  return 1
}
