#!/usr/bin/env bash
# Starts a real Infinispan server with TLS enabled, for the tls_* tests in
# hotrod-protocol/tests/live_server.rs. Not part of any CI workflow yet:
# run it by hand, run the tests, then run teardown.sh. See issue #83 for
# wiring this into ci.yml the way ci/infinispan-cluster/ already is (#78).
#
# Usage:
#   ci/infinispan-tls/setup.sh
#   cargo test --test live_server tls_ -- --ignored
#   ci/infinispan-tls/teardown.sh

set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")"

mkdir -p generated
cd generated

if [ ! -f ca-cert.pem ]; then
  openssl genrsa -out ca-key.pem 2048
  openssl req -x509 -new -nodes -key ca-key.pem -sha256 -days 3650 \
    -out ca-cert.pem -subj "/CN=hotrod-test-ca"
fi

if [ ! -f keystore.p12 ]; then
  openssl genrsa -out server-key.pem 2048
  openssl req -new -key server-key.pem -out server.csr -subj "/CN=localhost"
  printf 'subjectAltName = DNS:localhost,IP:127.0.0.1\n' > server-ext.cnf
  openssl x509 -req -in server.csr -CA ca-cert.pem -CAkey ca-key.pem -CAcreateserial \
    -out server-cert.pem -days 3650 -sha256 -extfile server-ext.cnf
  openssl pkcs12 -export -in server-cert.pem -inkey server-key.pem \
    -certfile ca-cert.pem -name server -out keystore.p12 -passout pass:secret123
  chmod 644 keystore.p12
fi

cd ..

podman rm -f infinispan-tls-test >/dev/null 2>&1 || true
podman run -d --name infinispan-tls-test \
  -p 21222:11222 \
  -e USER=unused \
  -e PASS=unused-but-required \
  -v "$PWD/infinispan.xml:/opt/infinispan/server/conf/infinispan.xml:ro,Z" \
  -v "$PWD/users.properties:/opt/infinispan/server/conf/users.properties:ro,Z" \
  -v "$PWD/groups.properties:/opt/infinispan/server/conf/groups.properties:ro,Z" \
  -v "$PWD/generated/keystore.p12:/opt/infinispan/server/conf/keystore.p12:ro,Z" \
  infinispan/server:15.1

for i in $(seq 1 30); do
  if podman logs infinispan-tls-test 2>&1 | grep -q "ISPN080001"; then
    echo "Infinispan is up on 127.0.0.1:21222, CA cert at $PWD/generated/ca-cert.pem"
    exit 0
  fi
  sleep 2
done

echo "Infinispan did not come up in time"
podman logs infinispan-tls-test
exit 1
