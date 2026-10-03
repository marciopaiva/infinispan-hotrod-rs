#!/usr/bin/env bash
# Starts a real Infinispan server with TLS enabled, for the tls_* tests in
# hotrod-protocol/tests/live_server.rs. Also runs as the tls-test job in
# ci.yml on every push to main and every PR (issue #83); run it by hand
# the same way for local iteration, then run teardown.sh.
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

docker rm -f infinispan-tls-test >/dev/null 2>&1 || true
docker run -d --name infinispan-tls-test \
  -p 21222:11222 \
  -e USER=unused \
  -e PASS=unused-but-required \
  -v "$PWD/infinispan.xml:/opt/infinispan/server/conf/infinispan.xml:ro" \
  -v "$PWD/users.properties:/opt/infinispan/server/conf/users.properties:ro" \
  -v "$PWD/groups.properties:/opt/infinispan/server/conf/groups.properties:ro" \
  -v "$PWD/generated/keystore.p12:/opt/infinispan/server/conf/keystore.p12:ro" \
  infinispan/server:15.1

for i in $(seq 1 30); do
  if docker logs infinispan-tls-test 2>&1 | grep -q "ISPN080001"; then
    echo "Infinispan is up on 127.0.0.1:21222, CA cert at $PWD/generated/ca-cert.pem"
    exit 0
  fi
  sleep 2
done

echo "Infinispan did not come up in time"
docker logs infinispan-tls-test
exit 1
