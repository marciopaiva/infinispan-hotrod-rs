//! Integration test against a real Infinispan server.
//!
//! Ignored by default so `cargo test` stays offline. Run explicitly with
//! `cargo test --test live_server -- --ignored` against a server configured
//! like `ci/infinispan/infinispan.xml` (PLAIN SASL, a `default` cache). The
//! release workflow does this against a server it starts itself: this same
//! put/get/remove sequence is what caught the ERROR_RESPONSE opcode bug
//! during manual testing, which byte buffers built in unit tests could not.
//!
//! Connection details come from environment variables so the test can also
//! be pointed at a local server, with defaults matching the CI fixture.

use hotrod_protocol::{Expiration, HotRodConnection, VersionedResult};

fn env_or(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_string())
}

async fn connect() -> HotRodConnection {
    let addr = env_or("INFINISPAN_ADDR", "127.0.0.1:11222");
    let user = env_or("INFINISPAN_USER", "testuser");
    let pass = env_or("INFINISPAN_PASS", "testpass");

    let mut conn = HotRodConnection::connect(&addr, "").await.expect("connect");
    conn.authenticate_plain("", &user, &pass)
        .await
        .expect("authenticate");
    conn
}

async fn connect_with_scram() -> HotRodConnection {
    let addr = env_or("INFINISPAN_ADDR", "127.0.0.1:11222");
    let user = env_or("INFINISPAN_USER", "testuser");
    let pass = env_or("INFINISPAN_PASS", "testpass");

    let mut conn = HotRodConnection::connect(&addr, "").await.expect("connect");
    conn.authenticate_scram(&user, &pass)
        .await
        .expect("authenticate with SCRAM-SHA-512");
    conn
}

async fn connect_with_digest() -> HotRodConnection {
    let addr = env_or("INFINISPAN_ADDR", "127.0.0.1:11222");
    let user = env_or("INFINISPAN_USER", "testuser");
    let pass = env_or("INFINISPAN_PASS", "testpass");

    let mut conn = HotRodConnection::connect(&addr, "").await.expect("connect");
    conn.authenticate_digest(&user, &pass)
        .await
        .expect("authenticate with DIGEST-SHA-256");
    conn
}

#[tokio::test]
#[ignore]
async fn put_get_remove_roundtrip() {
    let mut conn = connect().await;

    conn.put(
        b"ci-key",
        b"ci-value",
        Expiration::Default,
        Expiration::Default,
    )
    .await
    .expect("put");

    let value = conn.get(b"ci-key").await.expect("get");
    assert_eq!(value, Some(b"ci-value".to_vec()));

    let removed = conn.remove(b"ci-key").await.expect("remove");
    assert!(removed);

    let value = conn.get(b"ci-key").await.expect("get after remove");
    assert_eq!(value, None);
}

#[tokio::test]
#[ignore]
async fn scram_authenticated_connection_can_put_get_remove() {
    let mut conn = connect_with_scram().await;

    conn.put(
        b"ci-scram-key",
        b"ci-scram-value",
        Expiration::Default,
        Expiration::Default,
    )
    .await
    .expect("put");

    let value = conn.get(b"ci-scram-key").await.expect("get");
    assert_eq!(value, Some(b"ci-scram-value".to_vec()));

    let removed = conn.remove(b"ci-scram-key").await.expect("remove");
    assert!(removed);
}

#[tokio::test]
#[ignore]
async fn digest_authenticated_connection_can_put_get_remove() {
    let mut conn = connect_with_digest().await;

    conn.put(
        b"ci-digest-key",
        b"ci-digest-value",
        Expiration::Default,
        Expiration::Default,
    )
    .await
    .expect("put");

    let value = conn.get(b"ci-digest-key").await.expect("get");
    assert_eq!(value, Some(b"ci-digest-value".to_vec()));

    let removed = conn.remove(b"ci-digest-key").await.expect("remove");
    assert!(removed);
}

#[tokio::test]
#[ignore]
async fn put_if_absent_only_stores_when_missing() {
    let mut conn = connect().await;
    conn.remove(b"ci-put-if-absent").await.expect("cleanup");

    let stored = conn
        .put_if_absent(
            b"ci-put-if-absent",
            b"first",
            Expiration::Default,
            Expiration::Default,
        )
        .await
        .expect("put_if_absent");
    assert!(stored);

    let stored_again = conn
        .put_if_absent(
            b"ci-put-if-absent",
            b"second",
            Expiration::Default,
            Expiration::Default,
        )
        .await
        .expect("put_if_absent again");
    assert!(!stored_again);

    let value = conn.get(b"ci-put-if-absent").await.expect("get");
    assert_eq!(value, Some(b"first".to_vec()));

    conn.remove(b"ci-put-if-absent").await.expect("cleanup");
}

#[tokio::test]
#[ignore]
async fn replace_only_writes_when_key_exists() {
    let mut conn = connect().await;
    conn.remove(b"ci-replace").await.expect("cleanup");

    let replaced_missing = conn
        .replace(
            b"ci-replace",
            b"value",
            Expiration::Default,
            Expiration::Default,
        )
        .await
        .expect("replace on missing key");
    assert!(!replaced_missing);

    conn.put(
        b"ci-replace",
        b"original",
        Expiration::Default,
        Expiration::Default,
    )
    .await
    .expect("put");

    let replaced = conn
        .replace(
            b"ci-replace",
            b"updated",
            Expiration::Default,
            Expiration::Default,
        )
        .await
        .expect("replace");
    assert!(replaced);

    let value = conn.get(b"ci-replace").await.expect("get");
    assert_eq!(value, Some(b"updated".to_vec()));

    conn.remove(b"ci-replace").await.expect("cleanup");
}

#[tokio::test]
#[ignore]
async fn versioned_replace_and_remove_detect_staleness() {
    let mut conn = connect().await;
    conn.remove(b"ci-versioned").await.expect("cleanup");

    conn.put(
        b"ci-versioned",
        b"v1",
        Expiration::Default,
        Expiration::Default,
    )
    .await
    .expect("put");

    let versioned = conn
        .get_with_version(b"ci-versioned")
        .await
        .expect("get_with_version")
        .expect("entry should exist");
    assert_eq!(versioned.value, b"v1");

    // A write from elsewhere changes the version before the versioned call
    // below runs, so that call must be rejected as stale.
    conn.put(
        b"ci-versioned",
        b"v2",
        Expiration::Default,
        Expiration::Default,
    )
    .await
    .expect("concurrent put");

    let stale_replace = conn
        .replace_if_unmodified(
            b"ci-versioned",
            b"v3",
            versioned.version,
            Expiration::Default,
            Expiration::Default,
        )
        .await
        .expect("replace_if_unmodified");
    assert_eq!(stale_replace, VersionedResult::Stale);

    let current = conn
        .get_with_version(b"ci-versioned")
        .await
        .expect("get_with_version")
        .expect("entry should exist");
    assert_eq!(current.value, b"v2");

    let applied_replace = conn
        .replace_if_unmodified(
            b"ci-versioned",
            b"v3",
            current.version,
            Expiration::Default,
            Expiration::Default,
        )
        .await
        .expect("replace_if_unmodified");
    assert_eq!(applied_replace, VersionedResult::Success);

    let value = conn.get(b"ci-versioned").await.expect("get");
    assert_eq!(value, Some(b"v3".to_vec()));

    let final_version = conn
        .get_with_version(b"ci-versioned")
        .await
        .expect("get_with_version")
        .expect("entry should exist");

    let stale_remove = conn
        .remove_if_unmodified(b"ci-versioned", final_version.version.wrapping_add(1))
        .await
        .expect("remove_if_unmodified");
    assert_eq!(stale_remove, VersionedResult::Stale);

    let removed = conn
        .remove_if_unmodified(b"ci-versioned", final_version.version)
        .await
        .expect("remove_if_unmodified");
    assert_eq!(removed, VersionedResult::Success);

    let value = conn.get(b"ci-versioned").await.expect("get after remove");
    assert_eq!(value, None);

    let missing_remove = conn
        .remove_if_unmodified(b"ci-versioned", final_version.version)
        .await
        .expect("remove_if_unmodified on missing key");
    assert_eq!(missing_remove, VersionedResult::NotFound);
}
