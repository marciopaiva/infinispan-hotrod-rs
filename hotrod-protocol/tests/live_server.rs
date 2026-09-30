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
//!
//! `clippy::await_holding_lock` is allowed crate-wide below: `LIVE_SERVER_LOCK`
//! is a plain `std::sync::Mutex` held across `.await` on purpose. Each
//! `#[tokio::test]` drives its own single-threaded runtime on its own OS
//! thread, so holding it there blocks that thread, not a cooperative task
//! sharing it with others; that is exactly the serialization the lock is for.

#![allow(clippy::await_holding_lock)]

use std::net::SocketAddr;
use std::sync::Mutex;

use hotrod_protocol::{Expiration, HotRodCluster, HotRodConnection, VersionedResult};

/// Serializes every test in this file against the same live server. Most
/// tests only touch their own keys and would be fine running concurrently,
/// but `clear()` wipes the whole cache, and cargo's default parallel test
/// execution would otherwise let it run at the same time as another test
/// relying on a key it just removed. A poisoned lock (one test panicked
/// while holding it) still lets the next test acquire it, since a plain
/// assertion failure elsewhere is not a reason to fail every other test too.
static LIVE_SERVER_LOCK: Mutex<()> = Mutex::new(());

fn lock_live_server() -> std::sync::MutexGuard<'static, ()> {
    LIVE_SERVER_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

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

/// Seed addresses for a multi-node cluster, needed only by the
/// `HotRodCluster` tests below: hash-aware routing has nothing to route to
/// on the single-node fixture the other tests in this file use. Point this
/// at a local cluster with a `<distributed-cache>` (see
/// `docs/adr/0003-hash-aware-routing-scope.md`).
fn cluster_seed_addrs() -> Vec<SocketAddr> {
    env_or(
        "INFINISPAN_CLUSTER_ADDRS",
        "127.0.0.1:11222,127.0.0.1:11322",
    )
    .split(',')
    .map(|addr| addr.trim().parse().expect("valid socket address"))
    .collect()
}

fn cluster_cache_name() -> String {
    env_or("INFINISPAN_CLUSTER_CACHE", "distributed")
}

async fn connect_cluster() -> HotRodCluster {
    let user = env_or("INFINISPAN_USER", "testuser");
    let pass = env_or("INFINISPAN_PASS", "testpass");

    let mut cluster = HotRodCluster::connect(&cluster_seed_addrs(), &cluster_cache_name())
        .await
        .expect("connect");
    cluster
        .authenticate_plain("", &user, &pass)
        .await
        .expect("authenticate");
    cluster
}

#[tokio::test]
#[ignore]
async fn cluster_put_get_remove_roundtrip() {
    let _guard = lock_live_server();
    let mut cluster = connect_cluster().await;

    cluster
        .put(
            b"ci-cluster-key",
            b"ci-cluster-value",
            Expiration::Default,
            Expiration::Default,
        )
        .await
        .expect("put");

    let value = cluster.get(b"ci-cluster-key").await.expect("get");
    assert_eq!(value, Some(b"ci-cluster-value".to_vec()));

    let removed = cluster.remove(b"ci-cluster-key").await.expect("remove");
    assert!(removed);

    let value = cluster
        .get(b"ci-cluster-key")
        .await
        .expect("get after remove");
    assert_eq!(value, None);
}

/// Distinct keys hash to different segments, so this exercises routing to
/// more than one node's connection rather than always the seed. Whether
/// each request actually lands on the computed primary owner (as opposed to
/// landing correctly only via a server-side redirect) is confirmed manually
/// by watching each node's stats while this test runs, per
/// `docs/adr/0003-hash-aware-routing-scope.md`.
#[tokio::test]
#[ignore]
async fn cluster_routes_many_keys_to_their_owners() {
    let _guard = lock_live_server();
    let mut cluster = connect_cluster().await;

    let keys: Vec<Vec<u8>> = (0..50)
        .map(|i| format!("ci-cluster-routing-{i}").into_bytes())
        .collect();

    for key in &keys {
        cluster
            .put(key, b"value", Expiration::Default, Expiration::Default)
            .await
            .unwrap_or_else(|err| panic!("put {key:?}: {err}"));
    }

    for key in &keys {
        let value = cluster
            .get(key)
            .await
            .unwrap_or_else(|err| panic!("get {key:?}: {err}"));
        assert_eq!(value, Some(b"value".to_vec()));
    }

    for key in &keys {
        let removed = cluster
            .remove(key)
            .await
            .unwrap_or_else(|err| panic!("remove {key:?}: {err}"));
        assert!(removed);
    }
}

#[tokio::test]
#[ignore]
async fn put_get_remove_roundtrip() {
    let _guard = lock_live_server();
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
    let _guard = lock_live_server();
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
    let _guard = lock_live_server();
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
    let _guard = lock_live_server();
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
    let _guard = lock_live_server();
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
    let _guard = lock_live_server();
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

#[tokio::test]
#[ignore]
async fn contains_key_reflects_presence() {
    let _guard = lock_live_server();
    let mut conn = connect().await;
    conn.remove(b"ci-contains-key").await.expect("cleanup");

    let missing = conn
        .contains_key(b"ci-contains-key")
        .await
        .expect("contains_key on missing key");
    assert!(!missing);

    conn.put(
        b"ci-contains-key",
        b"value",
        Expiration::Default,
        Expiration::Default,
    )
    .await
    .expect("put");

    let present = conn
        .contains_key(b"ci-contains-key")
        .await
        .expect("contains_key on existing key");
    assert!(present);

    conn.remove(b"ci-contains-key").await.expect("cleanup");
}

#[tokio::test]
#[ignore]
async fn ping_succeeds_against_a_live_server() {
    let _guard = lock_live_server();
    let mut conn = connect().await;
    conn.ping().await.expect("ping");
}

#[tokio::test]
#[ignore]
async fn stats_returns_node_statistics() {
    let _guard = lock_live_server();
    let mut conn = connect().await;

    let stats = conn.stats().await.expect("stats");
    assert!(
        !stats.is_empty(),
        "a live server should report at least one statistic"
    );
}

#[tokio::test]
#[ignore]
async fn size_and_clear_reflect_cache_contents() {
    let _guard = lock_live_server();
    let mut conn = connect().await;

    conn.put(
        b"ci-size-clear",
        b"value",
        Expiration::Default,
        Expiration::Default,
    )
    .await
    .expect("put");

    let size_before = conn.size().await.expect("size");
    assert!(size_before >= 1, "the key just put should be counted");

    conn.clear().await.expect("clear");

    let size_after = conn.size().await.expect("size after clear");
    assert_eq!(size_after, 0);

    let value = conn.get(b"ci-size-clear").await.expect("get after clear");
    assert_eq!(value, None);
}

#[tokio::test]
#[ignore]
async fn get_all_and_put_all_roundtrip() {
    let _guard = lock_live_server();
    let mut conn = connect().await;

    let keys: Vec<Vec<u8>> = (0..5)
        .map(|i| format!("ci-bulk-{i}").into_bytes())
        .collect();
    for key in &keys {
        conn.remove(key).await.expect("cleanup");
    }

    let entries: Vec<(Vec<u8>, Vec<u8>)> = keys
        .iter()
        .map(|key| {
            (
                key.clone(),
                format!("value-for-{}", String::from_utf8_lossy(key)).into_bytes(),
            )
        })
        .collect();

    conn.put_all(entries.clone(), Expiration::Default, Expiration::Default)
        .await
        .expect("put_all");

    let fetched = conn.get_all(keys.clone()).await.expect("get_all");
    assert_eq!(fetched.len(), keys.len());
    for (key, value) in &entries {
        assert_eq!(fetched.get(key), Some(value));
    }

    for key in &keys {
        conn.remove(key).await.expect("cleanup");
    }
}

#[tokio::test]
#[ignore]
async fn cluster_contains_key_reflects_presence() {
    let _guard = lock_live_server();
    let mut cluster = connect_cluster().await;
    cluster
        .remove(b"ci-cluster-contains-key")
        .await
        .expect("cleanup");

    let missing = cluster
        .contains_key(b"ci-cluster-contains-key")
        .await
        .expect("contains_key on missing key");
    assert!(!missing);

    cluster
        .put(
            b"ci-cluster-contains-key",
            b"value",
            Expiration::Default,
            Expiration::Default,
        )
        .await
        .expect("put");

    let present = cluster
        .contains_key(b"ci-cluster-contains-key")
        .await
        .expect("contains_key on existing key");
    assert!(present);

    cluster
        .remove(b"ci-cluster-contains-key")
        .await
        .expect("cleanup");
}

#[tokio::test]
#[ignore]
async fn cluster_ping_succeeds_against_the_seed() {
    let _guard = lock_live_server();
    let mut cluster = connect_cluster().await;
    cluster.ping().await.expect("ping");
}

#[tokio::test]
#[ignore]
async fn cluster_stats_returns_node_statistics() {
    let _guard = lock_live_server();
    let mut cluster = connect_cluster().await;

    let stats = cluster.stats().await.expect("stats");
    assert!(
        !stats.is_empty(),
        "a live server should report at least one statistic"
    );
}

/// `size`/`clear` on `HotRodCluster` always target the seed, but `clear` is
/// still cluster-wide: it wipes the same `distributed` cache
/// `cluster_routes_many_keys_to_their_owners` uses, so this relies on
/// `LIVE_SERVER_LOCK` the same as the non-cluster `size_and_clear` test does.
#[tokio::test]
#[ignore]
async fn cluster_size_and_clear_reflect_cache_contents() {
    let _guard = lock_live_server();
    let mut cluster = connect_cluster().await;

    cluster
        .put(
            b"ci-cluster-size-clear",
            b"value",
            Expiration::Default,
            Expiration::Default,
        )
        .await
        .expect("put");

    let size_before = cluster.size().await.expect("size");
    assert!(size_before >= 1, "the key just put should be counted");

    cluster.clear().await.expect("clear");

    let size_after = cluster.size().await.expect("size after clear");
    assert_eq!(size_after, 0);

    let value = cluster
        .get(b"ci-cluster-size-clear")
        .await
        .expect("get after clear");
    assert_eq!(value, None);
}

#[tokio::test]
#[ignore]
async fn cluster_get_all_and_put_all_roundtrip() {
    let _guard = lock_live_server();
    let mut cluster = connect_cluster().await;

    let keys: Vec<Vec<u8>> = (0..5)
        .map(|i| format!("ci-cluster-bulk-{i}").into_bytes())
        .collect();
    for key in &keys {
        cluster.remove(key).await.expect("cleanup");
    }

    let entries: Vec<(Vec<u8>, Vec<u8>)> = keys
        .iter()
        .map(|key| {
            (
                key.clone(),
                format!("value-for-{}", String::from_utf8_lossy(key)).into_bytes(),
            )
        })
        .collect();

    cluster
        .put_all(entries.clone(), Expiration::Default, Expiration::Default)
        .await
        .expect("put_all");

    let fetched = cluster.get_all(keys.clone()).await.expect("get_all");
    assert_eq!(fetched.len(), keys.len());
    for (key, value) in &entries {
        assert_eq!(fetched.get(key), Some(value));
    }

    for key in &keys {
        cluster.remove(key).await.expect("cleanup");
    }
}
