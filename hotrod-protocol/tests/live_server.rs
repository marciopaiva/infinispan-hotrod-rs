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

use hotrod_protocol::{Expiration, HotRodConnection};

fn env_or(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_string())
}

#[tokio::test]
#[ignore]
async fn put_get_remove_roundtrip() {
    let addr = env_or("INFINISPAN_ADDR", "127.0.0.1:11222");
    let user = env_or("INFINISPAN_USER", "testuser");
    let pass = env_or("INFINISPAN_PASS", "testpass");

    let mut conn = HotRodConnection::connect(&addr, "").await.expect("connect");
    conn.authenticate_plain("", &user, &pass)
        .await
        .expect("authenticate");

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
