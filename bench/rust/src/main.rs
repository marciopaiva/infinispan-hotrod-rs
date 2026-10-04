use std::env;
use std::net::SocketAddr;
use std::time::{Duration, Instant};

use hotrod_protocol::{Expiration, HotRodClient, HotRodConnection};

fn env_or<T: std::str::FromStr>(name: &str, default: T) -> T {
    env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn keys(prefix: &str, n: usize) -> Vec<Vec<u8>> {
    (0..n)
        .map(|i| format!("{prefix}-{i}").into_bytes())
        .collect()
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> hotrod_protocol::Result<()> {
    let addr = env::var("BENCH_ADDR").unwrap_or_else(|_| "127.0.0.1:11222".to_string());
    let cache = env::var("BENCH_CACHE").unwrap_or_else(|_| "default".to_string());
    let user = env::var("BENCH_USER").unwrap_or_else(|_| "testuser".to_string());
    let pass = env::var("BENCH_PASS").unwrap_or_else(|_| "testpass".to_string());
    let warmup: usize = env_or("BENCH_WARMUP", 2000);
    let iters: usize = env_or("BENCH_ITERS", 20000);
    let value_size: usize = env_or("BENCH_VALUE_SIZE", 100);
    let bulk_repeats: usize = env_or("BENCH_BULK_REPEATS", 5);
    let clear_iters: usize = env_or("BENCH_CLEAR_ITERS", 50);

    let mut conn = HotRodConnection::connect(&addr, &cache).await?;
    conn.authenticate_plain("", &user, &pass).await?;

    let value = vec![0u8; value_size];
    let value2 = vec![1u8; value_size];

    let warmup_keys = keys("warmup", warmup);
    for key in &warmup_keys {
        conn.put(key, &value, Expiration::Default, Expiration::Default)
            .await?;
        conn.get(key).await?;
    }

    let main_keys = keys("main", iters);

    let start = Instant::now();
    for key in &main_keys {
        conn.put(key, &value, Expiration::Default, Expiration::Default)
            .await?;
    }
    report("put", iters, start.elapsed());

    let start = Instant::now();
    for key in &main_keys {
        conn.get(key).await?;
    }
    report("get", iters, start.elapsed());

    let start = Instant::now();
    for key in &main_keys {
        conn.contains_key(key).await?;
    }
    report("contains_key", iters, start.elapsed());

    let start = Instant::now();
    for key in &main_keys {
        conn.replace(key, &value2, Expiration::Default, Expiration::Default)
            .await?;
    }
    report("replace", iters, start.elapsed());

    let mut versions = Vec::with_capacity(iters);
    let start = Instant::now();
    for key in &main_keys {
        let versioned = conn
            .get_with_version(key)
            .await?
            .expect("key was just replaced");
        versions.push(versioned.version);
    }
    report("get_with_version", iters, start.elapsed());

    let start = Instant::now();
    for (key, version) in main_keys.iter().zip(&versions) {
        conn.replace_if_unmodified(
            key,
            &value,
            *version,
            Expiration::Default,
            Expiration::Default,
        )
        .await?;
    }
    report("replace_if_unmodified", iters, start.elapsed());

    let mut versions = Vec::with_capacity(iters);
    for key in &main_keys {
        let versioned = conn
            .get_with_version(key)
            .await?
            .expect("key still present after replace_if_unmodified");
        versions.push(versioned.version);
    }

    let start = Instant::now();
    for (key, version) in main_keys.iter().zip(&versions) {
        conn.remove_if_unmodified(key, *version).await?;
    }
    report("remove_if_unmodified", iters, start.elapsed());

    let remove_keys = keys("remove", iters);
    let remove_entries: Vec<(Vec<u8>, Vec<u8>)> = remove_keys
        .iter()
        .cloned()
        .map(|k| (k, value.clone()))
        .collect();
    conn.put_all(remove_entries, Expiration::Default, Expiration::Default)
        .await?;

    let start = Instant::now();
    for key in &remove_keys {
        conn.remove(key).await?;
    }
    report("remove", iters, start.elapsed());

    let pia_keys = keys("pia", iters);
    let start = Instant::now();
    for key in &pia_keys {
        conn.put_if_absent(key, &value, Expiration::Default, Expiration::Default)
            .await?;
    }
    report("put_if_absent", iters, start.elapsed());

    let bulk_keys = keys("bulk", iters);

    let mut put_all_elapsed = Duration::ZERO;
    for _ in 0..bulk_repeats {
        let entries: Vec<(Vec<u8>, Vec<u8>)> = bulk_keys
            .iter()
            .cloned()
            .map(|k| (k, value.clone()))
            .collect();
        let start = Instant::now();
        conn.put_all(entries, Expiration::Default, Expiration::Default)
            .await?;
        put_all_elapsed += start.elapsed();
    }
    report("put_all", iters * bulk_repeats, put_all_elapsed);

    let mut get_all_elapsed = Duration::ZERO;
    for _ in 0..bulk_repeats {
        let start = Instant::now();
        conn.get_all(bulk_keys.iter().cloned()).await?;
        get_all_elapsed += start.elapsed();
    }
    report("get_all", iters * bulk_repeats, get_all_elapsed);

    // remove_all is skipped: hotrod-protocol no longer has it (v0.4.0
    // removed it, its opcode 0x45 never existed in the real protocol).

    let start = Instant::now();
    for _ in 0..iters {
        conn.size().await?;
    }
    report("size", iters, start.elapsed());

    let start = Instant::now();
    for _ in 0..iters {
        conn.stats().await?;
    }
    report("stats", iters, start.elapsed());

    let start = Instant::now();
    for _ in 0..iters {
        conn.ping().await?;
    }
    report("ping", iters, start.elapsed());

    let start = Instant::now();
    for _ in 0..clear_iters {
        conn.clear().await?;
    }
    report("clear", clear_iters, start.elapsed());

    // `iter`/`iter_with` (#53) are only exposed on `RemoteCache`, unlike
    // every operation above, which runs straight on the single
    // `HotRodConnection`: server-side iteration's public entry point
    // needs the client wrapper that `nodes_and_owned_segments` lives
    // on, even against this single-node server. A second, dedicated
    // connection for just this section, same as the Java side already
    // does for everything via `RemoteCacheManager`.
    let seed: SocketAddr = addr.parse().expect("BENCH_ADDR must be host:port");
    let client = HotRodClient::connect(&[seed]).await?;
    client.authenticate_plain("", &user, &pass).await?;
    let remote_cache = client.cache(&cache);

    let iter_keys = keys("iter", iters);
    let iter_entries: Vec<(Vec<u8>, Vec<u8>)> = iter_keys
        .iter()
        .cloned()
        .map(|key| (key, value.clone()))
        .collect();
    remote_cache
        .put_all(iter_entries, Expiration::Default, Expiration::Default)
        .await?;

    let start = Instant::now();
    let mut iterated = 0usize;
    let mut cursor = remote_cache.iter().await?;
    while cursor.next_entry().await?.is_some() {
        iterated += 1;
    }
    report("iter", iterated, start.elapsed());

    remote_cache.clear().await?;

    Ok(())
}

fn report(label: &str, ops: usize, elapsed: Duration) {
    let ms = elapsed.as_secs_f64() * 1000.0;
    let ops_per_ms = ops as f64 / ms;
    let us_per_op = (elapsed.as_secs_f64() * 1_000_000.0) / ops as f64;
    println!("{label}: {ops} ops in {ms:.2}ms ({ops_per_ms:.2} ops/ms, {us_per_op:.2} us/op avg)");
}
