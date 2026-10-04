# Bench

An informal, dev-only comparison between `hotrod-protocol` and the
official Java Hot Rod client, run against the same local server. This
exists to get a rough sense of whether `hotrod-protocol` performs better
or on par with the Java client on a developer machine. It is not a
rigorous benchmark: no repeated runs, no statistics, no isolated hardware.
Nothing produced here is meant to end up in the repository's README or in
`hotrod-protocol/CHANGELOG.md`.

## Prerequisites

* Docker, to run the same Infinispan fixture used by CI.
* Maven, to build and run the Java side. It is not part of this project's
  own toolchain, only of this dev tool.

## Running the server

Same fixture as `.github/workflows/release.yml`, from the repository
root:

```bash
docker run -d --name infinispan-bench \
  -p 11222:11222 \
  -e USER=unused \
  -e PASS=unused-but-required \
  -v "$PWD/ci/infinispan/infinispan.xml:/opt/infinispan/server/conf/infinispan.xml:ro" \
  -v "$PWD/ci/infinispan/users.properties:/opt/infinispan/server/conf/users.properties:ro" \
  -v "$PWD/ci/infinispan/groups.properties:/opt/infinispan/server/conf/groups.properties:ro" \
  infinispan/server:15.1
```

Stop it afterwards with `docker rm -f infinispan-bench`.

## Running the benchmarks

```bash
cargo run -p hotrod-bench --release
```

```bash
cd bench/java
mvn -q compile exec:java
```

Each side runs sequentially over a single connection: no concurrency, no
pooling, one operation timed at a time, to isolate the protocol's own
cost from anything else. Every operation prints a line in the same
format:

```
put: 20000 ops in 123.45ms (162.02 ops/ms, 6.17 us/op avg)
get: 20000 ops in 98.76ms (202.51 ops/ms, 4.94 us/op avg)
```

## Operations covered

Both sides exercise every operation `hotrod-protocol`'s `HotRodConnection`
exposes, minus the two exceptions below, in the same fixed order, each
against its own set of keys so one operation's side effects never change
what the next one measures: `put`, `get`, `contains_key`, `replace`,
`get_with_version`, `replace_if_unmodified`, `remove_if_unmodified`,
`remove`, `put_if_absent`, `put_all`, `get_all`, `size`, `stats`, `ping`,
`clear`. `clear` runs last among those, since it wipes the whole cache,
not just the bench's own keys.

`iter` (`RemoteCache::iter`/`iter_with`, #53) runs after that, against
`retrieveEntries(null, batchSize)` on the Java side, both with the same
default batch size (`10_000`, `hotrod-protocol`'s own default): the
closest direct equivalent either client has to a full-cache scan. Unlike
every operation above, this one is only exposed on `RemoteCache`, not on
`HotRodConnection`, so the Rust side opens a second, dedicated connection
through `HotRodClient` just for this section. The Java side already used
the equivalent `RemoteCacheManager`/`RemoteCache` wrapper for every
operation above, so this is not a new asymmetry there, only on the Rust
side.

Two operations are excluded, because neither side has a matching public
call to compare:

* **`ping`**: `hotrod-protocol` exposes an explicit `ping()`. `RemoteCache`
  has no public equivalent, the Java client only pings internally on
  startup, so the Java side skips this line entirely.
* **`remove_all`**: not benchmarked on either side. `hotrod-protocol`
  removed it in v0.4.0: it sent opcode `0x45`, which does not exist in
  the real Hot Rod protocol (confirmed against the official protocol
  reference and against a real server, which rejected it with "Unknown
  operation 69"). There is no bulk remove in the protocol at all, which
  is also why `RemoteCache` has no public `removeAll(Set)`.

`hotrod-protocol`'s `HotRodClient` (multi-node topology routing) is out
of scope: comparing it fairly needs a distributed cache across a
multi-node cluster, a different setup than this single-connection bench.

## Parameters

Both sides read the same environment variables, so a run can be adjusted
without recompiling:

| Variable | Default | Meaning |
| --- | --- | --- |
| `BENCH_ADDR` | `127.0.0.1:11222` | Server address |
| `BENCH_CACHE` | `default` | Cache name |
| `BENCH_USER` | `testuser` | SASL PLAIN username |
| `BENCH_PASS` | `testpass` | SASL PLAIN password |
| `BENCH_WARMUP` | `2000` | Warm-up iterations, discarded |
| `BENCH_ITERS` | `20000` | Measured iterations per operation |
| `BENCH_VALUE_SIZE` | `100` | Value size in bytes |
| `BENCH_BULK_REPEATS` | `5` | Repeats of `put_all`/`get_all` over the same batch |
| `BENCH_CLEAR_ITERS` | `50` | Repeats of `clear`, kept low since it is destructive |

## Results

One run of each side, default parameters, same local server, immediately
one after the other. A single sample, not an average over multiple runs,
so treat the numbers as a rough order of magnitude, not a precise
measurement.

| Operation | Rust ops/ms | Rust us/op | Java ops/ms | Java us/op |
| --- | --- | --- | --- | --- |
| `put` | 11.32 | 88.31 | 7.80 | 128.27 |
| `get` | 11.75 | 85.10 | 8.24 | 121.34 |
| `contains_key` | 11.93 | 83.82 | 8.31 | 120.40 |
| `replace` | 11.61 | 86.14 | 7.97 | 125.40 |
| `get_with_version` | 12.22 | 81.82 | 8.14 | 122.82 |
| `replace_if_unmodified` | 11.16 | 89.60 | 7.85 | 127.33 |
| `remove_if_unmodified` | 11.47 | 87.17 | 7.67 | 130.33 |
| `remove` | 11.42 | 87.55 | 8.15 | 122.73 |
| `put_if_absent` | 11.36 | 88.01 | 8.19 | 122.11 |
| `put_all` | 917.16 | 1.09 | 677.90 | 1.48 |
| `get_all` | 959.79 | 1.04 | 659.32 | 1.52 |
| `size` | 2.19 | 456.07 | 1.63 | 612.99 |
| `stats` | 7.26 | 137.82 | 5.65 | 177.04 |
| `ping` | 12.08 | 82.79 | n/a | n/a |
| `clear` | 3.69 | 271.33 | 4.63 | 215.92 |
| `iter` | 529.57 | 1.89 | 241.88 | 4.13 |

`ping` has no Java number, per the exclusion noted above. `clear` is the
one operation where the Java client comes out ahead in this run; every
other operation, including the newly added `iter`, favors
`hotrod-protocol`.
