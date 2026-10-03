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
`clear`. `clear` runs last, since it wipes the whole cache, not just the
bench's own keys.

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
| `put` | 11.72 | 85.30 | 8.03 | 124.60 |
| `get` | 12.15 | 82.31 | 8.49 | 117.80 |
| `contains_key` | 12.46 | 80.23 | 8.65 | 115.63 |
| `replace` | 11.96 | 83.62 | 8.34 | 119.84 |
| `get_with_version` | 12.24 | 81.67 | 8.50 | 117.64 |
| `replace_if_unmodified` | 11.57 | 86.45 | 8.16 | 122.54 |
| `remove_if_unmodified` | 11.88 | 84.18 | 8.42 | 118.83 |
| `remove` | 11.79 | 84.82 | 8.38 | 119.39 |
| `put_if_absent` | 11.83 | 84.51 | 8.47 | 118.08 |
| `put_all` | 802.00 | 1.25 | 669.34 | 1.49 |
| `get_all` | 812.85 | 1.23 | 685.90 | 1.46 |
| `size` | 2.29 | 435.89 | 2.00 | 500.98 |
| `stats` | 7.55 | 132.40 | 6.23 | 160.51 |
| `ping` | 12.32 | 81.16 | n/a | n/a |
| `clear` | 3.52 | 284.07 | 5.50 | 181.77 |

`ping` has no Java number, per the exclusion noted above. `clear` is the
one operation where the Java client comes out ahead in this run; every
other operation favors `hotrod-protocol`, roughly by a third to a half
less time per call.
