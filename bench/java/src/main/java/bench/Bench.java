package bench;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.infinispan.client.hotrod.MetadataValue;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;

public final class Bench {

    public static void main(String[] args) throws Exception {
        String addr = env("BENCH_ADDR", "127.0.0.1:11222");
        String cacheName = env("BENCH_CACHE", "default");
        String user = env("BENCH_USER", "testuser");
        String pass = env("BENCH_PASS", "testpass");
        int warmup = envInt("BENCH_WARMUP", 2000);
        int iters = envInt("BENCH_ITERS", 20000);
        int valueSize = envInt("BENCH_VALUE_SIZE", 100);
        int bulkRepeats = envInt("BENCH_BULK_REPEATS", 5);
        int clearIters = envInt("BENCH_CLEAR_ITERS", 50);

        String[] hostPort = addr.split(":");
        String host = hostPort[0];
        int port = Integer.parseInt(hostPort[1]);

        ConfigurationBuilder builder = new ConfigurationBuilder();
        builder.addServer().host(host).port(port);
        builder
                .security()
                .authentication()
                .enable()
                .username(user)
                .password(pass)
                .realm("default")
                .saslMechanism("PLAIN");

        RemoteCacheManager manager = new RemoteCacheManager(builder.build());
        RemoteCache<String, byte[]> cache = manager.getCache(cacheName);

        byte[] value = new byte[valueSize];
        byte[] value2 = new byte[valueSize];
        Arrays.fill(value2, (byte) 1);

        List<String> warmupKeys = keys("warmup", warmup);
        for (String key : warmupKeys) {
            cache.put(key, value);
            cache.get(key);
        }

        List<String> mainKeys = keys("main", iters);

        long start = System.nanoTime();
        for (String key : mainKeys) {
            cache.put(key, value);
        }
        report("put", iters, System.nanoTime() - start);

        start = System.nanoTime();
        for (String key : mainKeys) {
            cache.get(key);
        }
        report("get", iters, System.nanoTime() - start);

        start = System.nanoTime();
        for (String key : mainKeys) {
            cache.containsKey(key);
        }
        report("contains_key", iters, System.nanoTime() - start);

        start = System.nanoTime();
        for (String key : mainKeys) {
            cache.replace(key, value2);
        }
        report("replace", iters, System.nanoTime() - start);

        List<Long> versions = new ArrayList<>(iters);
        start = System.nanoTime();
        for (String key : mainKeys) {
            MetadataValue<byte[]> versioned = cache.getWithMetadata(key);
            versions.add(versioned.getVersion());
        }
        report("get_with_version", iters, System.nanoTime() - start);

        start = System.nanoTime();
        for (int i = 0; i < mainKeys.size(); i++) {
            cache.replaceWithVersion(mainKeys.get(i), value, versions.get(i));
        }
        report("replace_if_unmodified", iters, System.nanoTime() - start);

        versions = new ArrayList<>(iters);
        for (String key : mainKeys) {
            MetadataValue<byte[]> versioned = cache.getWithMetadata(key);
            versions.add(versioned.getVersion());
        }

        start = System.nanoTime();
        for (int i = 0; i < mainKeys.size(); i++) {
            cache.removeWithVersion(mainKeys.get(i), versions.get(i));
        }
        report("remove_if_unmodified", iters, System.nanoTime() - start);

        List<String> removeKeys = keys("remove", iters);
        Map<String, byte[]> removeEntries = new HashMap<>();
        for (String key : removeKeys) {
            removeEntries.put(key, value);
        }
        cache.putAll(removeEntries);

        start = System.nanoTime();
        for (String key : removeKeys) {
            cache.remove(key);
        }
        report("remove", iters, System.nanoTime() - start);

        List<String> piaKeys = keys("pia", iters);
        start = System.nanoTime();
        for (String key : piaKeys) {
            cache.putIfAbsent(key, value);
        }
        report("put_if_absent", iters, System.nanoTime() - start);

        List<String> bulkKeys = keys("bulk", iters);
        Set<String> bulkKeySet = new HashSet<>(bulkKeys);
        Map<String, byte[]> bulkEntries = new HashMap<>();
        for (String key : bulkKeys) {
            bulkEntries.put(key, value);
        }

        long putAllNanos = 0;
        for (int r = 0; r < bulkRepeats; r++) {
            start = System.nanoTime();
            cache.putAll(bulkEntries);
            putAllNanos += System.nanoTime() - start;
        }
        report("put_all", iters * bulkRepeats, putAllNanos);

        long getAllNanos = 0;
        for (int r = 0; r < bulkRepeats; r++) {
            start = System.nanoTime();
            cache.getAll(bulkKeySet);
            getAllNanos += System.nanoTime() - start;
        }
        report("get_all", iters * bulkRepeats, getAllNanos);

        // remove_all is skipped: hotrod-protocol no longer has it (v0.4.0
        // removed it, no bulk remove exists in the real protocol), and the
        // Java client has no public bulk removeAll(Set) equivalent either.

        start = System.nanoTime();
        for (int i = 0; i < iters; i++) {
            cache.size();
        }
        report("size", iters, System.nanoTime() - start);

        start = System.nanoTime();
        for (int i = 0; i < iters; i++) {
            cache.serverStatistics();
        }
        report("stats", iters, System.nanoTime() - start);

        System.out.println("ping: no public equivalent on RemoteCache, skipped");

        start = System.nanoTime();
        for (int i = 0; i < clearIters; i++) {
            cache.clear();
        }
        report("clear", clearIters, System.nanoTime() - start);

        // RemoteCacheManager.close() races with its own idle/reconnect
        // handling on shutdown and logs a harmless but noisy stack trace.
        // Results are already printed, so exit immediately instead.
        System.exit(0);
    }

    private static List<String> keys(String prefix, int n) {
        List<String> result = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            result.add(prefix + "-" + i);
        }
        return result;
    }

    private static void report(String label, int ops, long elapsedNanos) {
        double ms = elapsedNanos / 1_000_000.0;
        double opsPerMs = ops / ms;
        double usPerOp = (elapsedNanos / 1000.0) / ops;
        System.out.printf(
                "%s: %d ops in %.2fms (%.2f ops/ms, %.2f us/op avg)%n", label, ops, ms, opsPerMs, usPerOp);
    }

    private static String env(String name, String defaultValue) {
        String value = System.getenv(name);
        return value != null ? value : defaultValue;
    }

    private static int envInt(String name, int defaultValue) {
        String value = System.getenv(name);
        return value != null ? Integer.parseInt(value) : defaultValue;
    }
}
