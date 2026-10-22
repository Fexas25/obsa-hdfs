import java.io.IOException;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.zip.CRC32;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * Same-workload comparison of OBS FileSystem implementations through the Hadoop API only, so the
 * Rust adapter (OBSFileSystemRust) and the Huawei Java OBSA (OBSFileSystem) run identical code.
 * One workload per JVM; prints one JSON line. Content is verified (CRC32) so both do the same work.
 *
 * <pre>
 * java ObsCompareBench --impl rust|java --bucket B --workload W [--threads N] [--files N]
 *     [--size-mb N] [--reads N] [--read-kb N] [--conf k=v ...] [--prefix P]
 * </pre>
 *
 * Workloads: write-read (sequential write then read of --files x --size-mb), random-read (--reads
 * positioned reads of --read-kb from one --size-mb file), metadata (create/stat/list/delete of
 * --files 1 KiB files), rename-dir (rename a directory of --files small files). Credentials and
 * endpoint come from OBS_LIVE_ENDPOINT / OBS_LIVE_ACCESS_KEY / OBS_LIVE_SECRET_KEY (and optional
 * OBS_LIVE_SESSION_TOKEN); they are never printed.
 */
public final class ObsCompareBench {
  private static final Map<String, String> IMPLS = new LinkedHashMap<>();

  static {
    IMPLS.put("rust", "org.apache.hadoop.fs.obs.OBSFileSystemRust");
    IMPLS.put("java", "org.apache.hadoop.fs.obs.OBSFileSystem");
  }

  private final Map<String, String> args;
  private final FileSystem fs;
  private final Path root;
  private final int threads;
  private final Map<String, Object> result = new LinkedHashMap<>();

  private ObsCompareBench(Map<String, String> args, List<String> conf) throws IOException {
    this.args = args;
    String impl = required("impl");
    String className = IMPLS.get(impl);
    if (className == null) throw new IllegalArgumentException("--impl must be rust or java");
    Configuration c = new Configuration();
    c.set("fs.obs.impl", className);
    c.setBoolean("fs.obs.impl.disable.cache", true);
    c.set("fs.obs.endpoint", env("OBS_LIVE_ENDPOINT"));
    c.set("fs.obs.access.key", env("OBS_LIVE_ACCESS_KEY"));
    c.set("fs.obs.secret.key", env("OBS_LIVE_SECRET_KEY"));
    // Same transport for both: the implementations default differently (Rust https, OBSA http).
    String secure = System.getenv("OBS_LIVE_SECURE");
    c.setBoolean("fs.obs.connection.ssl.enabled", secure == null || !secure.equals("false"));
    String token = System.getenv("OBS_LIVE_SESSION_TOKEN");
    if (token != null && !token.isEmpty()) c.set("fs.obs.session.token", token);
    for (String kv : conf) {
      int eq = kv.indexOf('=');
      if (eq <= 0) throw new IllegalArgumentException("--conf expects key=value: " + kv);
      c.set(kv.substring(0, eq), kv.substring(eq + 1));
    }
    fs = FileSystem.get(URI.create("obs://" + required("bucket")), c);
    root = new Path("/" + args.getOrDefault("prefix", "obsa-bench") + "/" + required("workload")
        + "-" + impl + "-" + UUID.randomUUID());
    threads = Integer.parseInt(args.getOrDefault("threads", "4"));
    result.put("impl", impl);
    result.put("workload", required("workload"));
    result.put("threads", threads);
    result.put("conf", conf);
    result.put("ssl", c.getBoolean("fs.obs.connection.ssl.enabled", true));
  }

  private String required(String key) {
    String v = args.get(key);
    if (v == null) throw new IllegalArgumentException("missing --" + key);
    return v;
  }

  private static String env(String name) {
    String v = System.getenv(name);
    if (v == null || v.isEmpty()) throw new IllegalArgumentException(name + " is not set");
    return v;
  }

  private int intArg(String key, int def) {
    return Integer.parseInt(args.getOrDefault(key, Integer.toString(def)));
  }

  // ---- workloads ---------------------------------------------------------------------------

  private void writeRead() throws Exception {
    int files = intArg("files", threads);
    long size = (long) intArg("size-mb", 64) << 20;
    List<Path> paths = new ArrayList<>();
    for (int i = 0; i < files; i++) paths.add(new Path(root, "f" + i));
    long[] crcs = new long[files];
    long[] writeMs = new long[files];
    long start = System.nanoTime();
    parallel(files, i -> {
      long t = System.nanoTime();
      crcs[i] = write(paths.get(i), size, i);
      writeMs[i] = (System.nanoTime() - t) / 1_000_000;
    });
    long writeNs = System.nanoTime() - start;
    long[] readMs = new long[files];
    start = System.nanoTime();
    parallel(files, i -> {
      long t = System.nanoTime();
      long crc = readAll(paths.get(i));
      readMs[i] = (System.nanoTime() - t) / 1_000_000;
      if (crc != crcs[i]) throw new IOException("content mismatch in " + paths.get(i));
    });
    long readNs = System.nanoTime() - start;
    double mb = files * (double) size / (1 << 20);
    result.put("files", files);
    result.put("size_mb", size >> 20);
    result.put("write_mib_s", round(mb / (writeNs / 1e9)));
    result.put("read_mib_s", round(mb / (readNs / 1e9)));
    result.put("write_file_ms", percentiles(writeMs));
    result.put("read_file_ms", percentiles(readMs));
  }

  private void randomRead() throws Exception {
    long size = (long) intArg("size-mb", 256) << 20;
    int reads = intArg("reads", 2000);
    int len = intArg("read-kb", 64) << 10;
    Path file = new Path(root, "random");
    write(file, size, 7);
    long[] latUs = new long[reads];
    long start = System.nanoTime();
    // One shared stream per thread, positioned reads at uniformly random offsets.
    parallelStreams(file, reads, (in, i, buf, rnd) -> {
      long offset = (long) (rnd.nextDouble() * (size - len));
      long t = System.nanoTime();
      in.readFully(offset, buf, 0, len);
      latUs[i] = (System.nanoTime() - t) / 1000;
    }, len);
    long ns = System.nanoTime() - start;
    result.put("size_mb", size >> 20);
    result.put("reads", reads);
    result.put("read_kb", len >> 10);
    result.put("reads_per_s", round(reads / (ns / 1e9)));
    result.put("read_us", percentiles(latUs));
  }

  private void metadata() throws Exception {
    int files = intArg("files", 500);
    Path dir = new Path(root, "meta");
    fs.mkdirs(dir);
    byte[] small = new byte[1024];
    new Random(1).nextBytes(small);
    long[] createUs = new long[files], statUs = new long[files], deleteUs = new long[files];
    long t0 = System.nanoTime();
    parallel(files, i -> {
      long t = System.nanoTime();
      try (FSDataOutputStream out = fs.create(new Path(dir, "s" + i), false)) {
        out.write(small);
      }
      createUs[i] = (System.nanoTime() - t) / 1000;
    });
    long createNs = System.nanoTime() - t0;
    t0 = System.nanoTime();
    parallel(files, i -> {
      long t = System.nanoTime();
      FileStatus s = fs.getFileStatus(new Path(dir, "s" + i));
      statUs[i] = (System.nanoTime() - t) / 1000;
      if (s.getLen() != small.length) throw new IOException("bad length " + s);
    });
    long statNs = System.nanoTime() - t0;
    long listStart = System.nanoTime();
    int listed = fs.listStatus(dir).length;
    long listMs = (System.nanoTime() - listStart) / 1_000_000;
    if (listed != files) throw new IOException("listed " + listed + " of " + files);
    t0 = System.nanoTime();
    parallel(files, i -> {
      long t = System.nanoTime();
      if (!fs.delete(new Path(dir, "s" + i), false)) throw new IOException("delete failed " + i);
      deleteUs[i] = (System.nanoTime() - t) / 1000;
    });
    long deleteNs = System.nanoTime() - t0;
    result.put("files", files);
    result.put("create_per_s", round(files / (createNs / 1e9)));
    result.put("stat_per_s", round(files / (statNs / 1e9)));
    result.put("delete_per_s", round(files / (deleteNs / 1e9)));
    result.put("list_ms", listMs);
    result.put("create_us", percentiles(createUs));
    result.put("stat_us", percentiles(statUs));
    result.put("delete_us", percentiles(deleteUs));
  }

  private void renameDir() throws Exception {
    int files = intArg("files", 200);
    Path src = new Path(root, "src"), dst = new Path(root, "dst");
    fs.mkdirs(src);
    parallel(files, i -> {
      try (FSDataOutputStream out = fs.create(new Path(src, "d" + (i % 10) + "/r" + i), false)) {
        out.write(i);
      }
    });
    long t = System.nanoTime();
    if (!fs.rename(src, dst)) throw new IOException("rename returned false");
    long ms = (System.nanoTime() - t) / 1_000_000;
    if (fs.exists(src)) throw new IOException("source survived rename");
    result.put("files", files);
    result.put("rename_ms", ms);
  }

  // ---- helpers -----------------------------------------------------------------------------

  private long write(Path path, long size, int seed) throws IOException {
    byte[] buf = new byte[1 << 20];
    new Random(seed).nextBytes(buf);
    CRC32 crc = new CRC32();
    try (FSDataOutputStream out = fs.create(path, true)) {
      for (long done = 0; done < size; ) {
        int n = (int) Math.min(buf.length, size - done);
        out.write(buf, 0, n);
        crc.update(buf, 0, n);
        done += n;
      }
    }
    return crc.getValue();
  }

  private long readAll(Path path) throws IOException {
    byte[] buf = new byte[1 << 20];
    CRC32 crc = new CRC32();
    try (FSDataInputStream in = fs.open(path)) {
      for (int n; (n = in.read(buf)) > 0; ) crc.update(buf, 0, n);
    }
    return crc.getValue();
  }

  interface Task {
    void run(int i) throws Exception;
  }

  interface StreamTask {
    void run(FSDataInputStream in, int i, byte[] buf, Random rnd) throws Exception;
  }

  private void parallel(int count, Task task) throws Exception {
    ExecutorService pool = Executors.newFixedThreadPool(threads);
    try {
      List<Future<?>> futures = new ArrayList<>();
      for (int i = 0; i < count; i++) {
        final int n = i;
        futures.add(pool.submit(() -> {
          task.run(n);
          return null;
        }));
      }
      for (Future<?> f : futures) f.get();
    } finally {
      pool.shutdownNow();
    }
  }

  private void parallelStreams(Path file, int count, StreamTask task, int bufLen) throws Exception {
    ExecutorService pool = Executors.newFixedThreadPool(threads);
    try {
      List<Future<?>> futures = new ArrayList<>();
      for (int t = 0; t < threads; t++) {
        final int worker = t;
        futures.add(pool.submit(() -> {
          byte[] buf = new byte[bufLen];
          Random rnd = new Random(1000 + worker);
          try (FSDataInputStream in = fs.open(file)) {
            for (int i = worker; i < count; i += threads) task.run(in, i, buf, rnd);
          }
          return null;
        }));
      }
      for (Future<?> f : futures) f.get();
    } finally {
      pool.shutdownNow();
    }
  }

  private static Map<String, Object> percentiles(long[] values) {
    long[] v = values.clone();
    Arrays.sort(v);
    Map<String, Object> p = new LinkedHashMap<>();
    p.put("p50", pick(v, 0.50));
    p.put("p95", pick(v, 0.95));
    p.put("p99", pick(v, 0.99));
    p.put("max", v.length == 0 ? 0 : v[v.length - 1]);
    return p;
  }

  private static long pick(long[] sorted, double q) {
    if (sorted.length == 0) return 0;
    return sorted[Math.min(sorted.length - 1, (int) Math.ceil(q * sorted.length) - 1)];
  }

  private static double round(double v) {
    return Math.round(v * 100) / 100.0;
  }

  private void run() throws Exception {
    long cpu0 = processCpuNs(), gc0 = gcMs(), t0 = System.nanoTime();
    try {
      switch (required("workload")) {
        case "write-read": writeRead(); break;
        case "random-read": randomRead(); break;
        case "metadata": metadata(); break;
        case "rename-dir": renameDir(); break;
        default: throw new IllegalArgumentException("unknown workload");
      }
      result.put("ok", true);
    } finally {
      result.put("wall_ms", (System.nanoTime() - t0) / 1_000_000);
      result.put("cpu_ms", (processCpuNs() - cpu0) / 1_000_000);
      result.put("gc_ms", gcMs() - gc0);
      result.put("peak_rss_mb", peakRssMb());
      try {
        fs.delete(root, true);
      } finally {
        fs.close();
      }
    }
  }

  private static long processCpuNs() {
    java.lang.management.OperatingSystemMXBean os = ManagementFactory.getOperatingSystemMXBean();
    return os instanceof com.sun.management.OperatingSystemMXBean
        ? ((com.sun.management.OperatingSystemMXBean) os).getProcessCpuTime() : -1;
  }

  private static long gcMs() {
    long total = 0;
    for (GarbageCollectorMXBean gc : ManagementFactory.getGarbageCollectorMXBeans()) {
      total += Math.max(0, gc.getCollectionTime());
    }
    return total;
  }

  /** Linux VmHWM (includes native memory); -1 elsewhere. */
  private static long peakRssMb() {
    try {
      for (String line : Files.readAllLines(Paths.get("/proc/self/status"), StandardCharsets.UTF_8)) {
        if (line.startsWith("VmHWM:")) {
          return Long.parseLong(line.replaceAll("[^0-9]", "")) / 1024;
        }
      }
    } catch (IOException | RuntimeException e) {
      // Not Linux.
    }
    return -1;
  }

  private static String json(Object v) {
    if (v instanceof Map) {
      StringBuilder b = new StringBuilder("{");
      for (Map.Entry<?, ?> e : ((Map<?, ?>) v).entrySet()) {
        if (b.length() > 1) b.append(',');
        b.append(json(e.getKey().toString())).append(':').append(json(e.getValue()));
      }
      return b.append('}').toString();
    }
    if (v instanceof List) {
      StringBuilder b = new StringBuilder("[");
      for (Object o : (List<?>) v) {
        if (b.length() > 1) b.append(',');
        b.append(json(o));
      }
      return b.append(']').toString();
    }
    if (v instanceof Number || v instanceof Boolean) return v.toString();
    return '"' + String.valueOf(v).replace("\\", "\\\\").replace("\"", "\\\"") + '"';
  }

  public static void main(String[] argv) throws Exception {
    Map<String, String> args = new LinkedHashMap<>();
    List<String> conf = new ArrayList<>();
    for (int i = 0; i < argv.length; i++) {
      if (!argv[i].startsWith("--") || i + 1 >= argv.length) {
        throw new IllegalArgumentException("expected --key value, got " + argv[i]);
      }
      String key = argv[i].substring(2), value = argv[++i];
      if (key.equals("conf")) conf.add(value);
      else args.put(key, value);
    }
    ObsCompareBench bench = new ObsCompareBench(args, Collections.unmodifiableList(conf));
    try {
      bench.run();
    } catch (Exception e) {
      bench.result.put("ok", false);
      bench.result.put("error", e.getClass().getSimpleName() + ": " + e.getMessage());
      System.out.println(json(bench.result));
      System.exit(1);
    }
    System.out.println(json(bench.result));
    System.exit(0);
  }
}
