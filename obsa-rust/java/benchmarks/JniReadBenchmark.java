import com.sun.net.httpserver.HttpServer;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.fs.obs.NativeObs;

/** Cached seek+read microbenchmark, not HTTP or Hadoop throughput. Run without competing workloads. */
public final class JniReadBenchmark {
  interface Checked { void run() throws Exception; }
  private static void invalid(Checked call) throws Exception {
    try { call.run(); throw new AssertionError("invalid buffer accepted"); }
    catch (IllegalArgumentException expected) {}
  }
  private static void guards(long input, byte[] content) throws Exception {
    ByteBuffer owner = ByteBuffer.allocateDirect(32);
    for (int i = 0; i < owner.capacity(); i++) owner.put(i, (byte) 77);
    owner.position(5).limit(21);
    ByteBuffer slice = owner.slice();
    owner.clear();
    NativeObs.nativeSeek(input, 0);
    if (NativeObs.nativeReadDirect(input, slice, 3, 8) != 8) throw new AssertionError("slice count");
    for (int i = 0; i < owner.capacity(); i++) {
      byte expected = i >= 8 && i < 16 ? content[i - 8] : 77;
      if (owner.get(i) != expected) throw new AssertionError("slice sentinel " + i);
    }
    invalid(() -> NativeObs.nativeReadDirect(input, ByteBuffer.allocate(32), 0, 1));
    invalid(() -> NativeObs.nativeReadDirect(input, owner.asReadOnlyBuffer(), 0, 1));
    invalid(() -> NativeObs.nativeReadDirect(input, null, 0, 0));
    invalid(() -> NativeObs.nativeReadDirect(-1, owner, 0, 0));
    for (int[] bounds : new int[][] {{-1, 1}, {0, -1}, {30, 3}, {Integer.MAX_VALUE, 1}, {1, Integer.MAX_VALUE}})
      invalid(() -> NativeObs.nativeReadDirect(input, owner, bounds[0], bounds[1]));
    byte[] next = new byte[1];
    if (NativeObs.nativeReadInto(input, next, 0, 1) != 1 || next[0] != content[8])
      throw new AssertionError("invalid calls changed stream position");
    NativeObs.nativeSeek(input, content.length);
    if (NativeObs.nativeReadDirect(input, ByteBuffer.allocateDirect(0), 0, 0) != 0)
      throw new AssertionError("zero capacity");
    if (NativeObs.nativeReadDirect(input, owner, 0, 1) != -1) throw new AssertionError("EOF");
  }
  private static void read(long input, byte[] array, ByteBuffer buffer, int length, boolean direct) throws Exception {
    NativeObs.nativeSeek(input, 0);
    int count = direct ? NativeObs.nativeReadDirect(input, buffer, 0, length)
        : NativeObs.nativeReadInto(input, array, 0, length);
    if (count != length) throw new AssertionError("short cached read");
  }
  public static void main(String[] args) throws Exception {
    int rounds = args.length == 0 ? 7 : Integer.parseInt(args[0]);
    if (rounds < 1 || rounds > 100) throw new IllegalArgumentException("rounds must be 1..100");
    byte[] content = new byte[1048576];
    for (int i = 0; i < content.length; i++) content[i] = (byte) (1 + i * 31);
    AtomicInteger gets = new AtomicInteger();
    HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.createContext("/bucket/file", exchange -> {
      try {
        exchange.getResponseHeaders().set("ETag", "\"benchmark\"");
        if (exchange.getRequestMethod().equals("HEAD")) {
          exchange.getResponseHeaders().set("Content-Length", Integer.toString(content.length));
          exchange.sendResponseHeaders(200, -1);
        } else if (exchange.getRequestMethod().equals("GET")) {
          gets.incrementAndGet();
          String range = exchange.getRequestHeaders().getFirst("Range");
          int first = 0, last = content.length - 1;
          if (range != null) {
            String[] pair = range.substring(6).split("-");
            first = Integer.parseInt(pair[0]); last = Integer.parseInt(pair[1]);
            exchange.getResponseHeaders().set("Content-Range", "bytes " + first + "-" + last + "/" + content.length);
          }
          exchange.sendResponseHeaders(range == null ? 200 : 206, last - first + 1);
          exchange.getResponseBody().write(content, first, last - first + 1);
        } else exchange.sendResponseHeaders(405, -1);
      } finally { exchange.close(); }
    });
    server.start();
    long fs = 0, input = 0;
    try {
      fs = NativeObs.nativeCreate("{\"endpoint\":\"127.0.0.1:" + server.getAddress().getPort()
          + "\",\"bucket\":\"bucket\",\"secure\":false,\"bucket_mode\":\"object\","
          + "\"access_key\":\"test\",\"secret_key\":\"test\","
          + "\"readahead_chunk_size\":1048576,\"readahead_max_chunks\":0}");
      input = Long.parseLong(NativeObs.nativeCall(fs, "open", "{\"path\":\"/file\"}"));
      guards(input, content);
      byte[] array = new byte[content.length];
      ByteBuffer buffer = ByteBuffer.allocateDirect(content.length);
      read(input, array, buffer, content.length, false);
      read(input, array, buffer, content.length, true);
      if (!Arrays.equals(content, array)) throw new AssertionError("array warm content");
      for (int i = 0; i < content.length; i++)
        if (buffer.get(i) != content[i]) throw new AssertionError("direct warm content");
      int warmGets = gets.get();
      ThreadMXBean cpu = ManagementFactory.getThreadMXBean();
      boolean measureCpu = cpu.isCurrentThreadCpuTimeSupported();
      if (measureCpu && !cpu.isThreadCpuTimeEnabled()) cpu.setThreadCpuTimeEnabled(true);
      System.out.println("mode,round,bytes,iterations,wall_ms,thread_cpu_ms,mib_s,p50_us,p95_us,p99_us,measured_gets");
      for (int size : new int[] {1, 4096, 1048576}) {
        int iterations = size == 1 ? 1000000 : size == 4096 ? 500000 : 10000;
        int warmup = Math.min(10000, Math.max(100, 32 * 1024 * 1024 / size));
        for (int i = 0; i < warmup; i++) {
          read(input, array, buffer, size, false); read(input, array, buffer, size, true);
        }
        long[] samples = new long[iterations];
        for (int round = 1; round <= rounds; round++) {
          for (int turn = 0; turn < 2; turn++) {
            boolean direct = (round + turn) % 2 == 0;
            long cpuStart = measureCpu ? cpu.getCurrentThreadCpuTime() : 0;
            long started = System.nanoTime();
            for (int i = 0; i < iterations; i++) {
              long operation = System.nanoTime();
              read(input, array, buffer, size, direct);
              samples[i] = System.nanoTime() - operation;
              byte first = direct ? buffer.get(0) : array[0];
              byte last = direct ? buffer.get(size - 1) : array[size - 1];
              if (first != content[0] || last != content[size - 1]) throw new AssertionError("read content");
            }
            long elapsed = System.nanoTime() - started;
            long cpuElapsed = measureCpu ? cpu.getCurrentThreadCpuTime() - cpuStart : -1;
            int requests = gets.get() - warmGets;
            if (requests != 0) throw new AssertionError("measurement performed GETs: " + requests);
            Arrays.sort(samples);
            System.out.printf(Locale.ROOT, "%s,%d,%d,%d,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f,%d%n",
                direct ? "direct" : "array", round, size, iterations, elapsed / 1e6, cpuElapsed / 1e6,
                (double) size * iterations * 1e9 / elapsed / 1048576,
                samples[iterations / 2] / 1e3, samples[(int) (iterations * .95)] / 1e3,
                samples[(int) (iterations * .99)] / 1e3, requests);
          }
        }
      }
    } finally {
      if (input != 0) NativeObs.nativeCloseStream(input);
      if (fs != 0) NativeObs.nativeClose(fs);
      server.stop(0);
    }
  }
}

