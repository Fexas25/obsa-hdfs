import com.sun.net.httpserver.HttpServer;
import java.lang.management.ManagementFactory;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hadoop.fs.obs.NativeObs;

public class JniQueuedMemoryProbe {
  public static void main(String[] args) throws Exception {
    if (args.length != 2 || !(args[1].equals("into") || args[1].equals("legacy"))) {
      throw new IllegalArgumentException("Usage: JniQueuedMemoryProbe <1..64 threads> <legacy|into>");
    }
    int count = Integer.parseInt(args[0]), bytes = 8 * 1024 * 1024;
    if (count < 1 || count > 64) throw new IllegalArgumentException("threads must be 1..64");
    boolean into = args[1].equals("into");
    CountDownLatch ready = new CountDownLatch(count), start = new CountDownLatch(1);
    CountDownLatch arrived = new CountDownLatch(1), release = new CountDownLatch(1);
    AtomicReference<Throwable> failure = new AtomicReference<>();
    AtomicInteger total = new AtomicInteger(), gets = new AtomicInteger();
    HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.createContext("/bucket/file", exchange -> {
      try {
        exchange.getResponseHeaders().set("ETag", "\"version\"");
        if (exchange.getRequestMethod().equals("HEAD")) {
          exchange.getResponseHeaders().set("Content-Length", "1");
          exchange.sendResponseHeaders(200, -1);
        } else if (exchange.getRequestMethod().equals("GET")) {
          gets.incrementAndGet();
          arrived.countDown();
          try {
            if (!release.await(30, TimeUnit.SECONDS)) throw new java.io.IOException("gate timed out");
          } catch (InterruptedException error) { throw new java.io.IOException(error); }
          exchange.getResponseHeaders().set("Content-Range", "bytes 0-0/1");
          exchange.sendResponseHeaders(206, 1);
          exchange.getResponseBody().write(7);
        } else {
          exchange.sendResponseHeaders(405, -1);
        }
      } finally { exchange.close(); }
    });
    server.start();
    long fs = 0, input = 0;
    Thread[] workers = new Thread[count];
    byte[] array = new byte[bytes];
    // Only one read can return data; all other readers reach EOF and do not
    // write this region. Allocate/touch it before the memory baseline.
    Arrays.fill(array, (byte) 99);
    try {
      fs = NativeObs.nativeCreate("{\"endpoint\":\"127.0.0.1:" + server.getAddress().getPort()
          + "\",\"bucket\":\"bucket\",\"bucket_mode\":\"object\",\"secure\":false,\"request_timeout_ms\":30000}");
      input = Long.parseLong(NativeObs.nativeCall(fs, "open", "{\"path\":\"/file\"}"));
      final long handle = input;
      NativeObs.nativeRead(handle, 0);
      NativeObs.nativeReadInto(handle, array, 0, 0);
      for (int i = 0; i < count; i++) {
        workers[i] = new Thread(() -> {
          try {
            ready.countDown(); start.await();
            int n;
            if (into) n = NativeObs.nativeReadInto(handle, array, 0, bytes);
            else {
              byte[] result = NativeObs.nativeRead(handle, bytes);
              n = result.length;
              if (n != 0 && result[0] != 7) throw new AssertionError("content");
            }
            total.addAndGet(Math.max(0, n));
          } catch (Throwable error) { failure.compareAndSet(null, error); }
        }, "queued-read-" + i);
        workers[i].start();
      }
      if (!ready.await(10, TimeUnit.SECONDS)) throw new AssertionError("workers not ready");
      com.sun.management.OperatingSystemMXBean os = (com.sun.management.OperatingSystemMXBean)
          ManagementFactory.getOperatingSystemMXBean();
      long before = os.getCommittedVirtualMemorySize();
      if (before < 0) throw new UnsupportedOperationException("committed virtual memory unavailable");
      start.countDown();
      if (!arrived.await(10, TimeUnit.SECONDS)) throw new AssertionError("GET not reached");
      long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
      while (true) {
        int entered = 0;
        for (Thread worker : workers) {
          for (StackTraceElement frame : worker.getStackTrace()) {
            if (frame.getClassName().equals(NativeObs.class.getName()) && frame.getMethodName().startsWith("nativeRead")) {
              entered++; break;
            }
          }
        }
        if (entered == count) break;
        if (System.nanoTime() > deadline) throw new AssertionError("workers did not enter JNI");
        Thread.sleep(10);
      }
      long[] samples = new long[10];
      for (int i = 0; i < samples.length; i++) {
        Thread.sleep(50);
        samples[i] = os.getCommittedVirtualMemorySize();
        if (samples[i] < 0) throw new UnsupportedOperationException("committed virtual memory unavailable");
      }
      Arrays.sort(samples);
      long unblocked = System.nanoTime();
      release.countDown();
      for (Thread worker : workers) worker.join(10000);
      long drain = System.nanoTime() - unblocked;
      for (Thread worker : workers) if (worker.isAlive()) throw new AssertionError("worker stuck");
      if (failure.get() != null) throw new AssertionError(failure.get());
      if (total.get() != 1 || gets.get() != 1 || (into && array[0] != 7)) throw new AssertionError("read correctness");
      System.out.println("{\"mode\":\"" + args[1] + "\",\"threads\":" + count + ",\"request_bytes\":" + bytes
          + ",\"before_commit\":" + before + ",\"blocked_commit\":" + samples[5]
          + ",\"delta_commit\":" + (samples[5] - before) + ",\"drain_ns\":" + drain + "}");
    } finally {
      start.countDown(); release.countDown();
      try {
        for (Thread worker : workers) if (worker != null) worker.join(10000);
      } finally {
        try {
          if (input != 0) NativeObs.nativeCloseStream(input);
        } finally {
          try {
            if (fs != 0) NativeObs.nativeClose(fs);
          } finally { server.stop(0); }
        }
      }
    }
  }
}
