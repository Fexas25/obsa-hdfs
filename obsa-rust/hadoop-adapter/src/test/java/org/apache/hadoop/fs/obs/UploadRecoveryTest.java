package org.apache.hadoop.fs.obs;

import static org.junit.Assert.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.*;
import java.util.stream.Stream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.junit.*;

/** Real JVM/JNI/HTTP recovery, using local journal fixtures and no real bucket. */
public class UploadRecoveryTest {
  private static final ObjectMapper JSON = new ObjectMapper();
  private HttpServer server;
  private Path directory;
  private OBSFileSystemRust fs;
  private Configuration conf;
  private final AtomicInteger aborts = new AtomicInteger();
  private final AtomicInteger puts = new AtomicInteger();
  private final AtomicInteger completes = new AtomicInteger();
  private volatile int abortStatus = 204;
  private volatile String abortCode;
  private volatile boolean failPart;
  private volatile CountDownLatch abortStarted;
  private volatile CountDownLatch abortRelease;
  private final List<String> targets = Collections.synchronizedList(new ArrayList<>());

  @Before
  public void start() throws Exception {
    directory = Files.createTempDirectory(Paths.get("target"), "upload-recovery-");
    server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.createContext("/bucket/", exchange -> {
      try {
        String method = exchange.getRequestMethod();
        String query = exchange.getRequestURI().getRawQuery();
        int code = 200;
        String body = "";
        if (method.equals("DELETE") && query != null && query.startsWith("uploadId=")) {
          aborts.incrementAndGet();
          CountDownLatch started = abortStarted;
          CountDownLatch release = abortRelease;
          if (started != null && release != null) {
            started.countDown();
            try {
              if (!release.await(10, TimeUnit.SECONDS)) throw new IOException("abort test gate timed out");
            } catch (InterruptedException error) {
              Thread.currentThread().interrupt();
              throw new IOException(error);
            }
          }
          targets.add(exchange.getRequestURI().getPath() + "?"
              + java.net.URLDecoder.decode(query, "UTF-8"));
          code = abortStatus;
          if (code != 204) body = "<Error><Code>" + (abortCode != null ? abortCode : (code == 404 ? "NoSuchUpload" : "AccessDenied"))
              + "</Code><Message>injected cleanup failure</Message></Error>";
        } else if (method.equals("HEAD")) {
          code = 404;
        } else if (method.equals("GET")) {
          body = "<ListBucketResult><IsTruncated>false</IsTruncated></ListBucketResult>";
        } else if (method.equals("POST") && query != null && query.startsWith("uploads")) {
          body = "<InitiateMultipartUploadResult><UploadId>live-id</UploadId></InitiateMultipartUploadResult>";
        } else if (method.equals("PUT")) {
          puts.incrementAndGet();
          if (failPart) {
            code = 403;
            body = "<Error><Code>AccessDenied</Code><Message>injected part failure</Message></Error>";
          } else {
            exchange.getResponseHeaders().set("ETag", "\"part-etag\"");
          }
        } else if (method.equals("POST") && query != null && query.startsWith("uploadId=")) {
          completes.incrementAndGet();
          body = "<CompleteMultipartUploadResult><ETag>\"complete-etag\"</ETag></CompleteMultipartUploadResult>";
        } else {
          code = 400;
        }
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Connection", "close");
        exchange.sendResponseHeaders(code, bytes.length == 0 ? -1 : bytes.length);
        if (bytes.length != 0) exchange.getResponseBody().write(bytes);
      } finally { exchange.close(); }
    });
    server.start();
    conf = new Configuration(false);
    conf.set("fs.obs.endpoint", "127.0.0.1:" + server.getAddress().getPort());
    conf.set("fs.obs.access.key", "test");
    conf.set("fs.obs.secret.key", "test");
    conf.setBoolean("fs.obs.connection.ssl.enabled", false);
    conf.set("fs.obs.bucket.mode", "object");
    conf.set("fs.obs.multipart.threshold", "1");
    conf.set("fs.obs.retry.max.attempts", "0");
    conf.set("fs.obs.upload.journal.dir", directory.resolve("unused-common").toString());
    conf.set("fs.obs.bucket.bucket.upload.journal.dir", directory.resolve("selected").toString());
    fs = open(conf);
  }

  private OBSFileSystemRust open(Configuration config) throws IOException {
    OBSFileSystemRust result = new OBSFileSystemRust();
    result.initialize(URI.create("obs://bucket"), config);
    return result;
  }

  @After
  public void stop() throws Exception {
    try { if (fs != null) fs.close(); }
    finally {
      if (server != null) server.stop(0);
      if (directory != null) {
        try (Stream<Path> paths = Files.walk(directory)) {
          for (Path path : (Iterable<Path>) paths.sorted(Comparator.reverseOrder())::iterator)
            Files.delete(path);
        }
      }
    }
  }

  private Path namespace() throws Exception {
    fs.recoverPendingUploads(1); // initializes and locks the selected namespace
    assertFalse(Files.exists(directory.resolve("unused-common")));
    try (Stream<Path> paths = Files.list(directory.resolve("selected"))) {
      return paths.findFirst().get();
    }
  }

  private Path record(Path namespace, String key, String id) throws Exception {
    byte[] identity = JSON.writeValueAsBytes(Arrays.asList(key, id));
    StringBuilder name = new StringBuilder();
    for (byte b : MessageDigest.getInstance("SHA-1").digest(identity)) name.append(String.format("%02x", b & 255));
    ObjectNode record = JSON.createObjectNode().put("version", 1)
        .put("scope", namespace.getFileName().toString()).put("key", key).put("upload_id", id);
    Path path = namespace.resolve(name + ".json");
    Files.write(path, JSON.writeValueAsBytes(record));
    return path;
  }

  @Test
  public void partialFailuresRetainRecordsAnd404IsIdempotent() throws Exception {
    Path namespace = namespace();
    Path a = record(namespace, "orphan-a", "id-a");
    Path b = record(namespace, "目录/a +?", "id+ /&=");
    abortStatus = 403;
    OBSFileSystemRust.UploadRecoveryReport failed = fs.recoverPendingUploads(1);
    assertEquals(1, failed.getErrors().size());
    assertEquals(0, failed.getAborted());
    assertTrue(Files.exists(a) && Files.exists(b));
    assertThrows(UnsupportedOperationException.class, () -> failed.getErrors().clear());
    abortStatus = 204;
    assertEquals(1, fs.recoverPendingUploads(1).getAborted());
    abortStatus = 404;
    assertEquals(1, fs.recoverPendingUploads(1).getAborted());
    assertFalse(Files.exists(a) || Files.exists(b));
    assertEquals(0, fs.recoverPendingUploads(10).getAborted());
    assertEquals(3, aborts.get());
    assertEquals(new HashSet<>(Arrays.asList("/bucket/orphan-a?uploadId=id-a",
        "/bucket/目录/a +??uploadId=id+ /&=")), new HashSet<>(targets));
  }

  @Test
  public void abortUploadDiscardsBufferedAndMultipartCreateWithoutCommit() throws Exception {
    for (boolean multipart : new boolean[] {false, true}) {
      fs.close();
      conf.set("fs.obs.multipart.threshold", multipart ? "1" : "128");
      fs = open(conf);
      org.apache.hadoop.fs.Path path = new org.apache.hadoop.fs.Path("/discard");
      try (FSDataOutputStream output = multipart
          ? fs.createNonRecursive(path, org.apache.hadoop.fs.permission.FsPermission.getFileDefault(),
              true, 4096, (short) 1, 64L * 1024 * 1024, null)
          : fs.create(path)) {
        output.write(1);
        fs.abortUpload(output);
        fs.abortUpload(output);
        assertThrows(IOException.class, () -> output.write(2));
        assertThrows(IOException.class, () -> output.flush());
      }
      assertEquals(multipart ? 1 : 0, aborts.get());
      assertEquals(0, fs.recoverPendingUploads(10).getAborted());
    }
    assertEquals(0, puts.get());
    assertEquals(0, completes.get());
  }

  @Test
  public void abortRejectsForeignStreamsWithoutConsumingThem() throws Exception {
    try (OBSFileSystemRust other = open(conf);
        FSDataOutputStream output = fs.create(new org.apache.hadoop.fs.Path("/owned"));
        FSDataOutputStream foreign = new FSDataOutputStream(new java.io.ByteArrayOutputStream(), null)) {
      output.write(1);
      assertThrows(IllegalArgumentException.class, () -> other.abortUpload(output));
      assertThrows(IllegalArgumentException.class, () -> fs.abortUpload(foreign));
      output.write(2);
      foreign.write(3);
      fs.abortUpload(output);
    }
    assertEquals(1, aborts.get());
    assertEquals(0, completes.get());
  }

  @Test
  public void abortAfterFilesystemCloseReportsIoAndConsumesJavaHandle() throws Exception {
    fs.close();
    conf.set("fs.obs.multipart.threshold", "128");
    fs = open(conf);
    try (FSDataOutputStream output = fs.create(new org.apache.hadoop.fs.Path("/closed-owner"))) {
      output.write(1);
      fs.close();
      assertThrows(IOException.class, () -> fs.abortUpload(output));
      fs.abortUpload(output);
      assertThrows(IOException.class, () -> output.write(2));
    }
    assertEquals(0, puts.get());
    assertEquals(0, completes.get());
    assertEquals(0, aborts.get());
  }

  @Test
  public void abortAlreadyCommittedStreamDoesNotDeleteOrRecommit() throws Exception {
    FSDataOutputStream output = fs.create(new org.apache.hadoop.fs.Path("/committed"));
    output.write(1);
    output.close();
    fs.abortUpload(output);
    fs.abortUpload(output);
    output.close();
    assertEquals(1, puts.get());
    assertEquals(1, completes.get());
    assertEquals(0, aborts.get());
  }

  @Test
  public void filesystemCloseDoesNotReleaseInFlightAbortJournalLock() throws Exception {
    FSDataOutputStream output = fs.create(new org.apache.hadoop.fs.Path("/aborting"));
    output.write(1);
    abortStarted = new CountDownLatch(1);
    abortRelease = new CountDownLatch(1);
    ExecutorService worker = Executors.newSingleThreadExecutor();
    try (OBSFileSystemRust other = open(conf)) {
      Future<?> abort = worker.submit(() -> { fs.abortUpload(output); return null; });
      assertTrue(abortStarted.await(5, TimeUnit.SECONDS));
      fs.close();
      assertThrows(IOException.class, () -> other.recoverPendingUploads(1));
      abortRelease.countDown();
      abort.get(5, TimeUnit.SECONDS);
      assertEquals(0, other.recoverPendingUploads(10).getAborted());
      assertEquals(1, aborts.get());
      assertEquals(0, completes.get());
    } finally {
      abortRelease.countDown();
      worker.shutdown();
      assertTrue(worker.awaitTermination(10, TimeUnit.SECONDS));
      output.close();
    }
  }

  @Test
  public void failedAbortClosesStreamAndRetainsRecoverableJournal() throws Exception {
    Path namespace = namespace();
    abortStatus = 403;
    try (FSDataOutputStream output = fs.create(new org.apache.hadoop.fs.Path("/retained-abort"))) {
      output.write(1);
      IOException error = assertThrows(IOException.class, () -> fs.abortUpload(output));
      assertTrue(error.getMessage().contains("AccessDenied"));
      assertThrows(IOException.class, () -> output.write(2));
      fs.abortUpload(output); // does not reuse a consumed native handle
    }
    // The existing Rust Drop fallback is asynchronous. Recovery skips it while active.
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
    OBSFileSystemRust.UploadRecoveryReport report;
    do {
      report = fs.recoverPendingUploads(10);
      assertTrue("cleanup fallback must finish", System.nanoTime() < deadline);
      Thread.yield();
    } while (report.getActive() != 0);
    assertEquals(1, report.getErrors().size());
    try (Stream<Path> paths = Files.list(namespace)) {
      assertEquals(1, paths.filter(path -> path.toString().endsWith(".json")).count());
    }
    abortStatus = 404;
    assertEquals(1, fs.recoverPendingUploads(10).getAborted());
    assertEquals(0, completes.get());
    assertEquals(0, puts.get());
  }

  @Test
  public void failedUploadWithAbsentMpuDoesNotLeaveARecoveryRecord() throws Exception {
    Path namespace = namespace();
    failPart = true;
    abortStatus = 404;
    FSDataOutputStream output = fs.create(new org.apache.hadoop.fs.Path("/failed-part"));
    try {
      output.write(1);
      IOException error = assertThrows(IOException.class, () -> output.close());
      assertTrue(error.getMessage(), error.getMessage().contains("AccessDenied"));
    } finally { output.close(); }
    try (Stream<Path> paths = Files.list(namespace)) {
      assertEquals(0, paths.filter(path -> path.toString().endsWith(".json")
          || path.toString().endsWith(".pending")).count());
    }
    assertEquals(0, fs.recoverPendingUploads(10).getAborted());
    assertEquals(1, aborts.get());
  }

  @Test
  public void onlyExplicit404NoSuchUploadMayRemoveRecoveryRecord() throws Exception {
    Path pending = record(namespace(), "classification", "known-id");
    for (String error : new String[] {"NoSuchBucket", "NoSuchKey", "", "NoSuchUpload"}) {
      abortStatus = error.equals("NoSuchUpload") ? 403 : 404;
      abortCode = error;
      OBSFileSystemRust.UploadRecoveryReport report = fs.recoverPendingUploads(10);
      assertEquals(0, report.getAborted());
      assertEquals(1, report.getErrors().size());
      assertTrue(Files.exists(pending));
    }
    abortStatus = 404;
    abortCode = "NoSuchUpload";
    OBSFileSystemRust.UploadRecoveryReport report = fs.recoverPendingUploads(10);
    assertEquals(1, report.getAborted());
    assertTrue(report.getErrors().isEmpty());
    assertFalse(Files.exists(pending));
    assertEquals(5, aborts.get());
  }

  @Test
  public void corruptRecordsAndLockConflictsDoNotAbortAnything() throws Exception {
    Path namespace = namespace();
    Path broken = namespace.resolve("broken.pending");
    Files.write(broken, new byte[] {'{'});
    OBSFileSystemRust.UploadRecoveryReport report = fs.recoverPendingUploads(10);
    assertEquals(1, report.getErrors().size());
    assertEquals(0, report.getAborted());
    assertTrue(Files.exists(broken));
    try (OBSFileSystemRust other = open(conf)) {
      assertThrows(IOException.class, () -> other.recoverPendingUploads(1));
      fs.close();
      assertEquals(1, other.recoverPendingUploads(10).getErrors().size());
    }
    assertEquals(0, aborts.get());
  }

  @Test
  public void activeJavaUploadIsSkippedAndCompletionRemovesItsRecord() throws Exception {
    try (FSDataOutputStream output = fs.create(new org.apache.hadoop.fs.Path("/live"))) {
      output.write(1);
      OBSFileSystemRust.UploadRecoveryReport report = fs.recoverPendingUploads(10);
      assertEquals(1, report.getActive());
      assertEquals(0, report.getAborted());
      assertTrue(report.getErrors().isEmpty());
    }
    assertEquals(0, fs.recoverPendingUploads(10).getActive());
    assertEquals(0, aborts.get());
  }

  @Test
  public void closingFilesystemKeepsInFlightRecoveryLockUntilCompletion() throws Exception {
    Path pending = record(namespace(), "in-flight", "id");
    abortStarted = new CountDownLatch(1);
    abortRelease = new CountDownLatch(1);
    ExecutorService worker = Executors.newSingleThreadExecutor();
    try (OBSFileSystemRust other = open(conf)) {
      Future<OBSFileSystemRust.UploadRecoveryReport> recovery =
          worker.submit(() -> fs.recoverPendingUploads(1));
      assertTrue(abortStarted.await(5, TimeUnit.SECONDS));
      fs.close();
      assertThrows(IOException.class, () -> fs.recoverPendingUploads(1));
      assertThrows(IOException.class, () -> other.recoverPendingUploads(1));
      assertTrue(Files.exists(pending));
      abortRelease.countDown();
      OBSFileSystemRust.UploadRecoveryReport report = recovery.get(5, TimeUnit.SECONDS);
      assertEquals(1, report.getAborted());
      assertTrue(report.getErrors().isEmpty());
      assertFalse(Files.exists(pending));
      assertEquals(0, other.recoverPendingUploads(1).getAborted());
      assertEquals(1, aborts.get());
    } finally {
      abortRelease.countDown();
      worker.shutdown();
      assertTrue(worker.awaitTermination(10, TimeUnit.SECONDS));
    }
  }

  @Test
  public void invalidLimitsMissingConfigurationAndClosedHandlesFailBeforeHttp() throws Exception {
    for (int limit : new int[] {Integer.MIN_VALUE, -1, 0, 10001, Integer.MAX_VALUE})
      assertThrows(IllegalArgumentException.class, () -> fs.recoverPendingUploads(limit));
    ObjectNode config = JSON.createObjectNode().put("bucket", "bucket");
    ObjectNode properties = config.putObject("hadoop");
    for (Map.Entry<String, String> entry : conf) properties.put(entry.getKey(), entry.getValue());
    long handle = NativeObs.nativeCreate(config.toString());
    try {
      for (String args : new String[] {"{}", "{\"limit\":null}", "{\"limit\":-1}", "{\"limit\":0}",
          "{\"limit\":10001}", "{\"limit\":1.5}", "{\"limit\":\"1\"}", "{\"limit\":18446744073709551615}"})
        assertThrows(IllegalArgumentException.class, () -> NativeObs.nativeCall(handle, "recoverPendingUploads", args));
    } finally { NativeObs.nativeClose(handle); }
    assertThrows(IllegalArgumentException.class,
        () -> NativeObs.nativeCall(handle, "recoverPendingUploads", "{\"limit\":1}"));
    Configuration missing = new Configuration(conf);
    missing.unset("fs.obs.upload.journal.dir");
    missing.unset("fs.obs.bucket.bucket.upload.journal.dir");
    try (OBSFileSystemRust other = open(missing)) {
      assertThrows(IllegalArgumentException.class, () -> other.recoverPendingUploads(1));
    }
    fs.close();
    assertThrows(IOException.class, () -> fs.recoverPendingUploads(1));
    assertEquals(0, aborts.get());
  }
}
