package org.apache.hadoop.fs.obs;

import static org.junit.Assert.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import org.junit.Test;

public class NativeListingLifecycleTest {
  private static final ObjectMapper JSON = new ObjectMapper();

  private long open(LocalObsServer server) throws IOException {
    ObjectNode config = JSON.createObjectNode().put("bucket", "bucket");
    config
        .putObject("hadoop")
        .put("fs.obs.endpoint", server.endpoint())
        .put("fs.obs.path.style.access", "true")
        .put("fs.obs.access.key", "test")
        .put("fs.obs.secret.key", "test")
        .put("fs.obs.bucket.mode", "object")
        .put("fs.obs.connection.ssl.enabled", "false");
    return NativeObs.nativeCreate(config.toString());
  }

  private long cursor(long fs) throws IOException {
    return Long.parseLong(NativeObs.nativeCall(fs, "listCursorOpen", "{\"path\":\"/\"}"));
  }

  private String next(long fs, long cursor) throws IOException {
    return NativeObs.nativeCall(fs, "listCursorNext", "{\"cursor\":" + cursor + "}");
  }

  private void assertClosed(long fs, long cursor) throws IOException {
    try {
      next(fs, cursor);
      fail("cursor must have been released");
    } catch (IOException expected) {
      assertTrue(expected.getMessage().contains("listing"));
    }
  }

  @Test
  public void eofAndExplicitReleaseRetireHandles() throws Exception {
    try (LocalObsServer server = new LocalObsServer(false)) {
      long fs = open(server);
      try {
        long first = cursor(fs);
        assertTrue(JSON.readTree(next(fs, first)).path("next_marker").isNull());
        assertClosed(fs, first);
        long second = cursor(fs);
        NativeObs.nativeReleaseListing(second);
        NativeObs.nativeReleaseListing(second);
        assertClosed(fs, second);
        long failed = cursor(fs);
        server.listResponses.add("<ListBucketResult>");
        try {
          next(fs, failed);
          fail("malformed response must fail");
        } catch (IOException expected) {
        }
        assertClosed(fs, failed);
      } finally {
        NativeObs.nativeClose(fs);
      }
    }
  }

  @Test
  public void handlesAreOwnerScopedAndFilesystemCloseRetiresThem() throws Exception {
    try (LocalObsServer server = new LocalObsServer(false)) {
      long first = open(server), second = open(server);
      try {
        long cursor = cursor(first);
        assertClosed(second, cursor);
        NativeObs.nativeClose(first);
        // Cleanup is idempotent after owner close, and another owner cannot use it.
        NativeObs.nativeReleaseListing(cursor);
        assertClosed(second, cursor);
      } finally {
        NativeObs.nativeClose(first);
        NativeObs.nativeClose(second);
      }
    }
  }

  @Test
  public void queuedAbandonedIteratorReleasesNativeHandle() throws Exception {
    try (LocalObsServer server = new LocalObsServer(false)) {
      long fs = open(server);
      try {
        long cursor = cursor(fs);
        ListingCleanup cleanup = new ListingCleanup(new Object(), cursor);
        // Exercise the queue deterministically instead of depending on System.gc().
        cleanup.enqueue();
        java.lang.reflect.Field field = ListingCleanup.class.getDeclaredField("LIVE");
        field.setAccessible(true);
        java.util.Set<?> live = (java.util.Set<?>) field.get(null);
        long deadline = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(5);
        while (live.contains(cleanup) && System.nanoTime() < deadline) Thread.sleep(10);
        assertFalse("queue worker must release abandoned cursor", live.contains(cleanup));
        assertClosed(fs, cursor);
        assertEquals(0, server.lists.get());
      } finally {
        NativeObs.nativeClose(fs);
      }
    }
  }

  @Test
  public void filesystemCloseDuringOpenPreventsLateStreamRegistration() throws Exception {
    java.util.concurrent.CountDownLatch entered = new java.util.concurrent.CountDownLatch(1);
    java.util.concurrent.CountDownLatch resume = new java.util.concurrent.CountDownLatch(1);
    java.util.concurrent.ExecutorService worker = java.util.concurrent.Executors.newSingleThreadExecutor();
    com.sun.net.httpserver.HttpServer server = com.sun.net.httpserver.HttpServer.create(
        new java.net.InetSocketAddress("127.0.0.1", 0), 0);
    server.createContext("/bucket/", exchange -> {
      try {
        if (!exchange.getRequestMethod().equals("HEAD")) {
          exchange.sendResponseHeaders(500, -1);
          return;
        }
        entered.countDown();
        if (!resume.await(5, java.util.concurrent.TimeUnit.SECONDS)) {
          exchange.sendResponseHeaders(500, -1);
          return;
        }
        exchange.getResponseHeaders().set("Content-Length", "4");
        exchange.getResponseHeaders().set("ETag", "\"revision\"");
        exchange.sendResponseHeaders(200, -1);
      } catch (InterruptedException interrupted) {
        Thread.currentThread().interrupt();
      } finally {
        exchange.close();
      }
    });
    server.start();
    long handle = 0;
    try {
      ObjectNode config = JSON.createObjectNode().put("bucket", "bucket")
          .put("endpoint", "127.0.0.1:" + server.getAddress().getPort()).put("secure", false)
          .put("bucket_mode", "object").put("access_key", "test").put("secret_key", "test");
      handle = NativeObs.nativeCreate(config.toString());
      final long owner = handle;
      java.util.concurrent.Future<String> opening = worker.submit(
          () -> NativeObs.nativeCall(owner, "open", "{\"path\":\"/file\"}"));
      assertTrue("Open must reach HEAD before close", entered.await(5, java.util.concurrent.TimeUnit.SECONDS));
      NativeObs.nativeClose(owner);
      resume.countDown();
      try {
        String unexpected = opening.get(5, java.util.concurrent.TimeUnit.SECONDS);
        NativeObs.nativeCloseStream(Long.parseLong(unexpected));
        fail("An open completed after owner close must not publish a stream handle");
      } catch (java.util.concurrent.ExecutionException expected) {
        assertTrue(expected.getCause() instanceof IOException);
        assertTrue(expected.getCause().getMessage(),
            expected.getCause().getMessage().contains("filesystem closed while opening stream"));
      }
    } finally {
      resume.countDown();
      if (handle != 0) NativeObs.nativeClose(handle);
      worker.shutdownNow();
      try {
        assertTrue("Open worker must finish", worker.awaitTermination(5, java.util.concurrent.TimeUnit.SECONDS));
      } finally {
        server.stop(0);
      }
    }
  }
}
