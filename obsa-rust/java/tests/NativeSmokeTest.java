import com.sun.net.httpserver.HttpServer;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.hadoop.fs.obs.NativeObs;

/** Runs against a loopback HTTP server; no cloud account or credentials needed. */
public final class NativeSmokeTest {
  private static void check(boolean result, String message) {
    if (!result) throw new AssertionError(message);
  }

  private interface CheckedCall { void run() throws Exception; }

  private static void invalid(CheckedCall call) throws Exception {
    try {
      call.run();
      throw new AssertionError("invalid direct read accepted");
    } catch (IllegalArgumentException expected) {
    }
  }

  private static void directReads(long input, byte[] payload) throws Exception {
    ByteBuffer owner = ByteBuffer.allocateDirect(32);
    for (int i = 0; i < owner.capacity(); i++) owner.put(i, (byte) 77);
    owner.position(5).limit(21);
    ByteBuffer slice = owner.slice();
    owner.clear();
    NativeObs.nativeSeek(input, 0);
    check(NativeObs.nativeReadDirect(input, slice, 3, 8) == 8, "direct slice count");
    for (int i = 0; i < owner.capacity(); i++)
      check(owner.get(i) == (i >= 8 && i < 16 ? payload[i - 8] : 77), "direct sentinel");
    check(slice.position() == 0, "low-level read does not change buffer position");
    invalid(() -> NativeObs.nativeReadDirect(input, ByteBuffer.allocate(32), 0, 1));
    invalid(() -> NativeObs.nativeReadDirect(input, owner.asReadOnlyBuffer(), 0, 1));
    invalid(() -> NativeObs.nativeReadDirect(input, null, 0, 0));
    invalid(() -> NativeObs.nativeReadDirect(-1, owner, 0, 0));
    for (int[] bounds : new int[][] {{-1, 1}, {0, -1}, {30, 3}, {Integer.MAX_VALUE, 1}, {1, Integer.MAX_VALUE}})
      invalid(() -> NativeObs.nativeReadDirect(input, owner, bounds[0], bounds[1]));
    check(NativeObs.nativeRead(input, 1)[0] == payload[8], "invalid calls preserve stream position");
    NativeObs.nativeSeek(input, payload.length);
    check(NativeObs.nativeReadDirect(input, ByteBuffer.allocateDirect(0), 0, 0) == 0, "direct empty");
    check(NativeObs.nativeReadDirect(input, owner, 0, 1) == -1, "direct EOF");
  }

  public static void main(String[] args) throws Exception {
    ConcurrentHashMap<String, byte[]> objects = new ConcurrentHashMap<>();
    HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.createContext(
        "/bucket/",
        exchange -> {
          try {
            String key = exchange.getRequestURI().getPath().substring("/bucket/".length());
            String method = exchange.getRequestMethod();
            byte[] data = objects.get(key);
            if (method.equals("PUT")) {
              objects.put(key, exchange.getRequestBody().readAllBytes());
              exchange.getResponseHeaders().add("ETag", "\"mock\"");
              exchange.sendResponseHeaders(200, -1);
            } else if (method.equals("HEAD")) {
              if (data != null)
                exchange.getResponseHeaders().add("Content-Length", Integer.toString(data.length));
              exchange.sendResponseHeaders(data == null ? 404 : 200, -1);
            } else if (method.equals("GET") && data != null) {
              exchange.getResponseHeaders().add("ETag", "\"mock\"");
              check(
                  exchange.getRequestHeaders().getFirst("x-hws-fs-client") == null,
                  "GET must not contain fs-client");
              String range = exchange.getRequestHeaders().getFirst("Range");
              if (range != null) {
                String[] pair = range.substring(6).split("-");
                exchange
                    .getResponseHeaders()
                    .add("Content-Range", "bytes " + pair[0] + "-" + pair[1] + "/" + data.length);
                data =
                    Arrays.copyOfRange(
                        data, Integer.parseInt(pair[0]), Integer.parseInt(pair[1]) + 1);
              }
              exchange.sendResponseHeaders(range == null ? 200 : 206, data.length);
              exchange.getResponseBody().write(data);
            } else if (method.equals("GET") && key.isEmpty()) {
              byte[] xml =
                  "<ListBucketResult><IsTruncated>false</IsTruncated></ListBucketResult>"
                      .getBytes(StandardCharsets.UTF_8);
              exchange.sendResponseHeaders(200, xml.length);
              exchange.getResponseBody().write(xml);
            } else {
              exchange.sendResponseHeaders(404, -1);
            }
          } finally {
            exchange.close();
          }
        });
    server.start();
    long fs = 0;
    try {
      String config =
          "{\"endpoint\":\"127.0.0.1:"
              + server.getAddress().getPort()
              + "\",\"bucket\":\"bucket\",\"secure\":false,\"bucket_mode\":\"object\",\"access_key\":\"test\",\"secret_key\":\"test\"}";
      fs = NativeObs.nativeCreate(config);
      check(fs > 0, "valid filesystem handle");
      long out =
          Long.parseLong(
              NativeObs.nativeCall(fs, "create", "{\"path\":\"/file\",\"overwrite\":true}"));
      byte[] payload = "hello native OBS".getBytes(StandardCharsets.UTF_8);
      invalid(() -> NativeObs.nativeReadDirect(out, ByteBuffer.allocateDirect(1), 0, 1));
      try {
        NativeObs.nativeSkip(out, 1);
        throw new AssertionError("output stream skip must be rejected");
      } catch (IllegalArgumentException expected) {
        // Subsequent writes prove the rejected output handle remains usable.
      }
      for (int[] bounds :
          new int[][] {{-1, 1}, {0, -1}, {Integer.MAX_VALUE, 1}, {1, Integer.MAX_VALUE}}) {
        boolean rejected = false;
        try {
          NativeObs.nativeWrite(out, payload, bounds[0], bounds[1]);
        } catch (IllegalArgumentException expected) {
          rejected = true;
        }
        check(rejected, "write invalid bounds");
      }
      check(NativeObs.nativeWrite(out, payload, payload.length, 0) == 0, "zero length write");
      check(
          NativeObs.nativeWrite(out, payload, 0, payload.length) == payload.length, "write count");
      NativeObs.nativeFlush(out);
      NativeObs.nativeCloseStream(out);
      check(Arrays.equals(objects.get("file"), payload), "HTTP PUT payload");
      String status = NativeObs.nativeCall(fs, "status", "{\"path\":\"/file\"}");
      check(status.contains("\"length\":" + payload.length), "file status length");
      long input = Long.parseLong(NativeObs.nativeCall(fs, "open", "{\"path\":\"/file\"}"));
      try {
        NativeObs.nativeAbortStream(input);
        throw new AssertionError("input stream abort must be rejected");
      } catch (UnsupportedOperationException expected) {
        // Subsequent reads below prove the rejected input handle remains usable.
      }

      check(Arrays.equals(NativeObs.nativeRead(input, payload.length), payload), "native read");
      check(NativeObs.nativeRead(input, 1).length == 0, "EOF");
      check(NativeObs.nativeSkip(input, 1) == 0, "skip at EOF");
      NativeObs.nativeSeek(input, 0);
      check(NativeObs.nativeSkip(input, 6) == 6, "skip count");
      check(NativeObs.nativeSkip(input, -1) == 0, "negative skip");
      check(
          new String(NativeObs.nativeRead(input, 6), StandardCharsets.UTF_8).equals("native"),
          "seek/read");
      NativeObs.nativeSeek(input, 0);
      byte[] destination = new byte[payload.length + 6];
      Arrays.fill(destination, (byte) 99);
      check(
          NativeObs.nativeReadInto(input, destination, 3, payload.length) == payload.length,
          "readInto count");
      check(
          Arrays.equals(Arrays.copyOfRange(destination, 3, 3 + payload.length), payload),
          "readInto payload");
      check(
          destination[0] == 99 && destination[destination.length - 1] == 99, "readInto sentinels");
      check(NativeObs.nativeReadInto(input, destination, 0, 0) == 0, "readInto zero length");
      check(NativeObs.nativeReadInto(input, destination, 0, 1) == -1, "readInto EOF");
      for (int[] bounds :
          new int[][] {{-1, 1}, {0, -1}, {Integer.MAX_VALUE, 1}, {1, Integer.MAX_VALUE}}) {
        boolean rejected = false;
        try {
          NativeObs.nativeReadInto(input, destination, bounds[0], bounds[1]);
        } catch (IllegalArgumentException expected) {
          rejected = true;
        }
        check(rejected, "readInto invalid bounds");
      }
      boolean invalidBounds = false;
      try {
        NativeObs.nativeRead(input, -1);
      } catch (IllegalArgumentException expected) {
        invalidBounds = true;
      }
      check(invalidBounds, "negative read validation");
      directReads(input, payload);
      NativeObs.nativeCloseStream(input);
      invalid(() -> NativeObs.nativeReadDirect(input, ByteBuffer.allocateDirect(1), 0, 1));
      NativeObs.nativeCloseStream(input);
      byte[] largePayload = new byte[16 * 1024 * 1024 + 8];
      Arrays.fill(largePayload, (byte) 31);
      objects.put("large", largePayload);
      long large = Long.parseLong(NativeObs.nativeCall(fs, "open", "{\"path\":\"/large\"}"));
      try {
        ByteBuffer largeBuffer = ByteBuffer.allocateDirect(largePayload.length);
        check(NativeObs.nativeReadDirect(large, largeBuffer, 0, largeBuffer.capacity()) == 16 * 1024 * 1024,
            "direct per-call cap");
        check(largeBuffer.get(0) == 31 && largeBuffer.get(16 * 1024 * 1024 - 1) == 31,
            "large direct content");
        check(largeBuffer.get(16 * 1024 * 1024) == 0, "direct cap sentinel");
        check(NativeObs.nativeReadDirect(large, largeBuffer, 16 * 1024 * 1024, 8) == 8,
            "direct remainder");
        check(largeBuffer.get(largeBuffer.capacity() - 1) == 31, "direct remainder content");
      } finally {
        NativeObs.nativeCloseStream(large);
      }
      NativeObs.nativeClose(fs);
      boolean closed = false;
      try {
        NativeObs.nativeCall(fs, "status", "{}");
      } catch (IllegalArgumentException expected) {
        closed = true;
      }
      check(closed, "closed filesystem handle rejection");
      System.out.println(
          "JNI smoke test passed: create/write/close/status/open/read/EOF/seek/invalid"
              + " bounds/handle cleanup");
    } finally {
      if (fs != 0) NativeObs.nativeClose(fs);
      server.stop(0);
    }
  }
}
