import com.sun.net.httpserver.HttpServer;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hadoop.fs.obs.NativeObs;

/** Verifies exception classes through the real JNI boundary, without cloud access. */
public final class NativeErrorSmokeTest {
  public static void main(String[] args) throws Exception {
    AtomicReference<String> code = new AtomicReference<>("NoSuchBucket");
    HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.createContext("/bucket/", exchange -> {
      try {
        byte[] body = ("<Error><Code>" + code.get() + "</Code></Error>")
            .getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().add("x-obs-request-id", "jni-error-request");
        exchange.sendResponseHeaders(404, body.length);
        exchange.getResponseBody().write(body);
      } finally { exchange.close(); }
    });
    server.start();
    long fs = 0;
    try {
      fs = NativeObs.nativeCreate("{\"endpoint\":\"127.0.0.1:" + server.getAddress().getPort()
          + "\",\"bucket\":\"bucket\",\"secure\":false,\"bucket_mode\":\"object\","
          + "\"access_key\":\"test\",\"secret_key\":\"test\"}");
      for (String value : new String[] {"NoSuchBucket", "NoSuchUpload", "NoSuchCustomDomain", "NoSuchKey", ""}) {
        code.set(value);
        try {
          NativeObs.nativeCall(fs, "list", "{\"path\":\"/\"}");
          throw new AssertionError("expected failure for " + value);
        } catch (IOException error) {
          boolean pathMissing = value.isEmpty() || value.equals("NoSuchKey");
          if ((error instanceof FileNotFoundException) != pathMissing)
            throw new AssertionError("wrong exception class for " + value, error);
          if (!error.getMessage().contains("jni-error-request") || !error.getMessage().contains(value))
            throw new AssertionError("lost error details", error);
        }
      }
      System.out.println("JNI error smoke passed: service 404 versus missing path, error code and request ID");
    } finally {
      if (fs != 0) NativeObs.nativeClose(fs);
      server.stop(0);
    }
  }
}
