package org.apache.hadoop.fs.obs;

import com.sun.net.httpserver.*;
import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;
import javax.xml.parsers.DocumentBuilderFactory;
import org.w3c.dom.NodeList;

/** Reusable loopback OBS/PFS simulator. No cloud credentials or resources. */
final class LocalObsServer implements AutoCloseable {
  final HttpServer server;
  final NavigableMap<String, byte[]> data = new ConcurrentSkipListMap<>();
  final AtomicInteger heads = new AtomicInteger();
  final AtomicInteger gets = new AtomicInteger();
  final AtomicInteger modifies = new AtomicInteger();
  volatile boolean acknowledgeModifyWithoutAllocating;
  final AtomicInteger lists = new AtomicInteger();
  final Map<String, Map<String, String>> metadata = new java.util.concurrent.ConcurrentHashMap<>();
  final AtomicInteger metadataWrites = new AtomicInteger();
  final AtomicInteger flatLists = new AtomicInteger();
  final AtomicInteger directoryLists = new AtomicInteger();
  volatile boolean malformedContinuation;
  volatile String listErrorCode;
  volatile boolean omitHeadLength;
  volatile String headLengthOverride;
  volatile int headStatus = 200;
  volatile String getEtagOverride;
  final Queue<String> listResponses = new java.util.concurrent.ConcurrentLinkedQueue<>();
  boolean pfs;
  volatile boolean failCopy;
  volatile boolean malformedCopyEtag;
  volatile String failPutKey;
  volatile boolean failPutAfterCommit;
  final AtomicInteger putFailures = new AtomicInteger();
  volatile String missingDeleteKey;
  final AtomicInteger deleteMisses = new AtomicInteger();
  volatile int failedRangeStart = -1;
  final AtomicInteger rangeFailures = new AtomicInteger();
  /** Latest "sse-c algorithm|sse type" header pair seen per HTTP method. */
  final Map<String, String> sseHeadersByMethod = new java.util.concurrent.ConcurrentHashMap<>();
  /** Access key and security token of the latest signed request (signatures are not checked). */
  volatile String lastAccessKey;
  volatile String lastSecurityToken;
  /** Report the whole-object CRC32C (unsigned decimal) on HEAD, as OBS may. */
  volatile boolean sendCrc32c;
  /** Directory key → raw JSON body last PUT with ?x-obs-accesslabel. */
  final Map<String, String> accessLabels = new java.util.concurrent.ConcurrentHashMap<>();

  LocalObsServer(boolean pfs) throws IOException {
    this.pfs = pfs;
    server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.createContext("/bucket/", this::request);
    server.start();
  }

  String endpoint() {
    return "127.0.0.1:" + server.getAddress().getPort();
  }

  public void close() {
    server.stop(0);
  }

  private static Map<String, String> query(URI uri) throws Exception {
    Map<String, String> values = new HashMap<>();
    String raw = uri.getRawQuery();
    if (raw != null)
      for (String item : raw.split("&")) {
        String[] pair = item.split("=", 2);
        values.put(
            URLDecoder.decode(pair[0], "UTF-8"),
            pair.length > 1 ? URLDecoder.decode(pair[1], "UTF-8") : "");
      }
    return values;
  }

  private static String xml(String text) {
    return text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;");
  }

  private void send(HttpExchange e, int code, String body) throws IOException {
    byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
    e.sendResponseHeaders(code, bytes.length == 0 ? -1 : bytes.length);
    if (bytes.length > 0) e.getResponseBody().write(bytes);
  }

  private void error(HttpExchange e, int status, String code) throws IOException {
    send(e, status, "<Error><Code>" + code + "</Code><Message>mock error</Message></Error>");
  }

  private void request(HttpExchange e) throws IOException {
    try {
      e.getResponseHeaders().set("Connection", "close");
      String authorization = e.getRequestHeaders().getFirst("Authorization");
      if (authorization != null && authorization.startsWith("OBS ") && authorization.contains(":"))
        lastAccessKey = authorization.substring(4, authorization.lastIndexOf(':'));
      lastSecurityToken = e.getRequestHeaders().getFirst("x-obs-security-token");
      sseHeadersByMethod.put(e.getRequestMethod(), String.valueOf(
          e.getRequestHeaders().getFirst("x-obs-server-side-encryption-customer-algorithm")) + "|"
          + e.getRequestHeaders().getFirst("x-obs-server-side-encryption"));
      String key = e.getRequestURI().getPath().substring("/bucket/".length());
      String method = e.getRequestMethod();
      Map<String, String> q = query(e.getRequestURI());
      byte[] current = data.get(key);
      if (q.containsKey("x-obs-accesslabel")) {
        // PFS directory access labels; the key is the directory without trailing slash.
        if (method.equals("PUT")) {
          accessLabels.put(key, new String(body(e), StandardCharsets.UTF_8));
          send(e, 200, "");
        } else if (method.equals("GET")) {
          e.getResponseHeaders().set("Content-Type", "application/json");
          send(e, 200, accessLabels.getOrDefault(key, "{\"accesslabel\":[]}"));
        } else {
          accessLabels.remove(key);
          send(e, 204, "");
        }
        return;
      }
      if (method.equals("HEAD")) {
        heads.incrementAndGet();
        if (current == null) {
          e.sendResponseHeaders(404, -1);
          return;
        }
        if (!omitHeadLength)
          e.getResponseHeaders().set("Content-Length", headLengthOverride != null
              ? headLengthOverride : Integer.toString(current.length));
        e.getResponseHeaders()
            .set("ETag", "\"" + Integer.toHexString(Arrays.hashCode(current)) + "\"");
        if (sendCrc32c) {
          org.apache.hadoop.util.PureJavaCrc32C crc = new org.apache.hadoop.util.PureJavaCrc32C();
          crc.update(current, 0, current.length);
          e.getResponseHeaders().set("x-obs-checksum-crc32c", Long.toString(crc.getValue()));
        }
        e.getResponseHeaders().set("x-obs-meta-mode", key.endsWith("/") ? "40755" : "100644");
        metadata
            .getOrDefault(key, Collections.emptyMap())
            .forEach((name, value) -> e.getResponseHeaders().set("x-obs-meta-" + name, value));
        e.sendResponseHeaders(headStatus, -1);
      } else if (method.equals("GET") && key.isEmpty()) {
        lists.incrementAndGet();
        if (listErrorCode != null) {
          e.getResponseHeaders().set("x-obs-request-id", "hadoop-list-error");
          error(e, 404, listErrorCode);
          return;
        }
        String scripted = listResponses.poll();
        if (scripted != null) {
          send(e, 200, scripted);
          return;
        }
        if (malformedContinuation && q.containsKey("marker")) {
          send(e, 200, "<ListBucketResult>");
          return;
        }
        String prefix = q.getOrDefault("prefix", "");
        String delimiter = q.getOrDefault("delimiter", "");
        if (delimiter.isEmpty()) flatLists.incrementAndGet();
        else directoryLists.incrementAndGet();
        String marker = q.getOrDefault("marker", "");
        int maximum = Math.min(1000, Integer.parseInt(q.getOrDefault("max-keys", "1000")));
        NavigableMap<String, Integer> entries = new TreeMap<>();
        for (Map.Entry<String, byte[]> entry : data.entrySet()) {
          String name = entry.getKey();
          if (!name.startsWith(prefix)) continue;
          String tail = name.substring(prefix.length());
          int slash = tail.indexOf('/');
          if (delimiter.equals("/") && slash >= 0)
            entries.put(prefix + tail.substring(0, slash + 1), -1);
          else entries.put(name, entry.getValue().length);
        }
        entries = entries.tailMap(marker, false);
        boolean truncated = entries.size() > maximum;
        StringBuilder body =
            new StringBuilder("<ListBucketResult><IsTruncated>")
                .append(truncated)
                .append("</IsTruncated>");
        String last = "";
        int count = 0;
        for (Map.Entry<String, Integer> entry : entries.entrySet()) {
          if (count++ == maximum) break;
          String name = entry.getKey();
          last = name;
          if (entry.getValue() == -1) {
            body.append("<CommonPrefixes><Prefix>")
                .append(xml(name))
                .append("</Prefix></CommonPrefixes>");
          } else {
            body.append("<Contents><Key>")
                .append(xml(name))
                .append("</Key><Size>")
                .append(entry.getValue())
                .append("</Size><Mode>")
                .append(name.endsWith("/") ? "40755" : "100644")
                .append("</Mode></Contents>");
          }
        }
        if (truncated) body.append("<NextMarker>").append(xml(last)).append("</NextMarker>");
        send(e, 200, body.append("</ListBucketResult>").toString());
      } else if (method.equals("GET")) {
        gets.incrementAndGet();
        if (current == null) {
          error(e, 404, "NoSuchKey");
          return;
        }
        String range = e.getRequestHeaders().getFirst("Range");
        String tag = Integer.toHexString(Arrays.hashCode(current));
        String required = e.getRequestHeaders().getFirst("If-Match");
        if (required != null && !required.equals("\"" + tag + "\"")) {
          error(e, 412, "PreconditionFailed");
          return;
        }
        e.getResponseHeaders().set("ETag", getEtagOverride != null ? getEtagOverride : "\"" + tag + "\"");
        int status = 200;
        if (range != null) {
          String[] pair = range.substring(6).split("-");
          int start = Integer.parseInt(pair[0]);
          if (start == failedRangeStart
              && rangeFailures.getAndUpdate(n -> Math.max(0, n - 1)) > 0) {
            error(e, 503, "InjectedRangeFailure");
            return;
          }
          int end = Math.min(Integer.parseInt(pair[1]), current.length - 1);
          if (start > end) {
            error(e, 416, "InvalidRange");
            return;
          }
          e.getResponseHeaders()
              .set("Content-Range", "bytes " + start + "-" + end + "/" + current.length);
          current = Arrays.copyOfRange(current, start, end + 1);
          status = 206;
        }
        e.sendResponseHeaders(status, current.length == 0 ? -1 : current.length);
        if (current.length > 0) e.getResponseBody().write(current);
      } else if (method.equals("PUT")) {
        if (q.containsKey("metadata")) {
          if (current == null) {
            error(e, 404, "NoSuchKey");
            return;
          }
          Map<String, String> replaced = new HashMap<>();
          e.getRequestHeaders()
              .forEach(
                  (name, values) -> {
                    String lower = name.toLowerCase(Locale.ROOT);
                    if (lower.startsWith("x-obs-meta-"))
                      replaced.put(lower.substring(11), values.get(0));
                  });
          metadata.put(key, replaced);
          metadataWrites.incrementAndGet();
        } else if (q.containsKey("folder")) {
          String file = key.endsWith("/") ? key.substring(0, key.length() - 1) : key;
          if (data.containsKey(file)) {
            error(e, 409, "FileAlreadyExists");
            return;
          }
          int slash = file.lastIndexOf('/');
          if (slash >= 0 && !data.containsKey(file.substring(0, slash + 1))) {
            error(e, 404, "ParentNotFound");
            return;
          }
          data.put(file + "/", new byte[0]);
        } else if (q.containsKey("truncate")) {
          if (current == null) {
            error(e, 404, "NoSuchKey");
            return;
          }
          int n = Integer.parseInt(q.get("length"));
          if (n > current.length) {
            error(e, 409, "TruncateBiggerNotAllowed");
            return;
          }
          data.put(key, Arrays.copyOf(current, n));
        } else if (q.containsKey("modify")) {
          modifies.incrementAndGet();
          if (current == null) {
            error(e, 404, "NoSuchKey");
            return;
          }
          // Numeric boundary tests model an enormous existing file without
          // allocating it. Consume the request, but do not modify fixture data.
          if (acknowledgeModifyWithoutAllocating) {
            body(e);
            send(e, 200, "");
            return;
          }
          int offset = Integer.parseInt(q.get("position"));
          byte[] bytes = body(e);
          byte[] next = Arrays.copyOf(current, Math.max(current.length, offset + bytes.length));
          System.arraycopy(bytes, 0, next, offset, bytes.length);
          data.put(key, next);
        } else if (e.getRequestHeaders().getFirst("x-obs-copy-source") != null) {
          if (failCopy) {
            send(e, 200, "<Error><Code>CopyFailed</Code><Message>injected</Message></Error>");
            return;
          }
          String source =
              URLDecoder.decode(e.getRequestHeaders().getFirst("x-obs-copy-source"), "UTF-8")
                  .substring("/bucket/".length());
          byte[] bytes = data.get(source);
          if (bytes == null) {
            error(e, 404, "NoSuchKey");
            return;
          }
          data.put(key, bytes.clone());
          if (malformedCopyEtag) {
            send(e, 200, "<CopyObjectResult><ETag>&quot;&quot;copied&quot;&quot;</ETag></CopyObjectResult>");
            return;
          }
          send(
              e,
              200,
              "<CopyObjectResult><ETag>&quot;"
                  + Integer.toHexString(Arrays.hashCode(bytes))
                  + "&quot;</ETag></CopyObjectResult>");
          return;
        } else {
          if (pfs && data.containsKey(key + "/")) {
            error(e, 409, "FileAlreadyExists");
            return;
          }
          byte[] payload = body(e);
          if (key.equals(failPutKey)) {
            putFailures.incrementAndGet();
            if (failPutAfterCommit) data.put(key, payload);
            error(e, 503, "ServiceUnavailable");
            return;
          }
          data.put(key, payload);
        }
        e.getResponseHeaders().set("ETag", "\"mock\"");
        send(e, 200, "");
      } else if (method.equals("DELETE")) {
        if (missingDeleteKey != null
            && (key.equals(missingDeleteKey) || key.equals(missingDeleteKey + "/"))) {
          deleteMisses.incrementAndGet();
          error(e, 404, "NoSuchKey");
          return;
        }
        if (pfs
            && key.endsWith("/")
            && data.keySet().stream().anyMatch(k -> k.startsWith(key) && !k.equals(key))) {
          error(e, 409, "DirectoryNotEmpty");
          return;
        }
        if (data.remove(key) == null && pfs) {
          error(e, 404, "NoSuchKey");
          return;
        }
        send(e, 204, "");
      } else if (method.equals("POST") && q.containsKey("delete")) {
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        factory.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true);
        NodeList keys =
            factory.newDocumentBuilder().parse(e.getRequestBody()).getElementsByTagName("Key");
        for (int i = 0; i < keys.getLength(); i++) data.remove(keys.item(i).getTextContent());
        send(e, 200, "<DeleteResult/>");
      } else if (method.equals("POST") && q.containsKey("rename")) {
        if (current == null) {
          error(e, 404, "NoSuchKey");
          return;
        }
        String destination = q.get("name");
        if (key.endsWith("/") && !destination.endsWith("/")) destination += "/";
        Map<String, byte[]> moved = new HashMap<>();
        for (Map.Entry<String, byte[]> entry : new ArrayList<>(data.entrySet())) {
          if (entry.getKey().equals(key) || (key.endsWith("/") && entry.getKey().startsWith(key))) {
            moved.put(destination + entry.getKey().substring(key.length()), entry.getValue());
            data.remove(entry.getKey());
          }
        }
        data.putAll(moved);
        send(e, 200, "");
      } else error(e, 501, "NotImplemented");
    } catch (Exception ex) {
      error(e, 500, "MockFailure");
    } finally {
      e.close();
    }
  }

  private static byte[] body(HttpExchange e) throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    byte[] buf = new byte[8192];
    int n;
    while ((n = e.getRequestBody().read(buf)) >= 0) out.write(buf, 0, n);
    return out.toByteArray();
  }
}
