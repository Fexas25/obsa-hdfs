package org.apache.hadoop.fs.obs;

import static org.junit.Assert.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.math.BigInteger;
import org.junit.Test;

public class NativeConfigurationTest {
  private static final ObjectMapper JSON = new ObjectMapper();
  @Test
  public void missingHeadLengthFailsWithoutCachingEmptyFile() throws Exception {
    assertInvalidHeadRecovers(200, true, "Content-Length");
  }

  @Test
  public void unexpectedHeadStatusFailsWithoutCachingMetadata() throws Exception {
    for (int status : new int[] {201, 202, 206}) {
      assertInvalidHeadRecovers(status, false, "object HEAD (expected 200)");
    }
  }

  private void assertInvalidHeadRecovers(int headStatus, boolean omitLength, String expectedMessage)
      throws Exception {
    for (boolean pfs : new boolean[] {false, true}) {
      try (LocalObsServer server = new LocalObsServer(pfs)) {
        server.data.put("file", new byte[] {7, 8});
        server.omitHeadLength = omitLength;
        server.headStatus = headStatus;
        org.apache.hadoop.conf.Configuration config = new org.apache.hadoop.conf.Configuration(false);
        config.set("fs.obs.endpoint", server.endpoint());
        config.setBoolean("fs.obs.connection.ssl.enabled", false);
        config.set("fs.obs.bucket.mode", pfs ? "pfs" : "object");
        config.set("fs.obs.access.key", "test");
        config.set("fs.obs.secret.key", "test");
        config.setInt("fs.obs.retry.max.attempts", 0);
        try (OBSFileSystemRust fs = new OBSFileSystemRust()) {
          fs.initialize(java.net.URI.create("obs://bucket"), config);
          org.apache.hadoop.fs.Path path = new org.apache.hadoop.fs.Path("/file");
          java.io.IOException status = assertThrows(java.io.IOException.class,
              () -> fs.getFileStatus(path));
          assertTrue(status.getMessage(), status.getMessage().contains(expectedMessage));
          assertFalse(status instanceof java.io.FileNotFoundException);
          assertThrows(java.io.IOException.class, () -> fs.open(path));
          assertEquals(0, server.gets.get());
          server.omitHeadLength = false;
          server.headStatus = 200;
          assertEquals(2, fs.getFileStatus(path).getLen());
          try (org.apache.hadoop.fs.FSDataInputStream stream = fs.open(path)) {
            assertEquals(7, stream.read());
            assertEquals(8, stream.read());
            assertEquals(-1, stream.read());
          }
        }
      }
    }
  }

  private static final long EXCESSIVE =
      "32".equals(System.getProperty("sun.arch.data.model")) ? Integer.MAX_VALUE : Long.MAX_VALUE;
  private static final String[][] KEYS = {
    {"connection_maximum", "connection.maximum"},
    {"upload_active_blocks", "upload.active.blocks"},
    {"upload_total_active_blocks", "upload.total.active.blocks"},
    {"readahead_total_chunks", "readahead.total.chunks"}
  };

  private void assertRejected(ObjectNode config, String key) throws Exception {
    assertRejected(config, key, "permit limit");
  }

  private void assertRejected(ObjectNode config, String key, String reason) throws Exception {
    try {
      long fs = NativeObs.nativeCreate(config.toString());
      NativeObs.nativeClose(fs);
      fail("invalid configuration must be rejected");
    } catch (IllegalArgumentException expected) {
      assertTrue(expected.getMessage(), expected.getMessage().contains("fs.obs." + key));
      assertTrue(expected.getMessage().contains(reason));
    }
  }

  @Test
  public void unaddressableUploadCapacityFailsForDirectAndBucketConfiguration() throws Exception {
    BigInteger excessive =
        BigInteger.ONE.shiftLeft("32".equals(System.getProperty("sun.arch.data.model")) ? 31 : 63);
    for (String suffix : new String[] {"size", "threshold"}) {
      String key = "multipart." + suffix;
      assertRejected(
          JSON.createObjectNode().put("multipart_" + suffix, excessive), key, "buffer capacity");
      ObjectNode config = JSON.createObjectNode().put("bucket", "special");
      config
          .putObject("hadoop")
          .put("fs.obs." + key, "16M")
          .put("fs.obs.bucket.special." + key, excessive.toString());
      assertRejected(config, key, "buffer capacity");
    }
  }

  @Test
  public void directRustConfigurationReportsInvalidArgument() throws Exception {
    for (String[] key : KEYS) {
      ObjectNode config = JSON.createObjectNode().put(key[0], EXCESSIVE);
      // No endpoint: validation must reject the count before transport initialization.
      assertRejected(config, key[1]);
    }
  }

  @Test
  public void hadoopBucketOverridesAreValidatedAfterResolution() throws Exception {
    for (String[] key : KEYS) {
      ObjectNode config = JSON.createObjectNode().put("bucket", "special");
      config
          .putObject("hadoop")
          .put("fs.obs." + key[1], "1")
          .put("fs.obs.bucket.special." + key[1], Long.toString(EXCESSIVE));
      assertRejected(config, key[1]);
    }
  }

  @Test
  public void malformedNativeArgumentsFailBeforeRequestsOrMutation() throws Exception {
    for (boolean pfs : new boolean[] {false, true}) {
      try (LocalObsServer server = new LocalObsServer(pfs)) {
        server.data.put("sentinel", new byte[] {7});
        ObjectNode config = JSON.createObjectNode().put("bucket", "bucket")
            .put("endpoint", server.endpoint()).put("secure", false)
            .put("bucket_mode", pfs ? "pfs" : "object")
            .put("access_key", "test").put("secret_key", "test");
        final long fs = NativeObs.nativeCreate(config.toString());
        try {
          for (String malformed : new String[] {"null", "[]", "true", "123", "\"path\""}) {
            assertThrows(IllegalArgumentException.class,
                () -> NativeObs.nativeCall(fs, "status", malformed));
          }
          String[][] invalid = {
              {"delete", "{\"path\":null,\"recursive\":true}"},
              {"delete", "{\"path\":12,\"recursive\":true}"},
              {"delete", "{\"path\":\"/sentinel\",\"recursive\":\"false\"}"},
              {"rename", "{\"path\":\"/sentinel\",\"destination\":42}"},
              {"rename", "{\"path\":\"/sentinel\",\"destination\":\"/target\",\"overwrite\":1}"},
              {"create", "{\"path\":\"/new\",\"permission\":4294967296}"},
              {"create", "{\"path\":\"/new\",\"overwrite\":null}"},
              {"create", "{\"path\":\"/new\",\"create_parent\":\"false\"}"},
              {"mkdirs", "{\"path\":\"/new\",\"permission\":-1}"},
              {"setPermission", "{\"path\":\"/sentinel\",\"permission\":4294967296}"},
              {"setOwner", "{\"path\":\"/sentinel\",\"owner\":123,\"group\":\"team\"}"},
              {"listPage", "{\"path\":\"/\",\"marker\":42}"},
              {"listCursorOpen", "{\"path\":\"/\",\"files_only\":1}"},
              {"listCursorOpen", "{\"path\":\"/\",\"recursive\":\"false\"}"}
          };
          for (String[] call : invalid) {
            assertThrows(call[0] + ": " + call[1], IllegalArgumentException.class,
                () -> NativeObs.nativeCall(fs, call[0], call[1]));
          }
          assertTrue(JSON.readTree(NativeObs.nativeCall(fs, "status", "{}")).path("is_dir").asBoolean());
          assertEquals(0, server.heads.get());
          assertEquals(0, server.lists.get());
          assertEquals(0, server.gets.get());
          assertEquals(0, server.metadataWrites.get());
          assertEquals(1, server.data.size());
          assertArrayEquals(new byte[] {7}, server.data.get("sentinel"));
        } finally {
          NativeObs.nativeClose(fs);
        }
      }
    }
  }
}
