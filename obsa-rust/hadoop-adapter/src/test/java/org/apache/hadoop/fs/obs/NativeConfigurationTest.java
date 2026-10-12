package org.apache.hadoop.fs.obs;

import static org.junit.Assert.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.math.BigInteger;
import org.junit.Test;

public class NativeConfigurationTest {
  private static final ObjectMapper JSON = new ObjectMapper();
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
}
