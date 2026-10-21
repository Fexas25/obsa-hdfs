package org.apache.hadoop.fs.obs;

import static org.junit.Assert.*;

import java.util.Base64;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.Test;

public class ServerSideEncryptionTest {
  private static ObsLocalContract open(String type, String key) throws Exception {
    Configuration conf = new Configuration(false);
    conf.set("fs.obs.server-side-encryption-type", type);
    if (key != null) conf.set("fs.obs.server-side-encryption-key", key);
    ObsLocalContract contract = new ObsLocalContract(conf, false);
    contract.init();
    return contract;
  }

  private static void roundTrip(FileSystem fs) throws Exception {
    Path file = new Path("/sse/file");
    try (FSDataOutputStream out = fs.create(file)) {
      out.write(new byte[] {1, 2, 3});
    }
    try (FSDataInputStream in = fs.open(file)) {
      assertEquals(1, in.read());
    }
  }

  @Test
  public void sseCKeyReachesWritesReadsAndMetadataQueries() throws Exception {
    ObsLocalContract contract = open("sse-c", Base64.getEncoder().encodeToString(new byte[32]));
    try {
      roundTrip(contract.getTestFileSystem());
      LocalObsServer server = contract.server();
      for (String method : new String[] {"PUT", "GET", "HEAD"})
        assertEquals(method, "AES256|null", server.sseHeadersByMethod.get(method));
    } finally {
      contract.teardown();
    }
  }

  @Test
  public void sseKmsMarksObjectCreationOnly() throws Exception {
    ObsLocalContract contract = open("sse-kms", null);
    try {
      roundTrip(contract.getTestFileSystem());
      LocalObsServer server = contract.server();
      assertEquals("null|kms", server.sseHeadersByMethod.get("PUT"));
      assertEquals("null|null", server.sseHeadersByMethod.get("GET"));
    } finally {
      contract.teardown();
    }
  }

  @Test
  public void invalidEncryptionSettingsFailInitialization() throws Exception {
    for (String[] setting : new String[][] {{"sse-c", null}, {"sse-c", "dG9vLXNob3J0"}, {"aes", null}}) {
      Configuration conf = new Configuration(false);
      conf.set("fs.obs.server-side-encryption-type", setting[0]);
      if (setting[1] != null) conf.set("fs.obs.server-side-encryption-key", setting[1]);
      ObsLocalContract contract = new ObsLocalContract(conf, false);
      try {
        RuntimeException error = assertThrows(IllegalArgumentException.class, contract::init);
        if (setting[1] != null) assertFalse(error.getMessage().contains(setting[1]));
      } finally {
        contract.teardown();
      }
    }
  }
}
