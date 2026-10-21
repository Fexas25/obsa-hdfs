package org.apache.hadoop.fs.obs;

import static org.junit.Assert.*;

import java.util.Random;
import org.apache.hadoop.util.PureJavaCrc32C;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.util.DataChecksum;
import org.junit.Test;

public class FileChecksumTest {
  private static void check(boolean pfs) throws Exception {
    ObsLocalContract contract = new ObsLocalContract(new Configuration(false), pfs);
    contract.init();
    try {
      contract.server().sendCrc32c = true;
      FileSystem fs = contract.getTestFileSystem();
      byte[] data = new byte[100_003];
      new Random(31).nextBytes(data);
      Path file = new Path("/crc/file");
      try (FSDataOutputStream out = fs.create(file)) {
        out.write(data);
      }
      PureJavaCrc32C expected = new PureJavaCrc32C();
      expected.update(data, 0, data.length);

      FileChecksum checksum = fs.getFileChecksum(file);
      assertNotNull(checksum);
      assertEquals("COMPOSITE-CRC32C", checksum.getAlgorithmName());
      // Equal to the checksum HDFS reports in COMPOSITE_CRC mode for the same bytes.
      assertEquals(
          new CompositeCrcFileChecksum((int) expected.getValue(), DataChecksum.Type.CRC32C, 512),
          checksum);
      assertEquals(checksum, fs.getFileChecksum(file, data.length));
      assertEquals(checksum, fs.getFileChecksum(file, Long.MAX_VALUE));

      // A prefix checksum cannot be derived from the whole-file CRC.
      assertNull(fs.getFileChecksum(file, data.length - 1));
      // Directories have no checksum.
      assertTrue(fs.mkdirs(new Path("/crc/dir")));
      assertNull(fs.getFileChecksum(new Path("/crc/dir")));

      // Different content gives a different checksum.
      Path other = new Path("/crc/other");
      try (FSDataOutputStream out = fs.create(other)) {
        out.write(data, 1, data.length - 1);
      }
      assertNotEquals(checksum, fs.getFileChecksum(other));

      // Without a service CRC there is no checksum, never a shared placeholder.
      contract.server().sendCrc32c = false;
      assertNull(fs.getFileChecksum(file));
      assertNull(fs.getFileChecksum(other));
    } finally {
      contract.teardown();
    }
  }

  @Test
  public void objectBucketReportsServiceCrc32cAsCompositeChecksum() throws Exception {
    check(false);
  }

  @Test
  public void pfsReportsServiceCrc32cAsCompositeChecksum() throws Exception {
    check(true);
  }
}
