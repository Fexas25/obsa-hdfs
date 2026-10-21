package org.apache.hadoop.fs.obs;

import static org.junit.Assert.*;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.Test;

public class AccessLabelXAttrTest {
  private static byte[] utf8(String text) {
    return text.getBytes(StandardCharsets.UTF_8);
  }

  @Test
  public void accessLabelRoundTripsOnDirectoriesOnly() throws Exception {
    ObsLocalContract contract = new ObsLocalContract(new Configuration(false), true);
    contract.init();
    try {
      FileSystem fs = contract.getTestFileSystem();
      Path dir = new Path("/labelled/目录");
      assertTrue(fs.mkdirs(dir));
      // Reference format: labels separated by ';', read back each followed by ';'.
      fs.setXAttr(dir, "AccessLabel", utf8("role-a;role b;"));
      assertEquals("role-a;role b;", new String(fs.getXAttr(dir, "AccessLabel"), StandardCharsets.UTF_8));
      Map<String, byte[]> all = fs.getXAttrs(dir);
      assertEquals(Collections.singleton("AccessLabel"), all.keySet());
      assertEquals(Arrays.asList("AccessLabel"), fs.listXAttrs(dir));
      assertTrue(fs.getXAttrs(dir, Arrays.asList("other")).isEmpty());
      assertEquals(1, fs.getXAttrs(dir, Arrays.asList("AccessLabel", "other")).size());

      fs.removeXAttr(dir, "AccessLabel");
      assertEquals("", new String(fs.getXAttr(dir, "AccessLabel"), StandardCharsets.UTF_8));

      // Root has no labels and cannot be labelled.
      assertTrue(fs.getXAttrs(new Path("/")).isEmpty());
      assertNull(fs.getXAttr(new Path("/"), "AccessLabel"));
      assertThrows(IllegalArgumentException.class,
          () -> fs.setXAttr(new Path("/"), "AccessLabel", utf8("x")));

      // Files are rejected, missing paths report not found, other names are unsupported.
      Path file = new Path("/labelled/file");
      try (FSDataOutputStream out = fs.create(file)) {
        out.write(1);
      }
      assertThrows(IOException.class, () -> fs.setXAttr(file, "AccessLabel", utf8("x")));
      assertThrows(IOException.class, () -> fs.getXAttrs(file));
      assertThrows(FileNotFoundException.class, () -> fs.getXAttrs(new Path("/missing")));
      assertThrows(IOException.class, () -> fs.setXAttr(dir, "user.other", utf8("x")));
      assertThrows(UnsupportedOperationException.class, () -> fs.getXAttr(dir, "user.other"));
      assertThrows(IOException.class, () -> fs.removeXAttr(dir, "user.other"));
    } finally {
      contract.teardown();
    }
  }
}
