package org.apache.hadoop.fs.obs;

import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.Random;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.Test;

public class JniBufferTest {
  @Test
  public void largeSlicedWriteAndDirectReadPreserveOffsetsAndSentinels() throws Exception {
    ObsLocalContract contract = new ObsLocalContract(new Configuration(false), false);
    contract.init();
    try {
      FileSystem fs = contract.getTestFileSystem();
      byte[] source = new byte[5 * 1024 * 1024];
      new Random(8723).nextBytes(source);
      int offset = 117, length = 2 * 1024 * 1024 + 319;
      Path file = new Path("/buffer");
      try (FSDataOutputStream out = fs.create(file)) {
        out.write(source, offset, length);
      }
      byte[] target = new byte[length + 74];
      Arrays.fill(target, (byte) 77);
      try (FSDataInputStream in = fs.open(file)) {
        in.readFully(target, 37, length);
        assertEquals(length, in.getPos());
        assertEquals(-1, in.read(target, 0, 1));
        assertEquals(0, in.read(target, 0, 0));
      }
      assertArrayEquals(
          Arrays.copyOfRange(source, offset, offset + length),
          Arrays.copyOfRange(target, 37, 37 + length));
      for (int i = 0; i < 37; i++) {
        assertEquals(77, target[i]);
        assertEquals(77, target[target.length - 1 - i]);
      }
    } finally {
      contract.teardown();
    }
  }

  @Test
  public void specialNamesSurviveHttpEncodingXmlAndPagination() throws Exception {
    ObsLocalContract contract = new ObsLocalContract(new Configuration(false), false);
    contract.init();
    try {
      FileSystem fs = contract.getTestFileSystem();
      Path parent = new Path("/names");
      String[] names = {"space name", "plus+percent%", "unicode-雪", "xml&<>name", "hash#query?"};
      for (int i = 0; i < names.length; i++) {
        try (FSDataOutputStream out = fs.create(new Path(parent, names[i]))) {
          out.write(i);
        }
      }
      assertEquals(names.length, fs.listStatus(parent).length);
      for (int i = 0; i < names.length; i++) {
        try (FSDataInputStream in = fs.open(new Path(parent, names[i]))) {
          assertEquals(i, in.read());
          assertEquals(-1, in.read());
        }
      }
      assertTrue(fs.rename(parent, new Path("/renamed")));
      assertEquals(names.length, fs.listStatus(new Path("/renamed")).length);
      assertTrue(fs.delete(new Path("/renamed"), true));
    } finally {
      contract.teardown();
    }
  }
}
