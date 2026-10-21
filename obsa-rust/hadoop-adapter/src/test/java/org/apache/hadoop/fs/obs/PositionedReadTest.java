package org.apache.hadoop.fs.obs;

import static org.junit.Assert.*;

import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.Test;

public class PositionedReadTest {
  private static void check(boolean pfs) throws Exception {
    Configuration conf = new Configuration(false);
    // Small chunks so one pread spans several concurrent ranged GETs.
    conf.set("fs.obs.readahead.chunk.size", "65536");
    ObsLocalContract contract = new ObsLocalContract(conf, pfs);
    contract.init();
    ExecutorService pool = Executors.newFixedThreadPool(8);
    try {
      FileSystem fs = contract.getTestFileSystem();
      byte[] source = new byte[3 * 1024 * 1024 + 123];
      new Random(4242).nextBytes(source);
      Path file = new Path("/pread");
      try (FSDataOutputStream out = fs.create(file)) {
        out.write(source);
      }
      try (FSDataInputStream in = fs.open(file)) {
        byte[] head = new byte[10];
        in.readFully(head);
        assertEquals(10, in.getPos());

        List<Future<?>> tasks = new ArrayList<>();
        Random random = new Random(7);
        for (int i = 0; i < 32; i++) {
          final int position = random.nextInt(source.length - 1);
          final int length = Math.min(source.length - position, 1 + random.nextInt(400_000));
          tasks.add(pool.submit(() -> {
            byte[] target = new byte[length + 6];
            Arrays.fill(target, (byte) 91);
            in.readFully(position, target, 3, length);
            assertArrayEquals(
                Arrays.copyOfRange(source, position, position + length),
                Arrays.copyOfRange(target, 3, 3 + length));
            for (int k = 0; k < 3; k++) {
              assertEquals(91, target[k]);
              assertEquals(91, target[target.length - 1 - k]);
            }
            return null;
          }));
        }
        for (Future<?> task : tasks) task.get();

        // The cursor is untouched by positioned reads; sequential reading continues.
        assertEquals(10, in.getPos());
        byte[] next = new byte[5];
        in.readFully(next);
        assertArrayEquals(Arrays.copyOfRange(source, 10, 15), next);

        // Served from the resident sequential range as well as over the network.
        byte[] resident = new byte[4];
        assertEquals(4, in.read(11, resident, 0, 4));
        assertArrayEquals(Arrays.copyOfRange(source, 11, 15), resident);

        byte[] tail = new byte[64];
        assertEquals(23, in.read(source.length - 23, tail, 0, 64));
        assertArrayEquals(
            Arrays.copyOfRange(source, source.length - 23, source.length),
            Arrays.copyOf(tail, 23));
        assertEquals(-1, in.read(source.length, tail, 0, 1));
        assertEquals(-1, in.read(source.length + 100L, tail, 0, 1));
        assertEquals(0, in.read(5, tail, 0, 0));
        assertThrows(EOFException.class, () -> in.readFully(source.length - 2, new byte[3]));
        assertThrows(EOFException.class, () -> in.read(-1, tail, 0, 1));
        assertThrows(IndexOutOfBoundsException.class, () -> in.read(0, tail, 60, 5));
        assertEquals(15, in.getPos());
        in.close();
        assertThrows(IOException.class, () -> in.read(0, tail, 0, 1));
      }
    } finally {
      pool.shutdownNow();
      contract.teardown();
    }
  }

  @Test
  public void objectBucketPositionedReadsAreConcurrentAndCursorIndependent() throws Exception {
    check(false);
  }

  @Test
  public void pfsPositionedReadsAreConcurrentAndCursorIndependent() throws Exception {
    check(true);
  }

  @Test
  public void positionedReadDetectsObjectReplacement() throws Exception {
    Configuration conf = new Configuration(false);
    conf.set("fs.obs.readahead.chunk.size", "2");
    ObsLocalContract contract = new ObsLocalContract(conf, false);
    contract.init();
    try {
      FileSystem fs = contract.getTestFileSystem();
      Path file = new Path("/replaced");
      try (FSDataOutputStream out = fs.create(file)) {
        out.write(new byte[] {1, 2, 3, 4});
      }
      try (FSDataInputStream in = fs.open(file)) {
        assertEquals(1, in.read());
        try (FSDataOutputStream out = fs.create(file, true)) {
          out.write(new byte[] {5, 6, 7, 8});
        }
        // The resident range still reflects the opened revision.
        byte[] two = new byte[2];
        assertEquals(2, in.read(0, two, 0, 2));
        assertArrayEquals(new byte[] {1, 2}, two);
        // A ranged GET pinned to the opened ETag never mixes in the new revision.
        IOException changed = assertThrows(IOException.class, () -> in.read(2, two, 0, 2));
        assertTrue(changed.getMessage(), changed.getMessage().toLowerCase().contains("changed"));
      }
    } finally {
      contract.teardown();
    }
  }
}
