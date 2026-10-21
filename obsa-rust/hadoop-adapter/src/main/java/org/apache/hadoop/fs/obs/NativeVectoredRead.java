package org.apache.hadoop.fs.obs;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.IntFunction;
import org.apache.hadoop.fs.FileRange;
import org.apache.hadoop.fs.FileSystem;

/**
 * Hadoop 3.3.5+ vectored read over the native positioned reader. Kept apart from the stream class
 * so FileRange is only resolved when readVectored is actually called: the adapter still loads on
 * Hadoop versions without vectored IO.
 *
 * <p>Argument errors are thrown synchronously, as in Hadoop's default implementation. Ranges past
 * EOF complete exceptionally with EOFException without any request; the remaining ranges are read
 * in one native call (merged and fetched concurrently) and complete before this method returns.
 * A failure of that call completes each of those ranges exceptionally with the same error.
 */
final class NativeVectoredRead {
  private NativeVectoredRead() {}

  static void read(
      long stream,
      List<? extends FileRange> ranges,
      IntFunction<ByteBuffer> allocate,
      int mergeGap,
      int maxMerged,
      FileSystem.Statistics statistics)
      throws IOException {
    for (FileRange range : ranges) {
      if (range.getLength() < 0)
        throw new IllegalArgumentException("length is negative in " + range);
      if (range.getOffset() < 0) throw new EOFException("position is negative in range " + range);
    }
    long length = NativeObs.nativeStreamLength(stream);
    List<FileRange> pending = new ArrayList<>();
    List<CompletableFuture<ByteBuffer>> futures = new ArrayList<>();
    for (FileRange range : ranges) {
      CompletableFuture<ByteBuffer> future = new CompletableFuture<>();
      range.setData(future);
      if (range.getOffset() > length - range.getLength()) {
        future.completeExceptionally(
            new EOFException("range " + range + " extends past end of file length " + length));
      } else {
        pending.add(range);
        futures.add(future);
      }
    }
    if (pending.isEmpty()) return;
    long[] positions = new long[pending.size()];
    byte[][] targets = new byte[pending.size()][];
    for (int i = 0; i < positions.length; i++) {
      positions[i] = pending.get(i).getOffset();
      targets[i] = new byte[pending.get(i).getLength()];
    }
    try {
      NativeObs.nativePreadVectored(
          stream, positions, targets, Math.max(0, mergeGap), Math.max(0, maxMerged));
    } catch (IOException | RuntimeException e) {
      for (CompletableFuture<ByteBuffer> future : futures) future.completeExceptionally(e);
      return;
    }
    long total = 0;
    for (int i = 0; i < targets.length; i++) {
      try {
        ByteBuffer buffer = allocate.apply(targets[i].length);
        buffer.put(targets[i]);
        buffer.flip();
        futures.get(i).complete(buffer);
        total += targets[i].length;
      } catch (RuntimeException e) {
        futures.get(i).completeExceptionally(e);
      }
    }
    if (statistics != null && total > 0) statistics.incrementBytesRead(total);
  }
}
