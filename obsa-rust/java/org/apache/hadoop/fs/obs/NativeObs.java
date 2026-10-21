package org.apache.hadoop.fs.obs;

import java.io.IOException;

/** Low-level native ABI, separate from the existing Java OBSFileSystem. */
public final class NativeObs {
  static {
    System.loadLibrary("obs_jni");
  }

  private NativeObs() {}

  public static native long nativeCreate(String configJson) throws IOException;

  /**
   * JSON operation dispatch. recoverPendingUploads accepts {"limit":1..10000} and returns
   * {"aborted":count,"active":count,"errors":[messages]}; requires a configured upload journal.
   * Arguments must be an object. Fields consumed by an operation must have their declared types;
   * permission is an unsigned 32-bit integer. Invalid values throw IllegalArgumentException.
   * Omitted fields retain defaults (including root path); marker/owner/group may be null.
   */
  public static native String nativeCall(long handle, String operation, String argsJson)
      throws IOException;

  public static native byte[] nativeRead(long stream, int length) throws IOException;

  public static native int nativeReadInto(long stream, byte[] data, int offset, int length)
      throws IOException;

  /**
   * Read up to 16 MiB into a writable direct buffer; offsets are relative to its capacity.
   * Does not change buffer position/limit. Caller must exclusively own the region until return.
   * On failure the stream cursor is restored, but the region may be partially overwritten.
   */
  public static native int nativeReadDirect(long stream, java.nio.ByteBuffer data, int offset, int length)
      throws IOException;

  /**
   * Positioned read of up to 16 MiB at a nonnegative position; the stream cursor is unchanged.
   * Thread-safe: concurrent calls on one stream do not wait for each other or for sequential
   * reads. Returns -1 at EOF for a nonempty request. On failure the array region may be
   * partially overwritten.
   */
  public static native int nativePreadInto(
      long stream, long position, byte[] data, int offset, int length) throws IOException;

  /** Length captured when the input stream was opened; makes no request. */
  public static native long nativeStreamLength(long stream) throws IOException;

  /**
   * Vectored positioned read: fills each targets[i] completely from positions[i]. Ranges whose
   * gap is at most mergeGap are fetched together while the merged span stays within maxMerged
   * bytes; spans are fetched concurrently. Any range past EOF fails the whole call before I/O. On
   * failure the target arrays may be partially written.
   */
  public static native void nativePreadVectored(
      long stream, long[] positions, byte[][] targets, int mergeGap, int maxMerged)
      throws IOException;

  public static native int nativeWrite(long stream, byte[] data, int offset, int length)
      throws IOException;

  public static native void nativeSeek(long stream, long position) throws IOException;

  public static native long nativeSkip(long stream, long amount) throws IOException;

  public static native void nativeFlush(long stream) throws IOException;

  /** Consumes a create stream without committing; input/append handles are rejected intact. */
  public static native void nativeAbortStream(long stream) throws IOException;

  public static native void nativeCloseStream(long stream) throws IOException;

  public static native void nativeClose(long handle) throws IOException;

  public static native void nativeReleaseListing(long handle);
}
