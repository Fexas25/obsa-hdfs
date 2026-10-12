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
   */
  public static native String nativeCall(long handle, String operation, String argsJson)
      throws IOException;

  public static native byte[] nativeRead(long stream, int length) throws IOException;

  public static native int nativeReadInto(long stream, byte[] data, int offset, int length)
      throws IOException;

  public static native int nativeWrite(long stream, byte[] data, int offset, int length)
      throws IOException;

  public static native void nativeSeek(long stream, long position) throws IOException;

  public static native void nativeFlush(long stream) throws IOException;

  /** Consumes a create stream without committing; input/append handles are rejected intact. */
  public static native void nativeAbortStream(long stream) throws IOException;

  public static native void nativeCloseStream(long stream) throws IOException;

  public static native void nativeClose(long handle) throws IOException;
}
