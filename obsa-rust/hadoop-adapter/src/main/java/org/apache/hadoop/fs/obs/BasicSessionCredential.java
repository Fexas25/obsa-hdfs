package org.apache.hadoop.fs.obs;

/**
 * Credentials supplied by the class named in {@code fs.obs.credentials.provider}. Same contract
 * as the Java OBSA reference: the class has a {@code (java.net.URI, Configuration)} constructor.
 * The native adapter also re-reads these getters every {@code
 * fs.obs.credentials.refresh.interval.ms}, so an implementation may return rotated values.
 */
public interface BasicSessionCredential {
  String getOBSAccessKeyId();

  String getOBSSecretKey();

  /** Session token for temporary credentials, or null/empty for permanent keys. */
  String getSessionToken();
}
