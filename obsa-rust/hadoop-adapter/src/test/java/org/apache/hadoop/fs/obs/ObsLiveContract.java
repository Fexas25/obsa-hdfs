package org.apache.hadoop.fs.obs;

import java.io.IOException;
import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.contract.AbstractFSContract;
import org.junit.Assume;

/**
 * Official Hadoop contracts against a real object bucket, configured only from the environment:
 * OBS_LIVE_ENDPOINT, OBS_LIVE_ACCESS_KEY, OBS_LIVE_SECRET_KEY, OBS_LIVE_BUCKET, and optionally
 * OBS_LIVE_SECURE=true and OBS_LIVE_SIGNATURE (obs | v2, default v2 for S3-compatible servers).
 * Tests are skipped when these are absent. Root-directory tests delete everything in the bucket
 * and run only with OBS_LIVE_ROOT_TESTS=true. Credentials are never logged.
 */
final class ObsLiveContract extends AbstractFSContract {
  private OBSFileSystemRust fs;

  ObsLiveContract(Configuration conf) {
    super(conf);
    ObsLocalContract.declareCapabilities(conf, false);
    conf.setBoolean("fs.contract.test.root-tests-enabled", "true".equals(System.getenv("OBS_LIVE_ROOT_TESTS")));
  }

  private static String env(String name) {
    String value = System.getenv(name);
    return value == null || value.isEmpty() ? null : value;
  }

  @Override
  public void init() throws IOException {
    String endpoint = env("OBS_LIVE_ENDPOINT"), bucket = env("OBS_LIVE_BUCKET");
    String access = env("OBS_LIVE_ACCESS_KEY"), secret = env("OBS_LIVE_SECRET_KEY");
    Assume.assumeTrue("OBS_LIVE_* not set", endpoint != null && bucket != null && access != null && secret != null);
    Configuration conf = getConf();
    conf.set("fs.obs.endpoint", endpoint);
    conf.set("fs.obs.access.key", access);
    conf.set("fs.obs.secret.key", secret);
    conf.setBoolean("fs.obs.connection.ssl.enabled", "true".equals(env("OBS_LIVE_SECURE")));
    conf.set("fs.obs.signature.type", env("OBS_LIVE_SIGNATURE") == null ? "v2" : env("OBS_LIVE_SIGNATURE"));
    conf.set("fs.obs.bucket.mode", "object");
    conf.setBoolean("fs.obs.path.style.access", true);
    conf.set("fs.obs.multipart.size", "5242880");
    fs = new OBSFileSystemRust();
    fs.initialize(URI.create("obs://" + bucket), conf);
  }

  @Override
  public FileSystem getTestFileSystem() {
    return fs;
  }

  @Override
  public String getScheme() {
    return "obs";
  }

  @Override
  public Path getTestPath() {
    return new Path("/obsa-live-contract");
  }
}
