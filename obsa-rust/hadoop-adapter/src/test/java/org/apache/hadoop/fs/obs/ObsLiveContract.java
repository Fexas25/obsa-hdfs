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
 * and run only with OBS_LIVE_ROOT_TESTS=true. With OBS_LIVE_CONTRACT_PFS=true the contracts run
 * against OBS_LIVE_PFS_BUCKET instead, in auto bucket mode with PFS capabilities (append).
 * Credentials are never logged.
 */
final class ObsLiveContract extends AbstractFSContract {
  private OBSFileSystemRust fs;

  private static final boolean PFS = "true".equals(System.getenv("OBS_LIVE_CONTRACT_PFS"));

  ObsLiveContract(Configuration conf) {
    super(conf);
    ObsLocalContract.declareCapabilities(conf, PFS);
    conf.setBoolean("fs.contract.test.root-tests-enabled", "true".equals(System.getenv("OBS_LIVE_ROOT_TESTS")));
  }

  private static String env(String name) {
    String value = System.getenv(name);
    return value == null || value.isEmpty() ? null : value;
  }

  @Override
  public void init() throws IOException {
    String endpoint = env("OBS_LIVE_ENDPOINT");
    String bucket = env(PFS ? "OBS_LIVE_PFS_BUCKET" : "OBS_LIVE_BUCKET");
    String access = env("OBS_LIVE_ACCESS_KEY"), secret = env("OBS_LIVE_SECRET_KEY");
    Assume.assumeTrue("OBS_LIVE_* not set", endpoint != null && bucket != null && access != null && secret != null);
    Configuration conf = getConf();
    conf.set("fs.obs.endpoint", endpoint);
    conf.set("fs.obs.access.key", access);
    conf.set("fs.obs.secret.key", secret);
    if (env("OBS_LIVE_SESSION_TOKEN") != null) conf.set("fs.obs.session.token", env("OBS_LIVE_SESSION_TOKEN"));
    conf.setBoolean("fs.obs.connection.ssl.enabled", "true".equals(env("OBS_LIVE_SECURE")));
    conf.set("fs.obs.signature.type", env("OBS_LIVE_SIGNATURE") == null ? "v2" : env("OBS_LIVE_SIGNATURE"));
    // PFS runs also exercise bucket type detection.
    conf.set("fs.obs.bucket.mode", PFS ? "auto" : "object");
    // Real OBS requires virtual-host addressing; S3Proxy and other local servers need path style.
    conf.setBoolean("fs.obs.path.style.access", "true".equals(env("OBS_LIVE_PATH_STYLE")));
    conf.set("fs.obs.multipart.size", "5242880");
    fs = new OBSFileSystemRust();
    fs.initialize(URI.create("obs://" + bucket), conf);
    if (PFS && !fs.hasPathCapability(new Path("/"), org.apache.hadoop.fs.CommonPathCapabilities.FS_APPEND)) {
      throw new IOException("bucket.mode=auto did not detect PFS bucket " + bucket);
    }
  }

  /**
   * Runs after Hadoop's teardown, which deletes the test path but only logs failures. Report any
   * residue (which would leak into later test classes sharing the bucket), then close the
   * filesystem: each test creates one.
   */
  @Override
  public void teardown() throws IOException {
    if (fs == null) return;
    try {
      Path dir = getTestPath();
      if (fs.exists(dir)) {
        String before = tree(dir);
        String retry;
        try {
          retry = "retry delete=" + fs.delete(dir, true);
        } catch (IOException | RuntimeException e) {
          retry = "retry delete failed: " + e;
        }
        System.err.println("LIVE-RESIDUE after teardown: tree=[" + before + " ] " + retry
            + " exists-after-retry=" + fs.exists(dir) + " tree-after=[" + tree(dir) + " ]");
      }
    } finally {
      fs.close();
      fs = null;
    }
  }

  private String tree(Path dir) {
    StringBuilder out = new StringBuilder();
    try {
      for (org.apache.hadoop.fs.FileStatus status : fs.listStatus(dir)) {
        out.append(' ').append(status.getPath().getName()).append(status.isDirectory() ? "/" : "");
        if (status.isDirectory()) out.append('{').append(tree(status.getPath())).append(" }");
      }
    } catch (IOException | RuntimeException e) {
      out.append(" <").append(e.getClass().getSimpleName()).append('>');
    }
    return out.toString();
  }

  @Override
  public FileSystem getTestFileSystem() {
    return fs;
  }

  @Override
  public String getScheme() {
    return "obs";
  }

  /**
   * A fresh directory per test. Besides isolating tests on a shared bucket, this keeps them
   * independent of S3Proxy's jclouds directory model: under concurrent requests it can keep
   * reporting a common prefix after every key beneath it was deleted (a flat LIST shows no keys,
   * a delimited LIST still shows the prefix), which no real S3/OBS service does.
   */
  private final Path testPath = new Path("/obsa-live-contract-" + java.util.UUID.randomUUID());

  @Override
  public Path getTestPath() {
    return testPath;
  }
}
