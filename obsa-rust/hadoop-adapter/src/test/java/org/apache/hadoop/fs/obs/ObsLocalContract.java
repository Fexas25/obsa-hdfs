package org.apache.hadoop.fs.obs;

import java.io.IOException;
import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.contract.AbstractFSContract;

/** Official Hadoop contracts over the actual JNI/HTTP stack, isolated per test. */
final class ObsLocalContract extends AbstractFSContract {
  private final boolean pfs;
  private LocalObsServer server;
  private OBSFileSystemRust fs;

  ObsLocalContract(Configuration conf, boolean pfs) {
    super(conf);
    this.pfs = pfs;
    // Safe only because every test owns a fresh loopback namespace.
    conf.setBoolean("fs.contract.test.root-tests-enabled", true);
    String[] supported = {
      "is-case-sensitive",
      "supports-getfilestatus",
      "supports-seek",
      "supports-positioned-readable",
      "create-visibility-delayed",
      "rename-returns-false-if-dest-exists",
      "rename-returns-false-if-source-missing"
    };
    for (String key : supported) conf.setBoolean("fs.contract." + key, true);
    String[] unsupported = {
      "create-overwrites-directory",
      "create-file-under-file-allowed",
      "rename-creates-dest-dirs",
      "rename-overwrites-dest",
      "rename-remove-dest-if-empty-dir",
      "supports-settimes",
      "supports-atomic-directory-delete",
      "supports-block-locality",
      "supports-concat",
      "rejects-seek-past-eof",
      "supports-seek-on-closed-file",
      "supports-available-on-closed-file",
      "supports-strict-exceptions",
      "supports-unix-permissions",
      "supports-file-reference",
      "supports-content-check",
      "supports-unbuffer",
      "supports-hflush",
      "supports-hsync"
    };
    for (String key : unsupported) conf.setBoolean("fs.contract." + key, false);
    // Both modes use multipart create: new files become visible on close.
    conf.setBoolean("fs.contract.is-blobstore", true);
    conf.setBoolean("fs.contract.supports-append", pfs);
    conf.setBoolean("fs.contract.supports-atomic-rename", pfs);
  }

  @Override
  public void init() throws IOException {
    server = new LocalObsServer(pfs);
    Configuration conf = getConf();
    conf.set("fs.obs.endpoint", server.endpoint());
    conf.set("fs.obs.access.key", "test");
    conf.set("fs.obs.secret.key", "test");
    conf.setBoolean("fs.obs.connection.ssl.enabled", false);
    conf.set("fs.obs.bucket.mode", pfs ? "pfs" : "object");
    if (conf.get("fs.obs.readahead.chunk.size") == null) conf.set("fs.obs.readahead.chunk.size", "65536");
    // Exercise continuation markers in ordinary contract directory trees.
    conf.setInt("fs.obs.paging.maximum", 3);
    conf.set("fs.obs.metadata.cache.ttl", "60000");
    fs = new OBSFileSystemRust();
    try {
      fs.initialize(URI.create("obs://bucket"), conf);
    } catch (IOException | RuntimeException e) {
      server.close();
      throw e;
    }
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
    return new Path("/contract");
  }

  @Override
  public void teardown() throws IOException {
    try {
      if (fs != null) fs.close();
    } finally {
      if (server != null) server.close();
    }
  }
}
