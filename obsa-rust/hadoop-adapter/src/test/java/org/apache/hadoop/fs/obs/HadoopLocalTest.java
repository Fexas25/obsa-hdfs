package org.apache.hadoop.fs.obs;

import static org.junit.Assert.*;

import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.security.PrivilegedExceptionAction;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.*;

/** Real Hadoop API -> JNI -> Rust -> loopback OBS protocol integration. */
public class HadoopLocalTest {
  private LocalObsServer server;
  private OBSFileSystemRust fs;
  private NavigableMap<String, byte[]> data;
  private AtomicInteger heads;

  @Before
  public void start() throws Exception {
    server = new LocalObsServer(false);
    data = server.data;
    heads = server.heads;
    open(false);
  }

  private void open(boolean pfs) throws Exception {
    open(pfs, null);
  }

  private void open(boolean pfs, String blockSize) throws Exception {
    if (fs != null) fs.close();
    server.pfs = pfs;
    Configuration conf = new Configuration(false);
    conf.set("fs.obs.endpoint", server.endpoint());
    conf.set("fs.obs.access.key", "test");
    conf.set("fs.obs.secret.key", "test");
    conf.setBoolean("fs.obs.connection.ssl.enabled", false);
    conf.set("fs.obs.bucket.mode", pfs ? "pfs" : "object");
    conf.set("fs.obs.readahead.chunk.size", "4");
    conf.set("fs.obs.metadata.cache.ttl", "60000");
    conf.setInt("fs.obs.paging.maximum", 3);
    conf.set("fs.obs.block.size", "64M");
    if (blockSize != null) conf.set("fs.obs.bucket.bucket.block.size", blockSize);
    fs = new OBSFileSystemRust();
    fs.initialize(URI.create("obs://bucket"), conf);
  }

  @After
  public void stop() throws Exception {
    try {
      if (fs != null) fs.close();
    } finally {
      server.close();
    }
  }

  @Test
  public void appendFollowsFileAndParentRenames() throws Exception {
    open(true);
    fs.mkdirs(new Path("/parent"));
    write("/parent/file", "a");
    try (FSDataOutputStream output = fs.append(new Path("/parent/file"))) {
      assertTrue(fs.rename(new Path("/parent/file"), new Path("/parent/moved")));
      output.write('b');
      assertTrue(fs.rename(new Path("/parent"), new Path("/renamed")));
      output.write('c');
    }
    assertArrayEquals("abc".getBytes(StandardCharsets.UTF_8), data.get("renamed/moved"));
    assertEquals(3, fs.getFileStatus(new Path("/renamed/moved")).getLen());
    assertFalse(data.containsKey("parent/file"));
    assertFalse(data.containsKey("parent/moved"));
  }

  @Test
  public void abortUploadRejectsAppendWithoutClosingIt() throws Exception {
    open(true);
    write("/append-abort", "a");
    try (FSDataOutputStream output = fs.append(new Path("/append-abort"))) {
      assertThrows(UnsupportedOperationException.class, () -> fs.abortUpload(output));
      output.write('b');
    }
    assertArrayEquals("ab".getBytes(StandardCharsets.UTF_8), data.get("append-abort"));
  }

  @Test
  public void bucketPayloadBudgetIsSharedAndFailedUploadReleasesIt() throws Exception {
    for (boolean pfs : new boolean[] {false, true}) {
      open(pfs);
      Configuration conf = new Configuration(fs.getConf());
      conf.set("fs.obs.buffer.total.bytes", "0");
      conf.set("fs.obs.bucket.bucket.buffer.total.bytes", "65536");
      fs.close();
      fs = new OBSFileSystemRust();
      fs.initialize(URI.create("obs://bucket"), conf);
      data.clear();
      data.put("source", "data".getBytes(StandardCharsets.UTF_8));
      FSDataOutputStream output = fs.create(new Path("/held"));
      try {
        output.write(new byte[65536]);
        try (FSDataInputStream input = fs.open(new Path("/source"))) {
          IOException pressure = assertThrows(IOException.class, () -> input.read());
          assertTrue(pressure.getMessage().contains("shared buffer budget exhausted"));
          assertEquals(0, input.getPos());
          assertThrows(IOException.class, () -> output.write(1));
          // The failed upload remains alive, but its staging bytes are released.
          byte[] bytes = new byte[4];
          input.readFully(bytes);
          assertArrayEquals("data".getBytes(StandardCharsets.UTF_8), bytes);
        }
        assertThrows(IOException.class, () -> output.close());
      } finally {
        try { output.close(); } catch (IOException ignored) { }
      }
      assertFalse(data.containsKey("held"));
      write("/successful", "next");
      try (FSDataInputStream input = fs.open(new Path("/successful"))) {
        assertEquals('n', input.read()); // successful PUT released transport body references
      }
    }
  }

  @Test
  public void ugiHomeAndRelativePathsFollowHadoopIdentity() throws Exception {
    UserGroupInformation alice = UserGroupInformation.createRemoteUser("adapter-alice");
    UserGroupInformation bob = UserGroupInformation.createProxyUser("adapter-bob", alice);
    for (boolean pfs : new boolean[] {false, true}) {
      data.clear();
      alice.doAs((PrivilegedExceptionAction<Void>) () -> {
        open(pfs);
        Path home = new Path("obs://bucket/user/adapter-alice");
        assertEquals(home, fs.getHomeDirectory());
        assertEquals(home, fs.getWorkingDirectory());
        fs.mkdirs(new Path("."));
        write("relative", "alice");
        assertArrayEquals("alice".getBytes(StandardCharsets.UTF_8),
            data.get("user/adapter-alice/relative"));
        fs.mkdirs(new Path("sub"));
        fs.setWorkingDirectory(new Path("sub"));
        assertEquals(new Path(home, "sub"), fs.getWorkingDirectory());
        assertEquals(home, fs.getHomeDirectory());
        return null;
      });
      bob.doAs((PrivilegedExceptionAction<Void>) () -> {
        // Hadoop home follows the caller; an existing FS working directory stays put.
        assertEquals(new Path("obs://bucket/user/adapter-bob"), fs.getHomeDirectory());
        assertEquals(new Path("obs://bucket/user/adapter-alice/sub"), fs.getWorkingDirectory());
        open(pfs);
        assertEquals(fs.getHomeDirectory(), fs.getWorkingDirectory());
        fs.mkdirs(new Path("."));
        write("relative", "bob");
        assertArrayEquals("bob".getBytes(StandardCharsets.UTF_8),
            data.get("user/adapter-bob/relative"));
        try (FSDataInputStream input = fs.open(new Path("relative"))) {
          assertEquals('b', input.read());
        }
        assertArrayEquals("alice".getBytes(StandardCharsets.UTF_8),
            data.get("user/adapter-alice/relative"));
        return null;
      });
    }
  }

  @Test
  public void configuredBlockSizeSurvivesCreateHeadListAndBucketOverrides() throws Exception {
    for (boolean pfs : new boolean[] {false, true}) {
      open(pfs, "32M");
      long expected = 32L * 1024 * 1024;
      assertEquals(expected, fs.getDefaultBlockSize());
      assertEquals(expected, fs.getDefaultBlockSize(new Path("/dir")));
      assertEquals(expected, fs.getFileStatus(new Path("/")).getBlockSize());
      write("/blocks/file", "payload");
      assertEquals(expected, fs.getFileStatus(new Path("/blocks/file")).getBlockSize());
      open(pfs, "32M"); // Fresh cache forces HEAD.
      FileStatus status = fs.getFileStatus(new Path("/blocks/file"));
      assertEquals(expected, status.getBlockSize());
      FileStatus listed = fs.listStatusIterator(new Path("/blocks")).next();
      assertEquals(expected, listed.getBlockSize());
      BlockLocation[] locations = fs.getFileBlockLocations(status, 0, status.getLen());
      assertEquals(1, locations.length);
      assertEquals(0, locations[0].getOffset());
      assertEquals(status.getLen(), locations[0].getLength());
      open(pfs, "0");
      assertEquals(128L * 1024 * 1024, fs.getDefaultBlockSize());
      open(pfs);
      assertEquals(64L * 1024 * 1024, fs.getDefaultBlockSize());
    }
  }

  @Test
  public void contentSummarySwitchSelectsTraversalAndHonorsBucketOverride() throws Exception {
    for (boolean pfs : new boolean[] {false, true}) {
      for (boolean optimized : new boolean[] {false, true}) {
        open(pfs);
        Configuration conf = new Configuration(fs.getConf());
        // Opposite common value ensures the bucket override actually wins.
        conf.setBoolean("fs.obs.content.summary.enable", !optimized);
        conf.setBoolean("fs.obs.bucket.bucket.content.summary.enable", optimized);
        fs.close();
        fs = new OBSFileSystemRust();
        fs.initialize(URI.create("obs://bucket"), conf);
        data.clear();
        data.put("summary/a", new byte[3]);
        data.put("summary/sub/b", new byte[5]);
        data.put("summary/empty/", new byte[0]);
        server.flatLists.set(0);
        server.directoryLists.set(0);
        ContentSummary summary = fs.getContentSummary(new Path("/summary"));
        assertEquals(8, summary.getLength());
        assertEquals(8, summary.getSpaceConsumed());
        assertEquals(2, summary.getFileCount());
        assertEquals(3, summary.getDirectoryCount());
        if (optimized) {
          assertEquals(1, server.flatLists.get());
          assertEquals(0, server.directoryLists.get());
        } else {
          assertEquals(0, server.flatLists.get());
          assertTrue(server.directoryLists.get() >= 3);
        }
        assertEquals(3, fs.getContentSummary(new Path("/summary/a")).getLength());
        assertEquals(1, fs.getContentSummary(new Path("/summary/empty")).getDirectoryCount());
        try {
          fs.getContentSummary(new Path("/missing-summary"));
          fail("missing path must fail for either traversal");
        } catch (FileNotFoundException expected) {
        }
      }
    }
  }

  private void openPermissions(boolean pfs, String mode) throws Exception {
    open(pfs);
    Configuration conf = new Configuration(fs.getConf());
    conf.set("fs.obs.permissions.mode", "none");
    conf.set("fs.obs.bucket.bucket.permissions.mode", mode);
    fs.close();
    fs = new OBSFileSystemRust();
    fs.initialize(URI.create("obs://bucket"), conf);
  }

  @Test
  public void namedOwnershipPreservesOtherMetadataAndSurvivesLists() throws Exception {
    openPermissions(true, "disguise");
    data.put("owned/file", new byte[] {1, 2, 3});
    Map<String, String> metadata = new HashMap<>();
    metadata.put("project", "preserve");
    metadata.put("mode", "100644");
    metadata.put("uid", "123");
    server.metadata.put("owned/file", metadata);
    Path path = new Path("/owned/file");
    fs.getFileStatus(path); // Prime cache before mutation.
    fs.setOwner(path, "alice", "engineering");
    fs.setOwner(path, null, "analytics");
    fs.setPermission(path, new FsPermission((short) 0600));
    FileStatus status = fs.getFileStatus(path);
    assertEquals("alice", status.getOwner());
    assertEquals("analytics", status.getGroup());
    assertEquals(0600, status.getPermission().toShort());
    status = fs.listStatus(new Path("/owned"))[0];
    assertEquals("alice", status.getOwner());
    assertEquals("analytics", status.getGroup());
    assertEquals(0600, status.getPermission().toShort());
    assertEquals("alice", fs.listStatusIterator(new Path("/owned")).next().getOwner());
    assertEquals("preserve", server.metadata.get("owned/file").get("project"));
    assertEquals("123", server.metadata.get("owned/file").get("uid"));
    assertEquals("100644", server.metadata.get("owned/file").get("mode"));
    assertArrayEquals(new byte[] {1, 2, 3}, data.get("owned/file"));
    data.put("owned/dir/", new byte[0]);
    fs.setOwner(new Path("/owned/dir"), "bob", "team");
    fs.setOwner(new Path("/owned/dir"), "carol", null);
    assertEquals("carol", fs.getFileStatus(new Path("/owned/dir")).getOwner());
    assertEquals("team", fs.getFileStatus(new Path("/owned/dir")).getGroup());
  }

  @Test
  public void ownershipModeAndInvalidNamesDoNotWriteMetadata() throws Exception {
    Path path = new Path("/file");
    data.put("file", new byte[0]);
    for (boolean pfs : new boolean[] {false, true}) {
      openPermissions(pfs, pfs ? "none" : "disguise");
      fs.setOwner(path, "alice", "team");
      fs.setPermission(path, new FsPermission((short) 0600));
    }
    assertEquals(0, server.metadataWrites.get());
    openPermissions(true, "disguise");
    for (String name : new String[] {null, "", "alice\r\nInjected: value", "雪"}) {
      try {
        fs.setOwner(path, name, null);
        fail("invalid name must fail");
      } catch (IllegalArgumentException expected) {
      }
    }
    assertEquals(0, server.metadataWrites.get());
    try {
      fs.setOwner(new Path("/missing-owner"), "alice", "team");
      fail("missing path must fail");
    } catch (FileNotFoundException expected) {
    }
    try {
      openPermissions(true, "invalid");
      fail("unknown mode must fail");
    } catch (IllegalArgumentException expected) {
    }
  }

  private void write(String path, String value) throws Exception {
    try (FSDataOutputStream out = fs.create(new Path(path), true)) {
      out.write(value.getBytes(StandardCharsets.UTF_8));
    }
  }

  private String read(String path) throws Exception {
    try (FSDataInputStream in = fs.open(new Path(path))) {
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      byte[] buf = new byte[3];
      int n;
      while ((n = in.read(buf)) >= 0) out.write(buf, 0, n);
      return new String(out.toByteArray(), StandardCharsets.UTF_8);
    }
  }

  @Test
  public void directFileWinsOverChildPrefixAcrossListingAndSummary() throws Exception {
    for (boolean warm : new boolean[] {false, true}) {
      for (int operation = 0; operation < 5; operation++) {
        open(false);
        data.clear();
        data.put("file", new byte[6]);
        data.put("file/child", new byte[5]);
        Path path = new Path("/file");
        if (warm) assertTrue(fs.getFileStatus(path).isFile());
        int lists = server.lists.get();
        if (operation == 0) {
          ContentSummary summary = fs.getContentSummary(path);
          assertEquals(6, summary.getLength());
          assertEquals(1, summary.getFileCount());
          assertEquals(0, summary.getDirectoryCount());
        } else if (operation == 1) {
          FileStatus[] entries = fs.listStatus(path);
          assertEquals(1, entries.length);
          assertTrue(entries[0].isFile());
          assertEquals(6, entries[0].getLen());
          assertEquals("/file", entries[0].getPath().toUri().getPath());
          assertEquals(64L * 1024 * 1024, entries[0].getBlockSize());
        } else {
          RemoteIterator<? extends FileStatus> entries = operation == 2
              ? fs.listStatusIterator(path) : fs.listFiles(path, operation == 4);
          assertTrue(entries.hasNext());
          FileStatus status = entries.next();
          assertTrue(status.isFile());
          assertEquals(6, status.getLen());
          assertEquals("/file", status.getPath().toUri().getPath());
          assertEquals(64L * 1024 * 1024, status.getBlockSize());
          assertFalse(entries.hasNext());
        }
        assertEquals(lists, server.lists.get());
        assertTrue(fs.getFileStatus(path).isFile());
      }
    }
  }

  @Test
  public void parentListingKeepsCollidingFileVisibleAndCachedAsFile() throws Exception {
    for (int operation = 0; operation < 3; operation++) {
      open(false);
      data.clear();
      data.put("file", new byte[6]);
      data.put("file/child", new byte[5]);
      Path root = new Path("/");
      FileStatus status;
      if (operation == 0) {
        FileStatus[] entries = fs.listStatus(root);
        assertEquals(1, entries.length);
        status = entries[0];
      } else {
        RemoteIterator<? extends FileStatus> entries = operation == 1
            ? fs.listStatusIterator(root) : fs.listFiles(root, false);
        assertTrue(entries.hasNext());
        status = entries.next();
        assertFalse(entries.hasNext());
      }
      assertTrue(status.isFile());
      assertEquals("/file", status.getPath().toUri().getPath());
      assertEquals(6, status.getLen());
      assertTrue(fs.getFileStatus(new Path("/file")).isFile());
      assertEquals(6, fs.getContentSummary(new Path("/file")).getLength());
    }
  }

  @Test
  public void recursiveListingAndSummaryHideFileDescendants() throws Exception {
    for (boolean summary : new boolean[] {false, true}) {
      open(false);
      data.clear();
      data.put("a", new byte[6]);
      data.put("a/", new byte[0]);
      data.put("a/b", new byte[5]);
      data.put("a/sub/c", new byte[7]);
      for (int i = 0; i < 7; i++) data.put("a/b" + i, new byte[4]); // Entire hidden pages.
      data.put("dir/live", new byte[2]);
      data.put("z", new byte[3]);
      if (summary) {
        ContentSummary value = fs.getContentSummary(new Path("/"));
        assertEquals(3, value.getFileCount());
        assertEquals(2, value.getDirectoryCount());
        assertEquals(11, value.getLength());
      } else {
        RemoteIterator<LocatedFileStatus> iterator = fs.listFiles(new Path("/"), true);
        Set<String> found = new HashSet<>();
        while (iterator.hasNext()) {
          LocatedFileStatus value = iterator.next();
          assertTrue(value.isFile());
          assertTrue(found.add(value.getPath().toUri().getPath()));
        }
        assertEquals(new HashSet<>(Arrays.asList("/a", "/dir/live", "/z")), found);
      }
    }
  }

  @Test
  public void explicitHiddenChildCannotBeReadOrMutatedEvenAfterCaching() throws Exception {
    for (boolean warm : new boolean[] {false, true}) {
      open(false);
      data.clear();
      data.put("a/b", new byte[6]);
      Path child = new Path("/a/b");
      if (warm) assertTrue(fs.getFileStatus(child).isFile());
      data.put("a", new byte[3]);
      assertFalse(fs.exists(child));
      try { fs.getFileStatus(child); fail("hidden status"); }
      catch (FileNotFoundException expected) { }
      try (FSDataInputStream ignored = fs.open(child)) { fail("hidden open"); }
      catch (FileNotFoundException expected) { }
      try { fs.listStatus(child); fail("hidden listing"); }
      catch (FileNotFoundException expected) { }
      try { fs.getContentSummary(child); fail("hidden summary"); }
      catch (FileNotFoundException expected) { }
      assertFalse(fs.delete(child, true));
      assertFalse(fs.rename(child, new Path("/target")));
      try (FSDataOutputStream ignored = fs.create(child, true)) { fail("hidden overwrite"); }
      catch (IOException expected) { }
      assertEquals(6, data.get("a/b").length);
      assertEquals(3, data.get("a").length);
      assertFalse(data.containsKey("target"));
    }
  }

  @Test
  public void recursiveIteratorReusesAncestorProbeAcrossTenPages() throws Exception {
    for (boolean pfs : new boolean[] {false, true}) {
      open(pfs);
      data.clear();
      for (int i = 0; i < 30; i++) data.put("dir/file" + i, new byte[1]);
      int beforeHeads = heads.get(), beforeLists = server.lists.get();
      RemoteIterator<LocatedFileStatus> iterator = fs.listFiles(new Path("/"), true);
      int count = 0;
      while (iterator.hasNext()) { iterator.next(); count++; }
      assertEquals(30, count);
      assertEquals(beforeLists + 10, server.lists.get());
      assertEquals(beforeHeads + (pfs ? 0 : 1), heads.get());
    }
  }

  @Test
  public void directoryIteratorFetchesOnlyConsumedPages() throws Exception {
    for (boolean pfs : new boolean[] {false, true}) {
      open(pfs);
      data.clear();
      for (int i = 0; i < 10; i++) data.put("dir/f" + i, new byte[i]);
      int before = server.lists.get();
      int headBefore = heads.get();
      fs.setWorkingDirectory(new Path("/"));
      RemoteIterator<FileStatus> iterator = fs.listStatusIterator(new Path("dir"));
      fs.setWorkingDirectory(new Path("/different"));
      assertEquals(before + 1, server.lists.get());
      assertTrue(iterator.hasNext());
      assertTrue(iterator.hasNext());
      assertEquals(before + 1, server.lists.get());
      Set<String> found = new HashSet<>();
      for (int i = 0; i < 3; i++) found.add(iterator.next().getPath().getName());
      assertEquals(before + 1, server.lists.get());
      while (iterator.hasNext()) {
        FileStatus value = iterator.next();
        assertEquals("/dir", value.getPath().getParent().toUri().getPath());
        assertTrue(found.add(value.getPath().getName()));
      }
      assertEquals(10, found.size());
      assertEquals(before + 4, server.lists.get());
      assertEquals(headBefore + (pfs ? 0 : 1), heads.get());
      assertFalse(iterator.hasNext());
      try {
        iterator.next();
        fail("must reject exhausted iterator");
      } catch (NoSuchElementException expected) {
      }
    }
  }

  @Test
  public void recursiveFileIteratorUsesFlatPagesWithoutWalkingDirectories() throws Exception {
    for (boolean pfs : new boolean[] {false, true}) {
      open(pfs);
      data.clear();
      for (int i = 0; i < 10; i++) data.put("tree/sub" + i + "/file", new byte[2]);
      int lists = server.lists.get(), beforeHeads = heads.get(), flat = server.flatLists.get();
      RemoteIterator<LocatedFileStatus> iterator = fs.listFiles(new Path("/tree"), true);
      assertEquals(lists + 1, server.lists.get());
      Set<Path> paths = new HashSet<>();
      while (iterator.hasNext()) {
        LocatedFileStatus status = iterator.next();
        assertTrue(status.isFile());
        assertTrue(paths.add(status.getPath()));
        assertEquals(1, status.getBlockLocations().length);
        assertEquals(2, status.getBlockLocations()[0].getLength());
      }
      assertEquals(10, paths.size());
      assertEquals(lists + 4, server.lists.get());
      assertEquals(flat + 4, server.flatLists.get());
      assertEquals(beforeHeads + (pfs ? 0 : 11), heads.get()); // Root plus ten distinct ancestors.
      RemoteIterator<LocatedFileStatus> abandoned = fs.listFiles(new Path("/tree"), true);
      int afterFirst = server.lists.get();
      ((Closeable) abandoned).close();
      try {
        abandoned.next();
        fail("closed iterator must fail");
      } catch (IOException expected) {
      }
      assertEquals(afterFirst, server.lists.get());
    }
  }

  @Test
  public void fileIteratorSkipsDirectoryOnlyPagesAndSupportsNonRecursiveMode() throws Exception {
    data.put("tree/a/", new byte[0]);
    data.put("tree/b/", new byte[0]);
    data.put("tree/c/", new byte[0]);
    data.put("tree/zfile", new byte[4]);
    data.put("tree/a/nested", new byte[1]);
    RemoteIterator<LocatedFileStatus> shallow = fs.listFiles(new Path("/tree"), false);
    assertEquals("zfile", shallow.next().getPath().getName());
    assertFalse(shallow.hasNext());
    RemoteIterator<LocatedFileStatus> recursive = fs.listFiles(new Path("/tree"), true);
    Set<String> names = new HashSet<>();
    while (recursive.hasNext()) names.add(recursive.next().getPath().getName());
    assertEquals(new HashSet<>(Arrays.asList("nested", "zfile")), names);
    RemoteIterator<LocatedFileStatus> file = fs.listFiles(new Path("/tree/zfile"), true);
    assertEquals(4, file.next().getLen());
    assertFalse(file.hasNext());
    assertFalse(fs.listFiles(new Path("/tree/b"), true).hasNext());
    try {
      fs.listFiles(new Path("/absent-files"), true);
      fail("missing path must fail");
    } catch (FileNotFoundException expected) {
    }
  }

  @Test
  public void directoryIteratorFailureIsSticky() throws Exception {
    for (int i = 0; i < 7; i++) data.put("dir/f" + i, new byte[0]);
    RemoteIterator<FileStatus> iterator = fs.listStatusIterator(new Path("/dir"));
    for (int i = 0; i < 3; i++) assertNotNull(iterator.next());
    server.malformedContinuation = true;
    IOException first;
    try {
      iterator.hasNext();
      throw new AssertionError("must reject malformed page");
    } catch (IOException expected) {
      first = expected;
    }
    int requests = server.lists.get();
    try {
      iterator.next();
      fail("failure must remain terminal");
    } catch (IOException expected) {
      assertSame(first, expected);
    }
    assertEquals(requests, server.lists.get());
  }

  private String listPage(String marker, String... names) {
    StringBuilder xml = new StringBuilder("<ListBucketResult><IsTruncated>");
    xml.append(marker != null).append("</IsTruncated>");
    if (marker != null) xml.append("<NextMarker>").append(marker).append("</NextMarker>");
    for (String name : names)
      xml.append("<Contents><Key>").append(name).append("</Key><Size>0</Size></Contents>");
    return xml.append("</ListBucketResult>").toString();
  }

  @Test
  public void directoryIteratorCanBeClosedBeforeExhaustion() throws Exception {
    for (int i = 0; i < 9; i++) data.put("dir/f" + i, new byte[0]);
    RemoteIterator<FileStatus> iterator = fs.listStatusIterator(new Path("/dir"));
    int requests = server.lists.get();
    ((Closeable) iterator).close();
    ((Closeable) iterator).close();
    try {
      iterator.next();
      fail("must reject explicitly closed iterator");
    } catch (IOException expected) {
    }
    assertEquals(requests, server.lists.get());
  }

  @Test
  public void directoryIteratorSkipsEmptyPagesAndCrossPageDuplicates() throws Exception {
    server.listResponses.add(listPage("a"));
    server.listResponses.add(listPage("b", "dir/file"));
    server.listResponses.add(listPage(null, "dir/file", "dir/other"));
    int before = server.lists.get();
    RemoteIterator<FileStatus> iterator = fs.listStatusIterator(new Path("/dir"));
    assertEquals("file", iterator.next().getPath().getName());
    assertEquals(before + 2, server.lists.get());
    assertEquals("other", iterator.next().getPath().getName());
    assertFalse(iterator.hasNext());
    assertEquals(before + 3, server.lists.get());
  }

  @Test
  public void directoryIteratorRejectsNonAdjacentMarkerCycles() throws Exception {
    server.listResponses.add(listPage("a", "dir/one"));
    server.listResponses.add(listPage("b", "dir/two"));
    server.listResponses.add(listPage("a", "dir/three"));
    RemoteIterator<FileStatus> iterator = fs.listStatusIterator(new Path("/dir"));
    assertEquals("one", iterator.next().getPath().getName());
    assertEquals("two", iterator.next().getPath().getName());
    try {
      iterator.next();
      fail("must reject cycle before exposing its page");
    } catch (IOException expected) {
      assertTrue(expected.getMessage().contains("marker"));
    }
  }

  @Test
  public void directoryIteratorHandlesFilesEmptyMissingAndClose() throws Exception {
    data.put("file", new byte[5]);
    RemoteIterator<FileStatus> file = fs.listStatusIterator(new Path("/file"));
    assertEquals(5, file.next().getLen());
    assertFalse(file.hasNext());
    data.put("empty/", new byte[0]);
    assertFalse(fs.listStatusIterator(new Path("/empty")).hasNext());
    try {
      fs.listStatusIterator(new Path("/missing"));
      fail("must report missing path");
    } catch (FileNotFoundException expected) {
    }
    RemoteIterator<FileStatus> buffered = fs.listStatusIterator(new Path("/"));
    assertTrue(buffered.hasNext());
    fs.close();
    try {
      buffered.next();
      fail("must reject closed filesystem");
    } catch (IOException expected) {
    }
  }

  @Test
  public void createReadSeekAndPositionedRead() throws Exception {
    write("/file", "abcdefghijklmnop");
    assertEquals(16, fs.getFileStatus(new Path("/file")).getLen());
    try (FSDataInputStream in = fs.open(new Path("/file"))) {
      in.seek(4);
      assertEquals('e', in.read());
      byte[] bytes = new byte[4];
      in.readFully(8, bytes);
      assertArrayEquals("ijkl".getBytes(StandardCharsets.UTF_8), bytes);
      assertEquals(5, in.getPos());
      in.seek(100);
      assertEquals(-1, in.read());
    }
    assertEquals("abcdefghijklmnop", read("/file"));
  }

  @Test
  public void overwriteAndEmptyFile() throws Exception {
    write("/file", "first");
    try {
      fs.create(new Path("/file"), false);
      fail("must reject existing file");
    } catch (FileAlreadyExistsException expected) {
    }
    write("/file", "second");
    assertEquals("second", read("/file"));
    write("/empty", "");
    assertTrue(fs.getFileStatus(new Path("/empty")).isFile());
    assertEquals("", read("/empty"));
  }

  @Test
  public void directoryListingAndSummary() throws Exception {
    write("/dir/a", "abc");
    write("/dir/sub/b", "de");
    FileStatus[] statuses = fs.listStatus(new Path("/dir"));
    assertEquals(2, statuses.length);
    ContentSummary summary = fs.getContentSummary(new Path("/dir"));
    assertEquals(2, summary.getFileCount());
    assertEquals(2, summary.getDirectoryCount());
    assertEquals(5, summary.getLength());
    int before = heads.get();
    assertEquals("abc", read("/dir/a"));
    assertEquals(before, heads.get());
  }

  @Test
  public void objectRenameAndRecursiveDelete() throws Exception {
    write("/source/a", "abc");
    fs.mkdirs(new Path("/destination"));
    assertTrue(fs.rename(new Path("/source"), new Path("/destination")));
    assertEquals("abc", read("/destination/source/a"));
    assertFalse(fs.exists(new Path("/source")));
    try {
      fs.delete(new Path("/destination"), false);
      fail("nonempty directory");
    } catch (PathIsNotEmptyDirectoryException expected) {
    }
    assertTrue(fs.delete(new Path("/destination"), true));
    assertFalse(fs.exists(new Path("/destination")));
  }

  @Test
  public void nonRecursiveCreateAndQualifiedPaths() throws Exception {
    try {
      fs.createNonRecursive(
          new Path("/missing/file"),
          new FsPermission((short) 0644),
          false,
          4096,
          (short) 1,
          1024,
          null);
      fail("missing parent");
    } catch (FileNotFoundException expected) {
    }
    fs.mkdirs(new Path("/work"));
    fs.setWorkingDirectory(new Path("/work"));
    write("relative", "hello");
    assertEquals("hello", read("obs://bucket/work/relative"));
    try {
      fs.open(new Path("obs://other/work/relative"));
      fail("wrong bucket");
    } catch (IllegalArgumentException expected) {
    }
  }

  @Test
  public void pfsAppendFlushTruncateAndDelete() throws Exception {
    open(true);
    write("/file", "abc");
    try (FSDataOutputStream out = fs.append(new Path("/file"))) {
      assertEquals(3, fs.getFileStatus(new Path("/file")).getLen());
      out.write("def".getBytes(StandardCharsets.UTF_8));
      out.hsync();
      assertEquals(6, fs.getFileStatus(new Path("/file")).getLen());
      assertArrayEquals("abcdef".getBytes(StandardCharsets.UTF_8), data.get("file"));
    }
    assertTrue(fs.truncate(new Path("/file"), 4));
    assertEquals("abcd", read("/file"));
    int before = heads.get();
    assertTrue(fs.delete(new Path("/file"), false));
    assertEquals(before, heads.get());
    assertFalse(fs.delete(new Path("/file"), false));
  }

  @Test
  public void pfsRecursiveDeleteAndDirectoryRename() throws Exception {
    open(true);
    write("/a/sub/file", "hello");
    assertTrue(fs.rename(new Path("/a"), new Path("/renamed")));
    assertEquals("hello", read("/renamed/sub/file"));
    assertTrue(fs.delete(new Path("/renamed"), true));
    assertTrue(data.isEmpty());
  }

  @Test
  public void directoryCannotBeOverwrittenByFile() throws Exception {
    fs.mkdirs(new Path("/dir"));
    open(false);
    try {
      fs.create(new Path("/dir"), true);
      fail("directory must not be replaced");
    } catch (FileAlreadyExistsException expected) {
    }
  }

  @Test
  public void sameLengthOverwriteFailsInsteadOfMixingVersions() throws Exception {
    write("/changing", "abcdefgh");
    try (FSDataInputStream in = fs.open(new Path("/changing"))) {
      byte[] first = new byte[4];
      in.readFully(first);
      assertArrayEquals("abcd".getBytes(StandardCharsets.UTF_8), first);
      data.put("changing", "12345678".getBytes(StandardCharsets.UTF_8));
      IOException changed = assertThrows(IOException.class, () -> in.read(new byte[4]));
      assertTrue(changed.getMessage().contains("RemoteFileChanged"));
      in.seek(0);
      assertThrows(IOException.class, () -> in.read(new byte[4]));
    }
  }

  @Test
  public void cachedHeadRevisionProtectsFirstGet() throws Exception {
    data.put("stale", "old".getBytes(StandardCharsets.UTF_8));
    fs.getFileStatus(new Path("/stale"));
    int before = heads.get();
    try (FSDataInputStream in = fs.open(new Path("/stale"))) {
      data.put("stale", "new".getBytes(StandardCharsets.UTF_8));
      IOException changed = assertThrows(IOException.class, () -> in.read(new byte[3]));
      assertTrue(changed.getMessage().contains("RemoteFileChanged"));
    }
    assertEquals(before, heads.get());
  }

  @Test
  public void renameDoesNotDeleteSourceAfterEmbeddedCopyFailure() throws Exception {
    write("/source/file", "keep this data");
    server.failCopy = true;
    try {
      fs.rename(new Path("/source"), new Path("/target"));
      fail("copy error must fail rename");
    } catch (IOException expected) {
      assertTrue(expected.getMessage().contains("CopyFailed"));
    }
    assertEquals("keep this data", read("/source/file"));
    assertFalse(fs.exists(new Path("/target/file")));
  }

  @Test
  public void failedMultiChunkReadKeepsJavaAndNativePositionsAligned() throws Exception {
    write("/read-failure", "abcdefghijklmnop");
    server.failedRangeStart = 4;
    server.rangeFailures.set(3); // Exhaust the initial request and two retries.
    try (FSDataInputStream in = fs.open(new Path("/read-failure"))) {
      byte[] bytes = new byte[8];
      try {
        in.read(bytes);
        fail("injected range failures must surface");
      } catch (IOException expected) {
        assertTrue(expected.getMessage().contains("InjectedRangeFailure"));
      }
      assertEquals(0, in.getPos());
      in.readFully(bytes);
      assertArrayEquals("abcdefgh".getBytes(StandardCharsets.UTF_8), bytes);
      assertEquals(8, in.getPos());
    }
  }
  @Test
  public void directoryConflictPreflightPreservesEveryObject() throws Exception {
    for (boolean rename : new boolean[] {false, true}) {
      for (String hidden : new String[] {"src/a/", "src/a/b"}) {
        open(false);
        data.clear();
        for (String key : new String[] {"src/", "src/a", hidden, "src/ok"}) {
          data.put(key, key.getBytes(StandardCharsets.UTF_8));
        }
        java.util.Map<String, byte[]> original = new java.util.TreeMap<>(data);
        IOException error = assertThrows(IOException.class, () -> {
          if (rename) {
            fs.rename(new Path("/src"), new Path("/dst"));
          } else {
            fs.delete(new Path("/src"), true);
          }
        });
        assertTrue(error.getMessage(), error.getMessage().contains("file prefix conflict"));
        assertEquals(original.keySet(), data.keySet());
        for (String key : original.keySet()) {
          assertArrayEquals(original.get(key), data.get(key));
        }
      }
    }
  }
  @Test
  public void singleFileConflictPreflightPreservesHiddenObjects() throws Exception {
    for (int operation = 0; operation < 3; operation++) {
      for (String hidden : new String[] {"a/", "a/b"}) {
        open(false);
        data.clear();
        for (String key : new String[] {"a", hidden}) {
          data.put(key, key.getBytes(StandardCharsets.UTF_8));
        }
        java.util.Map<String, byte[]> original = new java.util.TreeMap<>(data);
        final int action = operation;
        IOException error = assertThrows(IOException.class, () -> {
          if (action == 2) {
            fs.rename(new Path("/a"), new Path("/dst"));
          } else {
            fs.delete(new Path("/a"), action == 1);
          }
        });
        assertTrue(error.getMessage(), error.getMessage().contains("file prefix conflict"));
        assertEquals(original.keySet(), data.keySet());
        for (String key : original.keySet()) {
          assertArrayEquals(original.get(key), data.get(key));
        }
      }
    }
  }
  @Test
  public void streamsRejectOperationsAfterFilesystemClose() throws Exception {
    for (boolean pfs : new boolean[] {false, true}) {
      open(pfs);
      write("/closed-owner-input", "data");
      FSDataInputStream input = fs.open(new Path("/closed-owner-input"));
      FSDataOutputStream output = fs.create(new Path("/closed-owner-output"), true);
      fs.close();
      assertThrows(IOException.class, () -> input.getPos());
      assertThrows(IOException.class, () -> input.available());
      assertThrows(IOException.class, () -> input.read(new byte[0]));
      assertThrows(IOException.class, () -> input.read());
      assertThrows(IOException.class, () -> input.seek(0));
      assertThrows(IOException.class, () -> output.write(new byte[0]));
      assertThrows(IOException.class, () -> output.write(1));
      assertThrows(IOException.class, () -> output.flush());
      input.close();
      input.close();
      output.close();
      output.close();
      assertFalse(data.containsKey("closed-owner-output"));
    }
  }
  @Test
  public void liveFilesystemCannotBeInitializedTwice() throws Exception {
    write("/keep-initialized", "original");
    Configuration conf = new Configuration(fs.getConf());
    assertThrows(IOException.class, () -> fs.initialize(URI.create("obs://bucket"), conf));
    assertEquals("original", read("/keep-initialized"));
  }

  @Test
  public void reopeningFilesystemDoesNotReviveOldStreamsOrBufferedIterators() throws Exception {
    for (boolean pfs : new boolean[] {false, true}) {
      open(pfs);
      write("/old-file", "data");
      FSDataInputStream input = fs.open(new Path("/old-file"));
      FSDataOutputStream output = fs.create(new Path("/old-output"), true);
      RemoteIterator<FileStatus> iterator = fs.listStatusIterator(new Path("/"));
      assertTrue(iterator.hasNext());
      Configuration conf = new Configuration(fs.getConf());
      fs.close();
      fs.initialize(URI.create("obs://bucket"), conf);
      assertThrows(IOException.class, () -> input.getPos());
      assertThrows(IOException.class, () -> input.read(new byte[0]));
      assertThrows(IOException.class, () -> output.write(new byte[0]));
      assertThrows(IOException.class, () -> iterator.hasNext());
      assertThrows(IOException.class, () -> iterator.next());
      input.close();
      output.close();
      ((Closeable) iterator).close();
      assertEquals("data", read("/old-file"));
      assertFalse(data.containsKey("old-output"));
    }
  }
  private void renameWithOptions(Path source, Path destination, Options.Rename... options) throws Exception {
    java.lang.reflect.Method method = FileSystem.class.getDeclaredMethod(
        "rename", Path.class, Path.class, Options.Rename[].class);
    method.setAccessible(true);
    try {
      method.invoke(fs, source, destination, options);
    } catch (java.lang.reflect.InvocationTargetException error) {
      Throwable cause = error.getCause();
      if (cause instanceof Exception) throw (Exception) cause;
      throw new AssertionError(cause);
    }
  }

  @Test
  public void overwriteRenameDoesNotPredeleteDestinationOnFailure() throws Exception {
    for (int scenario = 0; scenario < 4; scenario++) {
      open(false);
      data.clear();
      data.put("source", "new".getBytes(StandardCharsets.UTF_8));
      data.put("target", "original".getBytes(StandardCharsets.UTF_8));
      if (scenario == 0) data.put("source/hidden", new byte[] {1});
      if (scenario == 3) data.put("target/hidden", new byte[] {2});
      server.failCopy = scenario == 1;
      Path destination = new Path(scenario == 2 ? "/source" : "/target");
      java.util.Map<String, byte[]> original = new java.util.TreeMap<>(data);
      assertThrows(IOException.class,
          () -> renameWithOptions(new Path("/source"), destination, Options.Rename.OVERWRITE));
      assertEquals(original.keySet(), data.keySet());
      for (String key : original.keySet()) assertArrayEquals(original.get(key), data.get(key));
      server.failCopy = false;
    }
  }

  @Test
  public void overwriteRenameReplacesFileThroughNativeCore() throws Exception {
    for (boolean pfs : new boolean[] {false, true}) {
    open(pfs);
    write("/overwrite-source", "new");
    write("/overwrite-target", "old");
    renameWithOptions(new Path("/overwrite-source"), new Path("/overwrite-target"), Options.Rename.OVERWRITE);
    assertEquals("new", read("/overwrite-target"));
    assertFalse(fs.exists(new Path("/overwrite-source")));
    }
  }

  @Test
  public void renameOptionsRejectExistingTargetWithoutOverwrite() throws Exception {
    open(false);
    write("/options-source", "new");
    write("/options-target", "old");
    assertThrows(FileAlreadyExistsException.class,
        () -> renameWithOptions(new Path("/options-source"), new Path("/options-target"), Options.Rename.NONE));
    assertEquals("new", read("/options-source"));
    assertEquals("old", read("/options-target"));
  }

  @Test
  public void unsupportedRenameOptionsPreserveObjects() throws Exception {
    open(false);
    data.clear();
    data.put("source/", new byte[0]);
    data.put("source/file", new byte[] {1});
    data.put("target/", new byte[0]);
    java.util.Set<String> original = new java.util.TreeSet<>(data.keySet());
    assertThrows(UnsupportedOperationException.class,
        () -> renameWithOptions(new Path("/source"), new Path("/target"), Options.Rename.OVERWRITE));
    assertEquals(original, data.keySet());
    assertThrows(UnsupportedOperationException.class,
        () -> renameWithOptions(new Path("/source/file"), new Path("/elsewhere"), Options.Rename.TO_TRASH));
    assertEquals(original, data.keySet());
  }
  @Test
  public void renameOptionsPreserveMissingAndNonDirectoryParentErrors() throws Exception {
    open(false);
    data.clear();
    write("/parent-error-source", "keep");
    write("/file-parent", "parent");
    assertThrows(FileNotFoundException.class,
        () -> renameWithOptions(new Path("/parent-error-source"), new Path("/missing-parent/child")));
    assertThrows(ParentNotDirectoryException.class,
        () -> renameWithOptions(new Path("/parent-error-source"), new Path("/file-parent/child")));
    assertEquals("keep", read("/parent-error-source"));
    assertEquals("parent", read("/file-parent"));
  }
  private FSDataOutputStream createWithFlags(int kind, Path path, EnumSet<CreateFlag> flags) throws Exception {
    FsPermission permission = new FsPermission((short) 0600);
    if (kind == 0) return fs.create(path, permission, flags, 4096, (short) 1, 1024, null);
    if (kind == 1) return fs.create(path, permission, flags, 4096, (short) 1, 1024, null, null);
    if (kind == 2) return fs.createNonRecursive(path, permission, flags, 4096, (short) 1, 1024, null);
    java.lang.reflect.Method method = FileSystem.class.getDeclaredMethod("primitiveCreate",
        Path.class, FsPermission.class, EnumSet.class, int.class, short.class, long.class,
        org.apache.hadoop.util.Progressable.class, Options.ChecksumOpt.class);
    method.setAccessible(true);
    try {
      return (FSDataOutputStream) method.invoke(fs, path, permission, flags, 4096, (short) 1, 1024L, null, null);
    } catch (java.lang.reflect.InvocationTargetException error) {
      Throwable cause = error.getCause();
      if (cause instanceof Exception) throw (Exception) cause;
      throw new AssertionError(cause);
    }
  }

  @Test
  public void createFlagOverloadsRejectInvalidCombinationsAndMissingCreate() throws Exception {
    for (int kind = 0; kind < 4; kind++) {
      open(false);
      data.clear();
      write("/flag-existing", "keep");
      final int entry = kind;
      assertThrows(IllegalArgumentException.class, () -> {
        try (FSDataOutputStream out = createWithFlags(entry, new Path("/flag-existing"),
            EnumSet.of(CreateFlag.APPEND, CreateFlag.OVERWRITE))) { out.write(1); }
      });
      assertEquals("keep", read("/flag-existing"));
      assertThrows(FileNotFoundException.class, () -> {
        try (FSDataOutputStream out = createWithFlags(entry, new Path("/flag-missing"),
            EnumSet.of(CreateFlag.OVERWRITE))) { out.write(1); }
      });
      assertFalse(data.containsKey("flag-missing"));
    }
  }

  @Test
  public void createFlagAppendUsesPfsAppendAndCreateFallback() throws Exception {
    for (int kind = 0; kind < 4; kind++) {
      open(true);
      data.clear();
      write("/flag-append", "a");
      try (FSDataOutputStream out = createWithFlags(kind, new Path("/flag-append"), EnumSet.of(CreateFlag.APPEND))) {
        assertEquals(1, out.getPos());
        out.write('b');
      }
      assertEquals("ab", read("/flag-append"));
      try (FSDataOutputStream out = createWithFlags(kind, new Path("/flag-new"),
          EnumSet.of(CreateFlag.CREATE, CreateFlag.APPEND))) { out.write('n'); }
      assertEquals("n", read("/flag-new"));
    }
  }

  @Test
  public void createFlagOverloadsRejectUnsupportedStorageFlags() throws Exception {
    for (int kind = 0; kind < 4; kind++) {
      open(false);
      data.clear();
      final int entry = kind;
      for (CreateFlag flag : CreateFlag.values()) {
        if (flag == CreateFlag.CREATE || flag == CreateFlag.APPEND || flag == CreateFlag.OVERWRITE) continue;
        assertThrows(UnsupportedOperationException.class, () -> {
          try (FSDataOutputStream out = createWithFlags(entry, new Path("/unsupported-flag"),
              EnumSet.of(CreateFlag.CREATE, flag))) { out.write(1); }
        });
        assertFalse(data.containsKey("unsupported-flag"));
      }
    }
  }

  @Test
  public void deleteOnExitRunsBeforeNativeCloseAndHonorsCancellation() throws Exception {
    for (boolean pfs : new boolean[] {false, true}) {
      open(pfs);
      data.clear();
      write("/exit-file", "remove");
      write("/exit-directory/child", "remove");
      write("/exit-kept", "keep");
      assertTrue(fs.deleteOnExit(new Path("/exit-file")));
      assertTrue(fs.deleteOnExit(new Path("/exit-directory")));
      assertTrue(fs.deleteOnExit(new Path("/exit-kept")));
      assertTrue(fs.cancelDeleteOnExit(new Path("/exit-kept")));
      assertFalse(fs.deleteOnExit(new Path("/exit-missing")));
      FSDataInputStream input = fs.open(new Path("/exit-kept"));
      fs.close();
      assertFalse(data.containsKey("exit-file"));
      assertFalse(data.keySet().stream().anyMatch(key -> key.startsWith("exit-directory/")));
      assertArrayEquals("keep".getBytes(StandardCharsets.UTF_8), data.get("exit-kept"));
      assertThrows(IOException.class, () -> input.read());
      input.close();
      fs.close();
    }
  }

  @Test
  public void deleteOnExitCannotBypassRustConflictChecks() throws Exception {
    open(false);
    data.clear();
    data.put("exit-conflict", new byte[] {1});
    data.put("exit-conflict/hidden", new byte[] {2});
    data.put("exit-dir/a", new byte[] {3});
    data.put("exit-dir/a/hidden", new byte[] {4});
    data.put("exit-clean", new byte[] {5});
    assertTrue(fs.deleteOnExit(new Path("/exit-conflict")));
    assertTrue(fs.deleteOnExit(new Path("/exit-dir")));
    assertTrue(fs.deleteOnExit(new Path("/exit-clean")));
    fs.close();
    assertFalse(data.containsKey("exit-clean"));
    assertEquals(new TreeSet<>(Arrays.asList("exit-conflict", "exit-conflict/hidden",
        "exit-dir/a", "exit-dir/a/hidden")), data.keySet());
    assertArrayEquals(new byte[] {1}, data.get("exit-conflict"));
    assertArrayEquals(new byte[] {2}, data.get("exit-conflict/hidden"));
    assertArrayEquals(new byte[] {3}, data.get("exit-dir/a"));
    assertArrayEquals(new byte[] {4}, data.get("exit-dir/a/hidden"));
    assertThrows(IOException.class, () -> fs.getFileStatus(new Path("/exit-conflict")));
    fs.close();
  }

  @Test
  public void appendNewBlockIsRejectedWithoutChangingContent() throws Exception {
    for (boolean pfs : new boolean[] {false, true}) {
      open(pfs);
      data.clear();
      write("/new-block", "keep");
      assertThrows(UnsupportedOperationException.class, () -> {
        try (FSDataOutputStream out = fs.append(new Path("/new-block"), true)) { out.write(1); }
      });
      assertThrows(UnsupportedOperationException.class, () -> {
        try (FSDataOutputStream out = fs.append(new Path("/new-block"), 4096, null, true)) { out.write(2); }
      });
      assertEquals("keep", read("/new-block"));
      if (pfs) {
        try (FSDataOutputStream out = fs.append(new Path("/new-block"), false)) { out.write('a'); }
        try (FSDataOutputStream out = fs.append(new Path("/new-block"), 4096, null, false)) { out.write('b'); }
        assertEquals("keepab", read("/new-block"));
      } else {
        assertThrows(UnsupportedOperationException.class, () -> fs.append(new Path("/new-block"), false));
      }
    }
  }

  @Test
  public void outputBuildersPreserveFlagAndNamespaceChecks() throws Exception {
    open(false);
    data.clear();
    write("/builder-file", "keep");
    for (boolean recursive : new boolean[] {false, true}) {
      FSDataOutputStreamBuilder<?, ?> invalid = fs.createFile(new Path("/builder-file")).append();
      if (recursive) invalid.recursive();
      assertThrows(IllegalArgumentException.class, () -> {
        try (FSDataOutputStream out = invalid.build()) { out.write(1); }
      });
      data.put("builder-file/hidden", new byte[] {2});
      FSDataOutputStreamBuilder<?, ?> overwrite = fs.createFile(new Path("/builder-file/hidden"));
      if (recursive) overwrite.recursive();
      assertThrows(IOException.class, () -> {
        try (FSDataOutputStream out = overwrite.build()) { out.write(1); }
      });
      assertArrayEquals("keep".getBytes(StandardCharsets.UTF_8), data.get("builder-file"));
      assertArrayEquals(new byte[] {2}, data.get("builder-file/hidden"));
      assertThrows(IOException.class, () -> fs.appendFile(new Path("/builder-file/hidden")).build());
    }
    open(true);
    data.clear();
    try (FSDataOutputStream out = fs.createFile(new Path("/builder-parent/file")).recursive().build()) {
      out.write('a');
    }
    try (FSDataOutputStream out = fs.appendFile(new Path("/builder-parent/file")).build()) {
      assertEquals(1, out.getPos());
      out.write('b');
    }
    assertEquals("ab", read("/builder-parent/file"));
  }

  @Test
  public void unsupportedMetadataUpdatesDoNotReportSuccess() throws Exception {
    for (boolean pfs : new boolean[] {false, true}) {
      open(pfs);
      data.clear();
      write("/metadata-file", "keep");
      FileStatus before = fs.getFileStatus(new Path("/metadata-file"));
      assertFalse(fs.setReplication(new Path("/metadata-file"), (short) 2));
      assertThrows(IllegalArgumentException.class,
          () -> fs.setReplication(new Path("/metadata-file"), (short) 0));
      assertThrows(UnsupportedOperationException.class,
          () -> fs.setTimes(new Path("/metadata-file"), 1234, -1));
      assertThrows(UnsupportedOperationException.class,
          () -> fs.setTimes(new Path("/metadata-file"), -1, 5678));
      fs.setTimes(new Path("/metadata-file"), -1, -1);
      FileStatus after = fs.getFileStatus(new Path("/metadata-file"));
      assertEquals(before.getModificationTime(), after.getModificationTime());
      assertEquals(before.getReplication(), after.getReplication());
      assertEquals("keep", read("/metadata-file"));
    }
  }

  @Test
  public void metadataDefaultsValidatePathsThroughRust() throws Exception {
    open(false);
    data.clear();
    data.put("metadata-parent", new byte[] {1});
    data.put("metadata-parent/hidden", new byte[] {2});
    assertThrows(FileNotFoundException.class,
        () -> fs.setReplication(new Path("/metadata-missing"), (short) 1));
    assertThrows(FileNotFoundException.class,
        () -> fs.setTimes(new Path("/metadata-missing"), -1, -1));
    assertThrows(FileNotFoundException.class,
        () -> fs.setReplication(new Path("/metadata-parent/hidden"), (short) 1));
    assertThrows(FileNotFoundException.class,
        () -> fs.setTimes(new Path("/metadata-parent/hidden"), 1234, -1));
    assertThrows(IllegalArgumentException.class,
        () -> fs.setReplication(new Path("obs://other/key"), (short) 1));
    assertThrows(IllegalArgumentException.class,
        () -> fs.setTimes(new Path("obs://other/key"), 1234, -1));
    fs.close();
    assertThrows(IOException.class,
        () -> fs.setReplication(new Path("/metadata-parent"), (short) 1));
    assertThrows(IOException.class,
        () -> fs.setTimes(new Path("/metadata-parent"), -1, -1));
    assertEquals(2, data.size());
    assertArrayEquals(new byte[] {1}, data.get("metadata-parent"));
    assertArrayEquals(new byte[] {2}, data.get("metadata-parent/hidden"));
  }

  @Test
  public void checksumQueriesValidatePathAndLengthWithoutInventingChecksum() throws Exception {
    for (boolean pfs : new boolean[] {false, true}) {
      open(pfs);
      data.clear();
      write("/checksum-file", "keep");
      assertNull(fs.getFileChecksum(new Path("/checksum-file")));
      assertNull(fs.getFileChecksum(new Path("/checksum-file"), 0));
      assertNull(fs.getFileChecksum(new Path("/checksum-file"), 2));
      assertThrows(IllegalArgumentException.class,
          () -> fs.getFileChecksum(new Path("/checksum-file"), -1));
      assertThrows(FileNotFoundException.class,
          () -> fs.getFileChecksum(new Path("/checksum-missing")));
      assertThrows(IllegalArgumentException.class,
          () -> fs.getFileChecksum(new Path("obs://other/key")));
      assertEquals("keep", read("/checksum-file"));
      fs.close();
      assertThrows(IOException.class,
          () -> fs.getFileChecksum(new Path("/checksum-file")));
    }
  }

  @Test
  public void checksumAndInheritedQueriesRejectHiddenDescendants() throws Exception {
    open(false);
    data.clear();
    data.put("query-file", new byte[] {1});
    data.put("query-file/hidden", new byte[] {2});
    Path hidden = new Path("/query-file/hidden");
    assertThrows(FileNotFoundException.class, () -> fs.getFileChecksum(hidden));
    assertThrows(FileNotFoundException.class, () -> fs.getFileChecksum(hidden, 1));
    assertThrows(FileNotFoundException.class, () -> fs.resolvePath(hidden));
    assertThrows(FileNotFoundException.class, () -> fs.getFileLinkStatus(hidden));
    assertThrows(FileNotFoundException.class, () -> fs.getFileBlockLocations(hidden, 0, 1));
    assertThrows(FileNotFoundException.class, () -> fs.listLocatedStatus(hidden));
    assertThrows(FileNotFoundException.class,
        () -> fs.access(hidden, org.apache.hadoop.fs.permission.FsAction.READ));
    RemoteIterator<LocatedFileStatus> entries = fs.listLocatedStatus(new Path("/"));
    assertTrue(entries.hasNext());
    assertEquals("/query-file", entries.next().getPath().toUri().getPath());
    assertFalse(entries.hasNext());
    assertEquals(2, data.size());
  }

  @SuppressWarnings("unchecked")
  private RemoteIterator<LocatedFileStatus> locatedWithFilter(Path path, PathFilter filter) throws Exception {
    java.lang.reflect.Method method = FileSystem.class.getDeclaredMethod("listLocatedStatus", Path.class, PathFilter.class);
    method.setAccessible(true);
    try { return (RemoteIterator<LocatedFileStatus>) method.invoke(fs, path, filter); }
    catch (java.lang.reflect.InvocationTargetException error) {
      Throwable cause = error.getCause();
      if (cause instanceof Exception) throw (Exception) cause;
      throw new AssertionError(cause);
    }
  }

  @Test
  public void locatedStatusFetchesPagesOnDemandAndCanCloseEarly() throws Exception {
    for (boolean pfs : new boolean[] {false, true}) {
      open(pfs);
      data.clear();
      for (int i = 0; i < 30; i++) data.put(String.format("located-%02d", i), new byte[] {1});
      int before = server.lists.get();
      RemoteIterator<LocatedFileStatus> entries = fs.listLocatedStatus(new Path("/"));
      assertEquals(1, server.lists.get() - before);
      assertTrue(entries instanceof Closeable);
      assertTrue(entries.hasNext());
      assertTrue(entries.hasNext());
      assertEquals("/located-00", entries.next().getPath().toUri().getPath());
      assertEquals(1, server.lists.get() - before);
      ((Closeable) entries).close();
      ((Closeable) entries).close();
      final RemoteIterator<LocatedFileStatus> closedEntries = entries;
      assertThrows(IOException.class, () -> closedEntries.hasNext());
      assertEquals(1, server.lists.get() - before);
      entries = fs.listLocatedStatus(new Path("/"));
      int count = 0;
      while (entries.hasNext()) { entries.next(); count++; }
      assertEquals(30, count);
      assertFalse(entries.hasNext());
      final RemoteIterator<LocatedFileStatus> exhausted = entries;
      assertThrows(NoSuchElementException.class, () -> exhausted.next());
      ((Closeable) entries).close();
    }
  }

  @Test
  public void locatedStatusFiltersAcrossPagesAndPreservesDirectoryEntries() throws Exception {
    open(false);
    data.clear();
    for (int i = 0; i < 12; i++) data.put(String.format("filtered-%02d", i), new byte[] {1});
    data.put("folder/child", new byte[] {2});
    RemoteIterator<LocatedFileStatus> entries = locatedWithFilter(new Path("/"),
        path -> path.getName().equals("filtered-10") || path.getName().equals("folder"));
    assertTrue(entries.hasNext());
    assertTrue(entries.hasNext());
    LocatedFileStatus file = entries.next();
    assertEquals("filtered-10", file.getPath().getName());
    assertTrue(file.isFile());
    assertEquals(1, file.getBlockLocations().length);
    assertTrue(entries.next().isDirectory());
    assertFalse(entries.hasNext());
    ((Closeable) entries).close();
    RemoteIterator<LocatedFileStatus> empty = locatedWithFilter(new Path("/"), path -> false);
    assertFalse(empty.hasNext());
    ((Closeable) empty).close();
  }

  @Test
  public void locatedStatusReleasesCursorOnFilterFailureAndRejectsClosedOwner() throws Exception {
    open(false);
    data.clear();
    for (int i = 0; i < 12; i++) data.put("failure-" + i, new byte[] {1});
    java.lang.reflect.Field liveField = ListingCleanup.class.getDeclaredField("LIVE");
    liveField.setAccessible(true);
    Set<?> live = (Set<?>) liveField.get(null);
    Set<Object> previous = new HashSet<>(live);
    RemoteIterator<LocatedFileStatus> failed = locatedWithFilter(new Path("/"), path -> {
      throw new IllegalStateException("filter failure");
    });
    Set<Object> added = new HashSet<>(live);
    added.removeAll(previous);
    assertEquals(1, added.size());
    int requests = server.lists.get();
    assertThrows(IllegalStateException.class, () -> failed.hasNext());
    assertTrue(Collections.disjoint(live, added));
    assertThrows(IOException.class, () -> failed.hasNext());
    ((Closeable) failed).close();
    assertEquals(requests, server.lists.get());
    RemoteIterator<LocatedFileStatus> buffered = locatedWithFilter(new Path("/"), path -> true);
    assertTrue(buffered.hasNext());
    fs.close();
    assertThrows(IOException.class, () -> buffered.hasNext());
    assertThrows(IOException.class, () -> buffered.next());
    ((Closeable) buffered).close();
  }

  private void primitiveMkdir(Path path, boolean createParent) throws Exception {
    java.lang.reflect.Method method = FileSystem.class.getDeclaredMethod("primitiveMkdir",
        Path.class, FsPermission.class, boolean.class);
    method.setAccessible(true);
    try { method.invoke(fs, path, new FsPermission((short) 0700), createParent); }
    catch (java.lang.reflect.InvocationTargetException error) {
      Throwable cause = error.getCause();
      if (cause instanceof Exception) throw (Exception) cause;
      throw new AssertionError(cause);
    }
  }

  @Test
  public void primitiveMkdirHandlesRootAndRelativePathsWithoutCreatingParents() throws Exception {
    for (boolean pfs : new boolean[] {false, true}) {
      open(pfs);
      data.clear();
      primitiveMkdir(new Path("/"), false);
      assertTrue(data.isEmpty());
      fs.mkdirs(new Path("/working"));
      fs.setWorkingDirectory(new Path("/working"));
      primitiveMkdir(new Path("child"), false);
      assertTrue(fs.getFileStatus(new Path("/working/child")).isDirectory());
      assertFalse(data.containsKey("child/"));
      assertThrows(FileNotFoundException.class,
          () -> primitiveMkdir(new Path("missing/child"), false));
      assertFalse(data.keySet().stream().anyMatch(key -> key.startsWith("working/missing/")));
      primitiveMkdir(new Path("recursive/child"), true);
      assertTrue(fs.getFileStatus(new Path("/working/recursive/child")).isDirectory());
    }
  }

  @Test
  public void primitiveMkdirPreservesFileParentAndHiddenDescendantChecks() throws Exception {
    open(false);
    data.clear();
    data.put("mkdir-file", new byte[] {1});
    data.put("mkdir-file/hidden", new byte[] {2});
    assertThrows(ParentNotDirectoryException.class,
        () -> primitiveMkdir(new Path("/mkdir-file/new"), false));
    assertThrows(FileNotFoundException.class,
        () -> primitiveMkdir(new Path("/mkdir-file/hidden/new"), false));
    assertThrows(IllegalArgumentException.class,
        () -> primitiveMkdir(new Path("obs://other/new"), false));
    assertEquals(2, data.size());
    assertArrayEquals(new byte[] {1}, data.get("mkdir-file"));
    assertArrayEquals(new byte[] {2}, data.get("mkdir-file/hidden"));
  }

  @Test
  public void deleteOnExitKeepsRegistrationDirectoryAfterWorkingDirectoryChanges() throws Exception {
    for (boolean pfs : new boolean[] {false, true}) {
      open(pfs);
      data.clear();
      write("/exit-first/same", "remove");
      write("/exit-second/same", "keep");
      fs.setWorkingDirectory(new Path("/exit-first"));
      assertTrue(fs.deleteOnExit(new Path("same")));
      fs.setWorkingDirectory(new Path("/exit-second"));
      fs.close();
      assertFalse(data.containsKey("exit-first/same"));
      assertArrayEquals("keep".getBytes(StandardCharsets.UTF_8), data.get("exit-second/same"));
    }
  }

  @Test
  public void deleteOnExitCancellationUsesQualifiedPathIdentity() throws Exception {
    open(false);
    data.clear();
    write("/cancel-directory/file", "keep");
    fs.setWorkingDirectory(new Path("/cancel-directory"));
    assertTrue(fs.deleteOnExit(new Path("file")));
    assertTrue(fs.cancelDeleteOnExit(new Path("obs://bucket/cancel-directory/file")));
    assertTrue(fs.deleteOnExit(new Path("/cancel-directory/file")));
    assertTrue(fs.cancelDeleteOnExit(new Path("file")));
    fs.close();
    assertArrayEquals("keep".getBytes(StandardCharsets.UTF_8), data.get("cancel-directory/file"));
  }

  @Test
  public void inheritedGlobMultiPathAndUsageRespectVisibleNamespace() throws Exception {
    open(false);
    data.clear();
    data.put("visible-file", new byte[] {1});
    data.put("visible-file/hidden", new byte[20]);
    data.put("visible-directory/child", new byte[] {2, 3, 4});
    FileStatus[] root = fs.globStatus(new Path("/visible-*"));
    assertEquals(2, root.length);
    assertNull(fs.globStatus(new Path("/visible-file/hidden")));
    assertEquals(0, fs.globStatus(new Path("/visible-file/*")).length);
    FileStatus[] filtered = fs.listStatus(new Path[] {new Path("/visible-file"),
        new Path("/visible-directory")}, path -> path.getName().equals("child"));
    assertEquals(1, filtered.length);
    assertEquals("/visible-directory/child", filtered[0].getPath().toUri().getPath());
    assertThrows(FileNotFoundException.class,
        () -> fs.listStatus(new Path[] {new Path("/visible-file/hidden")}));
    assertEquals(4, fs.getUsed());
    assertEquals(1, fs.getUsed(new Path("/visible-file")));
    assertEquals(3, fs.getUsed(new Path("/visible-directory")));
    assertThrows(FileNotFoundException.class,
        () -> fs.getUsed(new Path("/visible-file/hidden")));
  }

  @Test
  public void openFileBuilderHintsCannotBypassRustVisibilityOrSupplyContents() throws Exception {
    for (boolean pfs : new boolean[] {false, true}) {
      open(pfs);
      data.clear();
      write("/hint-file", "data");
      FileStatus hint = new FileStatus(999, false, 1, 1024, 0, new Path("obs://bucket/hint-file"));
      try (FSDataInputStream input = fs.openFile(new Path("/hint-file")).withFileStatus(hint).build().get()) {
        byte[] actual = new byte[4];
        input.readFully(actual);
        assertArrayEquals("data".getBytes(StandardCharsets.UTF_8), actual);
        assertEquals(-1, input.read());
      }
    }
    open(false);
    data.clear();
    data.put("hint-parent", new byte[] {1});
    data.put("hint-parent/hidden", new byte[] {2});
    Path hidden = new Path("/hint-parent/hidden");
    FileStatus hint = new FileStatus(1, false, 1, 1024, 0, hidden);
    java.util.concurrent.ExecutionException error = assertThrows(java.util.concurrent.ExecutionException.class,
        () -> fs.openFile(hidden).withFileStatus(hint).build().get());
    assertTrue(error.getCause() instanceof FileNotFoundException);
  }

  @Test
  public void copyFromLocalPreservesSourceAndObjectsWhenRustRejectsDestination() throws Exception {
    open(false);
    data.clear();
    data.put("copy-parent", new byte[] {1});
    data.put("copy-parent/hidden", new byte[] {2});
    java.nio.file.Path directory = java.nio.file.Files.createTempDirectory(
        java.nio.file.Paths.get("target"), "copy-local-");
    java.nio.file.Path local = directory.resolve("source");
    try {
      java.nio.file.Files.write(local, new byte[] {3});
      Path source = new Path(local.toAbsolutePath().toUri());
      assertThrows(IOException.class,
          () -> fs.copyFromLocalFile(true, false, source, new Path("/copy-parent/hidden")));
      assertArrayEquals(new byte[] {3}, java.nio.file.Files.readAllBytes(local));
      assertEquals(2, data.size());
      assertArrayEquals(new byte[] {1}, data.get("copy-parent"));
      assertArrayEquals(new byte[] {2}, data.get("copy-parent/hidden"));
      assertThrows(IOException.class,
          () -> fs.copyFromLocalFile(true, false, source, new Path("/copy-parent")));
      assertTrue(java.nio.file.Files.exists(local));
    } finally {
      java.nio.file.Files.deleteIfExists(local);
      java.nio.file.Files.deleteIfExists(directory);
    }
  }

  @Test
  public void copyFromLocalMovesSourceOnlyAfterSuccessfulUpload() throws Exception {
    java.nio.file.Path directory = java.nio.file.Files.createTempDirectory(
        java.nio.file.Paths.get("target"), "copy-local-success-");
    java.nio.file.Path local = directory.resolve("source");
    try {
      for (boolean pfs : new boolean[] {false, true}) {
        open(pfs);
        data.clear();
        java.nio.file.Files.write(local, "copied".getBytes(StandardCharsets.UTF_8));
        Path source = new Path(local.toAbsolutePath().toUri());
        fs.copyFromLocalFile(false, false, source, new Path("/local-copy"));
        assertTrue(java.nio.file.Files.exists(local));
        assertEquals("copied", read("/local-copy"));
        fs.copyFromLocalFile(true, false, source, new Path("/local-move"));
        assertFalse(java.nio.file.Files.exists(local));
        assertEquals("copied", read("/local-move"));
      }
    } finally {
      java.nio.file.Files.deleteIfExists(local);
      java.nio.file.Files.deleteIfExists(directory);
    }
  }

  @Test
  public void copyUploadFailureKeepsLocalSourceBeforeAndAfterRemoteCommit() throws Exception {
    java.nio.file.Path directory = java.nio.file.Files.createTempDirectory(
        java.nio.file.Paths.get("target"), "copy-local-failure-");
    java.nio.file.Path local = directory.resolve("source");
    try {
      for (boolean pfs : new boolean[] {false, true}) {
        for (boolean committed : new boolean[] {false, true}) {
          for (boolean existing : new boolean[] {false, true}) {
            open(pfs);
            data.clear();
            if (existing) write("/copy-failure", "old");
            java.nio.file.Files.write(local, "replacement".getBytes(StandardCharsets.UTF_8));
            Path source = new Path(local.toAbsolutePath().toUri());
            server.failPutAfterCommit = committed;
            server.failPutKey = "copy-failure";
            int failures = server.putFailures.get();
            assertThrows(IOException.class,
                () -> fs.copyFromLocalFile(true, true, source, new Path("/copy-failure")));
            assertTrue(server.putFailures.get() > failures);
            assertArrayEquals("replacement".getBytes(StandardCharsets.UTF_8), java.nio.file.Files.readAllBytes(local));
            server.failPutKey = null;
            if (committed) assertEquals("replacement", read("/copy-failure"));
            else if (existing) assertEquals("old", read("/copy-failure"));
            else assertFalse(fs.exists(new Path("/copy-failure")));
            fs.copyFromLocalFile(true, true, source, new Path("/copy-failure"));
            assertFalse(java.nio.file.Files.exists(local));
            assertEquals("replacement", read("/copy-failure"));
          }
        }
      }
    } finally {
      server.failPutKey = null;
      server.failPutAfterCommit = false;
      java.nio.file.Files.deleteIfExists(local);
      java.nio.file.Files.deleteIfExists(directory);
    }
  }

  /** Test-only destination: keep Hadoop local I/O, omit host-specific chmod/winutils. */
  public static final class PermissionlessLocalFileSystem extends LocalFileSystem {
    public PermissionlessLocalFileSystem() {
      super(new RawLocalFileSystem() {
        @Override
        public void setPermission(Path path, FsPermission permission) {
          // Permission propagation is outside this failure-path test.
        }
      });
    }
  }

  @Test
  public void failedDownloadMoveKeepsRemoteSourceAndCanRetry() throws Exception {
    java.nio.file.Path directory = java.nio.file.Files.createTempDirectory(
        java.nio.file.Paths.get("target"), "download-failure-");
    java.nio.file.Path local = directory.resolve("destination");
    try {
      for (boolean pfs : new boolean[] {false, true}) {
        open(pfs);
        fs.getConf().setClass("fs.file.impl", PermissionlessLocalFileSystem.class, FileSystem.class);
        fs.getConf().setBoolean("fs.file.impl.disable.cache", true);
        data.clear();
        write("/download-source", "abcdefghijkl");
        server.failedRangeStart = 4;
        server.rangeFailures.set(3);
        Path destination = new Path(local.toAbsolutePath().toUri());
        assertThrows(IOException.class,
            () -> fs.copyToLocalFile(true, new Path("/download-source"), destination, true));
        assertEquals(0, server.rangeFailures.get());
        assertArrayEquals("abcdefghijkl".getBytes(StandardCharsets.UTF_8), data.get("download-source"));
        server.failedRangeStart = -1;
        fs.copyToLocalFile(true, new Path("/download-source"), destination, true);
        assertArrayEquals("abcdefghijkl".getBytes(StandardCharsets.UTF_8), java.nio.file.Files.readAllBytes(local));
        assertFalse(data.containsKey("download-source"));
        java.nio.file.Files.delete(local);
      }
    } finally {
      server.failedRangeStart = -1;
      server.rangeFailures.set(0);
      java.nio.file.Files.deleteIfExists(local);
      java.nio.file.Files.deleteIfExists(directory);
    }
  }
}
