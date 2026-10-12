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
      assertEquals(headBefore, heads.get());
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
      assertEquals(beforeHeads, heads.get());
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
}
