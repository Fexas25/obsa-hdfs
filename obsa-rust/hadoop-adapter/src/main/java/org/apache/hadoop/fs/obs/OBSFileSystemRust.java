package org.apache.hadoop.fs.obs;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.*;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.ReadOnlyBufferException;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Progressable;

/** Opt-in native filesystem. Configure fs.obs.impl to this class. */
public final class OBSFileSystemRust extends FileSystem {
  private static final ObjectMapper JSON = new ObjectMapper();
  private URI uri;
  private Path workingDirectory;
  private volatile long nativeHandle;
  private long defaultBlockSize = 128L * 1024 * 1024;
  private boolean contentSummaryEnabled;

  @Override
  public synchronized void initialize(URI name, Configuration conf) throws IOException {
    if (nativeHandle != 0) throw new IOException("Filesystem is already initialized");
    super.initialize(name, conf);
    setConf(conf);
    if (!"obs".equals(name.getScheme()) || name.getHost() == null)
      throw new IOException("Expected obs://bucket URI");
    uri = URI.create("obs://" + name.getAuthority());
    workingDirectory =
        new Path("/user", UserGroupInformation.getCurrentUser().getShortUserName())
            .makeQualified(uri, new Path("/"));
    Map<String, String> properties = new HashMap<>();
    for (Map.Entry<String, String> entry : conf)
      if (entry.getKey().startsWith("fs.obs.")) properties.put(entry.getKey(), entry.getValue());
    resolveCredentials(conf, name.getHost(), properties);
    ObjectNode config = JSON.createObjectNode();
    config.put("bucket", name.getHost());
    config.set("hadoop", JSON.valueToTree(properties));
    nativeHandle = NativeObs.nativeCreate(config.toString());
    try {
      defaultBlockSize = call("blockSize", JSON.createObjectNode()).asLong();
      contentSummaryEnabled = call("contentSummaryEnabled", JSON.createObjectNode()).asBoolean();
    } catch (IOException error) {
      try {
        close();
      } catch (IOException cleanup) {
        error.addSuppressed(cleanup);
      }
      throw error;
    }
  }

  static void resolveCredentials(Configuration conf, String bucket, Map<String, String> properties)
      throws IOException {
    Configuration credentials = new Configuration(conf);
    // Bucket scope wins over common scope; the reference adapter's canonical
    // key wins over the shorter spelling found in the handoff appendix.
    String providers = null;
    for (String key :
        Arrays.asList(
            "fs.obs.bucket." + bucket + ".security.credential.provider.path",
            "fs.obs.bucket." + bucket + ".credential.provider.path",
            "fs.obs.security.credential.provider.path",
            "fs.obs.credential.provider.path")) {
      if (conf.get(key) != null) {
        providers = conf.get(key);
        break;
      }
    }
    if (providers != null) {
      String globalKey =
          org.apache.hadoop.security.alias.CredentialProviderFactory.CREDENTIAL_PROVIDER_PATH;
      LinkedHashSet<String> combined = new LinkedHashSet<>();
      for (String provider : providers.split(",")) {
        if (!provider.trim().isEmpty()) combined.add(provider.trim());
      }
      combined.addAll(conf.getTrimmedStringCollection(globalKey));
      credentials.set(globalKey, String.join(",", combined));
    }
    for (String suffix : Arrays.asList("access.key", "secret.key", "session.token")) {
      for (String prefix : Arrays.asList("fs.obs.", "fs.obs.bucket." + bucket + ".")) {
        String key = prefix + suffix;
        // Remove copied plaintext first, so disabling Hadoop's clear-text
        // fallback cannot accidentally leave it in the native configuration.
        properties.remove(key);
        char[] value = credentials.getPassword(key);
        if (value != null) {
          try {
            properties.put(key, new String(value));
          } finally {
            Arrays.fill(value, '\0');
          }
        }
      }
    }
  }

  @Override
  public String getScheme() {
    return "obs";
  }

  @Override
  public URI getUri() {
    return uri;
  }

  @Override
  public String getCanonicalServiceName() {
    // The URI host is a bucket identifier, not a resolvable service endpoint.
    return uri == null ? null : getScheme() + "://" + uri.getHost();
  }

  @Override
  protected void checkPath(Path path) {
    super.checkPath(path);
    URI supplied = path.toUri();
    // Hadoop skips authority checks entirely when the scheme is omitted.
    if (supplied.getScheme() == null && supplied.getAuthority() != null && uri != null
        && !supplied.getAuthority().equalsIgnoreCase(uri.getAuthority())) {
      throw new IllegalArgumentException("Wrong FS: " + path + ", expected: " + uri);
    }
  }

  @Override
  public long getDefaultBlockSize() {
    return defaultBlockSize;
  }

  @Override
  public long getDefaultBlockSize(Path path) {
    path(path);
    return defaultBlockSize;
  }

  private String path(Path value) {
    checkPath(value);
    return (value.isAbsolute() ? value : new Path(workingDirectory, value)).toUri().getPath();
  }

  private ObjectNode args(Path path) {
    return JSON.createObjectNode().put("path", path(path));
  }

  private JsonNode call(String operation, ObjectNode args) throws IOException {
    long handle = nativeHandle;
    if (handle == 0) throw new IOException("Filesystem closed");
    try {
      return JSON.readTree(NativeObs.nativeCall(handle, operation, args.toString()));
    } catch (IOException error) {
      String message = error.getMessage();
      if (message != null && message.startsWith("AlreadyExists:"))
        throw new FileAlreadyExistsException(message);
      if (message != null && message.startsWith("ParentNotDirectory:"))
        throw new ParentNotDirectoryException(message);
      if (message != null && message.startsWith("DirectoryNotEmpty:"))
        throw new PathIsNotEmptyDirectoryException(message);
      throw error;
    }
  }

  /** Result of one bounded cleanup pass; individual failures retain their journal records. */
  public static final class UploadRecoveryReport {
    private final int aborted;
    private final int active;
    private final List<String> errors;

    private UploadRecoveryReport(JsonNode value) {
      aborted = value.get("aborted").asInt();
      active = value.get("active").asInt();
      List<String> messages = new ArrayList<>();
      for (JsonNode error : value.get("errors")) messages.add(error.asText());
      errors = Collections.unmodifiableList(messages);
    }

    public int getAborted() { return aborted; }
    public int getActive() { return active; }
    public List<String> getErrors() { return errors; }
  }

  /**
   * Explicitly abort known pending MPUs in the configured local journal (1..10000 records).
   * Requires fs.obs.upload.journal.dir. This is an adapter extension, not a Hadoop FileSystem
   * override. It never replays writes or deletes final objects. Active uploads are skipped;
   * per-record failures are returned in the report, while setup failures throw IOException
   * (a missing journal configuration or invalid limit throws IllegalArgumentException).
   */
  public UploadRecoveryReport recoverPendingUploads(int limit) throws IOException {
    if (limit < 1 || limit > 10000)
      throw new IllegalArgumentException("recovery limit must be 1..10000");
    return new UploadRecoveryReport(
        call("recoverPendingUploads", JSON.createObjectNode().put("limit", limit)));
  }

  private static long nonnegativeLong(JsonNode value, String field) throws IOException {
    if (!value.isIntegralNumber() || !value.canConvertToLong() || value.longValue() < 0)
      throw new IOException("Native " + field + " cannot be represented as a nonnegative Hadoop long");
    return value.longValue();
  }

  private FileStatus status(JsonNode value) throws IOException {
    return new FileStatus(
        nonnegativeLong(value.path("length"), "length"),
        value.path("is_dir").asBoolean(),
        value.path("replication").asInt(1),
        value.path("block_size").asLong(128L * 1024 * 1024),
        value.path("modification_time").asLong(),
        value.path("access_time").asLong(),
        new FsPermission((short) value.path("permission").asInt()),
        value.path("owner").asText(),
        value.path("group").asText(),
        new Path(value.path("path").asText()).makeQualified(uri, workingDirectory));
  }

  @Override
  public FileStatus getFileStatus(Path path) throws IOException {
    return status(call("status", args(path)));
  }

  @Override
  public FileChecksum getFileChecksum(Path path, long length) throws IOException {
    if (length < 0) throw new IllegalArgumentException("Checksum length must be non-negative");
    getFileStatus(path);
    // No Hadoop-compatible checksum is available; OBS ETags are not interchangeable.
    return null;
  }

  @Override
  public boolean exists(Path path) throws IOException {
    return call("exists", args(path)).asBoolean();
  }

  @Override
  public FileStatus[] listStatus(Path path) throws IOException {
    JsonNode values = call("list", args(path));
    List<FileStatus> result = new ArrayList<>();
    for (JsonNode value : values) result.add(status(value));
    return result.toArray(new FileStatus[0]);
  }

  /** The first page validates the path; subsequent pages are fetched on demand. */
  @Override
  public RemoteIterator<FileStatus> listStatusIterator(Path path) throws IOException {
    return statusIterator(path, false, false);
  }

  private NativeStatusIterator statusIterator(Path path, boolean filesOnly, boolean recursive)
      throws IOException {
    NativeStatusIterator iterator = new NativeStatusIterator(path, filesOnly, recursive);
    try {
      iterator.hasNext();
      return iterator;
    } catch (IOException | RuntimeException error) {
      iterator.close();
      throw error;
    }
  }

  @Override
  public RemoteIterator<LocatedFileStatus> listFiles(Path path, boolean recursive)
      throws IOException {
    return new NativeLocatedIterator(statusIterator(path, true, recursive));
  }

  @Override
  public RemoteIterator<LocatedFileStatus> listLocatedStatus(Path path) throws IOException {
    return new NativeLocatedIterator(statusIterator(path, false, false));
  }

  @Override
  protected RemoteIterator<LocatedFileStatus> listLocatedStatus(Path path, PathFilter filter)
      throws IOException {
    Objects.requireNonNull(filter, "filter");
    return new FilteredLocatedIterator(statusIterator(path, false, false), filter);
  }

  private final class FilteredLocatedIterator
      implements RemoteIterator<LocatedFileStatus>, Closeable {
    private final NativeStatusIterator source;
    private final PathFilter filter;
    private FileStatus pending;

    FilteredLocatedIterator(NativeStatusIterator source, PathFilter filter) {
      this.source = source;
      this.filter = filter;
    }

    @Override
    public synchronized boolean hasNext() throws IOException {
      try {
        source.checkOpen();
        if (pending != null) return true;
        while (source.hasNext()) {
          FileStatus candidate = source.next();
          if (filter.accept(candidate.getPath())) {
            pending = candidate;
            return true;
          }
        }
        return false;
      } catch (IOException | RuntimeException error) {
        close();
        throw error;
      }
    }

    @Override
    public synchronized LocatedFileStatus next() throws IOException {
      if (!hasNext()) throw new NoSuchElementException("No more directory entries");
      FileStatus status = pending;
      pending = null;
      return new LocatedFileStatus(status,
          status.isFile() ? getFileBlockLocations(status, 0, status.getLen()) : null);
    }

    @Override
    public synchronized void close() {
      pending = null;
      source.close();
    }
  }

  private final class NativeLocatedIterator
      implements RemoteIterator<LocatedFileStatus>, Closeable {
    private final NativeStatusIterator source;

    NativeLocatedIterator(NativeStatusIterator source) {
      this.source = source;
    }

    @Override
    public boolean hasNext() throws IOException {
      return source.hasNext();
    }

    @Override
    public LocatedFileStatus next() throws IOException {
      FileStatus status = source.next();
      return new LocatedFileStatus(status,
          status.isFile() ? getFileBlockLocations(status, 0, status.getLen()) : null);
    }

    @Override
    public void close() {
      source.close();
    }
  }

  private final class NativeStatusIterator implements RemoteIterator<FileStatus>, Closeable {
    private final long filesystemHandle = nativeHandle;
    private final ObjectNode request;
    private final ListingCleanup cleanup;
    private Iterator<FileStatus> page = Collections.emptyIterator();
    private boolean finished, closed;
    private IOException failure;

    NativeStatusIterator(Path path, boolean filesOnly, boolean recursive) throws IOException {
      long cursor =
          call(
                  "listCursorOpen",
                  args(path).put("files_only", filesOnly).put("recursive", recursive))
              .asLong();
      request = JSON.createObjectNode().put("cursor", cursor);
      cleanup = new ListingCleanup(this, cursor);
    }

    private synchronized void checkOpen() throws IOException {
      if (nativeHandle == 0 || nativeHandle != filesystemHandle || closed)
        throw new IOException("Filesystem or iterator closed");
    }

    @Override
    public synchronized boolean hasNext() throws IOException {
      checkOpen();
      if (failure != null) throw failure;
      try {
        while (!page.hasNext() && !finished) {
          JsonNode response = call("listCursorNext", request);
          List<FileStatus> entries = new ArrayList<>();
          for (JsonNode value : response.path("entries")) entries.add(status(value));
          finished = response.path("next_marker").isNull();
          page = entries.iterator();
          if (finished) cleanup.release();
        }
        return page.hasNext();
      } catch (IOException error) {
        failure = error;
        page = Collections.emptyIterator();
        cleanup.release();
        throw error;
      } catch (RuntimeException error) {
        close();
        throw error;
      }
    }

    @Override
    public synchronized FileStatus next() throws IOException {
      if (!hasNext()) throw new NoSuchElementException("No more directory entries");
      return page.next();
    }

    @Override
    public synchronized void close() {
      closed = true;
      page = Collections.emptyIterator();
      cleanup.release();
    }
  }

  @Override
  public boolean mkdirs(Path path, FsPermission permission) throws IOException {
    return call("mkdirs", args(path).put("permission", permission.toShort() & 07777)).asBoolean();
  }

  @Override
  protected void primitiveMkdir(Path path, FsPermission permission, boolean createParent)
      throws IOException {
    path(path);
    Path qualified = path.makeQualified(uri, workingDirectory);
    Path parent = qualified.getParent();
    if (!createParent && parent != null && !getFileStatus(parent).isDirectory()) {
      throw new ParentNotDirectoryException("Parent is not a directory: " + parent);
    }
    if (!mkdirs(qualified, permission)) throw new IOException("Could not create directory: " + qualified);
  }

  @Override
  public void copyFromLocalFile(boolean deleteSource, boolean overwrite, Path source, Path destination)
      throws IOException {
    Configuration conf = getConf();
    if (!FileUtil.copy(FileSystem.getLocal(conf), source, this, destination,
        deleteSource, overwrite, conf)) {
      throw new IOException("Local copy did not complete: " + source + " -> " + destination);
    }
  }

  @Override
  public void copyFromLocalFile(boolean deleteSource, boolean overwrite, Path[] sources, Path destination)
      throws IOException {
    Configuration conf = getConf();
    if (!FileUtil.copy(FileSystem.getLocal(conf), sources, this, destination,
        deleteSource, overwrite, conf)) {
      throw new IOException("Local copy did not complete for all sources: " + destination);
    }
  }

  @Override
  public void copyToLocalFile(boolean deleteSource, Path source, Path destination,
      boolean useRawLocalFileSystem) throws IOException {
    if (deleteSource && getFileStatus(source).getPath().isRoot()) {
      throw new IOException("Cannot move the filesystem root to local storage");
    }
    Configuration conf = getConf();
    LocalFileSystem local = FileSystem.getLocal(conf);
    FileSystem target = new FilterFileSystem(useRawLocalFileSystem ? local.getRawFileSystem() : local) {
      @Override
      public boolean mkdirs(Path path) throws IOException {
        // FileUtil ignores false from recursive child copies; fail before source deletion.
        if (!super.mkdirs(path)) throw new IOException("Cannot create local copy directory: " + path);
        return true;
      }
    };
    try (CopySource scopedSource = new CopySource()) {
      if (!FileUtil.copy(scopedSource, source, target, destination, deleteSource, conf)) {
        throw new IOException("Copy to local did not complete: " + source + " -> " + destination);
      }
    }
  }

  /** FileUtil does not close directory iterators when a child copy fails. */
  private final class CopySource extends FilterFileSystem {
    private final Set<NativeStatusIterator> active = new HashSet<>();

    CopySource() {
      super(OBSFileSystemRust.this);
    }

    @Override
    public RemoteIterator<FileStatus> listStatusIterator(Path path) throws IOException {
      NativeStatusIterator iterator = statusIterator(path, false, false);
      active.add(iterator);
      return new RemoteIterator<FileStatus>() {
        @Override
        public boolean hasNext() throws IOException {
          if (iterator.hasNext()) return true;
          active.remove(iterator);
          iterator.close();
          return false;
        }

        @Override
        public FileStatus next() throws IOException {
          return iterator.next();
        }
      };
    }

    @Override
    public void close() {
      // Scope owns only its cursors, never the enclosing filesystem.
      for (NativeStatusIterator iterator : active) iterator.close();
      active.clear();
    }
  }

  @Override
  public boolean deleteOnExit(Path path) throws IOException {
    return super.deleteOnExit(deletionIdentity(path));
  }

  @Override
  public boolean cancelDeleteOnExit(Path path) {
    return super.cancelDeleteOnExit(deletionIdentity(path));
  }

  private Path deletionIdentity(Path value) {
    // Hadoop URI normalization retains leading /..; Rust clamps traversal at root.
    // Registration and cancellation must identify the same object without requiring I/O.
    Deque<String> components = new ArrayDeque<>();
    for (String component : path(value).split("/")) {
      if (component.isEmpty() || component.equals(".")) continue;
      if (component.equals("..")) components.pollLast();
      else components.addLast(component);
    }
    return new Path(uri.getScheme(), uri.getAuthority(), "/" + String.join("/", components));
  }

  @Override
  public boolean delete(Path path, boolean recursive) throws IOException {
    return call("delete", args(path).put("recursive", recursive)).asBoolean();
  }

  @Override
  public boolean rename(Path source, Path destination) throws IOException {
    return call("rename", args(source).put("destination", path(destination))).asBoolean();
  }

  /** Route overwrite through Rust; Hadoop's default deletes the target first. */
  @Override
  public void rename(Path source, Path destination, Options.Rename... options) throws IOException {
    boolean overwrite = false;
    if (options != null) {
      for (Options.Rename option : options) {
        if (option == Options.Rename.OVERWRITE) overwrite = true;
        else if (option != Options.Rename.NONE)
          throw new UnsupportedOperationException("Unsupported rename option: " + option);
      }
    }
    String sourcePath = path(source), destinationPath = path(destination);
    FileStatus sourceStatus = getFileStatus(source);
    if (sourcePath.equals(destinationPath))
      throw new FileAlreadyExistsException("Source and destination are the same path");
    FileStatus destinationStatus = null;
    try {
      destinationStatus = getFileStatus(destination);
    } catch (FileNotFoundException missing) {
      // Other service failures must propagate without modifying either path.
    }
    if (destinationStatus != null) {
      if (sourceStatus.getPath().equals(destinationStatus.getPath()))
        throw new FileAlreadyExistsException("Source and destination are the same path");
      if (!overwrite) throw new FileAlreadyExistsException(destination.toString());
      if (sourceStatus.isDirectory() != destinationStatus.isDirectory())
        throw new IOException("Source and destination must have the same type");
      if (destinationStatus.isDirectory())
        throw new UnsupportedOperationException("Replacing an existing directory is unsupported");
    } else {
      Path parent = makeQualified(destination).getParent();
      if (parent != null && !getFileStatus(parent).isDirectory())
        throw new ParentNotDirectoryException(parent.toString());
    }
    if (!call("rename", args(source).put("destination", destinationPath)
        .put("overwrite", overwrite)).asBoolean())
      throw new IOException("Rename failed: " + source + " to " + destination);
  }

  @Override
  public boolean truncate(Path path, long length) throws IOException {
    return call("truncate", args(path).put("length", length)).asBoolean();
  }

  @Override
  public boolean setReplication(Path path, short replication) throws IOException {
    if (replication <= 0) throw new IllegalArgumentException("Replication must be positive");
    getFileStatus(path);
    // OBS controls durability; a successful status lookup does not change it.
    return false;
  }

  @Override
  public void setTimes(Path path, long modificationTime, long accessTime) throws IOException {
    getFileStatus(path);
    if (modificationTime == -1 && accessTime == -1) return;
    throw new UnsupportedOperationException("OBS does not support setting file times");
  }

  @Override
  public void setPermission(Path path, FsPermission permission) throws IOException {
    call("setPermission", args(path).put("permission", permission.toShort() & 07777));
  }

  @Override
  public void setOwner(Path path, String username, String groupname) throws IOException {
    call("setOwner", args(path).put("owner", username).put("group", groupname));
  }

  @Override
  public ContentSummary getContentSummary(Path path) throws IOException {
    JsonNode s = call(contentSummaryEnabled ? "summary" : "summaryDirectory", args(path));
    return new ContentSummary.Builder()
        .length(nonnegativeLong(s.path("length"), "length"))
        .fileCount(nonnegativeLong(s.path("file_count"), "file_count"))
        .directoryCount(nonnegativeLong(s.path("directory_count"), "directory_count"))
        .spaceConsumed(nonnegativeLong(s.path("space_consumed"), "space_consumed"))
        .build();
  }

  @Override
  public Path getWorkingDirectory() {
    return workingDirectory;
  }

  @Override
  public void setWorkingDirectory(Path path) {
    workingDirectory = new Path(path(path)).makeQualified(uri, workingDirectory);
  }

  @Override
  public FSDataInputStream open(Path path, int bufferSize) throws IOException {
    long stream = call("open", args(path)).asLong();
    return new FSDataInputStream(new NativeInput(stream));
  }

  @Override
  public FSDataOutputStream create(
      Path path,
      FsPermission permission,
      boolean overwrite,
      int bufferSize,
      short replication,
      long blockSize,
      Progressable progress)
      throws IOException {
    long stream =
        call(
                "create",
                args(path)
                    .put("overwrite", overwrite)
                    .put("permission", permission.toShort() & 07777))
            .asLong();
    return new FSDataOutputStream(new NativeOutput(this, stream, false, progress), statistics);
  }

  private static void validateCreateFlags(EnumSet<CreateFlag> flags) {
    CreateFlag.validate(flags);
    for (CreateFlag flag : flags) {
      if (flag != CreateFlag.CREATE && flag != CreateFlag.OVERWRITE && flag != CreateFlag.APPEND)
        throw new UnsupportedOperationException("Unsupported create flag: " + flag);
    }
  }

  @Override
  public FSDataOutputStream create(
      Path path, FsPermission permission, EnumSet<CreateFlag> flags, int bufferSize,
      short replication, long blockSize, Progressable progress, Options.ChecksumOpt checksum)
      throws IOException {
    validateCreateFlags(flags);
    boolean present = exists(path);
    CreateFlag.validate(path, present, flags);
    if (present && flags.contains(CreateFlag.APPEND)) return append(path, bufferSize, progress);
    return create(path, permission, flags.contains(CreateFlag.OVERWRITE), bufferSize,
        replication, blockSize, progress);
  }

  @Override
  protected FSDataOutputStream primitiveCreate(
      Path path, FsPermission permission, EnumSet<CreateFlag> flags, int bufferSize,
      short replication, long blockSize, Progressable progress, Options.ChecksumOpt checksum)
      throws IOException {
    return create(path, permission, flags, bufferSize, replication, blockSize, progress, checksum);
  }

  @Override
  public FSDataOutputStream createNonRecursive(
      Path path,
      FsPermission permission,
      boolean overwrite,
      int bufferSize,
      short replication,
      long blockSize,
      Progressable progress)
      throws IOException {
    long stream =
        call(
                "create",
                args(path)
                    .put("overwrite", overwrite)
                    .put("permission", permission.toShort() & 07777)
                    .put("create_parent", false))
            .asLong();
    return new FSDataOutputStream(new NativeOutput(this, stream, false, progress), statistics);
  }

  @Override
  public FSDataOutputStream createNonRecursive(
      Path path,
      FsPermission permission,
      EnumSet<CreateFlag> flags,
      int bufferSize,
      short replication,
      long blockSize,
      Progressable progress)
      throws IOException {
    validateCreateFlags(flags);
    boolean present = exists(path);
    CreateFlag.validate(path, present, flags);
    if (flags.contains(CreateFlag.APPEND) && present) return append(path, bufferSize, progress);
    return createNonRecursive(
        path,
        permission,
        flags.contains(CreateFlag.OVERWRITE),
        bufferSize,
        replication,
        blockSize,
        progress);
  }

  @Override
  public boolean hasPathCapability(Path path, String capability) throws IOException {
    path(path);
    String normalized =
        org.apache.hadoop.fs.impl.PathCapabilitiesSupport.validatePathCapabilityArgs(
            path, capability);
    if (normalized.equals(CommonPathCapabilities.FS_APPEND)
        || normalized.equals(CommonPathCapabilities.FS_TRUNCATE))
      return call("isPfs", args(path)).asBoolean();
    return super.hasPathCapability(path, normalized);
  }

  @Override
  public FSDataOutputStream append(Path path, int bufferSize, Progressable progress)
      throws IOException {
    long position = getFileStatus(path).getLen();
    long stream = call("append", args(path)).asLong();
    return new FSDataOutputStream(new NativeOutput(this, stream, true, progress), statistics, position);
  }

  @Override
  public FSDataOutputStream append(Path path, int bufferSize, Progressable progress, boolean newBlock)
      throws IOException {
    if (newBlock) {
      throw new UnsupportedOperationException("OBS does not support append to a new HDFS block");
    }
    return append(path, bufferSize, progress);
  }

  @Override
  public synchronized void close() throws IOException {
    long handle = nativeHandle;
    try {
      // Hadoop deleteOnExit must still reach Rust's namespace checks.
      super.close();
    } finally {
      nativeHandle = 0;
      if (handle != 0) NativeObs.nativeClose(handle);
    }
  }

  private final class NativeInput extends FSInputStream implements ByteBufferReadable, StreamCapabilities {
    private final long filesystemHandle = nativeHandle;
    private long handle, position;
    private final byte[] one = new byte[1];

    NativeInput(long handle) {
      this.handle = handle;
    }

    private void checkOpen() throws IOException {
      if (handle == 0 || nativeHandle == 0 || nativeHandle != filesystemHandle)
        throw new IOException("Stream closed");
    }

    @Override
    public synchronized int read() throws IOException {
      return read(one, 0, 1) < 0 ? -1 : one[0] & 255;
    }

    @Override
    public synchronized int read(byte[] buffer, int offset, int length) throws IOException {
      checkOpen();
      Objects.requireNonNull(buffer);
      if (offset < 0 || length < 0 || offset > buffer.length - length)
        throw new IndexOutOfBoundsException();
      if (length == 0) return 0;
      int count = NativeObs.nativeReadInto(handle, buffer, offset, length);
      if (count > 0) {
        position += count;
        statistics.incrementBytesRead(count);
      }
      return count;
    }

    @Override
    public synchronized int read(ByteBuffer buffer) throws IOException {
      checkOpen();
      Objects.requireNonNull(buffer);
      if (buffer.isReadOnly()) throw new ReadOnlyBufferException();
      if (!buffer.hasRemaining()) return 0;
      int start = buffer.position();
      int count;
      if (buffer.hasArray()) {
        count = read(buffer.array(), buffer.arrayOffset() + start, buffer.remaining());
      } else if (buffer.isDirect()) {
        count = NativeObs.nativeReadDirect(handle, buffer, start, buffer.remaining());
        if (count > 0) {
          position += count;
          statistics.incrementBytesRead(count);
        }
      } else {
        throw new IllegalArgumentException("Unsupported buffer storage");
      }
      if (count > 0) buffer.position(start + count);
      return count;
    }

    @Override
    public boolean hasCapability(String capability) {
      return StreamCapabilities.READBYTEBUFFER.equalsIgnoreCase(capability);
    }

    @Override
    public synchronized long skip(long amount) throws IOException {
      checkOpen();
      if (amount <= 0) return 0;
      long skipped = NativeObs.nativeSkip(handle, amount);
      position += skipped;
      return skipped;
    }

    @Override
    public synchronized void seek(long value) throws IOException {
      checkOpen();
      if (value < 0) throw new EOFException("Negative seek");
      NativeObs.nativeSeek(handle, value);
      position = value;
    }

    @Override
    public synchronized long getPos() throws IOException {
      checkOpen();
      return position;
    }

    @Override
    public synchronized int available() throws IOException {
      checkOpen();
      return 0; // No network I/O is promised to be non-blocking.
    }

    @Override
    public synchronized boolean seekToNewSource(long target) throws IOException {
      checkOpen();
      return false;
    }

    @Override
    public synchronized void close() throws IOException {
      long value = handle;
      handle = 0;
      if (value != 0) NativeObs.nativeCloseStream(value);
    }
  }

  /**
   * Discard a create/createNonRecursive upload opened by this instance. Repeated abort or close
   * is harmless; later writes fail. Does not undo an already completed upload. Remote cleanup
   * errors propagate, but the stream stays closed and any journal remains available for recovery.
   * Append streams are unsupported because their already committed bytes cannot be rolled back.
   */
  public void abortUpload(FSDataOutputStream output) throws IOException {
    Objects.requireNonNull(output, "output");
    OutputStream wrapped = output.getWrappedStream();
    if (!(wrapped instanceof NativeOutput) || ((NativeOutput) wrapped).owner != this)
      throw new IllegalArgumentException("Upload belongs to a different filesystem");
    ((NativeOutput) wrapped).abort();
  }

  private static final class NativeOutput extends OutputStream
      implements Syncable, StreamCapabilities {
    private long handle;
    private final OBSFileSystemRust owner;
    private final long filesystemHandle;
    private final boolean append;
    private final Progressable progress;

    NativeOutput(OBSFileSystemRust owner, long handle, boolean append, Progressable progress) {
      this.owner = owner;
      this.filesystemHandle = owner.nativeHandle;
      this.handle = handle;
      this.append = append;
      this.progress = progress;
    }

    synchronized void abort() throws IOException {
      if (append) throw new UnsupportedOperationException("Cannot abort an append stream");
      long value = handle;
      handle = 0;
      if (value != 0) {
        try {
          NativeObs.nativeAbortStream(value);
        } catch (IllegalArgumentException error) {
          // Filesystem close can retire the native handle before abort reaches JNI.
          throw new IOException("Upload stream is closed or unavailable", error);
        }
      }
    }

    private void checkOpen() throws IOException {
      if (handle == 0 || owner.nativeHandle == 0 || owner.nativeHandle != filesystemHandle)
        throw new IOException("Stream closed");
    }

    @Override
    public synchronized void write(int value) throws IOException {
      write(new byte[] {(byte) value}, 0, 1);
    }

    @Override
    public synchronized void write(byte[] bytes, int offset, int length) throws IOException {
      checkOpen();
      Objects.requireNonNull(bytes);
      if (offset < 0 || length < 0 || offset > bytes.length - length)
        throw new IndexOutOfBoundsException();
      NativeObs.nativeWrite(handle, bytes, offset, length);
      if (progress != null) progress.progress();
    }

    @Override
    public synchronized void flush() throws IOException {
      checkOpen();
      NativeObs.nativeFlush(handle);
    }

    @Override
    public boolean hasCapability(String capability) {
      return append && (HFLUSH.equalsIgnoreCase(capability) || HSYNC.equalsIgnoreCase(capability));
    }

    @Override
    public synchronized void hflush() throws IOException {
      checkOpen();
      if (!append)
        throw new UnsupportedOperationException("Multipart create becomes durable on close");
      NativeObs.nativeFlush(handle);
    }

    @Override
    public synchronized void hsync() throws IOException {
      hflush();
    }

    @Override
    public synchronized void close() throws IOException {
      long value = handle;
      handle = 0;
      if (value != 0) NativeObs.nativeCloseStream(value);
    }
  }
}
