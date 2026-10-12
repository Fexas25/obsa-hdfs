# obsa-rust 附录：API 对照、配置全表、异常映射、Wire 细节

> 与 `PROMPT_NEXT_AI.md` 配套使用。下一 AI **必须**在实现每个 Java 方法前查阅本附录对应行。

---

## A. Hadoop `OBSFileSystem` 方法 → Rust 实现矩阵

说明列：
- **Rust 层**：`FS` = `obs-rust-core/src/fs.rs`；`JNI` = Java 薄层保留；`Hybrid` = Java 继续负责（Rust 不重写）
- **HEAD 目标**：Rust 优化目标（非 Java 现状）
- **分支**：`P` = PFS/`enablePosix`，`O` = 对象桶

| # | Java 方法 (OBSFileSystem.java) | Rust 层 | HEAD 目标 | P/O | 实现要点 |
|---|--------------------------------|---------|-----------|-----|----------|
| 1 | `initialize(URI, Configuration)` | FS + JNI | — | — | 解析 `fs.obs.*` → `ObsConfig`；`nativeCreate`；`init_bucket_mode`；MemArtsCC 可选仍 Java |
| 2 | `getScheme()` → `"obs"` | JNI | — | — | 常量 |
| 3 | `getUri()` / `getDefaultPort()` | JNI | — | — | 来自 initialize |
| 4 | `getWorkingDirectory()` / `setWorkingDirectory` | FS 状态 | 0 | — | 纯内存 Path |
| 5 | `getHomeDirectory()` | FS | 0 | — | `/user/<shortUser>` 约定或配置 |
| 6 | `open(Path, int bufSize)` | FS + io | 0–1 | P/O | cache hit→0；else HEAD；PFS inode hint；包装 `ObsInputStream` |
| 7 | `open(Path, Configuration jobConf)` | FS | 同 open | — | OpenFileWithJobConf；读 status hint |
| 8 | `create(..., overwrite, bufferSize, replication, blockSize)` | FS + io | 0–1 | P/O | negative cache；overwrite=true 直接写；`ObsOutputStream` |
| 9 | `create(Path, ..., EnumSet<CreateFlag> flags, ...)` | FS | 同 create | — | CREATE/OVERWRITE/APPEND 标志 |
| 10 | `createNonRecursive(...)` | FS | 0–1 | P | **必须**验证父目录存在（LIST/HEAD/cache） |
| 11 | `append(Path, int, Progressable)` | FS + pfs_append | 0–1 | P | 对象桶 Unsupported 或 MPU hack；PFS `PfsAppendStream` |
| 12 | `truncate(Path, long newLength)` | FS + pfs | 0 | P | `posix_truncate` + x-obs-hdfs；O 桶 Unsupported |
| 13 | `exists(Path)` | FS | 0 | — | cache 或 getFileStatus |
| 14 | `getFileStatus(Path)` | FS | 0–1 | P/O | **顺序** HEAD→LIST；见附录 B |
| 15 | `listStatus(Path)` | FS + listing | 0 | P/O | LIST delimiter `/`；back-fill cache |
| 16 | `listStatus(Path, boolean recursive)` | FS | 0 | — | recursive: flat LIST |
| 17 | `listFiles(Path, boolean recursive)` | FS | 0 | — | flat LIST O(pages) |
| 18 | `listLocatedStatus(Path)` | FS | 0 | — | listStatus + BlockLocation 单块 |
| 19 | `listLocatedStatus(Path, PathFilter)` | FS | 0 | — | filter 在 Rust 侧 |
| 20 | `delete(Path, boolean recursive)` | FS + pfs | 0 | P/O | P: dual-key；O: list+batch；trash 见 §12.7 |
| 21 | `rename(Path src, Path dst)` | FS | 0–1 | P/O | P: POST rename；O: copy+delete |
| 22 | `mkdirs(Path, FsPermission)` | FS + pfs | 0 | P/O | P: PUT folder；O: PUT `key/` 空对象 + meta dir |
| 23 | `getContentSummary(Path)` | FS | 0 | P/O | flag enable 时 flat LIST；else Java super 递归 |
| 24 | `setPermission(Path, FsPermission)` | FS | 0 | P | PUT metadata；O no-op |
| 25 | `setOwner(Path, user, group)` | FS | 0 | P | 同上 |
| 26 | `getFileBlockLocations(FileStatus, start, len)` | FS | 0 | — | 单 BlockLocation localhost |
| 27 | `getFileChecksum(Path)` | FS/Hybrid | 1 | — | HEAD ETag/MD5；可对齐 Java |
| 28 | `setXAttr/getXAttr/listXAttrs/removeXAttr` | FS 或 Hybrid | 0–1 | — | x-obs-meta 或 Java 保留 |
| 29 | `copyFromLocalFile(...)` | Hybrid | — | — | 可先完全 Java LocalFileSystem + create |
| 30 | `close()` | FS + JNI | — | — | 释放 native handle、abort 未完成 MPU |
| 31 | `getDelegationToken` / `addDelegationTokens` | Hybrid | — | — | Java `ObsDelegationTokenManger` |
| 32 | `checkPermission` + AuthorizeProvider | Hybrid | — | — | Java security 包 |
| 33 | MemArtsCC `getMemArtsCCClient` + read policy | Hybrid | — | — | **规格未要求 Rust 重写 CC**；read policy 可继续 Java InputStream |
| 34 | `getCanonicalServiceName()` | JNI | — | — | 配置开关 |
| 35 | metrics / traffic / audit | Hybrid + tracing | — | — | Rust `tracing` + 可选 Java metrics consumer |

---

## B. getFileStatus 决策树（Rust 必须实现且优于 Java）

### B.1 通用前置

1. `key = path_to_key(path, working_dir)`（与 Java 一致，见附录 C）
2. `if key.is_empty()` → 返回 root `FileStatus::dir("", 0o755)`
3. **StatusCache.get(key)**：
   - `Some(Some(status))` → return（**0 HEAD**）
   - `Some(None)` → `FileNotFoundException`（negative cache，**0 HEAD**）
4. 否则进入探测

### B.2 PFS 分支（Rust 优化版 vs Java SDK getAttribute）

Java 现用 `getAttribute`（类似 HEAD）。Rust 规格要求：

1. **HEAD**（带 `x-hws-fs-client:true`）解析 mode/inode/dentry/dht
2. 若 HEAD 404 → **LIST** prefix=`maybe_add_trailing_slash(key)` delimiter=`/` max-keys=1（或 CommonPrefixes 判断目录）
3. 仍失败 → `NotFound`
4. 成功 → `put_positive` + 更新 inode hint 缓存

从 LIST 条目构造 `FileStatus` 时 MUST 解析：`Mode`(octal)、`Uid`、`Gid`、`InodeNo`、`FsDentryNo`、`FsLinkCount`。

### B.3 对象桶分支（对齐 OBSObjectBucketUtils.innerGetObjectStatus）

1. HEAD `key`
2. 若 404 且 key 无尾 `/` → HEAD `key/`
3. 若仍 404 → LIST 判断空目录（prefix + delimiter）— Java `isFolderEmpty`
4. 目录启发：`contentLength==0` 或 `_$folder$` 或 meta dir marker

Rust 可在 cache 命中后 **跳过** 上述全部（目标 0 HEAD）。

### B.4 禁止

- 不要 **并行** HEAD + LIST（规格：TPC-DS 优化用 **顺序短路**）

---

## C. Path ↔ Key 规则（与 OBSCommonUtils 一致）

```text
pathToKey(fs, path):
  if !path.isAbsolute → path = join(workingDir, path)
  if path URI path component empty → return ""
  return uri.path.substring(1)   // 去掉 leading '/'

keyToPath(key) → Path("/" + key)

maybeAddTrailingSlash(key):
  if key non-empty && !endsWith('/') → key + '/'
  else → key
```

**注意**：OBS object key **never** 以 `/` 开头；Hadoop Path **always** 以 `/` 开头。

---

## D. OBS ListObjects 正确 URL（修复 store/obs.rs）

当前骨架用 `sub_resource("prefix", ...)` **可能错误**。标准 OBS List Bucket：

```http
GET /?prefix=foo/bar/&delimiter=/&marker=xxx&max-keys=1000 HTTP/1.1
Host: bucket.obs.region.myhuaweicloud.com
```

Path-style:

```http
GET /bucket?prefix=...&delimiter=... HTTP/1.1
Host: endpoint
```

**CanonicalizedResource** 签名时：

```text
/bucket/?prefix=foo&delimiter=%2F&max-keys=1000
```

子资源参数名需包含在签名 resource 中（规格列举：`uploadId`, `partNumber`, `delete`, `rename`, `name`, `truncate`, `length`, `modify`, `position`, `folder`）。List 的 `prefix`/`marker`/`delimiter`/`max-keys` 也在 query 中参与 canonical resource（按 OBS V2 规则排序）。

下一 AI 任务：**对照 obsa-hdfs 或 esdk 源码** 写单测：给定 AK/SK/mock server 验证 StringToSign。

---

## E. V2 签名逐步算法（实现 checklist）

输入：`method`, `content_md5`, `content_type`, `date` (RFC7231 GMT), `x-obs-* headers`, `canonical_resource`, `SK`

1. `canonical_amz = sort lowercase all headers where name starts with x-obs-`, format `name:value\n` concatenated
2. **排除** `x-hws-fs-*`、`Authorization`、`Date`（Date 单独一行）
3. `string_to_sign = method + "\n" + content_md5 + "\n" + content_type + "\n" + date + "\n" + canonical_amz + canonical_resource`
4. `signature = Base64(HMAC-SHA1(SK, UTF-8(string_to_sign)))`
5. `Authorization: OBS {AK}:{signature}`
6. 若有 session token：`x-obs-security-token: ...`（参与 x-obs canonicalization）
7. **每次重试必须重新 Date + 重签**

空 body DELETE/HEAD：`content_md5=""`, `content_type=""`

Batch delete POST：body XML 的 MD5 → `Content-MD5` header + 签名中的 content_md5 行。

---

## F. HTTP 409 errorCode 完整表（业务分支）

| errorCode | HTTP | 典型操作 | Rust 处理 |
|-----------|------|----------|-------------|
| DirectoryNotEmpty | 409 | DELETE dir | recursive list + delete children + retry |
| TruncateBiggerNotAllowed | 409 | PUT truncate | 映射 Hadoop `IOException` 消息 |
| DirectoryAlreadyExists | 409 | PUT folder | mkdirs 返回 true |
| FileAlreadyExists | 409 | PUT folder | `AlreadyExists` / Java `FileAlreadyExistsException` |
| ParentIsFile | 409 | PUT folder | `ParentNotDirectory` |
| FsObjectConflict | 409 | various | 按 message 映射 |
| SnapshotReadOnly | 409 | DELETE/RENAME | AccessControlException 类 |

辅助方法（已有 `error.rs`，扩展 JNI 映射）：

- `is_not_found()`
- `is_conflict_with_code("TruncateBiggerNotAllowed")`
- 保留 XML `Code` 字符串 **完整**（Java 有时丢失 404 code，Rust 必须保留）

---

## G. 配置键全表（Rust ObsConfig 应覆盖 + Java 兼容）

### G.1 规格文档 Section 15/16 核心键

| Key | Default | Rust 字段 | 说明 |
|-----|---------|-----------|------|
| fs.obs.endpoint | required | endpoint | |
| fs.obs.access.key | required | access_key | |
| fs.obs.secret.key | required | secret_key | |
| fs.obs.session.token | — | session_token | STS |
| fs.obs.bucket.mode | auto | bucket_mode | auto/object/pfs |
| fs.obs.connection.maximum | max(16,2×cores) | connection_maximum | |
| fs.obs.connection.establish.timeout | 5000 | ms | |
| fs.obs.connection.timeout | 60000 | ms | |
| fs.obs.connection.ssl.enabled | false(Java)/true(Rust建议) | secure | |
| fs.obs.multipart.size | 16M | multipart_size | min 5M clamp |
| fs.obs.multipart.threshold | 16M | multipart_threshold | 小于则 single PUT |
| fs.obs.upload.active.blocks | 12 | upload_active_blocks | per-stream |
| fs.obs.upload.total.active.blocks | max(32,8×cores) | upload_total_active_blocks | global |
| fs.obs.readahead.chunk.size | 8M | readahead_chunk_size | |
| fs.obs.readahead.max.chunks | 8 | readahead_max_chunks | geometric cap |
| fs.obs.readahead.total.chunks | max(32,16×cores) | readahead_total_chunks | prefetch gate |
| fs.obs.metadata.cache.ttl | 1000 | ms positive | |
| fs.obs.negative.cache.ttl | 300 | ms negative | |
| fs.obs.metadata.cache.size | 4096 | entries | |
| fs.obs.paging.maximum | 1000 | paging_maximum | |
| fs.obs.pfs.append.concurrency | 4 | pfs_append_concurrency | |
| fs.obs.pfs.inode.hints | true | pfs_inode_hints | |
| fs.obs.trash.enabled | false | trash_enabled | |
| fs.obs.trash.dir | .obs_trash | trash_dir | |
| fs.obs.content.summary.enable | false | content_summary_enable | |
| fs.obs.path.style.access | true | path_style_access | false→vhost |
| fs.obs.access.log.enabled | false | access_log_enabled | |
| fs.obs.credential.provider.path | — | 待扩展 | Hadoop CredentialProvider |
| fs.obs.bucket.{BUCKET}.{suffix} | — | bucket_overrides | 与 s3a 相同覆盖语义 |

### G.2 Java OBSConstants 中 Rust Phase 4+ 需对齐的键（摘录）

- `fs.obs.fast.upload*`, `fs.obs.multiobjectdelete.*`, `fs.obs.block.size`
- `fs.obs.readahead.policy` / `memArtsCC` / `fs.obs.readahead.range`
- `fs.obs.retry.*`, `fs.obs.attempts.maximum`
- `fs.obs.server-side-encryption-type/key`
- `fs.obs.delegation.token.*`, `fs.obs.authorize.*`
- `fs.obs.metrics.*`, `fs.obs.buffer.dir`
- `fs.obs.hdfs.trash.*`, `fs.obs.client.dfs.list.enable`
- `fs.obs.permissions.mode` (none/disguise)

**策略**：未知键 **忽略但可 log warn**；已知键必须行为兼容。

---

## H. ObsError → Java 异常映射（JNI 层）

| ObsError | Java Exception |
|----------|----------------|
| NotFound | FileNotFoundException |
| AlreadyExists | FileAlreadyExistsException |
| ParentNotDirectory | ParentNotDirectoryException |
| DirectoryNotEmpty | PathIsNotEmptyDirectoryException |
| Conflict + DirectoryNotEmpty | 同上 |
| Conflict + TruncateBiggerNotAllowed | IOException (message) |
| MethodNotAllowed | OBSMethodNotAllowedException / IOException |
| Unsupported | UnsupportedOperationException |
| BadRequest | IllegalArgumentException / OBSIllegalArgumentException |
| Transport | IOException (cause) |
| Http 403 | AccessControlException (若 authorize) |

JNI 应传递：`message`, `error_code`, `request_id` 便于日志。

---

## I. 操作级 Wire + 缓存失效表

| 操作 | 成功后 cache 动作 |
|------|-------------------|
| create file | invalidate_ancestors(key); put_positive new file |
| mkdirs | invalidate_ancestors; put_positive dir |
| delete | invalidate(key); invalidate_prefix(key) |
| rename | invalidate src,dst prefixes |
| truncate | invalidate(key) |
| setPermission/setOwner | update cache entry if exists |
| listStatus | back_fill all children + parent |

---

## J. Contract Test 能力声明（obs.xml）

Rust 集成后必须仍满足（摘录）：

- `fs.contract.supports-append=true`
- `fs.contract.supports-atomic-directory-delete=true`
- `fs.contract.rename-returns-false-if-source-missing=true`
- `fs.contract.is-case-sensitive=true`
- … 完整见 `obsa-hdfs/.../contract/obs.xml`

测试命令：

```bash
cd d:/桌面/RUST-CLIENT/obsa-hdfs/hadoop-huaweicloud
mvn -q test -Dtest=TestOBSContractCreate,TestOBSContractDelete,TestOBSContractGetFileStatus,TestOBSContractMkdir,TestOBSContractOpen,TestOBSContractRename,TestOBSContractSeek,TestOBSContractAppend
```

---

## K. 依赖 crate 版本（workspace 已锁定）

```toml
tokio 1, hyper 1, hyper-util 0.1, http-body-util 0.1, tokio-rustls 0.26, rustls 0.23,
quick-xml 0.36, hmac 0.12, sha1 0.10, md-5 0.10, base64 0.22, bytes 1, dashmap 6,
serde 1, thiserror 2, tracing 0.1, jni 0.21, async-trait 0.1
```

禁止引入 OpenSSL（`native-tls` 默认 off）。

---

## L. 内存预算（规格）

- 写：`upload.total.active.blocks × multipart.size`
- 读：`readahead.total.chunks × readahead.chunk.size`
- Rust 无 JVM DirectMemory 限制，但需在文档警告 OOM 配置风险

---

## M. 参考类文件路径（Java 金标准）

| 主题 | 路径 |
|------|------|
| FileSystem 主类 | `obsa-hdfs/hadoop-huaweicloud/src/main/java/org/apache/hadoop/fs/obs/OBSFileSystem.java` |
| PFS 工具 | `.../OBSPosixBucketUtils.java` |
| 对象桶工具 | `.../OBSObjectBucketUtils.java` |
| 路径/重试/翻译 | `.../OBSCommonUtils.java` |
| 列表 | `.../OBSListing.java` |
| 写流 | `.../OBSBlockOutputStream.java` |
| 读流 | `.../input/OBSInputStream.java` |
| 常量 | `.../OBSConstants.java` |
| HDFS 包装 | `.../OBSHDFSFileSystem.java`, `OBS.java` |
| SPI | `src/main/resources/META-INF/services/org.apache.hadoop.fs.FileSystem` |

---

## N. 规格「Beyond HEAD Elimination」功能清单

| ID | 功能 | Rust 模块 |
|----|------|-----------|
| 12.1 | openFile() status hint | fs.rs OpenOptions |
| 12.2 | Manifest/Magic Committer 少 rename | 文档+兼容，不破坏直接 rename |
| 12.3 | Vectored IO range coalescing | io/input.rs 扩展 read_vectored |
| 12.4 | Directory marker x-obs-meta-dir | object mkdirs/delete |
| 12.5 | Batch delete XML + MD5 + fallback | store/obs.rs |
| 12.6 | getContentSummary flat LIST | fs.rs + listing |
| 12.7 | PFS trash posixRename | pfs/ops.rs + fs delete |
| 12.8 | Recursive delete deepest-first sort | pfs/ops.rs |
| 12.9 | ETag If-Match rename 安全 | object rename copy 路径 |

---

## O. 每个 Rust 源文件职责（行级期望）

| 文件 | 必须包含 |
|------|----------|
| `fs.rs` | `ObsFileSystem` 全部 Hadoop FS 语义；path_to_key；cache 集成；P/O 分支 |
| `store/obs.rs` | 所有 HTTP 动词；签名；错误 XML 解析；**修复 LIST query** |
| `transport/pool.rs` | HTTPS connector；retry；access log 字段 in/out bytes |
| `io/input.rs` | prefetch 状态机；seek disarm；try_acquire |
| `io/output.rs` | MPU barrier；single PUT；sem ordering stream then global |
| `pfs/ops.rs` | dual-key delete；recursive batch |
| `cache/status.rs` | invalidate_ancestors；list backfill |
| `obs-jni/lib.rs` | Runtime singleton；JNI_OnLoad |
| `obs-jni/filesystem.rs` | handle 表；block_on |

---

## P. 验收标准（Definition of Done）

1. `cargo build --workspace` 无 warning  ideally
2. `cargo test -p obs-rust-core` 全部通过
3. 加载 native 库后 Hadoop contract tests 通过
4. 相对 Java 同硬件：fio/自研 bench 读写 stat 有提升文档
5. Critical Rules 15 条有对应 unit/integration 测试或注释引用
