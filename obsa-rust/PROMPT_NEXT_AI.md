# obsa-rust 续作 AI 操作提示词（超详细主文档）

> **用法**：将 **本文件 + `PROMPT_NEXT_AI_APPENDIX.md`** 一并交给下一 AI。  
> 下一 AI 的第一条消息见 **§二十**。  
> 规格原始来源：用户提供的 **rust-obsa-prompt.pdf**（工作区 12 张截图，路径见 §十九）。

---

## 目录

1. [用户目标与成功标准](#一用户目标与成功标准)
2. [仓库与路径](#二仓库与路径)
3. [架构总览](#三架构总览)
4. [Critical Rules（逐条展开）](#四critical-rules逐条展开)
5. [HEAD Elimination 与逐操作规格](#五head-elimination-与逐操作规格)
6. [PFS Inode 快路径协议](#六pfs-inode-快路径协议)
7. [Transport / TLS / 连接池](#七transport--tls--连接池)
8. [ObjectStore 契约](#八objectstore-契约)
9. [ObsFileSystem（fs.rs）必须实现的 API](#九obsfilesystemfsrs必须实现的-api)
10. [IO 流（读/写/追加）详细行为](#十io-流读写追加详细行为)
11. [对象桶 vs PFS 分支总表](#十一对象桶-vs-pfs-分支总表)
12. [当前代码 inventory 与编译阻断项](#十二当前代码-inventory-与编译阻断项)
13. [修复食谱（按优先级）](#十三修复食谱按优先级)
14. [JNI 与 Java 集成规范](#十四jni-与-java-集成规范)
15. [六周 Phase 与交付物](#十五六周-phase-与交付物)
16. [测试策略](#十六测试策略)
17. [性能基准与验收](#十七性能基准与验收)
18. [Hybrid 边界（仍由 Java 负责的部分）](#十八hybrid-边界仍由-java-负责的部分)
19. [参考资产与文档](#十九参考资产与文档)
20. [给下一 AI 的第一条消息（复制即用）](#二十给下一-ai-的第一条消息复制即用)

**附录文件**：`PROMPT_NEXT_AI_APPENDIX.md`（方法矩阵、配置全表、异常映射、List URL、Wire 细节）

---

## 一、用户目标与成功标准

### 1.1 项目本质

把 **`obsa-hdfs`**（Maven 模块 `hadoop-huaweicloud`，`org.apache.hadoop.fs.obs.OBSFileSystem`，scheme **`obs://`**）的数据面与控制面核心 **重写为 Rust**，仓库名 **`obsa-rust`**，通过 JNI **无缝替换** Java 内嵌 OBS SDK 调用路径，面向 Spark / Hive / Flink 等生产负载。

### 1.2 硬约束

| 约束 | 说明 |
|------|------|
| 功能只多不少 | 不得砍掉 Java 已有对外 API；增强项（HEAD elimination、flat list CS、vectored read 等）是加分 |
| 性能必须更好 | 目标：**>2× 吞吐**、元数据 **≤50% 延迟**（相对原 Java obsa-hdfs）；参考 obsa-crazy 数字：写 ~441 MiB/s、读 ~1362 MiB/s、stat ~420k ops/s |
| Workspace | `d:\桌面\RUST-CLIENT\obsa-rust` |
| Java 金标准 | `d:\桌面\RUST-CLIENT\obsa-hdfs` |
| 用户语言 | **中文**汇报；代码英文为主 |

### 1.3 Definition of Done（DoD）

1. `cargo build --workspace` 成功（含 `obs-jni` cdylib）。
2. `cargo test -p obs-rust-core` 绿。
3. Java 侧加载 `obs_jni` 后 **Hadoop contract tests**（见附录 J）通过。
4. Critical Rules 15 条均有测试或代码审查可追踪注释。
5. 提供简短 benchmark 说明（读/写/stat/list 相对 Java 的提升或持平原因）。

---

## 二、仓库与路径

```
d:\桌面\RUST-CLIENT\
├── obsa-hdfs\                    # Java 参考实现（勿删）
│   └── hadoop-huaweicloud\
│       ├── pom.xml
│       └── src\main\java\org\apache\hadoop\fs\obs\*.java
└── obsa-rust\                    # Rust workspace（本任务）
    ├── Cargo.toml
    ├── PROMPT_NEXT_AI.md         # 本文件
    ├── PROMPT_NEXT_AI_APPENDIX.md
    ├── obs-rust-core\
    └── obs-jni\                  # 目前无 src/
```

**SPI 注册**（Java）：`META-INF/services/org.apache.hadoop.fs.FileSystem` → `org.apache.hadoop.fs.obs.OBSFileSystem`

---

## 三、架构总览

```
┌─────────────────────────────────────────────────────────┐
│  Hadoop (Spark Driver/Executor)                         │
│  OBSFileSystem.java  ←thin delegate→  JNI native methods│
└───────────────────────────┬─────────────────────────────┘
                            │ JNI (protobuf/json + byte[])
┌───────────────────────────▼─────────────────────────────┐
│  obs-jni (cdylib)                                       │
│  JNI_OnLoad, tokio Runtime, handle 表, block_on         │
└───────────────────────────┬─────────────────────────────┘
                            │
┌───────────────────────────▼─────────────────────────────┐
│  obs-rust-core                                          │
│  fs.rs ObsFileSystem  ←→  StatusCache / PfsOps          │
│       ↓                                                 │
│  io (input/output/pfs_append)                           │
│       ↓                                                 │
│  store::ObsStore impl ObjectStore                       │
│       ↓                                                 │
│  transport::HttpPool + auth::ObsSigner                  │
└─────────────────────────────────────────────────────────┘
                            │ HTTPS/HTTP (rustls)
                            ▼
                    Huawei Cloud OBS / PFS
```

**设计原则**：Java **只 delegate**；业务逻辑、并发、缓存、协议 **全部在 Rust**（规格 Section 14）。

---

## 四、Critical Rules（逐条展开）

| # | 规则 | 实现位置 | 测试建议 |
|---|------|----------|----------|
| 1 | GET **never** `x-hws-fs-client:true` | `store/obs.rs` `get()` | unit: GET headers 不含该键 |
| 2 | `x-hws-fs-*` 不进入 V2 签名 | `auth/signer.rs` + 签名后 merge | signer 单测已有，补集成 |
| 3 | DHT max-seen 单调 | `pfs/inode.rs` `InodeState` | 单测已有 |
| 4 | Inode hint 双字段 | `types.rs` `InodeHint::new` | 单测已有 |
| 5 | Delete 先 `/` 后无 slash | `pfs/ops.rs` | mock store 调用顺序断言 |
| 6 | Mkdirs 无 pre-HEAD | `pfs/ops.rs` / `fs.rs` | mock 禁止 head 调用 |
| 7 | Truncate 需 x-obs-hdfs | `store/obs.rs` `posix_truncate` | header 断言 |
| 8 | Mode 八进制 parse | `types.rs` / `xml/mod.rs` | 单测已有 |
| 9 | create → invalidate_ancestors | `fs.rs` create | cache 单测 |
| 10 | 小文件 single PUT | `io/output.rs` | 阈值边界测试 |
| 11 | complete-mpu 无 retry | `transport/pool.rs` idempotent=false | 注释+mock |
| 12 | batch delete fallback | `store/obs.rs` | 模拟 501 |
| 13 | PFS rename 源目录带 `/` | `pfs/ops.rs` | 单元测试 |
| 14 | PFS mutating + x-obs-hdfs | store pfs 方法 | — |
| 15 | 对象桶 PFS verb → Unsupported | `fs.rs` 分支 | JNI 映射 |

---

## 五、HEAD Elimination 与逐操作规格

以下与 pdf **Section 3** 一致；**Rust HEAD 列是目标**，不是 Java 现状。

### 5.1 Delete — Dual-Key（0 HEAD）

**路径** `delete("a/b", recursive)`：

1. `DELETE` key=`a/b/`，header `x-obs-hdfs:true`
2. 若 **204** → 曾是目录，成功 `true`
3. 若 **404** → 非目录，继续
4. 若 **409 DirectoryNotEmpty** 且 `recursive` → flat list 所有后代 → **按路径深度降序** batch delete → 再删 `a/b/`；非 recursive → 抛 DirectoryNotEmpty
5. `DELETE` key=`a/b`，`x-obs-hdfs:true`
6. **204** → 文件删除 `true`；**404** → 两者皆无 → `false`

**缓存**：`invalidate(key)` + `invalidate_prefix`

**Java 对照**：PFS 在 `OBSPosixBucketUtils`；Rust 在 `pfs/ops.rs` 已骨架化，需与 `fs.rs` 对接 trash（§5.10）。

### 5.2 Truncate — HDFS Header（0 HEAD）

- `PUT /key?truncate&length=N`，**必须** `x-obs-hdfs:true`
- 成功 200；目录 405；变大非法 409 `TruncateBiggerNotAllowed`；不存在 404
- **禁止** truncate 前 HEAD 取 length

### 5.3 Mkdirs — Direct newFolder（0 HEAD）

- `PUT /key?folder` empty body
- 200 → true
- 409 `DirectoryAlreadyExists` → true（幂等）
- 409 `FileAlreadyExists` → 异常
- 409 `ParentIsFile` → FileNotFoundException("parent is file")
- 404 → 父不存在 → **递归 mkdirs 父** 再重试

对象桶：无 PFS folder API 时用 **零字节 PUT `parent/key/`** + 可选 `x-obs-meta-dir: true`（规格 12.4）。

### 5.4 listStatus — ListEntry PFS 字段（0 HEAD）

1. `LIST` `delimiter=/`，prefix=目录 key（带尾 `/`）
2. 每个 `Contents` / `CommonPrefixes` → `FileStatus`（Mode octal → dir/file）
3. **无** per-entry HEAD
4. **back-fill** `StatusCache`（所有子项 + 父目录）

`listFiles(recursive=true)`：`delimiter=""` 单次 flat LIST，复杂度 O(pages)。

`listLocatedStatus`：同上 + 每个文件 `BlockLocation{offset:0, length:len, hosts:[local]}`。

### 5.5 Open — Cache + Inode Hint（0 HEAD on hit）

1. Cache 有 length + inode → 直接 `ObsInputStream::new(len, hint)`
2. Miss → **一次 HEAD** → 写 cache → open
3. 典型 pipeline：`listStatus` → `open` 应 **0 HEAD**（LIST 已填 inode）

**GET 读数据**：可带 hint headers（非 client header）。

### 5.6 Create — Negative Cache（0 HEAD 常见）

- `overwrite=true` → 直接 PUT/MPU
- `overwrite=false`：
  - negative cache 有 → 直接 create
  - else HEAD 检查存在
- 成功 → `invalidate_ancestors(key)`

Java `create()` 现 **总是** getFileStatus；Rust **必须更优**，但 JNI 语义需与 contract 一致（文件已存在时 throw）。

### 5.7 Rename (PFS) — Atomic（0–1 HEAD）

- `POST /src?rename&name=dst`，`x-obs-hdfs:true`
- 源为目录时 src key **尾 `/`**
- `overwrite=false` 时：仅当 cache 不知道 dst 是否存在才 HEAD dst
- 409 dst exists → false；404 src → false

### 5.8 setPermission / setOwner — Direct Metadata（0 HEAD）

- `PUT ?metadata`，`x-obs-metadata-directive: REPLACE`
- `x-obs-meta-mode`（octal）、uid、gid
- 从 cache 合并未改字段；对象桶 **no-op**

### 5.9 getContentSummary (PFS) — Single Flat LIST（0 HEAD）

- `delimiter=""` deep list 一次聚合 fileCount/dirCount/length
- 不对路径本身 HEAD；LIST 有结果即证明是目录
- Java 在 `obsContentSummaryEnable` 时有 V2 API；Rust 优先 flat LIST（规格）

### 5.10 Trash（PFS，可选配置）

- `fs.obs.trash.enabled=true`：delete 改为 **posixRename** 到 `fs.obs.trash.dir`（默认 `.obs_trash`）
- 规格 12.7

### 5.11 getFileStatus 探测顺序（重要）

**顺序短路**（规格 + 对象桶优化）：

1. StatusCache
2. **HEAD**（cheap）
3. **LIST**（expensive，仅 HEAD 失败）

**禁止** HEAD 与 LIST 并行。

附录 B 有 PFS/O 分支细节。

---

## 六、PFS Inode 快路径协议

### 6.1 服务端响应（HEAD/PUT）

- `x-hws-fs-inode-no: u64`
- `x-hws-fs-inode-dentryname: url-encoded verbatim`
- `x-hws-fs-dht-id: u64`

### 6.2 客户端 GET 回放

- 仅 replay：`inode-no`, `dentryname`, `dht-id`（max-seen）
- **不**发 `x-hws-fs-client`

### 6.3 LIST 字段

解析 `InodeNo` 入 cache — Java obsa-crazy **做不到**（SDK 不 parse）；Rust **必须**，这是性能优势。

### 6.4 dentryName 规则

** verbatim 使用服务端返回值**，禁止客户端重新 url-encode。

---

## 七、Transport / TLS / 连接池

### 7.1 模型（ossfs2/photon）

- HTTP/1.1 keep-alive
- 连接池 **按需增长** 至 `connection.maximum`，**不收缩**
- TCP keepalive **30s**
- Connect timeout **5s**；Read timeout **60s**（配置可改）

### 7.2 Retry（规格 Section 5）

| 类型 | 条件 | 策略 |
|------|------|------|
| 幂等 GET/HEAD/PUT/DELETE/Initiate MPU | 5xx、连接错误 | 线性退避 100ms，**最多 2 次** |
| complete-mpu | 任何 | **0 次重试** |
| 每次重试 | — | **重新 Date + 重签** |

### 7.3 TLS

- **仅 rustls**；`transport/tls.rs` 已 stub
- `ObsConfig.secure=true` 时 HttpPool 必须走 `HttpsConnector`（hyper-util + tokio-rustls）
- 证书：`rustls-native-certs` + 可选自定义 CA 配置（Phase 5）

### 7.4 Access Log

配置 `fs.obs.access.log.enabled=true` 时每请求一行：

```text
timestamp method path status out_bytes in_bytes duration_us conn_id
```

关闭时 **零开销**（branch on config，不 format）。

### 7.5 当前 transport 已知 bug

见 §十三：mod.rs 错误 export、`Request::builder().headers_mut()` hyper 1.x API、HTTPS 未连接。

---

## 八、ObjectStore 契约

完整 trait 见 `obs-rust-core/src/store/traits.rs`。

下一 AI **不得削弱** trait 方法；可增 `get_with_hint` 等扩展但需默认 impl 或 fs 层包装。

**语义要点**：

- `delete(key, hdfs_header)`：204/404 均 Ok(()) 在 dual-key 外层interpret
- `list(prefix, delimiter, marker, max_keys)`：返回 `ListPage` 含 PFS 字段
- `complete_multipart_upload`：内部 `idempotent(false)`
- `probe_posix_bucket` / `is_pfs`：构造 FS 时调用一次

**ObsStore 结构问题**：`is_pfs` 现需 `&mut self` init — 改为：

```rust
struct ObsStoreInner { ..., is_pfs: AtomicBool }
pub struct ObsStore(Arc<ObsStoreInner>);
```

---

## 九、ObsFileSystem（fs.rs）必须实现的 API

**文件**：`obs-rust-core/src/fs.rs` — **当前不存在，lib.rs 已 `pub mod fs` — 编译失败根因 #1**

### 9.1 建议结构

```rust
pub struct ObsFileSystem {
    store: Arc<ObsStore>,
    cache: Arc<StatusCache>,
    config: ObsConfig,
    pfs: PfsOps,  // 或 Arc
    upload_gate: Arc<Semaphore>,
    prefetch_gate: Arc<Semaphore>,
    working_dir: String,
    uri: String,
    short_user: String,
    block_size: i64,
}

impl ObsFileSystem {
    pub async fn from_config(config: ObsConfig) -> ObsResult<Self>;
    pub fn path_to_key(&self, path: &str) -> String;
    pub fn key_to_path(&self, key: &str) -> String;

    // --- metadata ---
    pub async fn get_file_status(&self, path: &str) -> ObsResult<FileStatus>;
    pub async fn exists(&self, path: &str) -> ObsResult<bool>;

    // --- io ---
    pub async fn open(&self, path: &str, opts: OpenOptions) -> ObsResult<ObsInputStream>;
    pub async fn create(&self, path: &str, opts: CreateOptions) -> ObsResult<ObsOutputStream>;

    // --- namespace ---
    pub async fn delete(&self, path: &str, recursive: bool) -> ObsResult<bool>;
    pub async fn mkdirs(&self, path: &str, permission: u32) -> ObsResult<bool>;
    pub async fn rename(&self, src: &str, dst: &str, overwrite: bool) -> ObsResult<bool>;
    pub async fn truncate(&self, path: &str, new_len: u64) -> ObsResult<()>;

    // --- listing ---
    pub async fn list_status(&self, path: &str) -> ObsResult<Vec<FileStatus>>;
    pub async fn list_files_recursive(&self, path: &str) -> ObsResult<Vec<FileStatus>>;

    // --- other ---
    pub async fn get_content_summary(&self, path: &str) -> ObsResult<ContentSummary>;
    pub async fn set_permission(&self, path: &str, mode: u32) -> ObsResult<()>;
    pub async fn set_owner(&self, path: &str, uid: u32, gid: u32) -> ObsResult<()>;
    pub async fn append_stream(&self, path: &str, pos: u64) -> ObsResult<PfsAppendStream>;
}
```

### 9.2 与 Java 行为对齐的注意事项

- `rename` 当 dst 是目录时 Java 会把 src **移入** dst（改 dstKey）— 见 `OBSPosixBucketUtils.renameBasedOnPosix`
- `create` 若 path 是目录 → `FileAlreadyExistsException`
- `getFileStatus`：`OBSFileConflictException` → `FileNotFoundException`（Java 特殊映射）

完整方法矩阵见 **附录 A**（35+ 行）。

---

## 十、IO 流（读/写/追加）详细行为

### 10.1 ObsInputStream（`io/input.rs`）

**Prefetch 算法**（规格 Section 7）：

1. 前 **3** 次 read 不 prefetch
2. 连续 3 次顺序 read → arm，`window=1`
3. 每跨 chunk 边界 `window *= 2`，cap=`readahead.max.chunks`（默认 8）
4. seek 乱序 → disarm，`window=1`
5. prefetch `try_acquire` on `prefetch_gate` — **永不阻塞**主 read
6. 缓存：`DashMap<chunk_idx, Pending|Ready>` — **当前实现有 JoinHandle 在 DashMap 内 await 的 bug**，应改为 channel/oneshot

**Chunk 大小**：`readahead.chunk.size`（默认 8MB）

### 10.2 ObsOutputStream（`io/output.rs`）

1. 缓冲 `BytesMut`，满 partSize → `split().freeze()` → spawn `upload_part`
2. 锁顺序：**先** stream sem **再** global sem
3. close：flush 尾块；**await 所有 part tasks**（当前 barrier 错误，需 join handles vec）
4. `total_written < multipart.threshold` → close 时 **single PUT**，不 Initiate MPU

**PutHeaders Default**：应只在 `store/traits.rs`，勿在 output.rs 重复 impl

### 10.3 PfsAppendStream（`io/pfs_append.rs`）

- 并发 depth = `pfs.append.concurrency`（默认 4）
- `PUT ?modify&position=` 可乱序完成
- `flush/hsync` = drain 全部 pending（真 flush）

---

## 十一、对象桶 vs PFS 分支总表

| 能力 | PFS | 对象桶 |
|------|-----|--------|
| mkdirs | PUT ?folder | PUT zero-byte `key/` |
| delete file | dual-key | DELETE key |
| delete dir | dual-key + recursive list | list + batch delete |
| rename | POST ?rename 原子 | copy tree + batch delete |
| truncate | PUT ?truncate | Unsupported |
| append | modify position | Unsupported（或 MPU 非标准） |
| setPermission | PUT metadata | no-op |
| getContentSummary fast | flat LIST | flat LIST + 启发式目录 |
| inode hint | 是 | 否 |

探测：`fs.obs.bucket.mode=auto` → HEAD bucket / FS status API → 设 `is_pfs`

---

## 十二、当前代码 inventory 与编译阻断项

### 12.1 已有 Rust 文件（28 个）

见 workspace glob；**无** `fs.rs`，**无** `obs-jni/src/**`。

### 12.2 编译阻断（必须按序修复）

| 优先级 | 问题 | 文件 |
|--------|------|------|
| P0 | `pub mod fs` 无文件 | 创建 `fs.rs` 或临时 `mod fs { ... }` stub |
| P0 | `HttpResponse` 从 request 模块 export 但定义在 pool | `transport/mod.rs` |
| P0 | `HttpRequest` 未在 request.rs 定义 | transport |
| P1 | hyper Request builder headers | `pool.rs` `execute_once` |
| P1 | HTTPS 未实现 | pool + tls |
| P1 | `ObsStore init` vs trait `is_pfs` | store/obs.rs |
| P2 | input.rs Chunk Pending await | io/input.rs |
| P2 | output.rs close barrier | io/output.rs |
| P2 | LIST query 格式 | store/obs.rs |
| P3 | 重复 `PutHeaders Default` | io/output.rs 删除 |

### 12.3 环境

上一会话 Windows **未安装 Rust**。下一 AI 应：

```powershell
# 安装后
cd d:\桌面\RUST-CLIENT\obsa-rust
cargo build --workspace
cargo test -p obs-rust-core
```

---

## 十三、修复食谱（按优先级）

### 13.1 修复 transport/mod.rs

```rust
pub use pool::{HttpPool, HttpResponse};
pub use request::RequestBuilder;
// 删除 HttpRequest 直到需要
```

### 13.2 修复 pool.rs 构建请求

使用：

```rust
let mut req = http::Request::builder().method(method.clone()).uri(uri);
let req = {
    let mut builder = req;
    for (k, v) in headers { ... insert ... }
    builder.body(req_body)?
};
```

勿依赖 `hyper::Request::builder().headers_mut()` 若不可用。

### 13.3 实现 fs.rs 最小可编译

先 stub 所有方法 `todo!()` 或 `Err(Unsupported)`，让 workspace compile；再逐个实现 get_file_status/delete/mkdirs。

### 13.4 ObsStore LIST 修复

改用 query：

```text
GET /{bucket}?prefix=...&delimiter=...&marker=...&max-keys=...
```

签名 resource：`/{bucket}/?prefix=...&...`（参数按 OBS 规范排序 — **对照 Java SDK**）

### 13.5 obs-jni 最小闭环

1. `nativeCreate(config_json) -> handle`
2. `nativeGetFileStatus(handle, path) -> jbyteArray` JSON
3. `nativeClose(handle)`

再扩展 open/read/write。

---

## 十四、JNI 与 Java 集成规范

### 14.1 Runtime

- `JNI_OnLoad`：`OnceCell<Runtime>`，`Runtime::new_multi_thread()`
- 每个 JNI 函数：`RT.block_on(async { ... })`
- **禁止** 在 tokio worker 内再 block_on

### 14.2 Handle 表

- `DashMap<jlong, Arc<ObsFileSystem>>`
- `DashMap<jlong, ObsInputStream>` 用于 read
- handle id：`AtomicI64`

### 14.3 Java 类骨架（规格 Section 14）

```java
public class OBSFileSystem extends FileSystem {
    private long nativeHandle;

    private static native long nativeCreate(String configJson);
    private static native byte[] nativeGetFileStatus(long handle, String path);
    private static native long nativeOpen(long handle, String path, int bufSize);
    private static native int nativeRead(long streamHandle, byte[] buf, int off, int len);
    private static native void nativeCloseStream(long streamHandle);
    private static native void nativeClose(long handle);
    // delete, mkdirs, rename, ...
}
```

**策略 A（推荐）**：新建 `OBSFileSystemRust extends OBSFileSystem` 或通过配置 `fs.obs.native.enabled=true` 切换。  
**策略 B**：直接改 OBSFileSystem 内部分支 — 风险大，需用户确认。

### 14.4 配置传递

JNI `configJson` = `serde_json` 序列化 `ObsConfig` + bucket + uri + userName，由 Java `Configuration` 遍历 `fs.obs.*` 构建。

### 14.5 Native 库加载

```java
static {
    System.loadLibrary("obs_jni"); // 或从 jar 提取到 temp
}
```

Maven：复制 `target/release/obs_jni.dll` / `.so` / `.dylib` 到 `src/main/resources/native/`。

---

## 十五、六周 Phase 与交付物

| Phase | 周 | 交付 | 验收 |
|-------|-----|------|------|
| 1 | 1–2 | transport 修复、signing、HEAD/GET/PUT/DELETE/LIST、xml、error、cache、**fs 骨架** | cargo test 绿 |
| 2 | 3 | PFS dual-key、truncate header、mkdirs errorCode、LIST PFS 字段、inode | 集成 mock OBS |
| 3 | 4 | prefetch read、MPU write、PFS append、**criterion bench** | bench 数字 |
| 4 | 5 | **JNI** + Java delegate + **contract tests** | mvn test contract |
| 5 | 6 | TLS 生产级、batch delete、content summary、trash、openFile、vectored IO | DoD §1.3 |

---

## 十六、测试策略

### 16.1 Rust 单元测试

- 已有：`auth/signer`, `cache/status`, `types`, `xml`, `pfs/inode`
- 需增：`fs` mock ObjectStore、`pfs/ops` delete 顺序、signer 与 OBS 官方样例向量

### 16.2 Rust 集成测试

- `tests/` 用 wiremock 或 mockito 模拟 OBS XML 响应
- 覆盖：409 分支、LIST 分页、MPU 完整流程

### 16.3 Java Contract

见附录 J 与 `obsa-hdfs/.../contract/obs.xml`

### 16.4 回归

`ITestOBS*.java` 集成测试 — Phase 4 后全量或抽样。

---

## 十七、性能基准与验收

### 17.1 指标

- 顺序读/写吞吐（MB/s）
- 随机 read stat QPS
- listStatus 1000 项延迟
- TPC-DS 类 **HEAD+LIST 次数**（应下降）

### 17.2 工具

- Rust：`criterion`（Phase 3 加入 dev-deps）
- 对比：同机同 bucket Java obsa-hdfs JAR

### 17.3 优化检查清单

- [ ] LIST back-fill 生效
- [ ] negative cache 生效
- [ ] single PUT 小文件
- [ ] prefetch 不 block
- [ ] 连接复用（keep-alive）

---

## 十八、Hybrid 边界（仍由 Java 负责的部分）

以下 **允许** Phase 4 仍走 Java，但须在 README 标明；**最终**功能只多不少：

| 模块 | Java 类 | 说明 |
|------|---------|------|
| MemArtsCC 读缓存 | `memartscc/*`, `OBSMemArtsCCInputStream` | 规格 read policy memArtsCC；Rust 可接 primary/advance |
| Delegation Token | `ObsDelegationTokenManger` | 安全 |
| AuthorizeProvider | `security/AuthorizeProvider` | 鉴权 |
| SSE-KMS/C | `SseWrapper` | 加密头 |
| Metrics Consumer | `BasicMetricsConsumer` | JMX |
| Audit | `OBSAuditLogger` | 可逐步迁到 Rust access log |

**读写主路径**必须在 Rust 才算完成项目目标。

---

## 十九、参考资产与文档

- 规格截图（12 张）：`C:\Users\珈百璃\.cursor\projects\d-RUST-CLIENT\assets\d_____RUST-CLIENT_*.jpg`
- 附录：`PROMPT_NEXT_AI_APPENDIX.md`
- Java 主类：`OBSFileSystem.java`（~2545 行）

---

## 二十、给下一 AI 的第一条消息（复制即用）

```text
你接手 obsa-rust 项目。请完整阅读：
1) d:\桌面\RUST-CLIENT\obsa-rust\PROMPT_NEXT_AI.md
2) d:\桌面\RUST-CLIENT\obsa-rust\PROMPT_NEXT_AI_APPENDIX.md

任务：将 obsa-hdfs 重写为 Rust+JNI，功能只多不少，性能优于 Java。

立即执行顺序：
【P0】安装/确认 Rust toolchain；cargo build --workspace，修复所有编译错误（含 fs.rs 缺失、transport mod 导出、HTTPS）。
【P0】实现 obs-rust-core/src/fs.rs（ObsFileSystem），按 PROMPT §五 HEAD elimination 与附录 A/B 实现 get_file_status/delete/mkdirs/list_status/open/create。
【P1】修复 io/input.rs prefetch 与 io/output.rs MPU close barrier。
【P1】修复 store/obs.rs ListObjects URL 与 V2 签名 resource（对照 obsa-hdfs Java SDK）。
【P2】实现 obs-jni + Java 薄 delegate；跑 hadoop-huaweicloud contract tests。

参考 Java：d:\桌面\RUST-CLIENT\obsa-hdfs\hadoop-huaweicloud

严格遵守 Critical Rules 15 条。用中文汇报进度，每阶段给出 cargo test / mvn test 结果。

不要未经用户要求 git commit。最小 diff，不过度重构。
```

---

## 二十一、原 PROMPT 精简表（保留）

### HEAD Elimination 一览

| Op | Java HEAD | Rust HEAD | Technique |
|----|-----------|-----------|-----------|
| delete | 1 | 0 | dual-key |
| truncate | 1 | 0 | x-obs-hdfs + 409 |
| mkdirs | 1 | 0 | PUT folder |
| listStatus | 1+N | 0 | ListEntry fields |
| rename PFS | 2 | 0–1 | atomic rename |
| open | 1 | 0 | cache |
| getFileStatus | 1 | 0 | cache |
| create | 1 | 0 | negative cache |
| exists | 1 | 0 | cache |
| setPermission/Owner | 1 | 0 | PUT metadata |
| getContentSummary | 1+N | 0 | flat LIST |

### 依赖（workspace）

见附录 K。

---

**文档版本**：2026-03-23 v2（超详细版 + 附录拆分）
