# obsa-rust

华为云 OBS/PFS 的 **Rust 存储核心**，通过 Rust JNI 桥接供 Hadoop 使用。文件操作、网络请求、签名、缓存、上传恢复与并发协调在 Rust 中实现；Java 负责 Hadoop 接口适配。

这是可运行并有本地回归覆盖的实现，**尚未完成真实 OBS/PFS 和生产作业验收**。本地模拟测试不能证明云端兼容性或生产性能。

## 从哪里开始

- [obs-rust-core](obs-rust-core/)：纯 Rust 核心；直接 Rust 集成从 [ObjectStore](obs-rust-core/src/store/traits.rs) 和 [ObsFileSystem](obs-rust-core/src/fs.rs) 开始。
- [obs-jni](obs-jni/)：Rust 原生动态库、句柄和 JNI 数据传递。
- [java](java/)：NativeObs 声明与 JVM 冒烟测试。
- [hadoop-adapter](hadoop-adapter/)：独立的 `OBSFileSystemRust`，实现 Hadoop FileSystem/流接口。
- [scripts](scripts/)：Windows 构建、JNI 测试和本地基准入口。
- [docs](docs/)：设计边界、测试证据、性能基线和剩余工作。

此子目录中的 Cargo workspace 包含两个 Rust crate。GitHub 仓库外层保留的原 Java 参考工程，与这里的 Rust 实现是不同目录。

## 构建与测试

仅使用 Rust 核心时不需要 Java/Maven：

```sh
cargo build --locked -p obs-rust-core
cargo test --locked -p obs-rust-core
```

Windows 完整本地验证需要 Rust stable、Visual Studio C++ 工具、Windows SDK、JDK 17+ 和 Maven：

```powershell
.\scripts\build-windows.ps1
.\scripts\test-jni.ps1
mvn -f hadoop-adapter/pom.xml test
```

其他平台可使用 `cargo build --locked --workspace` 和 `cargo test --locked --workspace`；本文的最新运行证据来自 Windows，不代表其他平台已验收。优化构建用 `cargo build --locked --release --workspace`。`cargo test` 不代替实际 JNI 动态库的构建。

Maven 默认使用公开 Hadoop 3.3.6；部署其他版本可通过 `-Dhadoop.version=...` 指定并重新验证。Windows JDK ZipFS 受限时可选 `-Pecj` 编译器配置。

## Hadoop 接入

将适配层 JAR 及其 Jackson 依赖加入应用 classpath，将构建的原生库目录加入 `java.library.path`。显式启用 Rust 实现：

```xml
<property><name>fs.obs.impl</name><value>org.apache.hadoop.fs.obs.OBSFileSystemRust</value></property>
<property><name>fs.obs.endpoint</name><value>YOUR_OBS_ENDPOINT</value></property>
<property><name>fs.obs.bucket.mode</name><value>auto</value></property>
```

通过 Hadoop CredentialProvider 提供 `fs.obs.access.key`、`fs.obs.secret.key` 和可选 `fs.obs.session.token`，也支持桶级别名。凭据目前在初始化时读取，尚无自动 STS 刷新或委托令牌。不要将真实凭据放进仓库。

## 已实现与行为边界

核心支持状态查询、读写、目录、分页列举、删除、重命名、追加、截断、容量统计与权限元数据，并提供缓存、有界预读、分片上传和已知 MPU 恢复。

- 普通桶读取默认检查 ETag，避免混合不同修订的数据。PFS 使用可变文件语义，不承诺快照读取。
- 新建上传文件在 close 时提交，`hflush/hsync` 不提供即时可见性；PFS append 同步等待已提交写入。普通桶 rename 是 copy/delete，不具备 HDFS 原子重命名保证。
- 上传初始化/完成及 PFS rename 不自动重试；已发往服务端的写入不能靠本地取消回滚。
- 可选 `fs.obs.buffer.total.bytes`（默认 0，关闭）统一计费受控数据缓冲；不包含所有 TLS/Hyper/分配器及元数据开销，不能当作进程 RSS 上限。
- `fs.obs.http.response.max.bytes` 默认 64 MiB；范围响应另受请求长度限制。大对象应分范围读取。
- 可选上传日志和主动恢复用于清理已知 MPU；丢失初始化响应、未落盘 ID 和断电场景仍有边界，不保证远端无残留。

配置、能力和接口覆盖详见 [API_COVERAGE.md](docs/API_COVERAGE.md)，预算详见 [BUFFER_BUDGET.md](docs/BUFFER_BUDGET.md)，恢复详见 [UPLOAD_RECOVERY.md](docs/UPLOAD_RECOVERY.md)。

## 当前本地验证（2026-10-13）

- 最新默认 Rust workspace debug：**338 通过、0 失败、15 ignored**；包含完成上传的分片列表预检，以及 GET 正文长度和成功状态校验。
- 下列综合结果来自该预检之前的版本：release **334 通过、0 失败、15 ignored**。
- `append-diagnostics`：**336 通过、0 失败、15 ignored**。
- 真实 JVM：debug 与 release JNI 冒烟均通过。
- Hadoop 3.3.6：报告 **291 项、0 失败、0 错误、12 跳过**。跳过对应普通桶不支持 append 和新建文件 close 才可见的能力边界。

Rust ignored 项是手动基准，未混入上述通过数。全部使用内存或本机 HTTP 模拟；没有真实云桶、生产作业或跨平台验收结论。详细运行证据见 [LOCAL_VALIDATION.md](docs/LOCAL_VALIDATION.md)。

近期修复包括 [元数据键处理](docs/METADATA_CASE.md)、[对象路径与签名](docs/OBJECT_KEY_PATH.md)、[GET 边界](docs/GET_VALIDATION.md)、[写入结果元数据](docs/WRITE_METADATA.md)、[XML 回车保真](docs/XML_OUTGOING_TEXT.md)、[删除回退预算](docs/DELETE_FALLBACK_BUDGET.md) 和 [分片 ETag/段号](docs/PART_ETAG.md)。

## 尚未完成

真实 OBS/PFS 与原厂 Hadoop 版本互通，Spark/Hive/Flink 作业，普通桶 copy/delete 并发修改保护，STS 自动刷新、委托令牌、SSE、完整能力声明及高级读取接口，以及百万目录规模和真实网络下的性能验收仍需推进。

[ADAPTER_PLAN.md](docs/ADAPTER_PLAN.md) 定义后续验收要求；[历史实现说明](docs/README_HISTORY.md) 保留各阶段配置细节、设计决定和历史基准。旧 `PROMPT_NEXT_AI*.md` 是目标规格，不是完成声明。历史微基准不代表当前生产吞吐。
