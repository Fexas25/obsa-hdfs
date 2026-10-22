# 主动放弃上传与进程退出后的清理

更新：2026-10-11。当前处理已知 upload ID 的 MPU 清理，不是断点续传或事务提交器。

## 主动放弃

Rust `ObsOutputStream::abort(self).await` 丢弃暂存数据，取消并等待分片退出，然后 abort
已知 MPU。小文件不发送 PUT；MPU 不发送 complete；成功回调不调用。远端清理错误返回
调用方，取消/失败保留 Drop 的尽力清理兜底。显式 abort、失败 close 的清理、Drop 后台
清理与日志恢复共用同一判断：成功响应或明确的 404 NoSuchUpload 才认为远端清理完成。
NoSuchBucket、其他 404 或不匹配状态仍属错误；日志保留。失败 close 即使清理成功，
仍返回原始上传错误，不触发成功回调。文件系统失效保护仍随流释放生效。

### Hadoop 创建流主动放弃

`OBSFileSystemRust.abortUpload(FSDataOutputStream output)` 支持由该实例的 create 或
createNonRecursive 返回的创建流：

```java
try (FSDataOutputStream output = fs.create(path)) {
  output.write(bytes);
  if (shouldDiscard) {
    fs.abortUpload(output); // 离开 try 时的 close 不会提交
  }
}
```

小文件只丢弃缓冲，不发送 PUT；MPU 取消并排空在途分片后发送 abort，不发送 complete。
方法同步返回清理结果；远端清理失败抛 IOException，流仍被关闭，Rust Drop 保留一次
尽力清理，配置日志后仍可显式恢复。重复 abort/close 不重新提交；abort 后 write/flush
失败。已成功 close 的对象不能通过此方法撤销，必须在提交前明确调用 abort。
此入口不会在任意 Java 业务异常发生时自动触发；普通 try-with-resources 的 close 仍有
提交语义，业务层需要主动决定何时放弃。

其他文件系统实例的流或非本适配器流抛 IllegalArgumentException，不消耗其句柄。
PFS append 抛 UnsupportedOperationException 并保持流可用；不承诺撤销已提交追加字节。
JNI 新增 nativeAbortStream：只接受创建流，输入/追加流被拒绝且句柄不变；无效/已关闭
句柄抛 IllegalArgumentException。发布时须配套更新 Java JAR 和原生库。
Java 同一流的 write/close/abort 互斥；中断 Java 线程不保证取消已进入 JNI 的清理操作。
文件系统关闭后，若 Java 流仍保留已失效的原生句柄，abortUpload 消耗该句柄并抛
IOException（保留原始 JNI 异常 cause）；随后重复 abort/close 不提交。已进入 Rust 的
abort 可在文件系统关闭后继续，日志锁维持到清理结束；本地并发回归已验证这一边界。

## 可选持久日志

设置 `fs.obs.upload.journal.dir` 或 `ObsConfig.upload_journal_dir`，支持桶级覆盖；默认
不启用。目录是应用配置的本地持久目录，应由应用独占管理，不存放用户业务文件。
Rust 核心记录日志；Rust API 和 Hadoop/JNI 扩展均提供显式恢复入口。

取得 upload ID 后，在阻塞工作线程写入记录、sync 文件、将 `.pending` 重命名为 `.json`，
然后才允许发送分片。记录只有格式版本、目标命名空间摘要、对象键和 upload ID，不含
AK/SK/token 或文件数据。对象键仍是业务元数据。命名空间由 endpoint、bucket、secure
和 path_style_access 配置确定，恢复时这些配置必须保持一致。2026-10-22 起 Hadoop 配置默认虚拟主机方式
（`fs.obs.path.style.access` 默认 false，与 SDK 一致），此前用默认配置写下的日志属于另一个命名空间，
需临时设 `fs.obs.path.style.access=true` 才能恢复。

每个命名空间持有 OS 文件锁，进程退出自动释放；同一目录和命名空间只允许一个文件系统
实例拥有，避免其他实例清理活跃 MPU。并行实例可配置独立日志目录。活跃流另外持有本地
活动标记，当前实例的恢复调用会跳过它们。关闭文件系统后若仍有流或清理任务持有日志，
锁会继续保持到这些引用释放。取锁失败不会永久缓存，稍后可以重试。

complete 或 abort 成功后移除记录；本地删除失败会保留记录供后续清理。Drop 的远端清理
失败也会留下记录。运行时之外丢弃流不再是唯一机会：记录可由后续实例读取。
Windows 本地回归以文件共享模式实际阻止删除，已验证成功 close/abort 不被本地清理失败
改判、记录保留且解除占用后能恢复；恢复报告只有本地记录也删除成功才增加 aborted，
否则进入 errors。远端已清理但本地删除失败时，下次恢复可能再次发送 abort。

## 显式恢复

在同目标配置的文件系统实例上调用：

```rust,ignore
let report = fs.recover_pending_uploads(100).await?;
// report.aborted：成功清理或明确返回 404 NoSuchUpload，且本地记录已删除的数量
// report.active：跳过的活动记录数量
// report.errors：保留待排查/重试的记录及错误
```

Rust 公共入口先校验 limit，非法值返回 InvalidArgument，不创建日志目录、不取得命名空间锁。
每次检查上限为 1..=10000 条候选记录，按目录顺序处理，不自动在启动时执行。仅对日志中
的 key/upload ID 发起 abort，不扫描整个桶，不删除最终对象，也不重放写入或 complete。
仅明确的 `404 NoSuchUpload` 视为 MPU 已不存在。`NoSuchBucket`、`NoSuchKey`、
没有错误码的 404，以及错误码为 NoSuchUpload 但状态码不是 404 的响应都保留日志并报告
错误，避免因桶/端点配置或代理错误丢失恢复依据。失败记录保留；重复运行可以继续处理。
错误分类依据：[华为云 NoSuchUpload](https://support.huaweicloud.com/intl/en-us/api-obs/obs_errorcode_0026.html)
与[取消多段上传 API](https://support.huaweicloud.com/intl/en-us/api-obs/obs_04_0103.html)（2026-10-11 核对）。

已完整写入但尚未重命名的 `.pending` 记录也可恢复。解析最多读取 64 KiB + 1 字节，
校验版本、命名空间和文件名身份；损坏、超长、不匹配或非普通文件只报告错误，不发远端
请求、不自动删除。活动或损坏记录也占本次检查配额。同一实例保留目录迭代进度，
小批量重复调用会继续向后扫描，避免被开头的活动或损坏记录长期挡住；到达目录末尾后
下一次调用重新扫描。游标仅在内存中，不排序、不自动调度，不收集整个目录列表。
目录新增记录最迟在后续完整扫描中发现；并发调用共享扫描进度，单次报告不是全目录快照。
取消恢复会释放本地活动标记并保留日志，后续扫描可再次尝试 abort；取消不证明远端未执行。

## Hadoop/JNI 主动恢复

在已配置 `fs.obs.upload.journal.dir` 的 `OBSFileSystemRust` 实例上调用：

```java
OBSFileSystemRust.UploadRecoveryReport report = fs.recoverPendingUploads(100);
int cleaned = report.getAborted(); // 包括服务端明确返回 404 NoSuchUpload 的 MPU
int skipped = report.getActive();
for (String error : report.getErrors()) {
  // 记录并排查；对应日志仍保留。不要仅凭方法返回就认定全部恢复成功。
}
```

这是适配器扩展，不是 Hadoop `FileSystem` 标准方法；通过通用 `FileSystem` 获取实例时，
先检查它是否为 `OBSFileSystemRust` 再调用。结果对象不可变，错误列表不可修改。
`limit` 必须为 1..10000 的整数。非法参数或未配置日志目录抛 `IllegalArgumentException`；
日志初始化/取锁失败抛 `IOException`；关闭实例后调用抛 `IOException`。单条日志或远端
abort 失败放入报告，允许继续处理本批其他记录。

底层使用已有 `NativeObs.nativeCall(handle, "recoverPendingUploads", "{\"limit\":100}")`，
返回 JSON `{ "aborted": 0, "active": 0, "errors": [] }`；JNI 独立校验缺失、负数、浮点数、
字符串及越界 limit，低层无效/关闭句柄抛 `IllegalArgumentException`。没有新增原生 ABI
符号。调用是同步的，可在应用自有维护线程执行；Java 线程中断不保证取消 Rust/HTTP 操作，
也不保证远端未执行。关闭与恢复并发时，已经进入 Rust 的调用可继续完成并暂时持有日志锁。
本地 JVM 并发回归已验证：关闭后拒绝新调用、在途恢复完成前保留锁、完成后其他实例可取锁。
不自动在 initialize/close 时启动恢复，不提供后台调度。

## 已验证与尚未保证

真实子进程写出两份 MPU 日志后调用 process::exit，跳过 Rust Drop；后续实例重新取得
锁并恢复。回归还覆盖发布前 `.pending`、目标桶隔离、恢复数量上限、远端失败保留重试、
活动上传跳过、同命名空间锁冲突、complete/abort 删除记录、损坏/超长记录零远端修改。
另覆盖小批量扫描跨过损坏记录、取消清理后重试、并发清理跳过在途记录和 404 幂等清理。

这些是本地进程退出及模拟服务验证，不是实际断电、磁盘故障或真实 OBS 生命周期验收。
取得服务端 upload ID 到首份完整本地记录之间仍有窗口；初始化响应丢失、日志目录丢失、
损坏且无法解析的记录不能由本机制恢复。Windows 文件已 sync，但没有声明目录项断电
持久性保证；Unix 额外 sync 目录的分支尚未在本环境运行。

完成请求已提交但响应丢失仍属不确定结果，不能据此自动重传文件。日志清理失败不撤销
已成功的提交；后续对已完成 MPU 的 abort 不应删除最终对象，该服务端行为仍需真实桶
验证。Hadoop/JNI 的本地 HTTP 回归覆盖桶级配置、批量上限、403 保留重试、404 清理、
活动 Java 上传跳过、正常完成移除记录、损坏记录、锁冲突以及非法参数/关闭句柄。
跨主机共享目录、网络文件系统锁语义和崩溃后自动调度尚未验收。日志目录初始化/取锁、目录扫描、记录读取与解析，以及逐 MPU 持久化已移到阻塞工作线程；
同实例初始化由异步互斥锁串行化并缓存成功结果。恢复逐条调度，不全量收集目录或预读全部
记录。取消等待不终止已经排队/开始的阻塞文件操作，结果丢弃时释放活动标记并保留日志，
读取工作线程不发送远端 abort。初始化工作线程持有独立的异步互斥守卫并发布缓存：
取消等待后，成功结果仍缓存到原文件系统实例，日志锁保持到该实例及相关引用释放；
同实例后续调用等待并复用原初始化，不另起竞争取锁。失败不缓存，后续可重试。取消
不会中止已排队/开始的初始化，也不能绕过正在阻塞的磁盘操作；初始化本身不发送远端请求。
记录删除也移到阻塞工作线程，删除任务持有活动标记及命名空间锁直到结束。取消等待
不能停止已排队/开始的删除，后台可能继续移除已确认清理或提交的记录。上传 close/abort
仍等待正常删除结果，删除失败只保留日志并告警，不把已完成的远端操作改判为失败；恢复
入口则继续将删除失败放入 errors。若成功提交后的删除等待被取消，不能据此判定对象未
提交或自动重传。资源 Drop 仍可能包含短同步系统调用，未宣称完全无阻塞；启用日志有
磁盘延迟及工作线程调度成本，未测量吞吐提升。本地使用 Rust 1.98.1 验证文件锁 API。
