# JNI ByteBuffer 读取与本地对照

2026-10-17，Windows / JDK 17，优化构建。Rust 读取算法、版本检查和预读策略保持不变。

## 接口与边界

Hadoop 输入流实现 `ByteBufferReadable`，仅声明 `in:readbytebuffer` 能力。
可写 heap ByteBuffer 复用原 byte[] 读取路径，并正确处理 arrayOffset、position 和 limit；
可写 direct ByteBuffer 委派 Rust `nativeReadDirect`。原 byte[] API 不切换实现。
byte[] 定位读取与 vectored read 已另行实现（见 POSITIONED_VECTORED_READ.md）；尚未实现 positioned ByteBuffer 或 Hadoop 的增强零拷贝读取接口。

直接缓冲区路径省掉 JNI 临时 Vec 和最后一次 Java 数组复制，但 Rust 仍将缓存范围复制到
调用方的缓冲区，因此不是端到端零拷贝。JNI 每次最多读取 16 MiB，允许正常短读。
调用方须独占所提供区域，不能在读取期间并发修改缓冲区或其别名，也不能手动释放底层内存。
JNI 在返回前保持缓冲区局部引用，不持有 JVM heap critical 指针，不在返回后保留地址。
JNI 本身再次检查 direct 类型、只读状态、负数和容量边界，不能只依赖 Hadoop 检查。

成功时 Hadoop 更新流位置、ByteBuffer.position 和 bytesRead 统计；limit 保持不变。
空缓冲区返回 0，非空缓冲区在 EOF 返回 -1。关闭后的流（包括空读取）报错。
异常时两个逻辑位置保持原值，但目标缓冲区内容可能已被部分覆盖；调用方须重试或丢弃内容。
Rust 的错误/取消恢复、远端版本变化检测和预读资源清理继续使用原实现。

调用方分配的 direct 内存与 Java 数组一样不计入 Rust 受控缓冲预算；它的生命周期由调用方
管理。HTTP 帧、范围缓存等原有预算不变，不能据此声称 JVM RSS 下降了某个固定值。

## 可重复微基准

先构建 release JNI，然后运行：

```powershell
.\scripts\benchmark-jni-read.ps1 -JavaHome $env:JAVA_HOME -Rounds 7 > read.csv
```

程序自带本机 HTTP 服务，先把 1 MiB 范围暖入缓存，测量每次 seek(0) + read。
每轮交替 array/direct 顺序，复用目标缓冲；每种尺寸分别执行 1000000、500000、10000 次，
输出吞吐、当前调用线程 CPU、p50/p95/p99 和 GET 数，并验证数据及所有测量轮次 GET=0。
计时不包含目标分配；每操作延迟不包含内容断言，整轮墙钟/CPU 包含轻量边界字节断言。
CPU 不包括其他线程，Windows CPU 计数粒度较粗；这不是网络、完整 Hadoop 作业或云端吞吐。
运行时避免其他构建/测试负载。分位数按每轮计算，以下是七轮指标的中位数，不是汇总分位数。

两次独立执行共 84 条有效记录，均无测量阶段 GET：

- 1 MiB，首次 array/direct 为 1379.744 / 11437.519 MiB/s，p99 为 2295.0 / 439.5 μs；
  重复执行为 1332.846 / 10276.759 MiB/s，p99 为 2397.6 / 353.5 μs。
  每轮配对吞吐比首次 6.32–10.97，重复 5.98–10.33；全部 14 对方向一致。
  每 10000 次调用的线程 CPU 中位数首次为 6687.5 / 828.125 ms，重复为 6890.625 / 937.5 ms。
- 4 KiB，首次 array/direct 为 3307.840 / 1466.515 MiB/s，p99 为 4.7 / 13.5 μs；
  重复为 2923.049 / 1442.018 MiB/s，p99 为 4.9 / 11.6 μs。
- 1 B，首次 array/direct 为 1.225 / 0.423 MiB/s，p99 为 1.5 / 10.5 μs；
  重复为 0.986 / 0.378 MiB/s，p99 为 3.3 / 12.2 μs。

因此只增加调用方显式选择的 ByteBuffer 接口，不把小块 byte[] 读取改走 direct。
直接缓冲区验证及 Java 方法调用存在固定成本，小块缓存读取没有收益。
没有根据这组缓存微基准改变读取块大小、预读默认值或宣称实际 OBS 吞吐提高上述倍数。
原始 CSV、编译产物和环境日志只保留在工作区，不上传。

## 本地验证

显式构建 debug/release JNI，Rust workspace 各 388 通过、0 失败、16 ignored；两种库的
真实 JVM 基础和异常分类冒烟均通过。新增 JNI 检查覆盖输出/关闭句柄、heap/只读/null
缓冲、负数/溢出/容量边界、切片哨兵、零容量、EOF、16 MiB 限制及余量读取。
Hadoop 两种配置各 347 项、0 失败、0 错误、12 跳过，分别核验 16 份 XML 的实际库目录。
新增四项测试在普通桶/PFS 下覆盖 heap/direct 切片、位置/limit、EOF、只读/空值、
关闭流/文件系统，以及首块失败和复制多个块后失败的恢复与重试。
Clippy 全目标/全特性通过，保持原 16 项风格/复杂度告警，没有新增。
项目内基准脚本另执行一轮，六条记录全部 GET=0；没有把这次入口验证混入两次七轮结果。

## Hadoop 缓冲池回退

`FSDataInputStream.read(ByteBufferPool, maxLength)` 继续使用 Hadoop 3.3.6 自带回退，
并非实现了增强零拷贝接口。它检测 ByteBufferReadable 后会向池申请 direct 缓冲；这与
以前申请 heap 缓冲不同，小块池式读取可能承担上述 direct 固定开销，不能承诺所有入口提速。
调用方需要让池提供可写 direct 缓冲，并对每个成功返回的非 null 缓冲调用 `releaseBuffer`。
关闭输入流不会代替归还，关闭后仍可显式归还；重复归还被拒绝。

新增两项本地回归覆盖普通桶/PFS：池提供小于 maxLength 的缓冲、部分 EOF、EOF 自动归还、
无池时拒绝、成功后显式释放、重复释放、关闭后释放、GET 重试失败和所属文件系统关闭后的
自动归还。release Hadoop 共 349 项、0 失败、0 错误、12 跳过；debug 上一批仍为 347 项。
本次仅增加测试和说明，未修改生产代码，未重复 Rust/JVM 构建。

## 并发与固定版本验证

独立 JVM 冒烟增加八个线程同时调用同一 nativeReadDirect 句柄，每线程使用独占缓冲，
合计读取 200 个带标识的记录，验证记录内部不交错、全局不重复不遗漏、最终 EOF，
并等待所有工作线程退出。这绕过 Hadoop 输入流的 Java synchronized，直接检查 JNI 的
原生流锁；不是并发吞吐基准，也不允许多个线程共享同一个目标区域。

Hadoop 新增 direct 版本变化测试，先用 HEAD 缓存 ETag，然后分别在首次 GET 前和已读
一个范围后覆盖对象。两个分支均验证 RemoteFileChanged、位置与 limit 保持、seek 后
错误继续锁定且不再发送 GET。该测试只针对普通对象桶；PFS 不承诺此快照语义。
写入后的缓存状态可能没有 ETag，此时既有策略在首次 GET 固定版本，不能把这一情形
误当作已经固定了 HEAD 版本。测试未增加额外生产 HEAD，也未改变固定版本的时机。

本批 debug/release 实际 JVM 并发/基础/异常冒烟均通过，Hadoop 两种配置各 352 项、
0 失败、0 错误、12 跳过，分别校验 16 份 XML 的库路径。生产代码未改，Rust 未重跑。

## JNI 排队临时内存实验（2026-10-18）

`nativeRead` 和 `nativeReadInto` 在等待同一原生流锁之前分配临时数组。
直接并发调用 NativeObs 时，等待者也会持有这块内存；通常由 Hadoop 输入流的
Java synchronized 串行化的请求不等同于这一场景。direct 读取不分配此临时数组。
JNI 和调用方数组仍不属于核心统一缓冲预算，不能把该预算解释为整个进程的内存上限。

新增可复现的 Windows/JDK 17 本地入口：先构建 release DLL，然后执行：

```powershell
.\scripts\benchmark-jni-queued-memory.ps1 -JavaHome D:\java17 -Rounds 3
```

可用 `-NativeLibraryPath` 指定已经完成构建、冻结的 DLL 目录做版本对照。
每个模式/并发数/轮次启动独立 JVM，输出一行 JSON。每轮依次测试 legacy/into
及 1、4、8、16 个线程；三轮共 24 行。不要同时运行编译、其他基准或重负载任务。

基准用一个字节的对象和每次 8 MiB 的读取请求放大临时分配问题，服务端用门闩阻塞
唯一一次 GET；线程就绪、调用方数组分配并触页后记录基线，所有线程进入 JNI 后
采样十次，取排序后第六个值。指标来自 HotSpot 的 committed virtual memory，
**不是 RSS、预算计费量或生产峰值**，也包含运行时分配及采样噪声；不支持此指标的
JVM 会失败，负的差值不能当成内存优化。释放门闩后核验总读取一个字节、一次 GET、
数据内容正确及所有线程退出。`drain_ns` 包含解除阻塞到线程 join 完成的时间，
不是逐请求 p99，也不代表持续吞吐。

实验候选把两处临时分配移入获得锁后的分支，并保持 Java 数组复制在锁释放后。
三轮中位数显示，16 个请求的额外提交内存从约 128.34 MiB 降到约 8.05 MiB，
候选在所有并发数下均约 8.05 MiB。但既有缓存读取基准每版本每顺序七轮的对照中，
1 MiB array 路径的中位耗时在正序增加 16.0%、反序增加 2.3%，p99 中位值分别
增加 32.8% 和 12.0%。未改动的 direct 对照也有波动，因此不能把全部差异归因于
代码，更不能据此宣称普遍提速。

**候选生产改动已撤回**，本批只保留基准入口与证据说明。排队临时内存问题仍待优化，
不能标为已修复。后续方案需要同时复测排队内存、单请求缓存性能及异常/关闭语义。
原始 CSV/JSONL 和候选 DLL 仅保留在本地工作区，不随源码发布。

项目内入口另以冻结基线完成三轮 24 次运行；恢复生产源码并重新编译 release JNI 后，
默认库路径的一轮 8 次运行与实际 JVM 基础/异常冒烟通过，四组非法参数均在启动服务前
被拒绝。这些入口验证不混入前述性能对照。生产源码与上一批一致，未重复完整 Rust/
Hadoop 回归；上一批 debug/release Rust 各 400 通过、16 忽略，Hadoop 各 361 项、
0 失败、0 错误、12 跳过是沿用的证据，不是本批新跑的结果。
