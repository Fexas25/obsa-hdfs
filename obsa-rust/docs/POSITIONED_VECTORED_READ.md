# 定位读取与向量化读取

2026-10-20。Hadoop 输入流新增原生 `PositionedReadable.read(long, byte[], int, int)` 与
Hadoop 3.3.5+ 的 `readVectored`。顺序读取、预读策略和版本检查算法不变。

## 为什么需要

Hadoop 默认的定位读取是在流监视器内 seek → read → seek 回原位：并发调用者串行化，
每次跳转还会取消顺序预读、丢弃常驻块。Parquet/ORC 读 footer 和列块大量使用定位读取，
Spark/Hive 中同一流常被多个线程并发定位读。默认 `readVectored` 则对每个范围依次调用
`readFully`，不合并相邻范围，也不并发。

## Rust 核心

- `ObsInputStream::positioned_reader()` 返回可克隆的 `PositionedReader`，共享打开时的长度、
  普通桶固定的 ETag（`OnceLock`，流与定位读谁先读谁固定）、PFS inode 提示和缓冲预算。
  它不持有、也不修改流游标、预读窗口或常驻块。
- `read_at(position, buf)`：按打开长度截断，0 表示 EOF。大请求从 `position` 起按
  `fs.obs.readahead.chunk.size` 切分，最多 `fs.obs.readahead.max.chunks` 个范围同时请求，
  按顺序复制。失败不报告部分字节数，也不是粘性失败。
- `read_resident_at`：整个请求落在流当前常驻块内时直接复制，不发请求；流已因版本变化失败时不使用。
- `read_ranges(ranges, merge_gap, max_merged)`：排序后合并间隙 ≤ `merge_gap` 且合并跨度
  ≤ `max_merged` 的范围（允许重叠和重复），合并跨度再按块大小切分，所有跨度的块共享
  `readahead.max.chunks` 并发上限，完成后分发到各缓冲区。任一范围越过打开长度，在发送请求前整体失败。

## JNI 与 Hadoop

- `nativePreadInto`：定位读取器单独登记（`READERS`），不经过流的异步互斥锁，因此并发定位读、
  以及与顺序读之间不会互相排队；仅对常驻块快路径 `try_lock` 流。关闭流/文件系统同时注销。
- `nativeStreamLength`、`nativePreadVectored(long[] positions, byte[][] targets, gap, max)`：
  JNI 按元素释放局部引用，避免大量范围耗尽 local frame。
- `OBSFileSystemRust` 输入流的定位读取不加 `synchronized`；`readFully(long, ...)` 沿用
  `FSInputStream` 默认循环。`getPos` 与顺序读取不受影响。
- `readVectored` 使用 Hadoop 的 `minSeekForVectorReads()`（默认 4 KiB）和
  `maxReadSizeForVectorReads()`（默认 1 MiB）作为合并参数，声明 `in:readvectored`。
  负长度同步抛 IllegalArgumentException，负偏移同步抛 EOFException（与 Hadoop 默认一致）；
  越过 EOF 的范围单独以 EOFException 完成，不发请求；其余范围一次原生调用读完后在返回前完成。
  原生调用失败时这些范围都以同一异常完成。
- 所有 `FileRange` 用法放在 `NativeVectoredRead`，仅在调用 `readVectored` 时加载；
  输入流类本身在没有向量化 IO 的旧 Hadoop 上仍可加载。
- 故意未实现 `ByteBufferPositionedReadable`：该接口只存在于 Hadoop 3.3.0+，实现它会使输入流类
  在 3.1.x（华为 MRS 常见版本）上无法加载。

## 已知边界

- 向量化读取先读入临时 `byte[]`，再复制到调用方分配的 ByteBuffer（含 direct），比理论最优多一次
  内存复制；临时数组和 Rust 侧暂存不计入 `fs.obs.buffer.total.bytes` 之外的预算。
- 定位读与向量化读每次调用一个 GET（或若干块 GET），不预读后续数据；小而密集的随机读取应依赖
  调用方的合并（如 Parquet 的向量化读）。
- 普通桶定位读在对象被替换后返回 RemoteFileChanged（412/ETag 不一致），常驻块仍返回打开时版本的数据。

## 本地验证

- Rust：定位读 5 项（游标独立、EOF 截断、并发切分与上限、版本共享、常驻快路径、失败非粘性），
  向量化 4 项（合并/重叠/空范围/EOF 边界、最大合并跨度与块切分并发、EOF 前置拒绝与溢出、版本变化）。
- Hadoop 3.3.6：官方 `AbstractContractSeekTest`（含定位读用例）两种桶 36 项；
  官方 `AbstractContractVectoredReadTest` direct/array 共 34 项；新增 `PositionedReadTest`
  3 项（32 个并发 readFully、游标不变、常驻快路径、EOF/越界/关闭、对象替换检测）。
- 全部为本机模拟服务；未在真实 OBS/PFS 或 Spark/Parquet 作业上测量吞吐。
