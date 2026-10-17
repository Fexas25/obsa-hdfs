# 本地验证记录 — 2026-10-11

环境：Windows、Rust 1.98.1、JDK 17、Maven 3.9.9、Hadoop 3.3.6、可选 ECJ 编译器。普通回归为 debug 构建；性能基准单独使用 release。全部请求到内存存储或 127.0.0.1，不使用真实桶。

## 结果

- Rust：完整回归 290 通过、0 失败；10 个手动性能基准默认 ignored。本轮完整重跑，并显式执行追加/重命名 release 基准。
- JNI 冒烟：本轮通过，包括输入流拒绝 abort 后仍可读、read/readInto 边界及句柄关闭。
- Hadoop/JVM：291 项，279 通过、12 按能力跳过，0 失败、0 错误。此前 PFS 追加/重命名竞争已修复，本轮增加确定性回归并保留原官方契约测试。

其中官方 Hadoop 契约共 236 项，224 通过、12 跳过。其余 55 项是凭据、JNI 配置/缓冲和适配层专项集成测试。

## 套件记录

- `UploadRecoveryTest`：13 通过，0 跳过。
- `CredentialResolutionTest`：6 通过，0 跳过。
- `HadoopLocalTest`：27 通过，0 跳过。
- `JniBufferTest`：2 通过，0 跳过。
- `NativeConfigurationTest`：3 通过，0 跳过。
- `NativeListingLifecycleTest`：3 通过，0 跳过。
- `ObsAppendContractTest`：8 通过，8 跳过。
- `ObsContentSummaryContractTest`：4 通过，0 跳过。
- `ObsCreateContractTest`：28 通过，4 跳过。
- `ObsDeleteContractTest`：16 通过，0 跳过。
- `ObsGetFileStatusContractTest`：40 通过，0 跳过。
- `ObsMkdirContractTest`：16 通过，0 跳过。
- `ObsOpenContractTest`：38 通过，0 跳过。
- `ObsRenameContractTest`：20 通过，0 跳过。
- `ObsRootDirectoryContractTest`：18 通过，0 跳过。
- `ObsSeekContractTest`：36 通过，0 跳过。

## 跳过依据

普通桶的 8 项 append 契约不适用；普通桶与 PFS 各有 2 项创建即时/flush 可见性契约不适用。两种模式的新建文件均走 multipart/单 PUT close 提交路径，明确不承诺 create 流的 hflush/hsync。PFS append 路径支持同步等待，已单独声明流能力。没有修改或覆盖上游测试方法来隐藏失败。

## 新增保护与证据

- 首轮官方契约发现 16 项失败；修复创建 builder 重载、PFS 目录覆盖、目录打开异常、关闭流检查、父目录存续及能力声明后通过。
- 官方目录测试通过每页 3 项的真实 HTTP 分页运行；根目录操作仅发生于每项测试独占的模拟服务。
- 5 MiB Java 数组中选取超过 2 MiB 的切片写入，跨越 1 MiB 原生批次；直接读取校验内容、位置、EOF 及两侧未修改哨兵。
- Unicode、空格、%、+、#、?、XML 保留字符的名称经过创建、分页列表、读取、复制重命名及删除；修复 CopyObject 源头 URL 编码。
- HTTP 200 内嵌复制错误会终止 rename，集成测试确认源文件内容保留。复制成功结果缺失、截断、根节点错误、无 ETag 均拒绝；MPU 完成截断同样拒绝。
- Range 响应验证状态、起止偏移、总长度与字节数；覆盖服务端忽略 Range、错误偏移、超长/短 body，以及合法 EOF 截断。
- 响应体限制覆盖 Content-Length 和 chunked 传输；HEAD 的对象长度不被误判为 body 长度。
- LIST/MPU 初始化的截断 XML、错误根节点、多文档、非法实体及 LIST 错误大小/分页字段不会被当作成功。

## 本次追加预算与读取恢复验证

- 8 字节的共享追加预算与 4 字节块：两个流提交 16 字节数据，阻塞时第二个流不能发起请求；放行后逐字节校验两个文件，活动请求体峰值不超过 8 字节。
- 大 write 中途取消后，该流后续 write 持续报错；drop 后额度归还，新流可取得全部额度并成功写入。
- 注入追加失败后，后续分块不再提交，flush 不报成功；新流仍可取得额度并完成。
- Hadoop 配置支持大小后缀与桶级覆盖；实际块大小受预算容量限制。
- 一次 read 已复制首块，次块失败时逻辑位置恢复到起点；取消次块等待同样可恢复，重试数据完整。
- JVM 集成注入次块连续三次 503，耗尽 HTTP 重试后断言 Java 位置仍为 0；再次读取返回原始前 8 字节，确认原生与 Java 位置同步。

追加预算是同一文件系统的请求体预算；不覆盖全进程内存。以上是小额度、确定性故障测试，不是吞吐或 RSS 基准。

## 本次 MPU 失败与取消清理验证

- 两个 5 MiB 分片：第二个持续阻塞，首个返回错误；close 能收到原始错误并取消阻塞分片，两个全局上传额度全部归还，complete 请求数为 0，abort 为 1。
- close 等待 abort 时取消：后续 Drop 仍持有上传 ID，发起第二次清理；放行后清理完成，complete 请求数为 0。
- 首次 abort 注入失败：保留原始分片错误，Drop 再尝试一次 abort，未出现无限重试。
- MPU 完成请求的 ETag 包含 `&`、`<`、`>` 时正确转义，结果 XML 结构有效。
- 分片完成等待直接轮询有界任务集合，不为每次等待创建临时任务容器；成功路径仍按分片编号提交。

上述保证限于本地任务/运行时生命周期。已到达服务端的请求不能本地回滚，进程退出可能中断 Drop 清理；真实桶残留 MPU 回收仍待验证。

## 本次重试与批量删除验证

- 501/505/507 直接返回原 HTTP 错误；测试同时检查错误类型和服务端请求次数，避免额外重试变成连接错误而被漏检。
- 批量删除收到 501 后，下一次请求直接为单对象 DELETE；回退中的权限错误仍正常传播。
- MPU 初始化和 PFS rename 收到 503 时仅发送一次，保留服务端错误；Complete MPU 原有禁止重试策略保持不变。
- 批量删除拒绝空 body、截断 XML、错误根节点、无 Code 的 Error、带命名空间的 Error 和 AccessDenied；合法 Quiet 空结果 `<DeleteResult/>` 通过。
- Quiet 返回及根节点依据 [华为批量删除对象 API](https://support.huaweicloud.com/api-obs/obs_04_0086.html)。响应异常只代表结果未确认，不能据此断言远端未删除任何对象。

本轮未新增吞吐基准；减少 501 重试来自请求计数验证。429、Retry-After、随机退避、总期限与结果不确定时的恢复协议仍待实现。

## 本次 HTTP 尝试期限验证

- 虚拟时钟中占满唯一连接额度：排队在 100 ms 结束，错误明确为 connection queue timeout；等待者移除，原额度释放后可再次申请。
- 取消仍在排队的请求：等待者移除，不吞占随后归还的额度。
- 本地 HTTP 服务分别延迟 600 ms 返回响应头和响应体，在 1000 ms 尝试期限下返回 body timeout；随后同一连接池（容量为 1）的请求成功，证明超时释放额度。旧实现会因分阶段重新计时而成功接收慢响应。

该期限覆盖单次 HTTP 尝试，不包括重试退避，也不约束整个多请求文件操作；没有给出吞吐提升倍数。

## 本次统一重试策略验证

- 相同 Hadoop 参数分别驱动签名请求和底层 HTTP：配置 0 时只发送一次并返回 503；配置 3 时在三次 503 后第四次成功，服务端请求数分别为 1 和 4。
- 耗尽重试后保留最后一次服务端 503、错误 Code/Message 和 request ID；底层 HTTP 保留原响应 body。
- 通用配置与桶级覆盖分别验证；桶级 0 可关闭重试与退避，负值、溢出值和单位后缀不被截断转换，按现有配置解析约定回退默认值。
- 虚拟时钟验证初次请求无等待，退避基数 25 ms 的前两次重试累计等待 75 ms；非幂等操作仍禁用自动重试。
- 默认行为保持最多 3 次请求；签名路径移除隐藏的两次重试上限。底层 HTTP 不再为中间 5xx 构造随后丢弃的错误字符串。未测量吞吐收益。

## 本次跨重试总期限验证

- 虚拟时钟中，总期限在 20 ms 取消持续等待并释放所持额度；默认 0 关闭时，模拟 120 秒请求正常返回。
- 连接池已占满、单次期限 1000 ms、总期限 20 ms：在总期限到达时退出，排队等待者释放，未触发新的重试。
- 签名请求和底层 HTTP 均覆盖退避阶段：503 后计划等待 60 秒，但 300 ms 总期限中断等待，服务端只收到一次请求。
- 服务端已返回响应头但持续不发送 body：总期限取消读取后，容量为 1 的同一连接池可以成功执行下一次请求。
- 通用配置和桶级覆盖校验总期限参数，包括桶级 0 关闭。总期限位于重试循环外，默认不改变已有请求次数/耗尽重试错误行为。

总期限只约束一次 HTTP 请求及其重试，不包含整个 copy/delete、分页目录或文件读写操作；发生超时不能据此断言服务端未提交写入。没有新增真实云或吞吐验收。

## 本次限流与 Retry-After 验证

- 签名和底层 HTTP 两条路径分别注入 429/503 与 `Retry-After: 1`，等待至少 1 秒后第二次请求成功，服务端计数为 2。
- 秒数、标准 IMF-fixdate、过去日期、空值、负值、小数、整数溢出和非法内容分别验证；非法值回退本地退避。
- 虚拟时钟验证服务端 2 秒等待优先于本地 100 ms；服务端 10 ms 不缩短本地 200 ms。请求总期限可取消服务端要求的 60 秒等待。
- 两条 HTTP 路径在 429 后收到 60 秒等待要求，300 ms 总期限生效且服务端只收到一次请求。
- MPU 初始化及 PFS rename 收到 429 时直接返回原状态，不自动重放。

协议依据：[RFC 9110 Retry-After](https://www.rfc-editor.org/rfc/rfc9110.html#name-retry-after)、[RFC 6585 429](https://www.rfc-editor.org/rfc/rfc6585.html#section-4)。当前日期解析覆盖标准 IMF-fixdate，旧式日期格式尚未验证；绝对日期依赖本机时钟。未实现全局自适应限速或随机退避；未配置请求总期限时长等待不会被自动截短。

## 本次 seek 预读复用验证

- 4 字节块、两个预读额度：顺序读完 12 字节后，seek 到偏移 17；分别验证目标预读正在等待与已经完成两种状态，返回字节 r、位置为 18，目标范围 GET 总数为 1。
- seek 保留目标任务期间修改对象：返回 RemoteFileChanged，后续读取保持失败且位置为 17；不会绕过原有 ETag 检查，目标范围仍仅请求一次。
- seek 到 EOF 取消阻塞预读，随后读取返回 0；测试等待并成功取得全部共享额度，覆盖取消后的额度回收。
- 非目标任务继续取消，seek 后顺序检测重新开始；未增加缓存容量或新建后台任务。

性能证据限于上述访问轨迹的目标 GET 计数（复用后 1 次），未测量真实云吞吐、总下载字节或进程 RSS。

## 本次顺序访问检测验证

- 8 字节块、64 字节文件，逐字节读取前 23 字节：每一步都确认预读额度未占用，GET 数只随实际进入的块增加；旧实现第三次小 read 即会占用预读额度。
- 第 24 字节完成第三块后，阻塞的后续预读启动；放行后完整读取并逐字节核对，8 个块恰好 8 次 GET，结束时额度全部归还。
- 读完两块后 seek，重新读完两块仍不预读，第三块结束才恢复；丢弃流后全部额度可重新取得。
- 保留大 read 流水线与预读容量回归；调整旧版本检测用例，使其明确等待真实预读启动后再修改对象，避免测试退化成仅覆盖前台 GET。

本轮改变预读启动阈值的计数方式，不是可配置的 sequential/random/adaptive 策略。验证的是请求触发与数据正确性，尚无真实云吞吐或下载字节基准。

## 本次读取检查点恢复验证

- 4 字节块，偏移 8 的 GET 注入一次失败：首次调用已复制前面的块但返回错误，重试读取一块后确认全部预读额度仍空闲，不把失败调用中的块重复计入顺序进度。
- 分别覆盖此前没有成功读取，以及此前已成功读取一个块的状态；后续完整读取逐字节一致，额度全部归还。
- 偏移 8 的 GET 阻塞后取消调用，确认外部位置仍为 0；重读首块不误触发预读，放行后完整读取成功。
- 检查点统一记录位置、顺序块计数与窗口；失败时立即恢复，取消时在下一次 read/seek 恢复。未清空现有预读任务，原有取消后复用请求回归继续通过。

该修改修复本地状态恢复，不撤销已经发出的请求或已经复制到调用方缓冲区的字节；失败调用仍通过错误报告，调用方不能把这部分缓冲当作成功读取。

## 本次大文件整数边界验证

- 复制计数对 4 GiB、4 GiB + 7 和 u64::MAX 剩余长度返回真实缓冲允许的字节数，覆盖短尾、空缓冲和 EOF；先取最小值再转换，避免直接缩窄剩余长度。
- 以 8 字节数据模拟上述三个逻辑文件长度：两次范围 GET 返回完整数据，位置前进到 8，不发生零进度。
- 以 31 字节数据模拟从 4 GiB、u64::MAX - 31 开始的文件尾：开启预读，8 次 GET 返回 31 字节，最后 3 字节短块正常；到达 EOF 或 seek 到 u64::MAX 后返回 0。
- 测试存储仅把高逻辑偏移映射到小数组，不分配超大文件；测试在本机 Windows 64 位目标运行。修复依据整数转换边界，尚未实际编译或运行 32 位平台。

本轮不改变合法 64 位常规读取的请求策略，也没有吞吐提升声明。

## 本次 LIST 重复对象验证

- 同一页重复对象键，分别使用相同与冲突的 size/ETag：均明确返回 duplicate object keys 错误，服务端仅请求一页。
- 普通桶及 PFS 递归删除注入重复对象：操作返回错误，原对象集合完整保留，没有删除子对象。
- 目录 rename 第一页正常、后续页包含重复对象：收集列表时失败，copy/delete 次数为 0，原对象集合不变。
- 原有 CommonPrefixes 去重以及目录对象元数据保留测试继续通过；检查复用已有页内集合，没有增加随全部目录条目增长的额外键索引。

本轮仅检查同一页 Contents 的重复键，不声称解决跨页重复、并发目录变化或 rename 原子性；list_all 仍全量累积结果。

## 本次按需分页与流式容量统计验证

- 构造分页器不发请求；每次 next_page 只取一页，空的截断页不会误判 EOF；EOF 后重复调用不再请求。
- 协议验证失败后保持同一错误，不继续取页；原有越界键、同页重复、循环标记、缺失标记、目录元数据合并与破坏性操作防护回归继续运行。
- 第一页完成后取消第二页等待，再次取页仍使用相同 marker，成功返回第二页；取消期间不提交游标状态。
- 100 页、每页一个文件，get_content_summary 返回 100 个文件、2 个目录、700 字节，恰好 100 次 LIST。
- 跨页累计长度为 u64::MAX + 1 时返回明确错误，不发生整数回绕。

容量统计按页丢弃文件条目；仍保留不同目录名和分页标记历史。未测量 RSS 或百万文件延迟；Java RemoteIterator 已在下一轮接入，见下节；删除/重命名仍使用完整列表校验以保留已有失败边界。

## 复现

```powershell
.\scripts\build-windows.ps1
.\scripts\test-jni.ps1 -JavaHome YOUR_JDK17
mvn -f hadoop-adapter/pom.xml -Pecj test
```

## 尚未证明

未验证真实 OBS/PFS、华为定制 Hadoop、Spark/Hive/Flink 作业、STS 长任务刷新、崩溃后的 MPU 回收、全局字节预算和 release 吞吐/尾延迟/RSS。复制重命名仍不原子；源对象并发变化到删源之间的条件保护尚未完成。保留父目录 marker 会增加必要的元数据请求，不能声称所有操作都更快。JNI 分配减少来自实现结构与回归校验，本轮未给出端到端加速倍数。

## Hadoop 按需目录迭代验证

- listStatusIterator 创建时读取首个可用页面，以满足缺失路径立即抛错的 Hadoop 契约，后续页面按需读取；每页 3 项、10 文件的遍历恰好 4 次 LIST，页内重复 hasNext 不增加请求，普通桶/PFS 均验证，且逐条状态不发 HEAD。
- 相对路径在构造时固定，修改工作目录不会改变后续请求；支持文件单项、空目录、缺失路径、迭代结束和文件系统关闭后拒绝访问。
- 空的截断页继续请求；跨页相同路径只返回一次；非相邻 marker 循环在返回该页条目前失败。
- 后续页面截断 XML 的 IOException 成为终止错误，重复调用不会再次发请求；Rust 恢复游标也拒绝停滞 marker。
- 无状态 JNI listPage 不持有额外原生句柄，也不提前读取下一页。Java 仍保存路径去重集合和 marker 历史，空间随目录条目/页数增长；目录变更期间不是一致性快照。

首轮完整契约发现缺失路径异常延迟到 hasNext，已改为创建时取首个可用页面，并重新运行完整套件。

本轮日志：`work/hadoop-pager-rust.log`、`work/hadoop-pager-jni.log`、`work/hadoop-pager-hadoop.log`（工作区外层）。没有真实云吞吐或百万文件内存测量。

## Rust 列表状态与 JNI 生命周期收敛

上一节记录的是无状态 JNI 的历史版本。本轮改为 Rust StatusCursor 持有跨页去重与 marker 历史；Java 已移除相应集合和校验。保留原 listPage ABI，新增游标操作以及 nativeReleaseListing；新版 Java 必须与新版 native 库配套部署。

- 两项 Rust 用例直接验证跨页去重、EOF 后零额外请求、非相邻 marker 循环与终止错误保持。
- 原普通桶/PFS 分页请求数、空页、缺失路径立即报错、损坏 XML、循环、相对路径和关闭检查回归继续通过。
- Hadoop 新增提前关闭测试，关闭两次安全且不再发 LIST。
- JNI 专项验证 EOF/失败/显式释放后的游标不能再使用，跨文件系统访问被拒绝，文件系统关闭后的重复释放安全。
- PhantomReference 通过显式入队测试回收工作线程，避免依赖 System.gc()；队列消费后游标访问失败，并确认未发 LIST。没有测量真实 GC 回收时延。

当前内存仍与去重路径/marker 数量相关，原生资源的 GC 兜底释放非确定性。显式 Closeable、EOF/错误、所属文件系统关闭是确定释放路径；已经进入的请求不因移除注册表而承诺远端取消。没有新增真实云或性能基准结果。

本轮日志：`work/core-listing-rust.log`、`work/core-listing-jni.log`、`work/core-listing-hadoop.log`。

## 逻辑块大小配置接通

- Rust 配置测试覆盖 64M 通用值、32M 桶级值，以及零、负数、超过 i64/u64 的值和非法文本回退。
- Hadoop 普通桶与 PFS 验证默认块大小两种重载、根目录、写后缓存、重新 HEAD、分页列表一致；文件块位置仍覆盖完整文件，未引入物理块切分。
- 非法桶级值回退 Rust 默认 128 MiB；无桶级值使用通用 64 MiB。
- 首轮网络测试受沙箱回环连接限制失败，授权本地网络后重跑；最终结果以上方汇总为准。
- rustfmt 在当前沙箱报告 Access denied，未能完成自动格式化；Java 格式化完成，Rust 编译与测试完成。

日志：work/block-size-rust.log、work/block-size-jni.log、work/block-size-hadoop.log（外层工作区）。未新增真实服务或性能测量。

## 容量统计开关

- Rust 单测验证默认关闭，以及通用 true/false 被相反桶级值覆盖。
- Java/JNI/HTTP 用例在普通桶与 PFS 下分别执行开关两分支；8 字节、2 文件、3 目录得到相同结果。
- 开启时该三条目场景恰好一次无 delimiter 的 LIST，关闭时没有平铺 LIST，至少三次目录 LIST（包括目录探测）；测试不将目录探测次数等同目录数。
- 文件、空目录与缺失路径覆盖两种算法。已有 Hadoop 容量统计契约在恢复默认 false 后仍通过。
- 开关决定 Hadoop 层是否使用 inherited super 遍历；对象访问、签名、网络、缓存仍由 Rust 执行。直接 Rust API 不受此 Hadoop 策略开关约束。

本轮日志：work/summary-switch-rust.log、work/summary-switch-jni.log、work/summary-switch-hadoop.log（外层工作区）。无真实桶或性能测量；Hadoop 默认汇总路径不新增 Rust 快速路径的 checked-add 保证。

## 元数据替换保留与缓存失效

- 两个 HTTP 回归验证：权限更新保留 uid/gid、user、project 和文件类型位；数值 owner 更新保留原 mode 和无关字段；先 HEAD 后 PUT。HEAD 404 时不发 REPLACE。
- 一个 Rust 文件系统回归验证权限/所有者更新失败后，不继续命中旧状态缓存。
- 既有 PFS mutation 头测试增加 HEAD 响应，并继续核对各写操作的 x-obs-hdfs。
- Rust 完整回归、JNI 冒烟与 Hadoop 契约重跑；未新增 Java 名称型所有者支持，也未验收其 disguise 模式兼容。

本轮日志：work/metadata-preserve-rust.log、work/metadata-preserve-jni.log、work/metadata-preserve-hadoop.log（外层工作区）。失败缓存有新增直接用例；取消通过已有 MutationInvalidation RAII 路径处理，本轮未新增针对元数据取消的独立用例。HEAD+PUT 不是原子元数据修改，真实服务仍待验证。

## Hadoop 名称型所有者及 disguise 展示

- Rust 配置单测验证默认 none 和桶级 disguise 覆盖。
- 新增两项 Java/JNI/HTTP 集成测试：名字写入、单字段 null 保留、权限展示、缓存失效、HEAD/全量 LIST/分页 LIST 一致、目录带 slash 键、无关元数据及原文件内容保留。
- 默认 none 与普通桶 disguise 不发生元数据 PUT；两个 null、空名、CRLF 注入及 Unicode 明确拒绝，缺失路径报错，未知模式初始化失败。
- 本地模拟服务器增加真实元数据 REPLACE/HEAD 行为，metadata PUT 不再被当成文件内容覆盖。
- 本轮没有真实服务验证，也不证明全部 Hadoop 安全扩展兼容；具体限制见 README。此前“名称型未接通”的记录是历史轮次结果。

日志：work/named-owner-rust.log、work/named-owner-jni.log、work/named-owner-hadoop.log（外层工作区）。

## OBS 专用凭据路径

- 三项新增凭据用例通过 Hadoop CredentialProviderFactory SPI 加载测试专用内存提供器，使用虚构值；测试完成后移除实例。工厂和服务注册仅位于 src/test，不进入生产 JAR。
- 10 月 7 日首次 JCEKS 测试因缺少 Windows winutils 而失败（2 项错误）；10 月 10 日改为真实 SPI 的内存测试。JCEKS 文件权限及落盘兼容性未验收，不能据此声称通过。
- 验证 OBS 提供器优先、全局缺失项回退、重复路径去重后的解析、禁用明文回退、共享 Configuration 不变。
- 验证短别名、桶级覆盖、同级标准键优先和桶级凭据别名；不支持的提供器方案报 IOException，不静默读明文。
- 本轮仅改 Java 凭据配置接线，完整 Hadoop/JVM 回归重跑；Rust 141 项与独立 JNI 冒烟沿用上一轮结果，未重复运行。Hadoop 套件本身仍通过 JNI 调用现有 Rust 库。

日志：work/provider-path-hadoop.log。未引入 STS 自动刷新、生产密钥或真实云请求。

## Hadoop listFiles 递归分页

- 新增 Rust 用例验证目录专属页面过滤为空后仍返回 continuation，并可读取下一页文件；EOF 后不再发请求。
- 两项 Java/JNI/HTTP 用例覆盖普通桶/PFS 的 10 个不同子目录文件，以每页 3 项恰好 4 次平铺 LIST 获取，none 模式不额外 HEAD；检查块位置、提前关闭不再请求。
- 验证非递归只返回直接子文件，递归跳过目录 marker，单文件、空目录、缺失路径处理正确。
- 复用已有 Rust marker 循环检测、跨页去重和 JNI 生命周期；未新增专门针对递归模式的取消/损坏页注入，不把共享路径覆盖写成新增专项验证。
- Rust 142 项、独立 JNI 冒烟和全部 Hadoop/JVM 套件本轮重跑；无真实云或吞吐/RSS 测量。

日志：work/recursive-pager-rust.log、work/recursive-pager-jni.log、work/recursive-pager-hadoop.log（外层工作区）。

## Release 列表基线

- 本轮仅修改测试夹具、基准入口及文档，生产实现未改变。
- 新增 ignored benchmark_paginated_listing，独立 release 执行通过；30 个正式样本，另有预热。所有样本断言条目数与完整 LIST 次数正确。
- release filesystem 可执行文件的普通回归 73 项通过、2 项 ignored。完整 Rust 142 项和 Java 259 通过/12 跳过沿用上一轮，未重复运行；独立 JNI 冒烟本轮未重跑。
- 最终保留样本单独运行，没有并行测试进程；此前有短暂测试重叠的首轮数据已舍弃，不用于报告。
- 固定 page_size=1000、模拟 1 ms/page，记录 Windows 定时器限制；cache=0 的 1 万/10 万条目，以及 cache=4096、TTL=60s 的 1 万条目，冷缓存、每组 1 次预热 + 5 次正式测量。
- 输出 first_ms/total_ms、首批与完整请求数，不测 RSS、CPU 或网络。分页保留路径/marker 历史，不承诺恒定内存；无法据此作真实云吞吐结论。

样本与环境见 benchmarks/listing-20261010.json。日志：work/listing-baseline-release.log、work/listing-baseline-filesystem.log。基准源码的 LF 归一化 SHA256 与 Cargo.lock SHA256 随样本保存。

## 缓存惰性 LRU 淘汰索引

- 完整 Rust 回归 144 通过（36 核心单测、73 文件系统、35 HTTP wire），3 个手动基准默认 ignored。独立 JNI 冒烟通过；Hadoop/JVM 271 项，259 通过、12 跳过、0 失败/错误，本轮全部重跑。
- 新增 5000 步确定性操作序列，与简单 LRU 模型逐步对照缓存键集合，覆盖访问提升、已有键更新、失效和淘汰；每步检查索引数量上限。
- 新增 1000 次前缀失效后重建，检查残留索引有界与同名新代次不被旧记录误淘汰；既有 8 线程并发容量、TTL、祖先失效、零容量测试保留并通过。
- release 列表基准独立执行，30 个正式样本及预热；测量期间未运行其他测试。夹具和 Cargo.lock 与基线相同，新增记录缓存源码 LF SHA256；原始基线未覆盖。
- cache=4096、1 万条目完整耗时中位数：全量 1451.58 → 200.10 ms（-86.2%），分页 1527.21 → 152.85 ms（-90.0%），请求数保持 10 次。cache=0 对照组完整耗时变化约 -0.1% 至 +2.4%。
- 惰性索引最多两倍容量的记录；大量访问后的集中刷新和周期重建可能产生延迟，尚无 RSS、并发尾延迟或真实云测量。rustfmt 的沙箱执行限制沿用此前记录，本轮手动调整格式，未宣称 rustfmt 检查通过。

样本：benchmarks/listing-cache-20261010.json。日志：work/cache-eviction-rust.log、work/cache-eviction-jni.log、work/cache-eviction-hadoop.log、work/cache-eviction-release.log（外层工作区）。

## 全量失效回收与混合并发回归

- 完整 Rust 回归 146 通过（38 核心单测、73 文件系统、35 HTTP wire），3 个手动基准默认 ignored；独立 JNI 冒烟通过；Hadoop/JVM 271 项，259 通过、12 跳过、0 失败/错误。本轮全部重跑。
- 全量失效测试覆盖空前缀、单斜杠、多斜杠；先制造失效残留，清空后检查条目数、索引长度及堆容量均为零，随后同名重新写入、读取正常。
- 8 线程混合用例：4 个读线程共 64000 次读取，4 个写线程共 4000 轮已有键更新、新键准入及不同频率失效。每轮写入检查容量上限；线程结束后检查每个存活条目恰有一项同代次索引、索引总数有界，再连续插入两倍容量验证最新条目保留。
- 此并发用例额外独立执行 20 次通过；重复次数不计作新增测试项。有限压力覆盖不能证明所有线程交错、没有死锁或固定的响应时限。
- 全量清空释放的是索引路径和堆分配；DashMap 分片容量不保证归还。局部失效的惰性残留仍然有界。未重跑 release 性能基准，也未新增 RSS 或尾延迟测量。

日志：work/cache-clear-rust.log、work/cache-clear-jni.log、work/cache-clear-hadoop.log、work/cache-clear-stress.log（外层工作区）。

## Release 缓存并发延迟基线

- 本轮仅新增测试夹具、基准开关及文档，生产代码未变。release cache_latency 目标显式运行 2 项通过：1 个算法检查、1 个手动性能基准。
- 同一 release 目标默认运行验证 1 项通过、1 项 ignored，避免普通测试意外触发性能负载；同时指定两个基准开关会明确失败。
- 三种场景各预热 1 轮、测量 5 轮，共 15 轮正式结果、30 万次写入。运行时未并行启动其他测试，但没有控制系统后台任务或绑定 CPU。单轮较短，不是长时间压力试验。
- 保存每轮 P50/P95/P99/最大值、写线程总耗时、读次数及命中数；输入准备与线程创建在计时之外。分位数按最近秩计算；汇总值是五轮各分位数的中位数，非合并 P99。没有保存逐操作耗时数组。
- 单写线程/混合读写/混合并清空的写入 P99 中位数为 2.9/26.0/14.5 微秒；观察到最大写入耗时 8.5284 毫秒。混合清空时缓存占用下降；其耗时不应用来证明比不清空更快。清空每轮 9 次，不足以作稳定尾分位结论。
- 完整 Rust 146 项、JNI 冒烟及 Hadoop 259 通过/12 跳过沿用前一轮；本轮不重复运行。release 列表基准、真实服务、读取尾延迟和 RSS 均未测。

样本：benchmarks/cache-latency-20261010.json。日志：work/cache-latency-release.log、work/cache-latency-default.log（外层工作区）。

## 缓存集中刷新：堆顶原地更新

- 完整 Rust 147 通过（38 核心单测、1 分位算法、73 文件系统、35 wire），5 个手动基准 ignored；JNI 冒烟通过；Hadoop/JVM 259 通过、12 跳过、0 失败/错误，本轮全部重跑。
- 修改前后独立 release cache_latency 均 3 项通过（两个手动基准和一个分位算法检查）。原混合负载保留；新增全量访问后单次准入的 128/4096 容量场景，每组预热 1 轮、测 5 轮，每轮 50 次。计时外准备输入和读取全部存活条目，计时新键写入，并检查实际最旧键被淘汰、容量不变。
- 专项五轮 P50 中位数：128 项 12.5 → 8.0 微秒（-36%），4096 项 449.6 → 339.8 微秒（-24.4%）。每轮仅 50 次，所以专项 P99 等于最大值，不作为稳定尾分位证据。
- 普通单写中位数仍 2.0 微秒；混合场景仍出现 3.1235 毫秒写入。顺序执行修改前、修改后测量，未交错随机化，也未控制后台调度。不能声称全局长尾消除或将此前异常值归因于堆刷新。
- 保留 LRU 模型、失效代次、索引上限和并发截点检查；8 线程混合读/写/淘汰/清空用例额外运行 20 次通过，不计入测试总数。
- 原始列表与延迟基线未覆盖，未重跑列表基准；无真实云、RSS、读取尾延迟或调度剖析。格式按已有沙箱限制手动维护，未声明 rustfmt 验证通过。

样本：benchmarks/cache-refresh-20261010.json（包含同一夹具的修改前后样本与源码哈希）。日志：work/cache-refresh-before.log、work/cache-refresh-after.log、work/cache-refresh-rust.log、work/cache-refresh-jni.log、work/cache-refresh-hadoop.log、work/cache-refresh-stress.log（外层工作区）。

## 上传名额等待中的分片失败

- 修复前新增的 write/close 故障用例均按预期失败：本流第一分片失败后，其归还的名额被预先排队的其他等待者获得，write/close 未能处理失败，在虚拟 2 秒期限内超时。记录保留于 upload-wait-before.log。
- 修复后完整 Rust 150 项通过（38 核心单测、1 分位算法、76 文件系统、35 wire），5 个手动基准 ignored；JNI 冒烟通过；Hadoop/JVM 259 通过、12 跳过、0 失败/错误，本轮全部重跑。
- 两个修复验证用例持续持有全部全局名额，确认 write 或 close 仍返回 injected part failure；不执行 complete，执行一次 abort，未提交目标文件，名额最终全部归还。虚拟期限是故障测试约束，不是新增的产品总超时配置。
- 第三个用例在已有分片阻塞时取消后续写入的名额等待，确认流保持 cancelled 失败状态；Drop 仍拥有并取消该分片，随后执行 abort，队列与名额无残留。
- 既有成功多分片/短尾块内容校验、失败分片与卡住兄弟任务、关闭期间取消、abort 失败兜底等用例均通过。成功分片在名额等待期间收集，等待任务句柄保持流所有权；名额申请 future 不随完成事件重建。
- 本轮未测吞吐、RSS 或真实服务；整体操作期限、统一字节预算与崩溃后的远端清理仍未完成。

日志：work/upload-wait-before.log、work/upload-wait-rust.log、work/upload-wait-jni.log、work/upload-wait-hadoop.log（外层工作区）。

## PFS 追加失败、共享额度与取消恢复

- 修复前 3 项新故障测试均复现虚拟 2 秒超时：flush 等待卡住的首请求、并发数量限制等待、共享字节额度被另一流占用。原有 76 项文件系统用例通过，失败日志保存在 append-wait-before.log。
- 修复后完整 Rust 154 项通过（38 核心单测、1 分位算法、80 文件系统、35 wire），5 个手动基准 ignored；JNI 冒烟通过；Hadoop/JVM 259 通过、12 跳过、0 失败/错误，本轮全部重跑。
- 首请求阻塞、后请求失败时，write/flush 均返回原始 injected append failure；取消首请求并由 flush 等待退出，验证活跃追加字节归零，不再发送后续块，流保持失败状态。
- 共享预算用例使另一流优先取得失败任务归还的额度；原流仍应处理自身失败。关闭两流后，用新流在首请求尚未放行时取得完整 8 字节预算，并验证最终内容，检查取消后的额度可复用。
- 正常乱序用例让后一个追加先成功，flush 处理其结果后被主动取消；放行首请求并恢复 flush，再继续追加，最终内容和位置正确，活跃追加字节归零。
- 复用原有取消 write、取消 flush 恢复、任务完成触发缓存失效等回归。测试期限只用于检测挂起，不是新产品超时配置；任务取消不能撤销已经提交的远端 PFS 写入。
- 没有新增吞吐、RSS、真实云或统一字节预算验收。

日志：work/append-wait-before.log、work/append-wait-rust.log、work/append-wait-jni.log、work/append-wait-hadoop.log（外层工作区）。

## 读取响应合并容量边界

- 新增极大逻辑范围、两个单字节帧用例，修改前触发 capacity overflow panic；原有 38 项核心单测通过。失败日志保留在 read-allocation-before.log。
- 改为先检查平台缓冲区容量，再用 Vec::try_reserve_exact 预留合并缓冲区；容量不合法返回 InvalidArgument，分配器报告的预留失败映射为 Io。单帧仍直接返回已有 Bytes，第二个非空帧出现时才创建合并缓冲区。
- 完整 Rust 155 项通过（39 核心单测、1 分位算法、80 文件系统、35 wire），5 个手动基准 ignored；JNI 冒烟通过；Hadoop/JVM 259 通过、12 跳过、0 失败/错误，本轮全部重跑。
- 既有单帧指针复用、空帧、多帧顺序、短响应、超范围数据、完整数据后的传输错误，以及读取失败位置恢复等回归保持通过。
- 极大范围测试只构造两个单字节帧并拒绝不可表示容量，没有申请巨量实际内存；没有注入真实物理内存不足，没有 32 位目标运行或新增吞吐/RSS 测量。统一读写字节预算仍未完成。

日志：work/read-allocation-before.log、work/read-allocation-rust.log、work/read-allocation-jni.log、work/read-allocation-hadoop.log（外层工作区）。

## 信号量并发配置边界

- 旧实现两项新回归均触发 Tokio MAX_PERMITS panic：正式 from_config 初始化及直接不返回错误的构造器；保留 semaphore-limits-before.log。修复后四项配置逐项超限拒绝、四类直接构造器不 panic 均通过。
- 正式初始化在连接池/桶探测前校验四项信号量配置；等于上限合法，超过上限返回带配置键的 InvalidArgument。直接构造器保留原 API，并对传给信号量的名额做表示范围限制。预读零值仍为零，连接/上传沿用最小 1。
- 配置单测覆盖上限、零值和桶级覆盖。新增两项 Java 测试分别经过直接 Rust JSON 和 Hadoop 桶级覆盖入口，逐项检查 IllegalArgumentException 与配置键；不提供 endpoint，确认发生在传输初始化前。
- 完整 Rust 159 项通过（41 核心单测、1 分位算法、82 文件系统、35 wire），5 个手动性能基准 ignored；JNI 冒烟通过；Hadoop/JVM 261 通过、12 跳过、0 失败/错误。本轮均完整重跑，没有新增性能测量。
- 该边界是运行库表示上限，不是推荐配置或内存控制策略。尚未完成全部配置审计、统一读写字节预算、32 位构建/运行及真实云桶验证。

日志：work/semaphore-limits-before.log、work/semaphore-limits-rust.log、work/semaphore-limits-jni.log、work/semaphore-limits-hadoop.log（外层工作区）。

## 上传分片和阈值容量边界

- 新增小输入/极大配置回归，旧实现成功接受单字节写入而未拒绝非法容量；失败日志保留在 upload-capacity-before.log。没有申请巨量内存，也没有在本机复现 32 位截断。
- 统一检查两项上传容量的 usize 转换和 isize 上限，保留分片/阈值最小值；单测覆盖零值归一、恰好上限、上限加一及 u64::MAX。
- 直接构造流的错误持续生效：写入、空写、flush、close 及无写入 close 均返回配置错误，检查 MemoryStore 请求记录为空；正式初始化也在缺少 endpoint 的情况下先报告具体非法容量。
- Java 新增直接 Rust JSON 和 Hadoop 桶级覆盖两条入口的容量校验，检查 IllegalArgumentException 与配置键。
- 完整 Rust 161 项通过（42 核心单测、1 分位算法、83 文件系统、35 wire），5 个手动基准 ignored；JNI 冒烟通过；Hadoop/JVM 262 通过、12 跳过、0 失败/错误。未新增性能基准。
- 可表示容量不是实际内存预算或服务端分片上限；未覆盖所有缓冲分配失败、真实内存压力、32 位执行或真实云桶验收。

日志：work/upload-capacity-before.log、work/upload-capacity-rust.log、work/upload-capacity-jni.log、work/upload-capacity-hadoop.log（外层工作区）。

## 上传跨阈值前瞻

- 新增直接检查缓冲长度、容量和已接收字节的核心回归，使用非法 URI 在本地使 MPU 初始化失败；旧实现已经复制 64 字节（预期 0），修改后空缓冲与 17 字节已有前缀两种情况均不复制本次输入、不扩展缓冲，close 持续失败。测试不建立网络连接。
- 成功路径以 12 MiB 阈值和 5 MiB 分片，覆盖一次写入恰好跨阈值、已有 7 MiB 缓冲再写 5 MiB，以及额外 11 字节尾部。比较完整字节内容、最终长度、3 个分片长度和只初始化/完成一次；全局名额为 1，同时覆盖分片排队。
- 原有小于阈值但大于一个分片的单 PUT、上传取消、失败传播和短尾块回归继续通过。
- 完整 Rust 163 项通过（43 核心单测、1 分位算法、84 文件系统、35 wire），5 个手动基准 ignored；JNI 冒烟通过；Hadoop/JVM 262 通过、12 跳过、0 失败/错误。本轮完整重跑，未新增性能测量。
- 初始化请求在同一次 write 内提前；低于阈值的累积缓冲、分片共享底层分配、跨流总内存尚无统一预算，不将局部容量检查视为 RSS 或生产吞吐证明。

日志：work/upload-lookahead-before.log、work/upload-lookahead-rust.log、work/upload-lookahead-jni.log、work/upload-lookahead-hadoop.log（外层工作区）。

## 失败上传流保留期间的资源回收

- 旧实现新增/强化的两项文件系统回归均出现虚拟 2 秒超时：取消 write 后 flush 报错但任务未停止；分片失败被 flush 观察后另一个卡住分片未取消。失败日志 upload-release-before.log 保留。
- 新增失败流保留用例分别通过 write 与 flush 观察第一个分片失败，验证第二个卡住任务在 close 前退出、全局两个名额归还、后续 flush/close 仍返回原始分片错误、不 complete、最终 abort 一次。
- 强化取消排队用例，在 Drop 之前等待原任务退出并检查名额归还，随后检查 Drop 远端清理；句柄持续归属流。
- 原有跨阈值单测改为直接检查内部 write_inner 暂存阶段，继续验证初始化前不复制新输入；再经公开 flush 确认失败流缓冲长度和容量均为零。保留关闭失败断言。
- 完整 Rust 164 项通过（43 核心单测、1 分位算法、85 文件系统、35 wire），5 个手动基准 ignored；JNI 冒烟通过；Hadoop/JVM 262 通过、12 跳过、0 失败/错误。本轮完整重跑，未新增性能测量。
- 任务取消需要运行时调度才能释放任务体/名额；尚未观察的错误和未再次操作的取消写入不会自动触发本逻辑。远端清理仍由 close/Drop 处理，统一字节预算、RSS/吞吐和真实桶验收仍未完成。

日志：work/upload-release-before.log、work/upload-release-rust.log、work/upload-release-jni.log、work/upload-release-hadoop.log（外层工作区）。

## PFS 追加取消写入后的额度回收

- 新增虚拟时间用例：4 字节共享额度、4 字节块，提交 8 字节 write 后取消等待；下次空 write 检测取消并报错，必须取消原来的卡住任务。旧实现等待任务退出超时，日志 append-release-before.log 保留。
- 分别设置并发 1/2，覆盖等待并发名额和等待字节额度的两条路径。原流仍保持存活，检查任务请求体计数归零，并让另一流用同一文件系统的全部 4 字节额度追加，核对最终 seednext 内容。
- 原流后续 flush/close 持续报告最初取消错误，只发出一个追加请求，未提交被取消的尾部；既有分片失败传播、flush 排空、缓存失效与取消 flush 后恢复用例保持通过。
- 完整 Rust 165 项通过（43 核心单测、1 分位算法、86 文件系统、35 wire），5 个手动基准 ignored；JNI 冒烟通过；Hadoop/JVM 262 通过、12 跳过、0 失败/错误。本轮完整重跑，未新增性能基准。
- 取消任务需运行时调度后退出；本逻辑在后续操作检测错误时触发，不是主动后台监控，也不承诺 PFS 已提交数据回滚或整体 RSS 上限。

日志：work/append-release-before.log、work/append-release-rust.log、work/append-release-jni.log、work/append-release-hadoop.log（外层工作区）。

## 读取错误后的预读资源回收（2026-10-11）

- 新增虚拟时间回归：顺序读取三个 4 字节块后触发两个预读，其中偏移 12 首次失败、偏移 16 卡住。旧实现返回传输错误后，另一预读仍在运行，等待退出出现虚拟 2 秒超时；保留 read-release-before.log。
- 修改后失败 read 回退到位置 12，取消卡住预读；保留输入流并申请全部两个共享名额成功，证明无需销毁流就可回收额度。随后允许被取消范围重新获取，在同一流重试读取后续 20 字节，核对内容、最终位置及名额全部归还。
- 所有已返回错误都清理驻留块与预读；只有版本变化/缺少版本保持持续失败。保留此前主动取消后恢复、seek 目标复用、重试游标与预读进度回退测试。
- 完整 Rust 166 项通过（43 核心单测、1 分位算法、87 文件系统、35 wire），5 个手动基准 ignored；JNI 冒烟通过；Hadoop/JVM 262 通过、12 跳过、0 失败/错误。本轮完整重跑，未新增性能测量。
- 错误后重试可能重新发出 GET；主动取消 future 保留原有可恢复语义。取消执行需要调度，不代表所有内存立即归还操作系统，统一内存预算及真实云桶验证仍未完成。

日志：work/read-release-before.log、work/read-release-rust.log、work/read-release-jni.log、work/read-release-hadoop.log（外层工作区）。

## HTTP 响应帧逐步合并（2026-10-11）

- 先把旧 Limited::collect().to_bytes() 提取为收集函数，再加入所有者寿命回归；64 个帧按请求逐个生成，旧实现峰值超过 2，回归失败，日志 http-frames-before.log 保留。
- 新实现逐帧读取、可失败的几何容量预留、复制后释放原帧；同一回归检查 64 字节内容完整、原帧所有者全部释放、存活峰值不超过 2。该计数仅针对测试 Body 提供的原始帧，不代表整个 HTTP 栈或进程内存上限。
- 新增单帧指针复用及空帧/trailers 检查；新增超限 Io 与收到足量数据后的晚到 Transport 错误检查。既有 chunked 限额、HEAD 长度语义、错误响应限制、Content-Range、响应体超时与连接名额复用的 wire 用例保持通过。
- 完整 Rust 169 项通过（46 核心单测、1 分位算法、87 文件系统、35 wire），5 个手动基准 ignored；JNI 冒烟通过；Hadoop/JVM 262 通过、12 跳过、0 失败/错误。本轮完整重跑，未新增性能基准。
- 限制的是累计响应数据及合并时请求的容量；分配器实际占用、扩容中的旧缓冲、Hyper 内部缓冲和并发响应仍可能额外占用内存。几何扩容存在重新复制成本，尚未进行 RSS/CPU/吞吐对比或真实分配失败注入。

日志：work/http-frames-before.log、work/http-frames-rust.log、work/http-frames-jni.log、work/http-frames-hadoop.log（外层工作区）。

## HTTP 收集器性能与已知长度预留（2026-10-11）

- 新增默认 ignored 的 release 基准与 -BenchmarkHttp 开关；四组帧大小、预热一轮/正式五轮/每策略每轮 16 次，轮换策略次序。先测旧方式与逐步合并，发现中等分帧回退；加入有界长度预留后同条件比较三条路径，均核对全部内容。计时不含帧源构造、结果校验与结果释放，不测网络或 RSS。
- 已校验 Content-Length/Content-Range 作为可选预留提示，仅第二个非空帧才分配，最多 8 MiB 且不超过响应限额。未知长度继续逐步合并。新增提示值过小/过大不填充或截断内容的测试，单帧测试使用极大提示仍保持原指针。
- 原始 40 组逐轮样本及计算口径保存于 docs/benchmarks/http-collection-20261011.json。第二轮中 8 MiB/1 KiB 的旧方式、未知长度、已知长度每轮平均耗时中位数分别 2.446/4.104/2.298 ms；8 MiB/64 KiB 分别 2.569/5.249/2.475 ms。未知长度仍有明显复制成本，不能省略该限制或将结果外推真实网络吞吐。
- 完整 Rust 170 项通过（47 核心单测、1 分位算法、87 文件系统、35 wire），6 个手动基准 ignored；JNI 冒烟通过；Hadoop/JVM 262 通过、12 跳过、0 失败/错误。HTTP 基准独立执行通过。
- 预留可能在服务端早退时多分配，限制的是申请上限而非总 RSS；输入基准复用缓存数据，未隔离系统调度噪声、未测 CPU profiler 或真实服务。

日志：work/http-bench-release.log、work/http-bench-hint-release.log、work/http-hint-rust.log、work/http-hint-jni.log、work/http-hint-hadoop.log（外层工作区）。

## 状态缓存 TTL 时间边界（2026-10-11）

- 旧缓存用 Instant::now()+ttl 构造截止点；直接 StatusCache 接口传入 Duration::MAX 时，本机复现 overflow when adding duration to instant，日志 cache-ttl-before.log 保留。
- 改为保存写入时刻、按 elapsed 与对应 TTL 比较。命中按当前条目的正/负类型选择缓存级 TTL，不增加每条 Duration 字段；已有键的两条更新路径均刷新写入时刻。
- 两项新回归覆盖 Duration::MAX/零 TTL 的正负组合、同名正负替换、显式失效，以及用测试设置条目年龄验证正负过期差异和替换刷新。既有实际短 TTL 过期、LRU 模型、并发容量和清空失效回归保持通过。
- 完整 Rust 172 项通过（49 核心单测、1 分位算法、87 文件系统、35 wire），6 个手动基准 ignored；JNI 冒烟通过；Hadoop/JVM 262 通过、12 跳过、0 失败/错误。本轮完整重跑，没有新增性能测量。
- 复现针对直接 Rust Duration 接口；没有证明普通 Hadoop 毫秒配置在本机必然溢出。读取时的时长比较与类型选择未单独测量开销，不宣称吞吐改进或统一内存预算完成。

日志：work/cache-ttl-before.log、work/cache-ttl-rust.log、work/cache-ttl-jni.log、work/cache-ttl-hadoop.log（外层工作区）。

## 状态查询与缓存失效的并发保护（2026-10-11）

get_file_status 在远程查询前记录缓存失效代次，返回后只在代次未变时回填。代次检查与回填由读锁保护，单键、前缀和祖先失效持写锁，防止检查通过后又与失效交错。普通缓存命中不获取此锁。

这修复了删除已完成、先前暂停的 HEAD 才返回并重新缓存旧文件状态的问题。保护覆盖状态查询的普通 HEAD、目录标记 HEAD、隐式目录判断和不存在结果。已经开始的查询仍可能返回旧快照，但失效后的旧查询不能继续污染缓存。

当前采用全缓存代次：无关路径的失效也可能抑制正在进行的查询回填，增加后续缓存未命中；本轮未测量锁开销或吞吐。LIST 回填、创建完成回调和 mkdir 写回尚未纳入此保护，也不能检测其他客户端对云端的修改。

- 新文件系统回归用通知与信号量暂停已取得快照的 HEAD，完成删除后释放；旧实现随后仍返回缓存中的旧文件，status-fence-before.log 保留失败证据。修复后再次查询返回 NotFound。
- 新缓存单测覆盖单键、前缀、祖先和根失效，验证旧代次的正负结果均不回填，新代次可回填，无关既有缓存保留。
- 完整 Rust 174 项通过（50 核心单测、1 分位算法、88 文件系统、35 wire），6 个手动基准 ignored；JNI 冒烟通过；Hadoop/JVM 262 通过、12 跳过、0 失败/错误。未运行真实 OBS/PFS 服务或新增性能测量。

日志：work/status-fence-before.log、work/status-fence-rust.log、work/status-fence-jni.log、work/status-fence-hadoop.log（外层工作区）。

## LIST 回填的失效保护（2026-10-11）

在上一轮状态查询保护基础上，完整目录列表、分页目录/文件列表及递归文件列表在发起 LIST 前记录缓存失效代次；子项和父目录回填使用该代次。PFS disguise 模式下补充元数据的 HEAD 也沿用列表开始时的代次，不能在子项处理时重新取代次而放行旧 LIST 快照。

本地回归先复现删除完成后迟到的 LIST 又缓存旧文件，修复后通过。另测 PFS 权限 HEAD 已获取旧快照、删除完成后才返回的场景；两项测试各覆盖四种列表入口。正在运行的列表仍可能返回旧快照，后续独立分页也不提供跨页一致性快照。

本轮未修改 StatusCache 的公开直接写入接口；backfill_from_list 工具接口仍由调用者负责并发协调，ObsFileSystem 不调用它。创建完成回调和 mkdir 写回仍待处理。继续采用全缓存代次，无关失效可能抑制回填；没有本轮性能数据，不宣称锁开销或吞吐改进，也没有真实云服务验证。

- 完整 Rust 176 项通过（50 核心单测、1 分位算法、90 文件系统、35 wire），6 个手动基准 ignored。
- JNI 冒烟通过；Hadoop/JVM 262 通过、12 跳过、0 失败/错误。历史 release 基准本轮未重测。
- 日志：work/list-fence-before.log、work/list-fence-rust.log、work/list-fence-jni.log、work/list-fence-hadoop.log（外层工作区）。

## 创建和 mkdir 成功回填的并发保护（2026-10-11）

创建流在完成父目录处理后记录缓存代次，mkdir 在操作前记录代次。成功返回时，缓存用同一写锁检查代次、推进代次、清除目标及祖先，然后仅在原代次仍匹配时缓存返回状态。成功完成也会阻止更早的 HEAD/LIST 查询继续回填。失效和条件回填之间没有解锁窗口。

发生并发失效时，不使用迟到响应构造缓存，也不保留目标路径的既有缓存：远端实际提交顺序可能与响应顺序不同，后续查询重新确认当前状态。无竞争时仍直接缓存长度、权限和块大小。全局代次意味着无关路径修改也可能触发保守失效；没有本轮性能测量。

本轮仅处理成功返回的缓存写回；创建或 mkdir 在服务端已提交但客户端报错、取消等情况下的失效清理仍需补齐，不提供跨客户端强一致性或远端事务保证。

- 旧实现的创建和 mkdir 删除交错均已复现，失败日志 write-fence-before.log 保留。
- 新回归覆盖 OBS/PFS 两种模式：PUT 已提交、响应暂停期间删除或覆盖；mkdir 已提交、响应暂停期间删除；无竞争创建和 mkdir 的权限与缓存命中。缓存单测验证完成写入会阻止旧查询回填，冲突时清除目标和祖先状态。
- 完整 Rust 180 项通过（51 核心单测、1 分位算法、93 文件系统、35 wire），6 个手动基准 ignored；JNI 冒烟通过；Hadoop/JVM 262 通过、12 跳过、0 失败/错误。未测真实云服务，历史性能基准未重跑。
- 日志：work/write-fence-before.log、work/write-fence-rust.log、work/write-fence-jni.log、work/write-fence-hadoop.log（外层工作区）。

## 创建和 mkdir 的不确定提交清理（2026-10-11）

创建流的成功回调现在持有缓存失效守卫；关闭失败、关闭被取消或流被丢弃而未执行回调时，守卫随流释放，清除目标前缀与祖先状态。mkdir 在开始远端修改前持有同类守卫，错误、取消及 PFS 返回 false 的退出路径都会执行清理。成功条件回填后解除守卫，保留上一轮验证的正常缓存命中和权限。

本地模拟在保存对象后暂停响应，再注入传输错误或取消任务，验证缓存不会继续保留旧长度或不存在结果。创建测试包含普通 PUT 与分段上传完成、首次创建与覆盖、OBS/PFS 两种模式，共 16 种组合；mkdir 测试包含两种模式与错误/取消，共 4 种组合，并检查祖先负缓存清理。既有成功回填和并发删除/覆盖回归仍通过。

该清理是客户端退出时的本地缓存失效，不是服务端回滚或提交确认。如果远端直到取消清理之后才真正提交，后续查询仍可能受服务端可见性及缓存 TTL 影响。丢弃尚未发起远端写入的流也会保守失效，可能增加后续查询；本轮没有新增性能测量，未验证真实云服务。

- 旧实现创建和 mkdir 均复现服务端提交后客户端失败仍命中负缓存，失败日志 uncertain-write-before.log 保留。
- 完整 Rust 182 项通过（51 核心单测、1 分位算法、95 文件系统、35 wire），6 个手动基准 ignored；JNI 冒烟通过；Hadoop/JVM 262 通过、12 跳过、0 失败/错误。
- 日志：work/uncertain-write-before.log、work/uncertain-write-rust.log、work/uncertain-write-jni.log、work/uncertain-write-hadoop.log（外层工作区）。历史 release 基准本轮未重跑。

## truncate 的全部退出路径失效（2026-10-11）

PFS truncate 在发出远端请求前持有单键失效守卫。成功、错误或 future 被取消时均清除目标缓存，并通过已有失效代次阻止更早的状态查询重新回填旧长度。原实现仅在成功时失效，已复现服务端截断后返回错误仍读取原长度，以及更早 HEAD 迟到回填的问题。

截断不改变目录成员，因此这里采用借用缓存和键的专用守卫，只失效单键，不构造键列表、不扫描前缀，也不增加 HEAD。错误和取消仍保持原结果；缓存清理不代表远端回滚或确定的提交状态。

本地模拟截断现在实际缩短对象数据；新增回归覆盖截断到零/非零长度的成功、提交后错误、提交后取消，以及旧 HEAD 跨越失败 truncate 的回填。既有 PFS mkdir/truncate 零 HEAD 回归保留。没有真实云服务验证或本轮性能测量；取消清理之后远端才提交的可见性问题仍不由本地失效保证。

- 旧实现两项新增回归均失败，日志 truncate-exit-before.log 保留（旧长度 6，实际应为 0 或 2）。
- 完整 Rust 184 项通过（51 核心单测、1 分位算法、97 文件系统、35 wire），6 个手动基准 ignored；JNI 冒烟通过；Hadoop/JVM 262 通过、12 跳过、0 失败/错误。
- 日志：work/truncate-exit-before.log、work/truncate-exit-rust.log、work/truncate-exit-jni.log、work/truncate-exit-hadoop.log（外层工作区）。历史 release 基准本轮未重跑。

## PFS 追加提交后退出的回归补充（2026-10-11）

核对发现追加流已经在每个后台请求退出时执行缓存失效，包含错误和任务取消，因此本轮没有修改生产实现或添加重复清理机制。补充提交之后暂停响应的模拟能力，验证追加成功或提交后返回传输错误时，即使流仍存活且尚未 flush/close，状态查询也能读到新长度；错误随后仍由 flush 正确返回。

另一项回归先提交追加数据，再取消尚在等待响应的 flush future 并丢弃流，等待后台任务退出后验证状态长度。它覆盖“取消 flush 后丢弃流”，不把单独取消 flush 当作取消远端任务；现有可恢复 flush 语义保持不变。测试使用当前线程运行时与通知/信号量确定交错，不靠短时间 sleep 猜测任务完成。

这验证的是本地提交后退出的清理，不提供远端回滚、取消后的提交确认或跨客户端一致性。没有真实云服务或本轮性能测量。

- 完整 Rust 186 项通过（51 核心单测、1 分位算法、99 文件系统、35 wire），6 个手动基准 ignored；JNI 冒烟通过；Hadoop/JVM 262 通过、12 跳过、0 失败/错误。
- 第一轮测试进程在网络权限授予前启动，wire 测试失败后终止；append-exit-restricted.log 保留。最终结果来自权限授予后重新启动的完整回归。
- 日志：work/append-exit-rust.log、work/append-exit-jni.log、work/append-exit-hadoop.log（外层工作区）。历史 release 基准本轮未重跑。

## 内部缓存回调与用户回调隔离（2026-10-11）

Rust 调用方对 ObsFileSystem 返回的创建流设置 on_close、对追加流设置 on_flush，原先会替换文件系统安装的缓存清理回调。新回归复现创建完成后仍命中负缓存，以及追加完成后仍命中旧长度。

现在创建流用 crate 内部 on_commit 槽保存成功回填及退出失效守卫，公开 on_close 独立保存用户回调。追加流用内部 on_invalidate 槽保存缓存失效，后台任务、flush 和 Drop 都先执行内部回调，再执行用户回调。用户回调重复设置仍只保留最后一个；创建失败时不调用成功回调，内部守卫仍随流退出清理。

没有改变公开方法签名，未增加云请求。内部与用户回调独立存储增加了少量流/任务状态，未测量本轮性能开销，不宣称吞吐提升或远端强一致性。

- 新回归覆盖创建成功/提交后失败、追加任务结束但尚未 flush、重复替换用户回调不执行旧回调；原有取消、失败和成功缓存回归继续通过。
- 完整 Rust 188 项通过（51 核心单测、1 分位算法、101 文件系统、35 wire），6 个手动基准 ignored；JNI 冒烟通过；Hadoop/JVM 262 通过、12 跳过、0 失败/错误。
- 日志：work/callback-isolation-before.log、work/callback-isolation-rust.log、work/callback-isolation-jni.log、work/callback-isolation-hadoop.log（外层工作区）。未运行真实云桶或重测历史 release 基准。

## 追加回调前归还共享字节配额（2026-10-11）

已复现后台追加请求完成后，用户同步回调仍持有该请求的共享字节配额，使另一条流无法继续写入。调整任务局部守卫的声明顺序，利用逆序析构，在请求 future/请求体释放后先释放配额，再执行内部缓存失效和用户回调。内部回调仍先于用户回调，字节限额及回调调用语义不变。

新回归采用双工作线程运行时、4 字节共享预算，故意阻塞第一条流的回调，并验证第二条流可以在回调释放前写入并 flush。旧实现等待 3 秒超时，修复后完整通过；测试始终释放回调且回调本身设有超时上限。同步回调仍占用其所在运行时线程，本修复不把它转换成异步回调，也不保证任意阻塞回调不会耗尽所有线程。

这是资源释放顺序修复，没有新增请求、分配或后台任务，也未进行吞吐、延迟分位或真实云服务基准测量。

- 完整 Rust 189 项通过（51 核心单测、1 分位算法、102 文件系统、35 wire），6 个手动基准 ignored；JNI 冒烟通过；Hadoop/JVM 262 通过、12 跳过、0 失败/错误。
- 日志：work/append-callback-budget-before.log、work/append-callback-budget-rust.log、work/append-callback-budget-jni.log、work/append-callback-budget-hadoop.log（外层工作区）。历史 release 基准本轮未重跑。

## 空追加写入的后台错误传播（2026-10-11）

PfsAppendStream 的空 write 现在检查已完成的后台任务，及时返回已发生的失败。原实现的分块循环在空输入时不执行，已完成的失败会被隐藏到下一次非空写入或 flush；新增回归复现空写入错误地返回 Ok(0)。

空写入只回收已完成任务，不等待尚未完成的请求，也不提交新数据。检测到失败后沿用原有粘性错误及取消其他任务的机制，后续空写入、非空写入、flush、close 返回同一错误。非空写入路径没有增加新的任务扫描。

回归以通知和信号量固定提交后失败的交错，并直接 poll 验证后台请求未完成时空 write 立即成功，不退化成 flush。没有新增云请求或性能基准测量。

- 完整 Rust 190 项通过（51 核心单测、1 分位算法、103 文件系统、35 wire），6 个手动基准 ignored；JNI 冒烟通过；Hadoop/JVM 262 通过、12 跳过、0 失败/错误。
- 日志：work/append-empty-error-before.log、work/append-empty-error-rust.log、work/append-empty-error-jni.log、work/append-empty-error-hadoop.log（外层工作区）。未验证真实 OBS/PFS，历史 release 基准本轮未重跑。

## GET 前检查范围的内存表示边界（2026-10-11）

读取范围在发出 ObjectStore GET 前检查其实际长度是否可表示为 usize 且不超过 isize::MAX，避免已知无法收集的范围仍触发请求。检查使用裁剪至文件末尾后的范围长度；配置很大的 chunk 并不导致短文件或短尾块被误拒绝。响应收集器保留同一检查，复用统一辅助函数。

新回归配置 isize::MAX+1 的范围，在模拟 GET 设置故障，旧实现确实进入 GET 并返回传输错误；修复后返回 InvalidArgument、GET 次数为零、游标不变。同一 chunk 配置读取 4 字节文件仍成功。没有实际分配巨大缓冲区，未执行 32 位平台运行验证。

本轮只是请求前的确定性边界检查，不保证所有可表示范围都能成功分配内存，也不替代全局字节预算。没有真实云服务或性能基准测量。

- 完整 Rust 191 项通过（51 核心单测、1 分位算法、104 文件系统、35 wire），6 个手动基准 ignored；JNI 冒烟通过；Hadoop/JVM 262 通过、12 跳过、0 失败/错误。
- 日志：work/read-range-preflight-before.log、work/read-range-preflight-rust.log、work/read-range-preflight-jni.log、work/read-range-preflight-hadoop.log（外层工作区）。历史 release 基准本轮未重跑。

## 范围响应的有界初始预留（2026-10-11）

读取收集器原先在第二个非空块到达时尝试预留整个请求范围。新回归用两个一字节块和随后的传输错误复现：isize::MAX 范围先触发巨大分配失败，无法读到真正的传输错误。

现在只把至多 8 MiB 的预期长度用作初始预留提示；若已收到的数据更多，则按实际所需容量预留。之后按所需容量或几何增长扩容，请求容量不超过校验后的范围长度，每次扩容均使用可失败的分配接口。8 MiB 以内的分块范围仍可一次预留，单块响应继续复用 Bytes；不会为仅收到少量数据的巨大范围一次预留全部长度。

两项新回归覆盖巨大范围早期传输错误，以及 9 MiB 分块响应跨越初始预留边界后的长度和逐字节内容。既有零拷贝、短响应、额外数据及迟到错误检查保持通过。大于 8 MiB 的分块范围可能增加扩容复制次数，未测量吞吐、RSS 或延迟变化；此改动也不构成全局内存预算。

- 完整 Rust 193 项通过（53 核心单测、1 分位算法、104 文件系统、35 wire），6 个手动基准 ignored；JNI 冒烟通过；Hadoop/JVM 262 通过、12 跳过、0 失败/错误。
- 日志：work/range-reserve-before.log、work/range-reserve-rust.log、work/range-reserve-jni.log、work/range-reserve-hadoop.log（外层工作区）。未执行真实云验证或重跑历史 release 基准。

## 范围收集 release 基准与扩容调整（2026-10-11）

增加独立 -BenchmarkRange 开关和默认 ignored 的 benchmark_range_collection。基准使用预先分配且各自独立的缓存帧，计时包括收集及惰性 Bytes 克隆，不包括源分配、结果校验和结果释放；一轮预热、五轮正式测量，每轮每策略八次，轮换执行顺序。比较旧整段预留、上一轮两倍扩容和最终四倍扩容。参考实现仅用于测试；三次运行原始数据保存在 docs/benchmarks/range-collection-20261011.json。

最终同一轮实验中，32 MiB 的 4 KiB、64 KiB、1 MiB 分帧场景，四倍扩容比两倍扩容的五轮平均耗时中位数减少约 29%～32%，但仍比旧整段预留慢约 19%～28%。8 MiB/64 KiB 场景接近原水平；单帧仍走零拷贝，小于几微秒的差异不宜外推。生产实现保留 8 MiB 初始预留提示上限，把后续扩容改为四倍，减少大响应的扩容复制。

代价是大响应到达过程中可能提前预留更多容量；这不是全局内存预算。保留旧整段预留更快的事实，以及试运行间整体速度波动，不用跨运行绝对值推断收益。以上是本机缓存内存数据的收集耗时，不是网络吞吐、RSS、尾延迟或真实云任务收益。

最终五轮平均耗时的中位数（微秒，顺序：整段预留 / 两倍扩容 / 四倍扩容）：

- 8 MiB / 65536 字节帧：2292.10 / 2321.19 / 2346.36。
- 32 MiB / 4096 字节帧：9385.65 / 16461.74 / 11168.27。
- 32 MiB / 65536 字节帧：10693.62 / 19905.40 / 13724.41。
- 32 MiB / 1048576 字节帧：10528.29 / 18613.40 / 13235.91。
- 32 MiB / 33554432 字节帧：2.14 / 2.29 / 1.99。

完整 Rust 193 项通过（53 核心单测、1 分位算法、104 文件系统、35 wire），7 个手动基准 ignored；JNI 冒烟通过；Hadoop/JVM 262 通过、12 跳过、0 失败/错误。三次基准均单独运行，没有与回归测试并行。

日志：work/range-benchmark-release.log、work/range-benchmark-growth4.log、work/range-benchmark-final.log、work/range-benchmark-rust.log、work/range-benchmark-jni.log、work/range-benchmark-hadoop.log（外层工作区）。

## 响应前断连的非幂等重试验证（2026-10-11）

新增本机 TCP 回归：服务端完整接收请求头和 Content-Length 指定的请求体后关闭连接，不发送响应。该场景模拟客户端无法判断提交结果，与已有 429/503 状态码回归互补。

配置允许两次重试时，分段上传初始化、分段上传完成、PFS 重命名和标记为非幂等的原始 POST 均只收到一次请求；签名 GET 与原始幂等 GET 均收到三次请求（初始请求加两次重试），作为正向对照。测试还检查完成上传 XML 与原始 POST 正文确实已收到，避免把请求前断连误当成响应丢失。

现有重试策略及关闭底层隐式重试的配置通过验证，本轮不修改生产逻辑。回归不证明远端是否提交，也不提供 exactly-once 或自动恢复不确定上传的保证。未执行真实云验证或新增性能测量。

- 完整 Rust 194 项通过（53 核心单测、1 分位算法、104 文件系统、36 wire），7 个手动基准 ignored；JNI 冒烟通过；Hadoop/JVM 262 通过、12 跳过、0 失败/错误。
- 日志：work/disconnect-retry-rust.log、work/disconnect-retry-jni.log、work/disconnect-retry-hadoop.log（外层工作区）。

## 普通上传累计长度溢出预检（2026-10-11）

普通上传流在复制数据、改变写入状态或初始化分段上传前，对整次写入的累计长度执行 checked_add。超出 u64 范围返回 InvalidArgument，保留此前接受的数据；未越界的后续写入仍可继续。这与 PFS 追加流已有的偏移预检保持一致。

新增单测通过注入接近 u64::MAX 的内部计数覆盖缓冲 PUT 与跨阈值 MPU 两条路径，不分配巨大对象。修复前复现 debug 整数加法溢出 panic；修复后验证原缓冲内容、容量、计数和上传状态不变，并验证恰好达到上界、零长度写入和再次超限的行为。该测试验证整数边界，不意味着服务支持如此巨大的对象；没有新增性能测量或真实云验证。

- Rust 195 通过（54 核心单测、1 分位算法、104 文件系统、36 wire）、7 ignored；JNI 冒烟通过；Hadoop/JVM 262 通过、12 跳过、0 失败/错误。
- 日志：外层 work/upload-length-before.log、work/upload-length-rust.log、work/upload-length-jni.log、work/upload-length-hadoop.log。

## 列举游标终止后释放历史集合容量（2026-10-11）

StatusCursor 在正常结束或进入永久失败状态时，替换路径去重与续页标记历史集合，释放其桶分配。此前 clear() 仅清空记录，在调用方继续持有终止游标时仍保留容量。仍在列举中的游标继续保留必要历史，以维持跨页去重和标记循环检测；本改动不使活跃列举内存变成常量。

两项单测覆盖最终页跨页去重、请求失败以及标记循环失败。修复前，记录 4096 个路径后的正常结束和失败均残留 7168 槽位；修复后两个历史集合的 capacity 均为零。页面状态提交提取为同步私有方法，无新增 await、请求或后台任务，取消语义保持不变。验证的是容器释放分配，不是进程 RSS 或吞吐基准；未执行真实云验证。

- Rust 197 通过（56 核心单测、1 分位算法、104 文件系统、36 wire）、7 ignored；JNI 冒烟通过；Hadoop/JVM 262 通过、12 跳过、0 失败/错误。
- 日志：外层 work/cursor-release-before.log、work/cursor-release-rust.log、work/cursor-release-jni.log、work/cursor-release-hadoop.log。

## 底层分页器终止状态的历史释放（2026-10-11）

ListPager 在最终页通过校验或页面校验永久失败后释放 seen_markers 中的字符串及集合容量。网络/请求构造等 fetch 错误仍保留已提交的续页标记和历史，允许再次请求同一页并检测跨页循环；取消也不提交页面状态。永久失败保留原错误和最后已提交标记，后续调用不再请求服务端。

新增三项单测覆盖最终页交付与 EOF、越界 key/非相邻标记循环的永久失败、请求构造失败后的历史保留。256 页标记历史在修复前终止后仍保留 448 槽位，修复后容量为零；请求失败后历史不变，后续循环仍可检出。继续通过已有分页取消恢复与按需请求测试。此改动不限制活跃分页器的历史增长，未测量 RSS、吞吐或真实云行为。

- Rust 200 通过（59 核心单测、1 分位算法、104 文件系统、36 wire）、7 ignored；JNI 冒烟通过；Hadoop/JVM 262 通过、12 跳过、0 失败/错误。
- 日志：外层 work/pager-release-before.log、work/pager-release-rust.log、work/pager-release-jni.log、work/pager-release-hadoop.log。

## 目录修改前拒绝跨页重复对象（2026-10-11）

增加修改操作专用的列表收集器，在完整收集并校验所有分页后检查跨页重复对象。普通桶递归删除、PFS 递归删除与普通桶目录重命名使用该收集器；重复键返回 Io 错误，即使重复记录的元数据一致也拒绝执行。只读列表接口与 StatusCursor 的既有跨页去重行为保持不变。

两项回归覆盖相隔一页的重复对象，以及相同/冲突元数据；删除测试覆盖普通桶与 PFS。修复前两项失败，修复后确认对象集合未变、没有子对象删除、没有重命名复制或源删除。PFS 仍可能先发送原有的目录 DELETE 探测并收到 DirectoryNotEmpty，然后进入递归列表校验。

检查使用借用已有键的 HashSet，不复制键字符串；代价是修改操作额外 O(n) 临时索引与一次扫描。只在已收集完整列表的修改路径执行，未增加网络请求。该检查不提供并发修改隔离或云端事务保证；流式内容统计的跨页重复处理未在本轮改变。未测量吞吐/RSS，未执行真实云验证。

- Rust 202 通过（59 核心单测、1 分位算法、106 文件系统、36 wire）、7 ignored；JNI 冒烟通过；Hadoop/JVM 262 通过、12 跳过、0 失败/错误。
- 日志：外层 work/mutation-pages-before.log、work/mutation-pages-rust.log、work/mutation-pages-jni.log、work/mutation-pages-hadoop.log。

## 内容统计拒绝跨页重复对象（2026-10-11）

get_content_summary 增加已见对象键集合，遇到跨页重复对象返回 Io 错误，不返回重复累计的数量和容量。即使重复记录元数据相同也拒绝；对象类型变化同样检出。CommonPrefixes 是目录提示，仍允许跨页重复并通过目录集合合并。

两项回归覆盖普通桶/PFS、相同记录、大小冲突、目录变文件、重复后不请求下一页，以及重复公共前缀的正常统计；正常路径与异常路径均不增加 HEAD。修复前重复对象测试失败，修复后全部通过。

键从每页条目移入集合，不克隆键，也不保留每个对象的完整 LIST 元数据。不过内存现在额外保留所有已见对象键及哈希索引，随对象数量和键长度增长；这是精确检查任意跨页重复所付出的代价。仍逐页请求，失败/完成时局部集合销毁。本改动不提供目录并发变更期间的一致性快照，未进行吞吐/RSS 测量或真实云验证。

- Rust 204 通过（59 核心单测、1 分位算法、108 文件系统、36 wire）、7 ignored；JNI 冒烟通过；Hadoop/JVM 262 通过、12 跳过、0 失败/错误。
- 日志：外层 work/summary-pages-before.log、work/summary-pages-rust.log、work/summary-pages-jni.log、work/summary-pages-hadoop.log。

## 修改前重复键校验的容量预留基准（2026-10-11）

完整列表已收集后，重复对象键校验为临时 HashSet 一次预留 entries.len() 容量，避免逐步扩容反复重哈希。键仍借用现有字符串，检查结束释放索引；此优化只作用于目录修改前的校验，不改变内容统计的流式对象键集合。列表中若含公共前缀，预留量可能高于实际插入的对象数。

新增独立入口 scripts/build-windows.ps1 -BenchmarkListValidation（release、与其他基准互斥）。1 次热身、5 轮测量，每轮各策略 3 次，交替执行顺序；包含索引分配/释放，不包含输入生成、断言或网络。保留旧增长策略为对照，最终预留策略调用生产函数。

最终同次运行的轮均值中位数：1 万对象/25 字节键从 0.7022 ms 降到 0.2332 ms；1 万/249 字节从 2.0176 降到 0.7599 ms；10 万/25 字节从 8.7598 降到 4.9029 ms；10 万/249 字节从 33.9639 降到 13.1200 ms。四场景耗时下降约 44%–67%。首次候选测量与最终测量的原始 80 条记录及方法保存于 docs/benchmarks/list-validation-20261011.json。

这些数字仅代表本机内存中的键校验阶段，不是端到端 rename/delete、云端吞吐或 RSS 改善。校验仍需要 O(n) 临时索引；未进行真实云验证。

- Rust 204 通过、8 ignored；JNI 冒烟通过；Hadoop/JVM 262 通过、12 跳过、0 失败/错误。
- 日志：外层 work/list-validation-release.log、work/list-validation-release-final.log、work/list-validation-rust.log、work/list-validation-jni.log、work/list-validation-hadoop.log。

## LIST 对象键与续页标记的首尾空白保真（2026-10-11）

LIST XML 解析取消全局 trim_text，保留 Key、CommonPrefixes/Prefix、NextMarker 文本中的首尾空白，防止对象身份和分页位置被改写。大小、权限模式、数值元数据、布尔值及既有 ETag/时间字段单独去除格式化空白。XML 文档完整性校验保持不变。

新增解析回归覆盖前导空格、尾随空格、双侧空格、全空格键和 Unicode 名称，并验证带空格的数字和布尔值继续正确解析。修复前复现 leading 前空格被删除。新增本机 HTTP 两页回归确认键/前缀保留，并将包含首尾空格及 & 的续页标记原样百分号编码进入第二个请求。

本轮修改限定 LIST 文本首尾空白处理；未扩展其他 XML 解析路径，也未测量吞吐/RSS 或执行真实云验证。

- Rust 206 通过（60 核心单测、1 分位算法、108 文件系统、37 wire）、8 ignored；JNI 冒烟通过；Hadoop/JVM 262 通过、12 跳过、0 失败/错误。
- 日志：外层 work/list-whitespace-before.log、work/list-whitespace-rust.log、work/list-whitespace-jni.log、work/list-whitespace-hadoop.log。

## LIST 字段文本分段与 CDATA（2026-10-11）

LIST 解析在字段结束标签处统一处理已累积文本，避免注释将一个值拆成多个 Text 事件后逐段覆盖，也支持 CDATA。普通 Text 进行 XML 实体解码，CDATA 按字面追加，不对其中的 &amp; 二次解码。对象键、前缀和标记延续首尾空白保真；数字和布尔值在完整文本拼接后转换。

新增解析回归覆盖 Key、Prefix、NextMarker、Size、Mode、Uid、ETag 和 IsTruncated 的混合文本。修复前分段 IsTruncated 触发错误；修复后断言完整字段值。新增本机两页 HTTP 回归验证解析结果及包含字面 &amp; 的续页标记编码，确认第二页收到完整标记。

修改仅限 LIST 解析，没有扩展其他 XML 解析器。本轮未进行性能测量或真实云验证；字段文本仍受已有 HTTP 响应体限额约束。

- Rust 208 通过（61 核心单测、1 分位算法、108 文件系统、38 wire）、8 ignored；JNI 冒烟通过；Hadoop/JVM 262 通过、12 跳过、0 失败/错误。
- 日志：外层 work/list-text-before.log、work/list-text-rust.log、work/list-text-jni.log、work/list-text-hadoop.log。

## LIST 消费字段的歧义校验（2026-10-11）

LIST 解析使用三个固定 u16 位掩码分别跟踪根级分页字段、当前 Contents 对象字段及当前 CommonPrefixes/Prefix。重复的已消费字段返回 Io 错误，包含自闭合元素；拒绝已知标量中的嵌套元素和相互嵌套的对象/前缀容器，防止后值覆盖前值或丢失先前对象。

两项解析回归覆盖重复 Key/Size/Mode/ETag/IsTruncated/NextMarker/Prefix、自闭合重复字段、嵌套标量及容器；同时验证对象和前缀间位标记重置、普通 Owner 元数据仍可忽略。修复前两项均失败。新增 HTTP 200 回归覆盖重复键、冲突分页布尔值、嵌套键，均返回解析错误且只请求一次；既有注释/CDATA/空白保真回归继续通过。

位掩码不引入逐对象集合分配。此处校验限定解析器已消费字段及上述结构，不是完整 XML Schema 验证；未知元数据仍按既有行为忽略。本轮未测量性能或执行真实云验证。

- Rust 211 通过（63 核心单测、1 分位算法、108 文件系统、39 wire）、8 ignored；JNI 冒烟通过；Hadoop/JVM 262 通过、12 跳过、0 失败/错误。
- 日志：外层 work/list-fields-before.log、work/list-fields-rust.log、work/list-fields-jni.log、work/list-fields-hadoop.log。

## 分段上传初始化 ID 完整解析（2026-10-11）

InitiateMultipartUploadResult 中的 UploadId 改为累积完整 Text/CDATA，保留内部空白和 CDATA 字面实体文本，完整值组装后沿用首尾格式化空白修剪。只接受根的直接子元素中唯一的 UploadId；重复、自闭合重复、嵌套/错位、缺失或仅空白的 ID 返回 Io 错误，避免后续上传或取消使用截断 ID。

两项解析回归覆盖注释分段、混合/纯 CDATA 及上述异常结构，修复前均失败。新增本机 HTTP 初始化到取消流程确认 up&amp;load 作为完整 ID 被百分号编码进 DELETE uploadId 查询参数，而不是只发送第一个文本片段。

本轮限定上传初始化响应。完成上传/复制的 ETag 解析仍需继续检查，未宣称所有 XML 字段均已完成同类修复。本轮未测量性能或执行真实云验证；无法取得有效 ID 的不确定初始化仍不具备自动服务端清理保证。

- Rust 214 通过（65 核心单测、1 分位算法、108 文件系统、40 wire）、8 ignored；JNI 冒烟通过；Hadoop/JVM 262 通过、12 跳过、0 失败/错误。
- 日志：外层 work/upload-id-before.log、work/upload-id-rust.log、work/upload-id-jni.log、work/upload-id-hadoop.log。

## 完成上传与复制结果的 ETag 完整解析（2026-10-11）

完成上传和复制共用 parse_result_etag，分别传入 CompleteMultipartUploadResult / CopyObjectResult 作为预期根节点，先验证完整文档，再消费唯一的直接子字段 ETag。普通文本实体解码、注释分段及 CDATA 完整拼接；CDATA 字面 &amp; 不再被丢失或二次解码。完整值去除外部格式化空白及一对双引号，兼容无引号值。

重复 ETag（含自闭合重复）、嵌套/错位字段、空白值、不配对引号和不完整响应均返回解析错误。原 public parse_complete_mpu_etag 保留 Option<String> 签名，支持两种结果根并拒绝无效文档；存储层直接调用带预期根的 Result 版本，避免重复文档验证，并保留原有嵌入服务错误检查。

新增两项解析和两项 HTTP 回归，覆盖两种操作的完整字段拼接与异常 HTTP 200 响应；HTTP 异常用例均只请求一次。修复前复现 ab&amp;cd 被截成 ab 及重复 ETag 被接受。原 UploadId、LIST、截断响应与服务错误测试继续通过。本轮未测量性能或执行真实云验证；完成请求解析失败不代表服务端未提交。

- Rust 218 通过（67 核心单测、1 分位算法、108 文件系统、42 wire）、8 ignored；JNI 冒烟通过；Hadoop/JVM 262 通过、12 跳过、0 失败/错误。
- 日志：外层 work/result-etag-before.log、work/result-etag-rust.log、work/result-etag-jni.log、work/result-etag-hadoop.log；测试夹具初次编译错误单独保存在 work/result-etag-test-compile.log。

## 错误码和错误消息的完整 XML 文本（2026-10-11）

parse_error_xml 在 Code/Message 结束标签处提交完整值，拼接普通文本、注释两侧文本及 CDATA；普通实体解码，CDATA 保持字面内容。空字段不再吞掉兄弟字段，未闭合字段不提交部分错误码，出现嵌套元素时丢弃该字段及其内部候选 Code/Message。保留错误响应尽力解析和原 (String, String) API，不要求错误体满足成功文档验证。

两项解析回归覆盖分段文本、纯 CDATA、空字段、截断字段及嵌套内容。修复前复现 DirectoryNotEmpty 被截成 Directory、消息仅保留前段。两项 HTTP 回归确认 409 PFS 错误码完整保留，以及完成上传/复制的 HTTP 200 中 CDATA 错误码优先于看似有效的 ETag，均返回错误且只请求一次。

保留多个完整非空同名字段时后值覆盖的既有诊断策略；本轮不是完整错误 XML Schema 验证。未进行性能或真实云验证，也不将错误解析失败视为服务端未提交的证明。

- Rust 222 通过（69 核心单测、1 分位算法、108 文件系统、44 wire）、8 ignored；JNI 冒烟通过；Hadoop/JVM 262 通过、12 跳过、0 失败/错误。
- 日志：外层 work/error-text-before.log、work/error-text-rust.log、work/error-text-jni.log、work/error-text-hadoop.log；补充深层嵌套覆盖前的一次通过记录为 work/error-text-rust-initial.log。

## LIST 消费字段实际层级校验（2026-10-11）

LIST 解析在已有字段位掩码检查之外维护实际元素深度。Contents/CommonPrefixes 必须是根的直接子节点；当前对象/前缀中被消费的字段必须在深度 3，根分页字段必须在深度 2。自闭合元素按父深度加一检查，防止未知扩展容器内同名字段被误当作正常对象键、大小、前缀或续页状态。

新增解析回归覆盖扩展中的 Key/Size/IsTruncated/NextMarker/Prefix 及包裹的对象/前缀容器，修复前失败；正常 Owner、无关扩展和根级 Prefix 继续兼容。新增 HTTP 200 回归确认错位字段和包裹对象均返回 Io 错误，不暴露伪造列表条目且只请求一次。前期空白、注释/CDATA、重复字段测试继续通过。

检查只增加一个深度计数，不增加逐对象集合分配或请求；并非完整 XML Schema 验证。本轮未测量性能或执行真实云验证。

- Rust 224 通过（70 核心单测、1 分位算法、108 文件系统、45 wire）、8 ignored；JNI 冒烟通过；Hadoop/JVM 262 通过、12 跳过、0 失败/错误。
- 日志：外层 work/list-depth-before.log、work/list-depth-rust.log、work/list-depth-jni.log、work/list-depth-hadoop.log。

## Hadoop UGI 初始工作目录（2026-10-11）

初始化工作目录由操作系统 user.name 改为 Hadoop 当前 UGI 的短用户名，避免代理用户相对路径写入系统用户目录。保留 Hadoop 3.3.6 继承的 getHomeDirectory 行为：home 随当前调用用户变化，已经设置的工作目录不因 doAs 切换而重置。该边界与原 Java 适配器一致；Rust 存储核心不变。

真实 Hadoop → JNI → Rust → 本机模拟服务器回归覆盖普通桶/PFS、remote/proxy 用户、初始化 home/working directory、相对路径读写、setWorkingDirectory 后 home 保持不变，以及不同用户创建文件系统实例后的路径归属。修复前因初始目录错误失败，修复后 Hadoop 全套 275 项中 263 通过、12 按能力跳过、0 失败/错误。不是服务端用户授权、Kerberos 或真实作业验收；没有新增性能测量。

日志：外层 work/ugi-home-before.log、work/ugi-home-hadoop.log。Rust 最近回归为 224 通过、8 ignored（list-depth 轮），本轮未改动或重跑 Rust。

## 共享数据预算与显式上传 abort（2026-10-11）

新增 fs.obs.buffer.total.bytes / ObsConfig.buffer_total_bytes，默认 0 关闭，可按桶覆盖。同一文件系统的当前读块、预读、普通上传暂存与请求体、PFS append 请求体共用逻辑数据字节额度。先申请后复制/GET，Bytes 克隆及切片共享租约；预算不足时预读降级，前台返回错误，避免部分持有预算相互等待。写失败持续报错并释放暂存区、取消任务，读失败保持逻辑位置以便释放压力后重试。

新增 Rust ObsOutputStream::abort(self).await，显式丢弃小文件缓冲或取消并等待分片退出后 abort 已知 MPU，不发送 PUT/complete 或调用成功回调。清理失败返回原错误，取消/失败保留 Drop 尽力清理兜底。未知 upload ID、完成响应丢失和进程崩溃后的清理仍未解决。

本轮不把逻辑数据限额当成进程内存上限：分配器空闲容量、Bytes 底层共享分配、HTTP/TLS 暂存、响应合并和元数据没有统一计入。完整作用域、超限行为与本地验收见 BUFFER_BUDGET.md（README 对应 docs/BUFFER_BUDGET.md）。未测新的性能数字。

- Rust 231 通过（72 核心单测、1 分位算法、113 文件系统、45 wire），8 ignored；JNI 冒烟通过；Hadoop 276 项中 264 通过、12 跳过、0 失败/错误。
- 新增验证：Bytes 克隆/切片归还、溢出保护；跨读/写/append 额度争用与恢复；预读降级；分片失败且 abort 阻塞时额度释放、取消 close 的清理；显式 abort 丢弃 PUT、取消分片、清理失败；JVM 两桶模式桶级配置与失败/成功 PUT 后额度释放。
- 日志：外层 work/buffer-budget-rust.log、work/buffer-budget-jni.log、work/buffer-budget-hadoop.log。

## HTTP/分配容量预算与已知 MPU 退出恢复（2026-10-11）

预算扩展到受控 Vec 的 capacity、扩容新旧分配共存，以及 HTTP 源帧和合并目标。底层分配的完整租约跟随 Bytes 克隆/切片，HTTP 返回到输入流不重复计费。真实 OBS Store/Pool/流共享预算；未知外部帧容量、TLS 内部、元数据和分配器开销仍不在精确计费范围，不能称为完整 RSS 上限。预算默认仍关闭，行为与范围见 BUFFER_BUDGET.md（README 对应 docs/BUFFER_BUDGET.md）。

可选 fs.obs.upload.journal.dir 在取得 upload ID 后、发送分片前持久化记录。Rust recover_pending_uploads(limit) 按目标命名空间和记录身份清理，活动标记及 OS 文件锁防止清理运行中的上传；失败和损坏记录保留，完整 .pending 可恢复。子进程直接退出绕过 Drop 后的恢复已实测，日志失败阻止分片提交且已知 ID 仍可 abort。记录不包含凭据/文件内容，不是断点续传。初始化响应丢失、落盘前窗口、真实断电及服务端行为仍有限制，详见 UPLOAD_RECOVERY.md。

本轮没有新性能百分比或 RSS 数据。真实 OBS/PFS 与跨平台验收仍待独立环境。

- Rust 242 通过（77 核心、1 分位算法、118 文件系统、46 wire），8 ignored；JNI 冒烟通过；Hadoop 276 项中 264 通过、12 跳过、0 失败/错误。
- 新增证据包括受控容量/切片/扩容峰值、HTTP 多帧/取消/延迟错误、端到端不重复收费、真实子进程退出后的日志锁与恢复、发布前 .pending、活动上传/目标隔离、恢复限额、损坏/超长拒绝，以及持久化失败不发送分片。先前只验证逻辑预算的模拟用例明确选用单帧响应；多帧瞬时容量由新增独立用例覆盖。Hadoop 极小预算用例提高为 64 KiB，以容纳新增计费的元数据响应。
- 日志：外层 work/capacity-recovery-rust.log、work/capacity-recovery-jni.log、work/capacity-recovery-hadoop.log。

## 恢复扫描进度与取消边界（2026-10-11）

同一日志实例保存目录迭代进度，小批量恢复继续向后扫描，避免反复处理开头的损坏或活动
记录。目录耗尽后重新开始后续扫描；仅保留目录句柄，不全量收集文件名，网络等待不持有
扫描互斥锁。进度不跨进程保存，新增文件由后续扫描发现，不提供全目录原子快照。

新增 3 项集成回归：损坏首记录不阻挡后续正常记录；取消在途恢复后日志保留且可重试，
并发恢复跳过已认领记录；404 清理日志且重复执行不再发起请求。
完整 Rust 245 通过、8 ignored；JNI 冒烟通过；Hadoop 276 项、264 通过、12 跳过。
日志：work/recovery-cursor-rust.log、work/recovery-cursor-jni.log、work/recovery-cursor-hadoop.log。
未运行新增性能基准、真实 OBS 或实际断电测试。

## Hadoop/JNI 显式 MPU 恢复（2026-10-11）

OBSFileSystemRust.recoverPendingUploads(limit) 使用现有 JNI JSON 通道调用 Rust 恢复，
返回不可变 UploadRecoveryReport。Java/JNI 均验证限额，逐记录错误与初始化异常分开返回。
新增 4 项 JVM→JNI→Rust→本机 HTTP 回归，覆盖桶级配置、限额、403 保留后重试、404
清理且重复执行不发请求、不可变错误列表、损坏日志、锁冲突后重试、活动 Java 上传跳过、
完成后删除日志、非法类型/数值与关闭句柄。中文及保留字符的 key/upload ID 经真实 HTTP
编码后仍匹配原始记录。恢复失败用例使用手工构造的合法日志；Rust 子进程退出测试继续通过。

Rust 完整回归 245 通过、8 ignored；JNI 冒烟通过；Hadoop 280 项、268 通过、12 跳过，
零失败/错误。日志为 work/jni-recovery-rust.log、work/jni-recovery-jni.log、
work/jni-recovery-hadoop.log。未运行新增性能基准或真实云测试。Java 线程中断不保证取消
已进入 Rust 的调用；不自动运行恢复，流级主动 abort 的 Hadoop 扩展仍未接入。

## 恢复入口无效参数与并发关闭（2026-10-11）

Rust recover_pending_uploads 先校验 limit，再初始化日志；非法值返回 InvalidArgument，
不会创建命名空间或取得文件锁。新增回归覆盖 0、10001、usize::MAX，以及其他实例已
持锁时仍优先返回参数错误。

新增 Hadoop/JNI 并发回归通过 CountDownLatch 控制远端 abort 的响应时机：恢复已发出
请求时关闭原实例，后续调用被拒绝，另一实例暂时无法取得日志锁；原恢复返回成功并移除
日志后，另一实例可取得锁。此测试固定现有 Arc 生命周期语义，没有把 close 改成等待或
取消全部在途操作。

Rust 246 通过、8 ignored；JNI 冒烟通过；Hadoop 281 项、269 通过、12 跳过，零失败。
日志 work/recovery-lifecycle-rust.log、work/recovery-lifecycle-jni.log、
work/recovery-lifecycle-hadoop.log。未运行新增性能基准、真实云或断电测试。

## 日志删除失败的真实文件系统注入（2026-10-11）

新增 3 项 Windows 专用回归：以 OpenOptionsExt 的共享模式允许读取但禁止删除，实际
触发日志 remove_file 失败，不改 ACL、不替换日志内容。验证成功提交后的清理失败不会
改变 close 成功或重复调用提交回调；显式 abort 成功后保留无法删除的日志且不提交对象；
恢复遇到删除失败时 aborted 不增加、errors 报告失败、活动标记释放，解除占用后可重试。
完成后的对象内容在模拟后端恢复后保持不变，已有日志的 404 清理分支也覆盖。

本轮仅补测试和文档，未改生产逻辑。Rust 249 通过、8 ignored；JNI 冒烟通过；Hadoop
281 项、269 通过、12 跳过，零失败。日志 work/journal-delete-rust.log、
work/journal-delete-jni.log、work/journal-delete-hadoop.log。这 3 项测试以 cfg(windows)
限定，不将 Windows 共享模式的证据推广为 Unix 权限、真实磁盘故障或断电保证；远端行为
仍由本地模拟，不是实际 OBS 生命周期验收。未新增性能测量。

## 恢复清理严格区分 404 错误码（2026-10-11）

旧实现把所有 NotFound 当作上传已不存在。新增 Rust 回归先在旧代码复现：NoSuchBucket
使两条日志被移除且 aborted=2。修复仅接受明确的 404 NoSuchUpload；NoSuchBucket、
NoSuchKey、无错误码 404，以及 403 NoSuchUpload 都保留记录并报告失败。已有模拟后端
的 abort_missing 改为准确返回 NoSuchUpload，避免用 NoSuchKey 冒充上传不存在。

新增 Rust 分类回归和 JVM→JNI→Rust→HTTP 分类回归，后者验证多种错误保留同一日志，
最终返回合法 404 NoSuchUpload 才删除。Rust 250 通过、8 ignored；JNI 冒烟通过；
Hadoop 282 项、270 通过、12 跳过，零失败。旧失败证据 work/recovery-errors-before.log；
完整日志 work/recovery-errors-rust.log、work/recovery-errors-jni.log、
work/recovery-errors-hadoop.log。此修复只收紧日志恢复的成功判定，不改普通对象 404
分类、流 abort 或重试策略。未运行新增性能基准、真实云和断电测试。

## 上传清理与日志恢复的幂等判定统一（2026-10-11）

发现恢复入口已接受明确的 404 NoSuchUpload，但输出流的显式 abort、失败 close 清理和
Drop 清理仍将它视作失败。新增测试先在旧代码复现三项失败：abort 报错、close 后残留
日志、Drop 后日志未移除。现在四条路径共用 abort_known_upload；只有成功响应或明确
404 NoSuchUpload 归一为成功，其他错误继续传播/保留恢复日志，已有重试策略不变。

新增 4 项 Rust 回归覆盖上述三条路径及 NoSuchBucket 在所有清理路径保留日志；失败
close 仍报告原始分片错误、不触发成功回调、不重复提交。新增 JVM→JNI→Rust→HTTP
回归模拟分片 403 后 abort 返回 404 NoSuchUpload：Java close 返回原始 AccessDenied，
本地日志移除且只发生一次 abort，无需后续恢复。

Rust 254 通过、8 ignored；JNI 冒烟通过；Hadoop 283 项、271 通过、12 跳过，零失败。
旧失败证据 work/abort-idempotency-before.log；完整日志 work/abort-idempotency-rust.log、
work/abort-idempotency-jni.log、work/abort-idempotency-hadoop.log。未新增吞吐/RSS 测量；
真实 OBS 生命周期、实际断电、取得 upload ID 前的不确定窗口仍待验证。

## Hadoop 创建流主动放弃（2026-10-11）

新增 OBSFileSystemRust.abortUpload(output) 和 JNI nativeAbortStream，调用 Rust 现有
ObsOutputStream.abort。Java 检查流归属及追加类型，JNI 再检查流种类；拒绝输入/追加流
时保留原句柄。接受创建流后先消耗句柄再清理，清理报错也不能恢复写入或通过 close 提交。
Java 流保持原有同步访问机制，重复 abort/close 不重复提交；配套 JAR/原生库须同时更新。

新增 4 项 Hadoop 集成回归：缓冲 PUT 与 createNonRecursive MPU 放弃、外来流拒绝后
仍可使用、abort 403 关闭流并保留日志且随后能恢复、PFS 追加拒绝后仍能写入并关闭。
JNI 冒烟增加输入句柄拒绝后仍可读的检查。Rust 254 通过、8 ignored；JNI 冒烟通过；
Hadoop 287 项、275 通过、12 跳过，零失败。日志 work/hadoop-abort-rust.log、
work/hadoop-abort-jni.log、work/hadoop-abort-hadoop.log。

没有新增回滚能力，已完成对象和已提交追加字节不能用 abort 撤销；Java 业务异常不会
自动选择 abort。未运行新增性能基准、真实 OBS 或断电测试。

## Hadoop 主动放弃的关闭生命周期（2026-10-11）

旧实现在文件系统已关闭后放弃仍持有 Java 句柄的上传流，会把 JNI 无效句柄错误泄露为
IllegalArgumentException。新增回归在旧实现复现，Hadoop 层现在把该异常转为带原始
cause 的 IOException；句柄继续保持已消耗，后续写入失败，重复 abort/close 不提交。
其他实例的流仍在 Java 归属检查阶段报告 IllegalArgumentException，不受本转换影响。

新增三项 JVM 回归：关闭文件系统后放弃；已经成功提交后再放弃不发 abort、不重复提交；
用 CountDownLatch 控制在途 abort，关闭文件系统不会提前释放日志锁，清理完成后其他
实例能取得锁。完整 Hadoop 290 项、278 通过、12 跳过，零失败。旧失败证据
work/abort-lifecycle-before.log；修复后日志 work/abort-lifecycle-hadoop.log。
本轮只改 Java 适配层、测试和文档，未重跑未变化的 Rust/JNI 套件，沿用上一轮 Rust
254 通过、8 ignored 及 JNI 冒烟通过的结果。真实 OBS、断电和性能基准未新增验证。

## 日志初始化与恢复读取移出异步工作线程（2026-10-11）

日志初始化/目录创建/取锁使用 spawn_blocking，异步互斥锁串行化同实例首次初始化，成功
结果缓存，失败不缓存。恢复的目录枚举、文件类型检查、限长读取、解析与身份校验也由
阻塞工作线程逐条执行；不预读整批文件或全量收集目录。活动标记随读取结果的守卫一起
返回，取消接收后守卫最终释放，文件保留且读取工作线程不执行远端 abort。

新增核心测试占用唯一阻塞线程，验证当前线程运行时的计时器仍可执行并取消排队读取；
后台任务完成后活动标记为空、日志仍存在。新增并发首次 create 测试确认同实例初始化
共享日志锁，两个活动上传都被恢复跳过，随后可正常 abort 清理。既有进程退出、取消、
故障重试及 Hadoop 锁生命周期回归继续通过。

Rust 256 通过、8 ignored；JNI 冒烟通过；Hadoop 290 项、278 通过、12 跳过，零失败。
日志 work/journal-worker-rust.log、work/journal-worker-jni.log、work/journal-worker-hadoop.log。
记录删除/资源 Drop 仍可能执行短同步系统调用。取消不能强制停止阻塞文件操作，取消
初始化后锁可能短暂继续占用；未承诺完全无阻塞、吞吐提升或磁盘故障超时上界。没有新增
性能、真实云或断电测试。

## 日志删除移出异步工作线程并保留取消保护（2026-10-11）

JournalEntry.remove 消耗守卫，将文件删除和活动标记/命名空间锁所有权一起交给
spawn_blocking。显式 abort、正常/失败 close、Drop 后台清理和恢复入口全部等待这一
删除路径。调用方取消等待不会过早释放保护，已排队/开始的删除仍可完成。close/abort
的删除失败继续只告警并保留日志；恢复报告继续把本地删除失败计入 errors。

新增单线程运行时测试占用唯一阻塞工作线程，取消排队删除后丢弃最后一个调用方日志
引用，确认记录仍存在、命名空间锁仍被占用；放行后台任务并等待结束后，记录删除、
守卫释放且其他实例可取锁。既有 Windows 删除失败注入、远端错误、取消与 Hadoop
关闭并发测试继续通过。成功提交后的等待被取消不代表远端未提交，不支持自动重放。

Rust 257 通过、8 ignored；JNI 冒烟通过；Hadoop 290 项、278 通过、12 跳过，零失败。
日志 work/journal-delete-worker-rust.log、work/journal-delete-worker-jni.log、
work/journal-delete-worker-hadoop.log。资源 Drop 仍可能有短同步系统调用；未声明完全
无阻塞或吞吐提升，未新增真实云、断电或性能基准验证。

## 日志初始化结果发布不随等待者取消而丢失（2026-10-11）

初始化使用 Arc<tokio::sync::Mutex<...>> 的 owned guard，守卫转交阻塞工作线程，取锁
成功后由工作线程发布缓存。取消等待不再释放初始化互斥保护并丢弃成功结果，避免同实例
另一次初始化与未结束的后台 open 竞争。成功结果持锁到文件系统及相关引用释放；失败
不缓存。调用方取消不能中断正在执行的文件操作，后续同实例调用会等待它结束。

新增两个单线程/受限阻塞线程回归：取消排队初始化后，成功结果仍属于原实例，其他实例
不能取锁，原实例复用结果且释放后其他实例能取锁；初始化因其他实例持锁失败且等待者
已取消时，释放冲突后仍能重试。第一项在旧实现确认失败。两项均确认不发送远端 abort。

Rust 259 通过、8 ignored；JNI 冒烟通过。Hadoop 首次 290 项中 277 通过、12 跳过、
1 错误：testRenameFileBeingAppended 在 PFS 模式出现 NoSuchKey。追加任务仍使用旧路径，
与 rename 的请求顺序有竞争；本轮没有修改相关实现或跳过测试。完整重跑 278 通过、
12 跳过不能消除该已观察到的缺口，后续必须单独修复并加确定性回归。
旧失败证据 work/journal-init-before.log；完整日志 work/journal-init-rust.log、
work/journal-init-jni.log、work/journal-init-hadoop.log（保留失败）、
work/journal-init-hadoop-recheck.log（完整重跑）。未新增真实云、断电或性能测量。

## PFS 追加与重命名协调（2026-10-11）

同一 ObsFileSystem 实例注册活动追加流；追加任务在排队前取得 namespace 读锁，持有到
请求结束，rename 取得写锁后再发送服务端请求。成功后更新源流的路径，目录移动同时
更新子路径；被覆盖目标的旧流失效。回收站移动复用同一入口。正常追加仍可并行。
重命名取消或不确定失败会使相关旧流失效，避免继续向可能已替换的路径写入；明确的
NotFound/FileAlreadyExists 拒绝不改路径。缓存失效回调跟随最新路径。

新增 6 项 Rust 回归覆盖阻塞追加/重命名排序、后续追加、覆盖目标保护、服务端提交后
取消等待、目录后代与相似前缀、失败/取消和弱引用回收；新增 Hadoop/JNI/HTTP 回归
验证打开的追加流跟随文件及父目录的连续移动。官方追加契约未修改或新增跳过。
完整 Rust 265 通过、8 ignored；JNI 冒烟通过；Hadoop 291 项，279 通过、12 跳过。
日志：work/append-rename-rust.log、work/append-rename-jni.log、work/append-rename-hadoop.log。

范围与代价：协调仅覆盖同一实例创建的追加流及其 rename/回收站移动，不覆盖其他实例、
其他进程、直接构造的 PfsAppendStream 或带外变更。当前实例级锁会让 rename 等待无关
文件的在途追加，也会在 rename 期间阻止新追加请求调度；未测新的吞吐或尾延迟，不声称
性能提升。请求取消不等于服务端撤销；未知结果仍须重新核实并打开路径。真实 PFS、
跨进程 inode 语义和 Linux 尚未验证。历史失败日志 journal-init-hadoop.log 继续保留。

使用原文件系统实现运行相同的阻塞追加回归，rename 提前完成，等待断言确定性失败，
证据保存在 work/append-rename-before.log。随后恢复修复版源码并校验 SHA256 相同。

## 追加/重命名按路径范围协调（2026-10-11）

替换上一节的实例级异步读写锁：短临界区维护活动追加计数和待处理 rename 范围，
不跨 await/网络请求持互斥锁。文件范围仅匹配自身；目录范围匹配自身及斜杠边界后的
子路径。rename 先保留源、目标范围，再等待范围内已有追加完成；新追加和流注册
仅等待与自身路径重叠的范围。重叠 rename 按登记顺序推进，不相交 rename 可并行。
同路径不同流的请求均纳入等待，请求许可保留目标身份到任务结束。

等待期间取消 rename 只释放范围，既有流仍可用；请求已开始后的取消保持原有的
不确定结果保护。成功、明确拒绝、失败、取消均释放范围并唤醒等待者。追加完成时
仅在存在 rename 保留范围时通知等待者；锁顺序统一为协调状态、流目标状态。

新增 5 项 Rust 回归，验证阻塞追加不阻塞无关 rename、慢 rename 不阻塞无关追加/
注册/rename、相似前缀隔离、重叠目录范围排序、等待取消、目标请求排空以及流丢弃后
许可仍有效。旧实例锁实现强制重新编译后，确定性无关 rename 测试失败；修复版本通过。
旧对照首次因复制保留时间戳复用了历史缓存，已标为无效证据单独保留，并更新对照脚本
在替换/恢复后更新时间戳。有效对照日志 work/append-scope-before.log。

完整 Rust 270 通过、8 ignored；JNI 冒烟通过；Hadoop 291 项，279 通过、12 按既有
能力跳过，零失败。日志 work/append-scope-rust.log、work/append-scope-jni.log、
work/append-scope-hadoop.log。仍只协调同实例操作；共享字节预算/连接池仍可施加资源
背压。保留一个短时元数据互斥区及线性范围扫描，尚未测大规模流数下吞吐、尾延迟或
真实 PFS，因此不声称吞吐提升百分比或消除全部等待。

## 追加/重命名性能基准与注册表开销（2026-10-11）

增加两项显式 ignored 的 release 基准及 -BenchmarkAppend 开关：协调器和完整
Rust 文件系统/流 + MemoryStore。1 轮预热、3 轮测量、场景顺序轮转；覆盖 32、1,024、
8,192 流，4/64 追加任务，同文件和目录移动，并保留吞吐、p50/p95/p99/max。
完整流基准始终 4 个追加任务，每次 4 KiB write + flush，最终验证字节和文件名。
原始 72 行记录及源码/环境散列见 docs/benchmarks/append-rename-20261011.json；
方法、负载限制、退化结果和剩余瓶颈见 APPEND_PERFORMANCE.md。

测量后将注册弱引用清理改为几何阈值触发，消除连续注册每次扫描整个表的成本；注册表
缓存路径，在重命名范围筛选后才锁相关目标。新回归覆盖高频注册/释放后的过期条目
回收、存活流保留及连续 retarget。元数据有额外路径存储，清理仍是惰性，未测 RSS。

8,192 不同文件的四任务协调器：三轮中位注册总耗时 75.49 → 8.07 ms，追加吞吐
17,103 → 106,002 ops/s，rename p99 2.305 → 0.376 ms。完整内存存储路径吞吐
11.14 → 17.71 MiB/s，但首批 rename p99 退化且重跑波动很大；同文件/目录场景并未
普遍改善。连续 rename 下仍观察到数百毫秒追加最大延迟，不能只凭较低 p99 判定完成。
本轮没有真实云、HTTP 数据路径或 Linux 性能测量，没有有界公平性保证。

Rust 271 通过、10 ignored；JNI 冒烟通过；Hadoop 291 项，279 通过、12 跳过，零失败。日志 work/append-bench-rust.log、
work/append-bench-jni.log、work/append-bench-hadoop.log。release 日志
work/append-bench-before.log、work/append-bench-after.log、work/append-bench-64-workers.log。

## 连续 rename 下追加公平性（2026-10-11）

受阻追加、注册和 rename 共享有序冲突队列；后来 rename 不能越过早到追加，早到
rename 也优先于后来追加。等待项跟随流的当前路径；多个兼容追加仍可并行，无关路径
不被队列前方的不相交操作阻塞。RAII 在成功、取消和流失效错误时清理等待项；追加
成功准入后由在途许可完成时通知，避免重复全体唤醒。不提供固定墙钟等待上限。

新增 6 项协调器及 1 项完整文件系统回归。前三项在旧实现确定性失败，日志
work/append-fair-before.log；验证后来 rename 被阻止、早到 rename 优先、追加取消、
目录多个追加排空、注册/取消以及失效错误退出；文件系统回归检查发送顺序和最终内容。
完整 Rust 278 通过、10 ignored；JNI 冒烟通过；Hadoop 279 通过、12 跳过，零失败。日志 work/append-fair-rust.log、
work/append-fair-jni.log、work/append-fair-hadoop.log。

重跑原有 4/64 任务 release 基准，仍 1 轮预热、3 轮测量，保留每组全部数据。
64 任务同文件/目录场景的每轮最大追加延迟中位，从 332.56/379.61 ms 降到
17.21/15.17 ms，但热点吞吐和 p99 存在代价。不能据此宣称所有指标提速或等待上限。
前一轮作为同机历史基线，不是随机交替配对 A/B。报告 APPEND_FAIRNESS.md，
原始新旧 96 行数据 docs/benchmarks/append-fairness-20261011.json；日志
work/append-fair-bench-4.log、work/append-fair-bench-64.log。未做真实云/Linux/RSS 测量。


## 公平队列定向通知（2026-10-11）

移除协调器全局 notify_waiters，追加/注册仅等待具体前序 rename，rename 等待具体
前序冲突项或相关流的最后一个在途许可。存在前序冲突时不再扫描目标注册表；许可退出
不获取协调器锁。受阻追加的完成通知随许可延续到请求结束，准入时不提前唤醒 rename。
FIFO 冲突顺序、路径随 rename 更新、取消移除队列以及结果不确定的失效保护保持不变。

新增 6 项确定性测试，使用计数 Waker 而非睡眠门限：无关追加完成不唤醒受阻追加；
无关流排空不唤醒 rename；准入不提前通知及取消正常通知；同流最后一个许可才通知；
rename、排队项取消和在途许可在订阅前完成时，等待立即就绪；取消排空等待时订阅计数归零。没有订阅时不调用 Notify，避免无等待者的通知内部锁。前两项在旧实现均失败，
证据 work/append-wake-before.log；新实现和既有公平性测试全部通过。

完整 Rust 284 通过、10 ignored；JNI 冒烟通过；Hadoop 291 项、279 通过、12 跳过，
零失败/错误。日志 work/append-wake-rust.log、work/append-wake-jni.log、
work/append-wake-hadoop.log。release 基准及范围见 [APPEND_WAKE.md](APPEND_WAKE.md)。


## 完整追加流瓶颈定位与路径索引（2026-10-11）

新增可选测试内计时，记录 MemoryStore 追加总耗时、对象锁等待/持锁耗时及显式旧前缀
复制字节；与整个 write+flush 的累计操作耗时对照。新增容量复用诊断模式，默认仍为
原来的整文件复制，全部结果验证最终文件字节。两种模拟模式均不是真实云性能。

生产协调器保留顺序 Vec 注册表，精确文件 rename 首次使用时建立路径到槽位的惰性索引。
后续仅查源/目标槽位；新增流更新有效索引，注册表压缩或目录移动使索引失效，下一次
文件 rename 按需重建。目录移动沿用原线性更新，不逐条维护索引。先取出源/目标索引桶
再更新目标，避免刚移动的源流被作为旧目标处理。FIFO 与定向通知不变。

新增 5 项协调器测试覆盖同名多流/目标失效、同键高频创建回收、目录覆盖、索引建立后
新增流、目录移动后的索引重建，以及压缩使槽位变化后的重建；新增 1 项诊断存储测试
覆盖快照、覆盖偏移及零填空洞。既有注册回收测试改为检查惰性清理边界，公平性和
数据契约未放宽。精确 rename 从索引移除过期项，Vec 弱引用槽位由几何阈值注册清理或
目录更新压缩；静默实例不自动全表清理。

已放弃的全路径分组注册表在目录基准明显退步，结果保留于
benchmarks/append-flow-grouped-20261011.json；没有同步该候选源码。

最终 Rust 290 通过、10 ignored；JNI 冒烟通过；Hadoop 291 项、279 通过、12 跳过，
零失败/错误。日志 work/append-flow-rust.log、work/append-flow-jni.log、
work/append-flow-hadoop.log。性能分析和完整对照见 [APPEND_FLOW.md](APPEND_FLOW.md)。


追加 p99 分段排查（2026-10-11）：新增逐操作对象锁等待、持锁和写入任务唤醒至轮询计时，
分离冷/热索引与模拟存储复制影响；重建旧版完成直接对照。默认模拟存储的尾延迟仍不稳定，
未宣称全面提速。生产端减少同一路径的重复缓存失效，公平协调队列不变。
本地 Rust 292 通过、11 ignored，详情和证据边界见 [追加尾延迟诊断](APPEND_TAIL.md)。

本轮 JNI 冒烟通过；Hadoop 291 项、279 通过、12 跳过，零失败/错误。
日志 work/append-tail-rust.log、work/append-tail-jni.log、work/append-tail-hadoop.log。


追加后续优化（2026-10-11）：单流索引槽位改为直接存储，减少小数组分配，
公平队列不变。新增默认关闭、有记录上限的 append-diagnostics feature，测量存储任务
派发、唤醒、清理与返回；完整流结果仍有改善和退步，不宣称全面提速。
默认 Rust 293 通过，诊断构建 295 通过。详见 [索引与任务计时](APPEND_INLINE.md)。

本轮两种 Rust 构建各有 11 ignored（性能基准）；JNI 冒烟通过；Hadoop 279 通过、
12 跳过，零失败/错误。日志 work/append-inline-rust.log、append-inline-features.log、
append-inline-jni.log、append-inline-hadoop.log。

追加容量复用排查（2026-10-11）：补齐预留容量/无重命名对照和扩容、目录扫描计时；
缩小索引布局候选未获稳定收益，已撤回，公平协调器保持原样。默认 Rust 294 通过、
诊断构建 296 通过（各 11 ignored），JNI 通过。详见 [容量复用对照](APPEND_LAYOUT.md)。

容量复用本轮 Hadoop：291 总计，279 通过、12 跳过，零失败/错误。日志位于工作区
work/append-layout-rust.log、append-layout-features.log、append-layout-jni.log、append-layout-hadoop.log。

模拟存储隔离（2026-10-11）：新增范围查询/对象锁基准后端，拆分追加完成与整个混合负载吞吐；
纯性能 576 组，诊断 192000 次追加。优化仅在测试后端，生产公平协调器不变。
默认 Rust 297、诊断构建 299 项通过（各 11 ignored）；JNI 通过，Hadoop 279 通过、12 跳过。
详见 [模拟瓶颈与生产路径区分](APPEND_ISOLATION.md)。

预算快路径实验（2026-10-11）：五批 480 条对照结果有改善也有退步，候选已撤回。
保留新增的大请求预算预留公平性测试，生产路径保持原样。默认 Rust 298、诊断构建 300 项通过，
各 11 ignored。详见 [预算实验与公平性回归](APPEND_BUDGET.md)。

预算实验最终适配层验证：JNI 冒烟通过；Hadoop 279 通过、12 跳过，零失败/错误。
日志为工作区 work/append-budget-{rust,features,jni,hadoop}.log。

首次索引与通知对照（2026-10-11）：独立比较紧凑索引键、JoinSet 和单句柄/JoinSet 混合候选，
1700 条有效记录均保留；三个生产候选因收益不稳定撤回。新增批量完成基准与取消回归。
默认 Rust 299、诊断构建 301 项通过，各 12 ignored。详见 [索引与通知对照](APPEND_NEXT.md)。

索引/通知最终适配层验证：JNI 冒烟通过；Hadoop 279 通过、12 跳过，零失败/错误。
日志位于工作区 work/append-next-final-{rust,features,jni,hadoop}.log。


首次索引/完成路径热点定位（2026-10-11）：保留组件探针与可关闭的扫描计数，1915 条记录；
重复键克隆候选因独立路径退步、完整流收益不稳定已撤回。公平性/取消语义保持，
默认 Rust 299、诊断构建 301 项通过，各 13 ignored。详见 [热点定位与取舍](APPEND_HOTSPOTS.md)。


批量扫描对照（2026-10-11）：单轮扩展至 128 MiB，两个独立候选共 840 条记录。
扫描量下降未转化为稳定无探针收益，候选均撤回；保留两个批量取消/预算公平性回归和可配置基准。
默认 Rust 301、诊断构建 303 项通过，各 13 ignored。详见 [批量重复扫描实验](APPEND_BATCH_SCAN.md)。


批量等待分段与配置对照（2026-10-11）：新增预算/并发参数和分段输出，共 360 条记录。
明确区分基准压力并发 64 与生产默认 4；本地批大小 64 的五批对照均支持默认 4，生产参数不变。
最终 release filesystem 默认 150、诊断 151 项通过，各 4 ignored。详见 [等待分段与默认并发](APPEND_BATCH_PHASES.md)。


异步延迟与并发/预算对照（2026-10-11）：新增不持锁的可取消模拟等待及 360 条短负载记录。
有等待时并发收益与零延迟场景不同；记录实际等待时长，生产默认配置不变。
release filesystem 默认 151、诊断 152 项通过，各 4 ignored。详见 [延迟对照与局限](APPEND_DELAY.md)。


### 2026-10-11：有延迟持续追加压力

新增有界存储、按持续时间运行的压力基准，4 流、每批 16 请求，对比并发 4/64 和预算 16 KiB/64 MiB，每组 65 秒、三次重复。全部数据及取消后排空断言通过。默认/诊断 release filesystem 分别 151/152 项通过，各 5 ignored。生产代码及默认值未变。详见[结果、取消覆盖和测量边界](APPEND_SUSTAINED.md)。


### 2026-10-12：有延迟追加与重命名混合压力

新增同文件冲突的持续重命名、追加和取消 flush 压力测试，4 组配置各 65 秒×3 次。记录各任务 p99、5 秒完成量、最长进展间隔并逐对象验证数据。默认/诊断 release filesystem 分别 153/154 项通过，各 6 ignored。生产路径与默认配置未改。详见[完整结果和测量边界](APPEND_MIXED.md)。


### 2026-10-12：签名和 HTTP 调试输出脱敏

修复 SignedRequest、HttpRequest、HttpResponse 自动 Debug 暴露认证信息、URI 或正文的问题；三个新增回归在旧实现均失败，修复后通过。完整 Rust workspace 307 项通过、0 失败、15 ignored，其中本地 HTTP 46 项通过。签名、传输数据和 JNI 接口未改；未重跑 Hadoop/JVM 或性能基准。详见[验证范围与兼容边界](DEBUG_REDACTION.md)。


### 2026-10-12：分片上传响应 ETag 校验

拒绝 UploadPart 成功响应中缺失或空白 ETag，避免空值进入合并段列表。新增三项 HTTP 回归，包含实际上传流 abort、不发送 complete、名额和共享预算复用；旧实现缺失 ETag 时返回空字符串的问题已复现。完整 Rust workspace 310 通过、0 失败、15 ignored，本地 HTTP 49 项通过。详见[修复及验证边界](PART_ETAG.md)。


### 2026-10-13：近期协议修复的综合复验

在 XML 回车保真、对象键路径、GET 边界、写入对象类型和段号入口修复后的同一份源码上执行：

- 默认 Rust workspace debug 构建与测试：334 通过、0 失败、15 ignored；核心 110、缓存 1、文件系统 153、HTTP 70。
- 默认 Rust workspace release 构建与测试：334 通过、0 失败、15 ignored。另用 cargo build --release 构建实际 JNI 动态库；cargo test 自身不保证生成可加载的顶层 DLL。
- append-diagnostics 特性：336 通过、0 失败、15 ignored；核心 111、缓存 1、文件系统 154、HTTP 70。此特性没有带入默认 JNI 库。
- 真实 JVM JNI smoke：debug 与 release DLL 均通过创建/写入/关闭/状态/打开/读取/EOF/seek/非法边界/句柄清理。
- Hadoop 3.3.6 本地适配器与契约测试：Maven BUILD SUCCESS，报告 291 项、0 failures、0 errors、12 skipped，即 279 项非跳过；跳过来自 ObsAppendContractTest 的 8 项和 ObsCreateContractTest 的 4 项。

全部使用本机模拟或内存后端，无真实 OBS/PFS。15 个 Rust ignored 均为需显式运行的手动基准，此次没有重新测量性能。未把历史吞吐数字当作当前发布性能保证。Maven 的已有依赖模型警告及 Windows linker 提示未导致构建失败。

工作区保存 part-number-after.log、integration-refresh-release.log、integration-refresh-features.log、integration-refresh-jni.log、integration-refresh-release-build.log、integration-refresh-release-jni-final.log、integration-refresh-hadoop.log 及解析后的套件摘要；只同步项目源码、测试和文档，不上传日志、产物或图片。


### 2026-10-13：404 分类后的跨层复验与修复

本轮首先补跑 e02e876 对应的 release、实际 JVM/JNI 与 Hadoop。Rust release 345 项通过、原有 JNI 冒烟通过，但 Hadoop 报告 291 项中 15 项错误，均涉及 PFS 建目录的 ParentNotFound：上一轮将所有路径分支收紧后漏掉了目录创建的父目录恢复语义。新增 Rust 故障注入先复现，再仅在 PfsOps::mkdirs 中允许 404/ParentNotFound 递归创建父目录；不将它加入通用路径缺失分类。原有桶错误不触发建目录的回归仍通过。

同时，实际 JVM 新测试复现 NoSuchBucket 被 JNI 映射为 FileNotFoundException。JNI guard 现在仅将 is_path_not_found 所识别的错误和既有 IsDirectory 转为 FileNotFoundException，其他明确的服务级 404 保留为 IOException。新增 NativeErrorSmokeTest 使用本机 HTTP 响应，经真实 JNI list 调用检查 NoSuchBucket、NoSuchUpload、NoSuchCustomDomain、NoSuchKey 和空码，断言异常类别、错误码及请求 ID。scripts/test-jni.ps1 已纳入此测试。无错误码 HEAD 的兼容边界没有改变。

最终同一份修复源码的结果：

- 默认 Rust workspace debug / release 各 **346 通过、0 失败、15 ignored**：核心 111、缓存 1、文件系统 158、HTTP 76。
- debug / release 均显式构建实际 JNI 动态库，真实 JVM 的 NativeSmokeTest 和 NativeErrorSmokeTest 全部通过。
- Hadoop 3.3.6：**291 项、0 failures、0 errors、12 skipped**，Maven BUILD SUCCESS。12 项跳过仍是既有能力边界，未增加跳过以规避失败。
- 本轮未重跑 append-diagnostics 和手动性能基准；没有真实 OBS/PFS 或生产作业结论。ParentNotFound 恢复由本地 Hadoop 后端与 Rust 故障注入验证，未据此声称所有云端错误码均已覆盖。

工作区保留 post-fs-* 初始验证日志、parent-missing-before.log、jni-error-before.log 和 cross-layer-final-* 最终日志；仅源码、测试、脚本及本说明上传，不提交日志、图片和构建产物。


### 2026-10-13：追加流在服务级重命名错误后的隔离

RenameGuard::finish 的最后一处宽泛 is_not_found 判断会让 NoSuchBucket 等服务级 404 走“确定路径缺失”的分支，保留已有追加流。改用 is_path_not_found 后，这些错误沿用其他非确定拒绝的保守策略：源和目标范围内的旧流要求重新打开，无关流继续工作。不改变公平队列、取消处理、重命名成功后的路径迁移或 FileAlreadyExists 的既有处理；NoSuchKey 和无错误码 404 仍保留路径缺失兼容行为。这是本地保守处理策略，不宣称服务端已经发生了部分提交。

新增协调器测试覆盖文件/目录范围、五种 404 错误码、源目标和相邻无关路径、队列释放；新增文件系统测试验证源流后续 write/close 失败且不发送追加，无关流正常写入，并能重新打开源流恢复写入。修复前 NoSuchBucket 用例复现旧流仍可用。

最终默认 Rust debug workspace **348 通过、0 失败、15 ignored**（核心 112、缓存 1、文件系统 159、HTTP 76）；append-diagnostics **350 通过、0 失败、15 ignored**（核心 113、缓存 1、文件系统 160、HTTP 76）。未运行手动性能基准，没有吞吐改善结论。本轮未重跑 release/JVM/Hadoop，它们上一轮通过的结果对应此修复之前的版本。工作区日志 append-missing-before.log、append-missing-after.log、append-missing-diagnostics.log 不提交。


### 2026-10-15：LIST 显式空标量验证

LIST 解析器原先将显式为空的 IsTruncated/Size 跳过，留下 false/0 默认值，可能把畸形分页响应误认为末页或将对象误报为零字节。现在仅对已识别作用域中的这两个字段拒绝空值，覆盖自闭合、空正文和空 CDATA；未知扩展字段与缺省字段的既有兼容规则不变。没有新增“所有字段必须存在”的约束。

两个本机 HTTP 回归包含六种非法响应和三种合法/兼容响应；修复前复现 IsTruncated 自闭合被接受，修复后正确报 Io 错误，Size=0、IsTruncated=false/0 和缺省字段仍通过。完整默认 Rust debug workspace **350 通过、0 失败、15 ignored**（核心 112、缓存 1、文件系统 159、HTTP 78）。本轮未重跑 release/JVM/Hadoop/append-diagnostics 或性能基准，无云端验收结论。list-empty-before.log 和 list-empty-after.log 仅保存在工作区。


### 2026-10-15：分片 ETag 引号与协议跨层复验

HttpResponse::etag 原先使用 trim_matches 剥掉任意数量的首尾引号，可将不成对或重复引号的响应改写成不同的分片标识。现在只允许移除一对完整外层引号，出现残余引号返回 None，由 UploadPart 现有检查报错；保留既有未加引号的不透明标识兼容性，不强制 MD5 格式。这不是所有 HTTP ETag 语法或重复头部的全面校验。

新增本机 HTTP 回归覆盖五种畸形值，包括单边、重复、内嵌引号及弱标识形式。已有真实 ObsOutputStream 清理测试扩展到畸形引号，确认 abort、无 complete、任务名额归还，以及 128 字节预算复用。修复前已复现畸形值被接受。

在包含 LIST 空标量与 ETag 引号修复的最终源码上：默认 Rust debug / release 各 **351 通过、0 失败、15 ignored**（核心 112、缓存 1、文件系统 159、HTTP 79）；显式构建 debug/release JNI 动态库，两种构建的 NativeSmokeTest 与 NativeErrorSmokeTest 均通过；Hadoop 3.3.6 **291 项、0 失败、0 错误、12 跳过**，BUILD SUCCESS。没有新增跳过项。append-diagnostics 与手动性能基准未在本轮重跑，未验证真实云端。

etag-quotes-before.log、etag-quotes-after.log 及 protocol-final-* 日志仅保存在工作区，不上传日志、图片或构建产物。


### 2026-10-15：请求路径的文件与子前缀冲突

普通桶允许同时存在对象 file 和子对象 file/child。按已确认的文件优先规则，直接对 /file 执行容量统计、普通列举、分页列举与递归文件列举时，应返回 file 本身，不能根据子前缀推断为目录。抽出共同的请求路径检查：优先复用正缓存，普通桶冷缓存非根路径 HEAD 一次，仅路径缺失允许继续 LIST；NoSuchBucket 等服务级错误直接传播。根路径与 PFS 不新增此 HEAD，续页不重复检查。冷缓存返回的路径和块大小与状态查询一致。

新增 Rust 回归覆盖冷/热缓存、五种列举入口、容量统计、长度/类型/路径/块大小和服务级 404。修复前容量统计将 6 字节对象误计为 5 字节子对象；修复后不再发送该路径的 LIST。已有请求计数回归继续验证列表回填后的 open 无额外 HEAD，以及续页无额外 HEAD。Hadoop 增加真实 Java → JNI → Rust → 本机 HTTP 的冲突测试，覆盖容量统计与各类列举，并保留惰性分页和关闭迭代器不追加请求的断言。

本节仅描述直接请求冲突路径的处理，不代表父目录跨页列举中所有潜在冲突或外部写入一致性已全面验收。无真实云服务或性能改善结论。

最终源码的默认 Rust workspace debug / release 各 **354 通过、0 失败、15 ignored**（核心 112、缓存 1、文件系统 162、HTTP 79）；显式构建 debug JNI 动态库后，NativeSmokeTest 与 NativeErrorSmokeTest 通过；Hadoop **292 项、0 失败、0 错误、12 跳过**，BUILD SUCCESS。release JVM、append-diagnostics 和手动性能基准本批未重跑。file-precedence-* 及早期 summary/listing-collision-* 日志仅保存在工作区。


### 2026-10-15：父目录前缀冲突与缓存回填

进一步发现父目录 LIST 先返回 CommonPrefixes 时，同名对象可能被同页去重跳过，并将目录状态写入缓存。普通桶的目录前缀/斜杠标记现在在返回与缓存前确认无斜杠的同名对象；命中文件时以文件状态返回。分页 files-only 过滤移到状态解析之后，避免把已解析为文件的前缀漏掉。跨页去重仍由无状态分页接口的调用方负责，Java 迭代器保留原有去重机制。

新增 Rust 回归覆盖父目录普通/分页列举、前缀先于文件的跨页返回、files-only 过滤、服务级 404 与 PFS 零新增 HEAD。确定性并发测试让前缀 HEAD 返回旧快照前删除对象，验证旧快照可返回但不得重新污染缓存。Hadoop 新增父目录 listStatus/listStatusIterator/listFiles(false) 端到端测试，检查后续 status 和内容统计仍按文件处理。修复前已复现父目录将文件返回为目录。

代价是普通桶未缓存的目录列表项增加同名对象 HEAD；文件列表项与 PFS 不增加此请求。该批为一致性修复，没有声称目录密集负载吞吐改善。未对所有外部并发写入及递归祖先遮蔽语义作全面验收。

最终源码默认 Rust workspace debug / release 各 **358 通过、0 失败、15 ignored**（核心 112、缓存 1、文件系统 166、HTTP 79）。显式构建两种 JNI 动态库后，debug/release 的 NativeSmokeTest 与 NativeErrorSmokeTest 均通过。Hadoop **293 项、0 失败、0 错误、12 跳过**，BUILD SUCCESS。未重跑 append-diagnostics 或性能基准。parent-collision-before.log、parent-collision-after.log 和 parent-probe-* 日志仅保留在工作区。


### 2026-10-15：递归列举与容量统计中的文件遮蔽

按确认的文件系统可见性规则，普通桶同时存在 a 与 a/b 时，父目录递归列举不返回 a/b，容量统计不计入它；冲突的 a/ 标记也不计为目录。递归列表和汇总逐条检查路径段祖先，遇到文件即停止；因此不依赖 LIST 页序，也不会把 a 与 ab 混淆。完整列表/汇总扫描复用祖先判断；无状态分页入口只在当页保留这些判断，后续页仍可复用已存在的状态缓存。PFS 不进行此类探测。

新增 Rust 测试覆盖对象、标记及多层后代、跨页子对象先返回、相邻路径名、祖先服务级 404，以及 PFS 零新增 HEAD。原有跨页重复对象检查仍处理隐藏条目；不可用遮蔽规则掩盖畸形分页。Hadoop 端到端验证根目录递归迭代和汇总长度/文件数/目录数，仍使用每页 3 项的本机 HTTP 模拟。

祖先探测增加普通桶未缓存祖先的 HEAD；完整扫描的祖先表按不同祖先数增长，无状态分页按当前页涉及的祖先数增长，属于元数据开销，不计入数据缓冲预算。目录密集负载的真实服务吞吐尚未测量。该批验证列举与容量统计的可见性，不承诺外部并发修改下的一致快照，也未声明所有显式子路径操作已经拒绝被遮蔽对象。

额外验证连续多个完全隐藏的页不会使 Hadoop 迭代提前 EOF；确定性取消测试验证祖先 HEAD 等待期间取消后，StatusCursor 重试相同 marker，不跳页。最终默认 Rust debug / release 各 **363 通过、0 失败、15 ignored**（核心 112、缓存 1、文件系统 171、HTTP 79）；append-diagnostics **365 通过、0 失败、15 ignored**。debug/release 实际 JNI 动态库的基础和异常分类 JVM 测试通过；Hadoop **294 项、0 失败、0 错误、12 跳过**，BUILD SUCCESS。没有新增跳过项，未运行性能基准或真实云验证。recursive-shadow-* 日志仅保留在工作区。


### 2026-10-15：显式隐藏路径的文件系统入口保护

按确认规则，普通桶存在文件 a 时，显式 /a/b 也不可通过文件系统访问。祖先检查在子路径缓存命中之前执行；状态提示 open 与带 marker 的列举续页同样检查。查询/打开/列举/统计返回本地 NoSuchKey，Hadoop 映射 FileNotFoundException；exists 返回 false，delete 和源路径缺失的 rename 沿用 false。创建、mkdir 和元数据入口也不能绕过该保护。PFS 不增加此类祖先 HEAD；ObjectStore 原始对象接口不变。

新增 Rust 测试验证已缓存子对象、open 状态提示、创建/覆盖、权限/所有者、重命名源目标和续页；断言隐藏对象内容不变，且无 GET、PUT、COPY、DELETE 等访问/修改请求。服务级祖先错误继续传播，不能变成 exists=false。Hadoop 新增冷/热缓存端到端回归，检查隐藏查询、打开、列举、统计、删除、重命名及覆盖失败后对象仍保留。

未缓存或被失效的普通桶祖先会增加 HEAD，即使子文件状态已缓存；原有权限/长度缓存仍保留。缓存 TTL 与外部修改限制仍适用，不承诺即时发现绕过文件系统的并发修改，也不撤销已经打开的流。该批未改变父目录批量删除/重命名对前缀内原始对象的处理规则。

最终默认 Rust workspace debug / release 各 **367 通过、0 失败、15 ignored**（核心 112、缓存 1、文件系统 175、HTTP 79）。显式构建 debug JNI 动态库后，基础与异常分类 JVM 冒烟通过；Hadoop **295 项、0 失败、0 错误、12 跳过**，BUILD SUCCESS。release JVM、append-diagnostics 和性能基准本批未重跑。explicit-shadow-* 日志仅保存在工作区。


### 2026-10-15：递归游标跨页复用祖先判断

无状态分页函数仍按页独立工作。StatusCursor 现在在一次递归迭代内保留已接受页面的祖先判断，避免反复 HEAD 相同目录。每页只生成新判断的增量，不复制整个历史表；页完成且通过 marker/去重接受之后才合并。取消或失败的取页不提交中间判断。EOF、请求失败和 marker 循环释放祖先表容量，Drop 按正常所有权释放。

确定性 Rust 场景中，100 个单项分页共享同一祖先，HEAD 从修复前 100 次降至 1 次，LIST 仍为 100 次，返回 100 个文件。新增取消测试先得出 dir 不是文件，再阻塞另一个祖先；取消后让 dir 成为文件，重试必须重新探测并隐藏子对象，证明未提交的判断没有泄漏。现有终止内存测试扩展为填充 4096 个祖先并验证释放后容量为零。Hadoop 新增普通桶/PFS 十页回归，要求 30 个文件、10 次 LIST，普通桶仅 1 次祖先 HEAD、PFS 0 次。

这是可复现的请求次数减少，不是生产吞吐/p99 结论。祖先表按本次游标涉及的不同祖先数量增长，属于元数据开销；新的扫描重新判断祖先，一次扫描内保留已接受判断，不保证外部并发修改的一致快照。

最终默认 Rust debug / release 各 **369 通过、0 失败、15 ignored**（核心 112、缓存 1、文件系统 177、HTTP 79）；append-diagnostics **371 通过、0 失败、15 ignored**。显式构建 debug 动态库后 JVM 基础/异常测试通过；Hadoop **296 项、0 失败、0 错误、12 跳过**，BUILD SUCCESS，包含实际 JNI 游标请求计数。release JVM 已在上一批显式隐藏路径保护版本上补跑通过，本批未重跑。未运行吞吐基准或真实云测试。cursor-ancestor-* 及 explicit-shadow-release-*/explicit-shadow-diagnostics.log 仅保存在工作区。


### 2026-10-15：祖先 HEAD 延迟手动对照

新增默认 ignored 的 recursive_cursor_ancestor_latency_benchmark；仅测试 MemoryStore 增加可选 HEAD 等待，默认 0 不等待，不持有对象锁等待。release、2 个 Tokio worker、64 个单项页、9 对样本，交替先后顺序。对照是同一版本的无状态递归分页与 StatusCursor，并非两个 Git 版本的整个客户端吞吐比较。两者都返回 64 个文件、发送 64 次 LIST；每次样本分别要求 64/1 次祖先 HEAD。

本机结果：无模拟等待时，中位 0.328/0.315 ms，差异很小，不据此声称 CPU 吞吐改善；请求 3 ms tokio sleep 时，中位 966.060/15.713 ms。Windows 计时/调度使实际等待明显高于 3 ms，因此不能把这组值解读为真实 3 ms 网络 RTT 下的性能。没有 HTTP/TLS、LIST 延迟、云服务限流或并发负载，也没有可靠的生产 p99 结论。确定性收益是重复祖先请求和对应等待次数减少。

复现：cargo test --locked --release -p obs-rust-core --test filesystem recursive_cursor_ancestor_latency_benchmark -- --ignored --nocapture。基准输出日志仅保存在工作区 cursor-ancestor-benchmark.log。

新增基准后的默认 debug/release 回归各 **369 通过、0 失败、16 ignored**，append-diagnostics **371 通过、0 失败、16 ignored**；新增 ignored 为本节手动基准，已显式执行通过。生产源码和 JNI/Hadoop 适配代码未改动，沿用上一批对应代码的 JVM/Hadoop 验证结果。cursor-benchmark-final-* 与 cursor-benchmark-diagnostics.log 仅在工作区保留。


### 2026-10-15：根目录不接受文件状态提示

Rust OpenOptions.status_hint 可以提供已有状态以避免查询，但根目录始终是目录。修复前伪造 FileStatus::file("/") 可使 open("/") 返回输入流；现在在读取提示之前拒绝根目录，返回 IsDirectory。普通桶和 PFS 回归均验证该错误且后端请求数为零。

默认 Rust workspace debug/release 各 **370 通过、0 失败、16 ignored**。未重跑 JNI/Hadoop/append-diagnostics 或手动基准；对应的上批结果不是本次修改的新增验收。root-open-hint-* 日志仅保存在工作区。


### 2026-10-15：目录批量修改前拒绝隐藏对象冲突

普通桶递归删除和目录重命名先完成全量分页与重复项验证，再以借用键集合检查文件前缀冲突。发现 a 与 a/b 或 a/ 共存时返回 Io，尚未执行复制、删除或父目录标记写入；保留原始对象供底层 ObjectStore 单独处理。PFS 保持原有路径。预检没有增加 HEAD，但需要 O(文件数) 的辅助元数据空间；不保证枚举期间或预检之后外部并发修改的一致性，也没有把 copy/delete 变成原子操作。

修复前确定性测试重现冲突目录操作返回成功。新增测试覆盖删除/重命名、子文件/目录标记、跨页及相反页序，断言对象快照不变且 PUT/COPY/DELETE 均为零。另测中文完整路径段：文件“文件”不阻断“文件夹/child”，合法目录仍可完整移动或删除。Hadoop 本机 HTTP/JNI 回归覆盖两种冲突和两类操作，逐个校验所有对象的键与字节内容均保留。

默认 Rust workspace debug/release 各 **372 通过、0 失败、16 ignored**（核心 112、缓存 1、文件系统 180、HTTP 79）；append-diagnostics **374 通过、0 失败、16 ignored**。显式构建 debug/release 动态库后，真实 JVM 基础与异常分类冒烟均通过。Hadoop **297 项、0 失败、0 错误、12 跳过**，BUILD SUCCESS。未执行新的性能基准或真实云验收；bulk-namespace-* 日志与补丁仅保存在工作区。


### 2026-10-15：批量预检取消与异常分组响应

补充确定性取消回归：普通桶目录删除/重命名读取第一页后，在第二页阻塞时丢弃操作，原始对象不变、PUT/COPY/DELETE 为零。随后插入新的隐藏后代并重新操作，必须从无 marker 的第一页重新扫描并拒绝新冲突，不能复用已取消预检的部分结果。

另修复递归批量枚举收到 CommonPrefixes 的边界。未指定 delimiter 的请求应枚举原始对象；若返回分组前缀，则不能证明已列出全部后代。修复前普通桶删除测试返回成功并修改对象。现在批量枚举在分页合并/去重前拒绝分组响应，包括前缀与同名目录标记重叠、ObjectStore 直接返回 is_common_prefix 的情况。普通桶删除/重命名和 PFS 递归删除均覆盖；PFS 原有直接目录删除失败后进入递归回退，但异常枚举不会导致子对象删除。正常只读分组列举保持原行为。

默认 workspace debug/release 各 **374 通过、0 失败、16 ignored**；append-diagnostics **376 通过、0 失败、16 ignored**。显式构建 debug/release 动态库后，实际 JVM 基础/异常冒烟通过；Hadoop **297 项、0 失败、0 错误、12 跳过**，BUILD SUCCESS。本批未新增云验证或性能结论。bulk-cancel-*、bulk-grouped-* 日志仅保存于工作区。


### 2026-10-16：目录存在性探测跨越空分页

状态查询在 HEAD 未找到目录时，用 LIST 判断隐式目录是否存在。原实现只检查第一页；空页带续页标记时，即使后续存在对象也会返回 NoSuchKey 并写入负缓存。根目录非递归删除有同类问题。现统一使用 ListPager，在得到非空页时立即结束，只有完整空枚举才判定为空；正常非空首页仍只请求一次 LIST。分页器同时验证对象前缀和续页循环，避免非法响应污染状态缓存。

新增普通桶/PFS 回归覆盖空首屏后出现对象、根目录空首屏后出现目录，以及越界键/循环 marker 报错后重新查询成功。修复前两个用例分别复现错误 NoSuchKey 和漏报 DirectoryNotEmpty。

workspace debug/release 各 **377 通过、0 失败、16 ignored**，append-diagnostics **379 通过、0 失败、16 ignored**。显式构建 debug/release JNI 动态库后的 JVM 基础/异常测试通过；Hadoop **297 项、0 失败、0 错误、12 跳过**。全部为本地验证，existence-pages-* 日志仅存于工作区。


### 2026-10-16：单文件修改保留隐藏后代

用户委托选择与项目规则一致的处理方式后，采用保守规则：普通桶同时存在 a 与 a/b 或 a/ 时，删除或移走 a、覆盖作为重命名目标的 a 均返回 Io，保留全部对象。此为项目行为选择，不代表已核验任何公司的内部规范。底层 ObjectStore 仍可用于显式处理原始对象。已有文件内容覆盖不移走祖先，不在本次删除/重命名规则的变更范围内。

检查在写操作前进行，以 max-keys=1 的带分隔符 LIST 探测后代；有结果立即拒绝，空续页继续，非法页/循环 marker 报错。普通单文件删除/移动通常新增一次 LIST；覆盖已有目标还需检查目标。PFS 不走该检查。预检无法阻止其他客户端在检查后并发修改，不构成原子性保证。

Rust 新增源文件删除（两种 recursive 值）、重命名、覆盖冲突目标、目录标记与子对象、空首屏与循环续页回归，要求对象快照不变且 PUT/COPY/DELETE 为零。旧缓存竞态测试先经 ObjectStore 清理冲突，再执行正常删除；旧延迟 LIST 测试限定门控及脚本响应只作用于根目录，避免把新的安全探测也阻塞。Hadoop 新增两种冲突的删除/重命名测试并逐对象核对字节内容。

最终 workspace debug/release 各 **379 通过、0 失败、16 ignored**，append-diagnostics **381 通过、0 失败、16 ignored**；显式构建 debug/release 动态库后实际 JVM 基础/异常冒烟通过；Hadoop **298 项、0 失败、0 错误、12 跳过**。file-conflict-* 日志仅存工作区，未进行真实云或性能验收。


### 2026-10-16：单文件探测取消与请求成本回归

新增确定性测试，在单文件删除/重命名的后代探测阻塞时取消，确认原始对象不变且 PUT/COPY/DELETE 为零；释放模拟门控后重新执行正常成功。另在源状态和目标缺失状态已缓存的前提下，校验普通桶单文件删除/移动恰好增加一次 LIST，PFS 为零。该测试证明请求计数，不是网络吞吐或尾延迟基准。

仅增加 Rust 测试，生产代码不变。默认 debug/release 各 **381 通过、0 失败、16 ignored**，诊断特性 **383 通过、0 失败、16 ignored**。实际 JVM 和 Hadoop 沿用上一批相同生产源码的结果，未重复执行。file-probe-* 日志仅保存在工作区。


### 2026-10-16：PFS 根目录内容操作本地拒绝

Rust 直接入口现在对 append_stream("/") 和 truncate("/") 返回 IsDirectory，在注册追加流、修改缓存或发起后端请求之前结束。根目录类型已知，无需额外 HEAD。修复前 append_stream 可返回根目录输出流，新增回归已复现；修复后同时断言追加和截断的错误类型及零后端请求。普通桶不支持追加/截断的行为保持不变。

默认 workspace debug/release 各 **382 通过、0 失败、16 ignored**，诊断特性 **384 通过、0 失败、16 ignored**；显式构建 debug/release 动态库后的实际 JVM 基础/异常冒烟通过；Hadoop **298 项、0 失败、0 错误、12 跳过**。本地日志 pfs-root-* 不上传，未进行真实云验收。


### 2026-10-16：LIST 父目录推断不覆盖有效元数据

修复列举目录后将已知父目录权限从 0700 覆盖为默认 0755 的问题。LIST 父目录回填只证明存在，不提供父目录的完整属性。现在完整列举、分页列举和缓存公共 backfill_from_list 路径均保留有效正缓存；过期或负缓存仍可用推断状态替换。检查与替换在同一缓存分片锁下完成，查询回填仍检查 invalidation epoch；保留已有状态不刷新其 TTL 或访问顺序。无需额外 HEAD/LIST，未声称吞吐改善。

新增普通桶/PFS、完整/分页四种路径测试，旧版本已复现 0700 变为 0755；另新增缓存单测验证 owner、修改时间、TTL/访问顺序不变，以及过期/负缓存替换与失效后迟到回填被拒绝。已有缓存失效与延迟查询竞态回归继续通过。本批仅修正父目录推断回填，不改变显式 HEAD/LIST 条目更新策略或缓存外部修改一致性边界。

workspace debug/release 各 **385 通过、0 失败、16 ignored**，诊断特性 **387 通过、0 失败、16 ignored**。显式构建 debug/release 动态库后，实际 JVM 基础/异常冒烟通过；Hadoop **298 项、0 失败、0 错误、12 跳过**，BUILD SUCCESS。directory-hint-* 日志和补丁仅保存在工作区，无真实云验收结论。


### 2026-10-16：Java 流检查所属文件系统关闭状态

原生文件系统关闭会清理所属流句柄，但 Java 流原先仅检查自身 handle 字段，导致 getPos/available/零长度读取等本地操作仍可能成功。现在 NativeInput/NativeOutput 在方法入口同时检查所属文件系统的 volatile nativeHandle；已关闭时抛 IOException，避免顺序调用场景继续使用已退役句柄。流 close 保持幂等，不增加 JNI/网络请求。该检查不是与文件系统 close 的全局原子屏障，不声称改变已经在途的 I/O 语义。

新增普通桶/PFS 的实际 Hadoop 回归，先打开输入和创建输出流，再关闭文件系统，校验 getPos、available、零长度/正常读写、seek、flush 均拒绝，重复关闭流无异常且未提交输出对象。旧版本在 getPos 应抛异常处失败；修复后 Hadoop **299 项、0 失败、0 错误、12 跳过**，BUILD SUCCESS。Rust/JNI 源码未修改，沿用上一批 debug/release 385、诊断 387 和 JVM 验证结果，没有重复执行这些未变组件的测试。stream-owner-* 日志仅保存在工作区。


### 2026-10-16：文件系统重建与旧 Java 资源隔离

Java initialize 现在与 close 使用同一实例锁，并在修改配置或创建原生对象之前拒绝对仍打开的实例再次初始化，避免覆盖原生句柄。关闭后仍可重新初始化。输入流、输出流和状态迭代器记录创建时的文件系统句柄，入口检查要求它仍与当前句柄一致，防止旧流的本地方法和已缓存的迭代器页面在重建后恢复可用。旧资源的重复关闭仍安全，不会关闭新实例。

旧实现两个新增测试均失败：重复初始化不抛 IOException；关闭重建后旧输入流 getPos 不抛 IOException。修复后覆盖普通桶/PFS 的旧输入流、旧输出流、已缓存迭代器拒绝访问，旧资源关闭后新实例仍能读取原文件且未提交旧输出。此为实例生命周期校验，不承诺将所有在途 I/O 与关闭/重建组成全局原子操作。

本次 Hadoop **301 项、0 失败、0 错误、12 跳过**，BUILD SUCCESS，使用实际 JNI 和本机 HTTP 模拟。Rust/JNI 源码未变，沿用对应源码已通过的 debug/release 385、诊断 387 和 JVM 冒烟结果，未重复运行。fs-generation-* 日志仅保存在工作区。


### 2026-10-16：覆盖重命名禁止 Java 默认预删除

检查本地 Hadoop 3.3.6 FileSystem 字节码确认其 Options.Rename 默认实现会先删除已存在目标，再调用双参数 rename。新增测试经该基类重载调用适配器，复现源文件与隐藏后代冲突时 Rust 拒绝重命名，但目标已被 Java 删除。现在适配器覆写该入口，先做只读状态检查，文件覆盖直接将 overwrite 传入 Rust。普通双参数 rename 行为保持不变；无覆盖选项时目标存在报 FileAlreadyExistsException，同路径拒绝，缺失父目录和文件父路径保留对应异常分类。覆盖已有目录及 TO_TRASH 在修改前明确 UnsupportedOperationException。

回归覆盖源/目标隐藏后代、复制失败、同路径操作后所有对象字节不变，普通桶/PFS 正常文件覆盖，无覆盖选项拒绝已有目标，目录覆盖与 TO_TRASH 拒绝，以及目标父目录错误分类。使用反射调用 Hadoop 基类的受保护重载，确认虚派发进入新实现，避免仅验证新暴露的便捷入口。本次不保证普通桶 copy/delete 原子性，也不保证服务端提交后异常或外部并发修改可回滚。

Hadoop 完整回归 **306 项、0 失败、0 错误、12 跳过**，BUILD SUCCESS。Rust/JNI 未修改，沿用相同源码已有的 debug/release 385、诊断 387 和 JVM 冒烟结果。rename-options-* 日志和本地字节码输出仅保存在工作区，不上传。无真实云验收结论。


### 2026-10-16：创建标志不再被 Hadoop 默认重载丢弃

本地 Hadoop 3.3.6 字节码确认递归 create flags 默认重载只提取 OVERWRITE，忽略 APPEND 与组合合法性。适配器现在接管带 ChecksumOpt 的递归入口和 primitiveCreate，非递归入口共享基础校验；通过 Hadoop CreateFlag.validate 检查存在性和组合，已有文件 APPEND 转入原生追加，CREATE+APPEND 在缺失时创建。未实现的 SYNC_BLOCK、NEW_BLOCK 等标志在创建流前明确拒绝，避免默默退化。ChecksumOpt 尚未下传；不存在跨实例存在性检查与写入的原子保证。

四种入口均覆盖非法 APPEND+OVERWRITE、缺失对象 OVERWRITE-only、PFS 追加位置/内容及 CREATE+APPEND 回退，并遍历其余所有枚举标志验证拒绝且没有创建对象。旧版本新增测试出现 2 失败、1 错误；修复后 Hadoop 完整回归 **309 项、0 失败、0 错误、12 跳过**，BUILD SUCCESS。Rust/JNI 未变，沿用相同源码 debug/release 385、诊断 387 和实际 JVM 冒烟结果。create-flags-* 日志仅在工作区，无真实云验收结论。


### 2026-10-16：退出删除先于原生文件系统关闭

原先 Java close 先释放 Rust 句柄，再调用 Hadoop super.close，导致后者 processDeleteOnExit 的 exists/delete 全部无法访问原生文件系统，错误被 Hadoop 按尽力清理语义吞掉。现在先执行 super.close，在 finally 中清零并释放原生句柄，确保退出删除仍经过 Rust 的状态和命名空间检查。取消注册、重复关闭保持原有语义；不保证与在途 I/O 或其他实例并发修改的全局原子性。

新增普通桶/PFS 文件与目录退出删除、取消注册保留、缺失路径不注册、关闭后旧流失效，以及普通桶文件/目录隐藏后代冲突保留全部字节并继续清理其他对象的测试。旧实现两项新增测试均失败；修复后 Hadoop 完整回归 **311 项、0 失败、0 错误、12 跳过**，BUILD SUCCESS。清理冲突仍由 Hadoop 记录并忽略，close 成功不表示所有注册路径已删除。Rust/JNI 未变，沿用同源码 debug/release 385、诊断 387 和 JVM 冒烟结果。delete-on-exit-* 日志仅在工作区。


### 2026-10-16：追加新块选项显式拒绝及 builder 路径验证

Hadoop 默认四参数 append 丢弃 newBlock 参数，PFS 因而把要求新 HDFS 块的调用当作普通追加。现在 newBlock=true 在打开流前报 UnsupportedOperationException；false 委派已有追加路径。两种布尔重载、两种桶类型均验证拒绝后内容不变，PFS false 保持追加位置和内容，普通桶 false 保持不支持。新增测试在旧实现上复现 true 没有抛错。

另验证 createFile builder 的递归/非递归入口拒绝 APPEND+OVERWRITE，覆盖被文件阻断的后代路径不会修改原对象，appendFile 同样拒绝被阻断路径，PFS builder 正常创建及追加成功。此处针对显式后代路径，未扩大删除/移动冲突规则到覆盖遮挡文件本身，也未新增 builder 实现。

Hadoop 完整回归 **313 项、0 失败、0 错误、12 跳过**，BUILD SUCCESS。Rust/JNI 未变，沿用同源码 debug/release 385、诊断 387 和 JVM 冒烟结果；无性能提升或真实云验收结论。append-options-* 日志仅保留本地。


### 2026-10-16：默认元数据操作不再绕过路径校验或误报成功

Hadoop setReplication 默认直接返回 true，setTimes 默认直接返回，均不检查路径。适配器现将两者接入 Rust getFileStatus；副本数必须为正数，但返回 false 表示未改变 OBS 策略。时间参数两个均为 -1 时只校验路径；实际修改明确 UnsupportedOperationException，避免将未实现的时间写入报告为成功。此改变会使要求保留时间的上层操作明确失败，未添加模拟时间或副本元数据。

新增普通桶/PFS 不支持操作的内容与元数据保留测试，以及不存在/被文件阻断后代/错误桶/关闭实例的路径校验。旧实现两个新增测试均失败。隐藏后代沿用 Rust 状态查询的 FileNotFoundException 分类，未改为 ParentNotDirectoryException。修复后 Hadoop 完整回归 **315 项、0 失败、0 错误、12 跳过**，BUILD SUCCESS。Rust/JNI 未变，沿用同源码 debug/release 385、诊断 387 和实际 JVM 冒烟结果。metadata-defaults-* 日志仅在工作区。


### 2026-10-17：校验和默认入口补齐路径校验

接续上次中断的测试。getFileChecksum 长度重载现在拒绝负长度并调用 Rust 状态查询，无长度入口通过 Hadoop 虚派发复用该实现。有效路径保持 null，表示没有 Hadoop 兼容校验和，不虚构 MD5 或把 ETag 用作文件校验和。

普通桶/PFS 回归覆盖有效路径、零/正/负长度、不存在路径、错误桶和关闭实例；普通桶还验证隐藏后代拒绝，及 resolvePath、getFileLinkStatus、getFileBlockLocations、listLocatedStatus、access 继承入口遵循 Rust 可见性规则。旧实现校验和两项新增测试失败。此次完整 Hadoop 回归 **317 项、0 失败、0 错误、12 跳过**，BUILD SUCCESS。Rust/JNI 未变，沿用同源码 debug/release 385、诊断 387 和 JVM 冒烟结果。checksum-paths-* 日志不上传。


### 2026-10-17：LocatedStatus 列举复用 Rust 分页

Hadoop 3.3.6 默认 listLocatedStatus 在构造迭代器时调用全量 listStatus，30 个对象、每页 3 条的本地测试在仅创建迭代器时已发 10 次 LIST。适配器现覆盖无过滤和带 PathFilter 的入口，使用 Rust 非递归游标；首屏仍验证路径，之后按需读取。Java 过滤器在迭代时执行，最多额外缓存一个匹配 FileStatus，保留目录条目且目录不生成块位置。过滤器运行时异常释放游标，显式关闭幂等，缓存命中前仍检查原生所属文件系统生命周期。listFiles 无过滤路径继续使用原包装，不引入过滤扫描。

新增普通桶/PFS 首屏恰好 1 次 LIST、重复 hasNext/提前关闭不请求后续页、完整遍历/EOF、跨页过滤、全部过滤掉、目录与块位置、过滤异常释放登记及关闭文件系统后拒绝缓存条目的回归。旧实现首屏计数为 10 且无 Closeable 支持。修复后 Hadoop **320 项、0 失败、0 错误、12 跳过**，BUILD SUCCESS。此为按需读取和请求计数证明，不是吞吐/p99 或 Java 堆峰值基准；遍历全部对象仍须读取全部页面。Rust/JNI 未变，沿用同源码 debug/release 385、诊断 387 和 JVM 冒烟结果。located-paging-* 日志不上传。


### 2026-10-17：primitiveMkdir 根目录与相对路径处理

Hadoop 默认 primitiveMkdir(path, permission, false) 对根路径执行 getFileStatus(null)，实际回归抛 IllegalArgumentException: null path。适配器现先验证桶并将路径按当前工作目录限定，再取父目录；根路径没有父目录时跳过父状态查询。非递归入口仍拒绝不存在或非目录父路径，实际创建调用原有 Rust mkdirs，递归入口也复用同一路径解析。

新增普通桶/PFS 根目录不创建对象、单段相对路径落在工作目录下、缺失父目录不创建、递归创建成功，及普通桶文件父路径/隐藏后代/错误桶拒绝且对象字节不变的测试。旧实现根路径测试失败；修复后 Hadoop **322 项、0 失败、0 错误、12 跳过**，BUILD SUCCESS。父状态检查和创建非原子，不改变跨实例竞争保证。Rust/JNI 未变，沿用同源码 debug/release 385、诊断 387 和 JVM 冒烟结果。primitive-mkdir-* 日志保留本地。


### 2026-10-17：退出删除固定登记时的工作目录

Hadoop 默认 deleteOnExit 将原始 Path 放入集合；相对路径在关闭时才经当前工作目录解析，注册后切换工作目录会改变删除目标，同一文件的相对/绝对路径也不能相互取消。适配器现在对注册与取消均先校验桶，再按调用时的工作目录 makeQualified。删除依旧由 Hadoop close 调用 Rust 执行，不绕过原有命名空间冲突检查。

旧实现相对/完整路径取消测试失败，单独的目录切换测试也复现原登记文件在关闭后仍存在。修复后普通桶/PFS 均验证原登记对象删除，切换后的同名对象字节保留；相对、绝对、含桶 URI 三种路径可互相取消。Hadoop 完整回归 **324 项、0 失败、0 错误、12 跳过**，BUILD SUCCESS。未实现跨实例重命名跟踪或对外部路径替换的身份绑定，退出删除仍按登记路径尽力执行。Rust/JNI 未变，沿用同源码 debug/release 385、诊断 387 和 JVM 冒烟结果。exit-paths-* 日志不上传。


### 2026-10-17：更多继承查询与本地复制入口回归

本批只增加集成回归，不修改生产代码。普通桶的 globStatus、多路径/过滤 listStatus、getUsed 共享文件优先可见性：隐藏后代不被枚举或计入用量，显式查询依照 Hadoop 各接口约定返回无匹配或 FileNotFoundException。openFile builder 在普通桶/PFS 中使用实际对象长度/内容，不采信提供的错误长度提示；隐藏路径即使附带 FileStatus 也通过失败 Future 返回 FileNotFoundException。

本地复制测试只在项目 target 下建立临时文件并清理。验证普通桶/PFS 成功复制保留源、成功移动删除源；普通桶隐藏目标或 overwrite=false 的已有目标被拒绝时，本地源和两份原始对象字节均不变。本批不声称目录、多源或上传中途失败的完整事务保证。

Hadoop 完整回归 **328 项、0 失败、0 错误、12 跳过**，BUILD SUCCESS。未发现这些已测继承路径需要新增生产修复。Rust/JNI 未变，沿用同源码 debug/release 385、诊断 387 和实际 JVM 冒烟结果。inherited-queries* 日志与临时文件不上传。


### 2026-10-17：默认本地移动复制的提交故障测试

模拟服务器增加仅针对指定普通 PUT 对象的故障开关：完整读取请求体后，在提交前或提交后返回 503 ServiceUnavailable。仅测试夹具改变，生产代码不变。普通桶/PFS、目标已存在/缺失、提交前/后共八组场景验证 copyFromLocalFile(deleteSource=true, overwrite=true) 抛 IOException 后本地源字节仍在。

提交前失败保留已有目标或目标缺失；提交后失败按不确定提交处理，允许并验证远端新内容可读，不声称回滚。故障清除后同一文件系统重试移动成功，才删除本地源。测试同时观察到实际 PUT 故障请求，避免在打开流之前失败造成假阳性。临时文件仅在项目 target 下创建并清理；该测试不涵盖 MPU、连接中断、多源或目录事务。

Hadoop 完整回归 **329 项、0 失败、0 错误、12 跳过**，BUILD SUCCESS。Rust/JNI 未变，沿用同源码 debug/release 385、诊断 387 和 JVM 冒烟结果。copy-failure.log 不上传，无真实云验收结论。


### 2026-10-17：下载移动读取失败保留源

实际 Windows Hadoop LocalFileSystem 写入在请求远端数据前因缺少 HADOOP_HOME/winutils 失败，原始测试未能验证目标故障场景。改用测试专用 LocalFileSystem 包装器，仅覆盖 RawLocalFileSystem.setPermission 为 no-op，保持 Hadoop FileSystem.copyToLocalFile/FileUtil 和实际本地文件 I/O；该替身只通过本测试实例配置启用，不修改生产实现或全局配置。

普通桶/PFS 使用 4 字节分段，在第二段 GET 注入三次 503 耗尽初始请求及两次重试，断言故障计数耗尽且远端源完整保留。清除故障后同实例重试移动成功，本地内容完整且远端源删除。临时文件仅在项目 target 下建立并清理。失败时本地目标允许部分写入，不承诺回滚；真实 Windows 本地后端及权限传播仍待环境具备后验证。

Hadoop 完整回归 **330 项、0 失败、0 错误、12 跳过**，BUILD SUCCESS。本批仅测试/文档，Rust/JNI 沿用同源码 debug/release 385、诊断 387 和 JVM 冒烟结果。download-failure* 日志不上传。


### 2026-10-17：根目录移动到本地在复制前拒绝

Hadoop 默认 copyToLocalFile 忽略 FileUtil.copy 的布尔结果，而 Rust delete("/", true) 固定返回 false。旧实现在根目录移动回归中未抛 IOException，不能据此报告根目录已经移动。适配器现在在 deleteSource=true 时先查询 Rust 返回的源状态，若状态路径为根目录则直接拒绝，不先写本地目标；普通 copyToLocalFile(deleteSource=false) 保持原流程。

普通桶/PFS 测试覆盖根路径、归一化的 /unused/..、moveToLocalFile 便捷重载均在本地目标出现前报错且远端对象保留；普通复制根目录成功并保留源。目标仍使用上一批的测试专用免 chmod LocalFileSystem，未改变真实 Windows 权限能力边界。Hadoop 完整回归 **331 项、0 失败、0 错误、12 跳过**，BUILD SUCCESS。该修复只处理根目录，非根目录移动仍沿用复制/删除的非事务语义。

Rust/JNI 未变，沿用同源码 debug/release 385、诊断 387 和 JVM 冒烟结果。root-local-move-* 日志与项目 target 临时文件不上传。


### 2026-10-17：本地移动源删除失败不再误报成功

Hadoop 默认单源/多源 copyFromLocalFile 丢弃 FileUtil.copy 的布尔返回值。测试专用 LocalFileSystem 令本地删除返回 false，旧实现新增回归未抛 IOException。适配器现在覆盖两个重载，继续委派同一 Hadoop FileUtil 完成复制；false 转成 IOException，其他异常原样传播。不是重新实现上传或本地文件算法，也不删除或回滚已上传对象。

回归覆盖单源、数组入口及 moveFromLocalFile 两个便捷重载：上传完成但本地源删除失败必须报错，两端内容仍可读取；deleteSource=false 的普通复制不受影响。本地删除 false 由夹具注入，不声称验证真实操作系统权限拒绝场景。既有单 PUT 提交故障、本地源保留、目标冲突及两桶正常复制回归继续通过。

Hadoop 完整回归 **332 项、0 失败、0 错误、12 跳过**，BUILD SUCCESS。多源/目录复制仍可能部分完成，不具备事务性。Rust/JNI 未变，沿用同源码 debug/release 385、诊断 387 和实际 JVM 冒烟结果。copy-result-* 日志和 target 临时文件不上传。


### 2026-10-17：下载移动检查远端删除布尔结果

此前根目录保护之外的 copyToLocalFile 仍由 Hadoop 默认实现忽略 FileUtil 返回值。现在保留根目录预检，按 useRawLocalFileSystem 选择相同本地目标后委派 FileUtil，并在 false 时抛 IOException，避免将未完成的移动报告为成功。

测试夹具针对指定 PFS 文件的目录键/文件键 DELETE 都返回 NoSuchKey，同时保留对象，以确定性触发 Rust dual_key_delete 的 false 分支；这是人工故障响应，不代表正常服务会如此保留对象。旧实现新增测试未抛错。修复后 raw 与带校验和本地目标均报告失败，断言确实出现两次删除响应、本地内容完整、模拟远端对象保留；恢复正常删除后重试成功。目标仍使用测试专用免 chmod LocalFileSystem，不覆盖真实 Windows 权限传播。

Hadoop 完整回归 **333 项、0 失败、0 错误、12 跳过**，BUILD SUCCESS。复制与删除仍非事务性，报错不回滚本地文件，也不能推断真实远端对象仍然存在。Rust/JNI 未变，沿用同源码 debug/release 385、诊断 387 和 JVM 冒烟结果。download-result-* 日志不上传。


### 2026-10-17：明确选择 debug/release 验证库

项目 build-windows.ps1 新增 -Release，普通构建与测试均使用 --locked；默认仍为 debug，既有基准继续使用 release。test-jni.ps1 新增受限的 -Profile debug/release 参数，运行前检查对应 DLL 并输出库目录。Maven 用 obs.native.profile（默认 debug）及可覆盖的 obs.native.library.path 选择原生库；Surefire 参数对库路径加引号，避免带空格的目录被拆分。

实际运行新脚本的 release workspace 构建和测试：**385 通过、0 失败、16 ignored**；release JNI 基础/异常冒烟通过。Hadoop release 全套 **333 项、0 失败、0 错误、12 跳过**，并逐一解析 16 份 Surefire XML，确认 java.library.path 都解析到 target/release。默认 debug 构建（SkipTests）及 JNI 两类冒烟通过；额外 3 项 NativeConfigurationTest 验证 Maven 默认路径为 target/debug，另 3 项验证带空格的自定义路径实际生效。

此批不修改 Rust/JNI 或 Hadoop 生产算法，不宣称跨平台验收。Windows linker 将正常创建导入库/对象的 stdout 标成 linker_messages 警告，构建退出成功；本地下载目标仍有前述 winutils 权限验证限制。validation-profile-* 日志、路径核验 JSON 和复制到 target 下的验证 DLL 均不上传。


### 2026-10-17：JNI 脚本避免覆盖 PowerShell 自动变量

将 JNI 冒烟脚本的参数变量改为 NativeProfile，保留 -Profile 作为别名，避免与 PowerShell 自动变量 $PROFILE 同名。库存在检查要求实际文件。通过 -NativeProfile release 和 -Profile debug 分别运行实际 JVM 基础/异常冒烟，均通过且输出正确库目录。仅脚本与文档改变，未重复运行未变的 Rust/Hadoop 回归；沿用上一批 release Rust 385、Hadoop 333 的完整验证。native-profile-name-* 日志不上传。


### 2026-10-17：全目标静态检查与诊断测试锁作用域

执行 cargo clippy --locked --workspace --all-targets --all-features，退出成功，初次输出 17 条诊断（包含 lib/test 重复项）。其中 await_holding_lock 指向追加容量诊断测试；代码已显式 drop MutexGuard 后再 await，不是已确认的运行时死锁。将该测试的锁与断言改为词法作用域，明确在后续异步 LIST 前结束 guard 生命周期，生产代码不变。

对应 all-features 文件系统测试 **1 通过、0 失败**；再次完整 Clippy 检查退出成功，await_holding_lock 提示消失，剩余 16 条为 manual_map、len_without_is_empty、参数数量、类型复杂度及测试布局等提示，未为清零提示进行大范围改写。本次不是无警告门禁，也不是没有其他缺陷的证明。未重复执行未变生产代码的完整回归，沿用 release Rust 385、Hadoop 333 与 debug/release JNI 冒烟结果。clippy-audit*、diagnostic-lock-scope-* 输出仅保存在本地。


### 2026-10-17：定位读取失败后的顺序位置恢复

补充实际 Hadoop FSDataInputStream → JNI → Rust → 本地 HTTP 的两项回归，均覆盖普通桶与 PFS。指定偏移 GET 连续 503 耗尽初次请求和两次重试后，定位 readFully 抛 IOException，顺序位置保持不变；清除故障后顺序读取与定位读取内容正确。另覆盖定位 readFully 越过 EOF、定位 read 返回 EOF、负位置和无效缓冲区范围，断言原位置与后续顺序读取仍正确。

现有 Hadoop 继承实现通过这些检查，未改生产逻辑。完整 Hadoop 回归 **335 项、0 失败、0 错误、12 跳过**，BUILD SUCCESS；使用默认 debug JNI 库。Rust/JNI 生产源码未变，沿用此前 release Rust 385 和 debug/release JVM 冒烟结果；本批未重跑 release Hadoop。positioned-read-regression.log 仅保存在本地。


### 2026-10-17：本地临时输出提交入口与标准 javac 构建

新增 completeLocalOutput 回归：普通桶/PFS 上传提交前 503 后本地暂存文件保留，恢复后重新提交成功且移除暂存文件；普通桶被文件遮住的目标拒绝提交，暂存文件和两个原始对象均保留。startLocalOutput 仍按 Hadoop 默认行为返回给定临时路径，不提前上传；completeLocalOutput 委派 moveFromLocalFile，沿用已接入的 Rust 检查，未改生产逻辑。

ECJ + debug JNI 完整 Hadoop 回归 **336 项、0 失败、0 错误、12 跳过**。随后不用 ecj profile，使用 JDK 17 标准 javac 执行 Maven clean test，从干净的 Hadoop 模块 target 构建，release JNI 完整回归同为 **336 项、0 失败、0 错误、12 跳过**。解析全部 16 份 Surefire XML，确认实际 java.library.path 为 target/release。默认编译器存在 source 8 bootstrap class path 和既有 deprecated API 提示，不是无警告构建。

此结果验证本机默认 Maven 编译路径，不代表 GitHub 托管 runner 或真实 OBS/PFS 已验收。Rust/JNI 生产源码未变；工作流仅在工作区保存待确认草案，未随本批上传。complete-local-output-* 日志及核验 JSON 不上传。


### 2026-10-17：目录下载失败释放游标，拒绝创建子目录时保留源

核对 Hadoop 3.3.6 FileUtil 实际字节码并复现两条问题。其目录递归复制在子文件抛错后没有关闭尚未遍历完的 RemoteIterator；新增测试旧实现仍留下 ListingCleanup 登记。下载现在使用只管理该次复制游标的 FilterFileSystem 作用域，正常 EOF 及时移除并关闭游标，复制退出时释放剩余游标，不关闭原文件系统。普通桶/PFS 的嵌套目录 GET 故障测试覆盖两层未完成分页游标，并确认 owner 仍可使用。GC 兜底仍用于其他调用方放弃的游标。

更严重的是 FileUtil 忽略递归子复制返回的 false。本地目标子目录 mkdirs 返回 false 的模拟回归中，旧流程将未复制的 3 个远端对象全部删除。下载目标现在用薄包装将 mkdirs false 转为 IOException，阻止继续执行源目录删除。回归覆盖普通桶/PFS × raw/带校验和本地目标四种组合，未复制源对象全部保留。夹具明确覆盖 RawLocalFileSystem 的 mkdirs 两种重载，不依赖真实主机权限拒绝。

修复后标准 JDK 17 javac 干净构建，release JNI 完整 Hadoop 回归 **338 项、0 失败、0 错误、12 跳过**；核验 16 份 XML 均使用 target/release。此前游标修复阶段 debug 完整回归 337 项通过，mkdir false 修复 focused debug 回归通过；最终完整结果以上述 release 338 为准。Rust/JNI 生产代码未改，未重复核心构建。

本次不是事务性目录移动：更早成功复制或删除的兄弟文件不回滚，失败后可能留下本地部分结果。FileUtil 其他布尔失败分支仍需继续核对。directory-download-*、directory-mkdir-*、directory-copy-* 日志与核验文件仅保存在工作区，不上传。


### 2026-10-17：退出删除登记与取消统一根路径别名

Hadoop Path/URI 归一化保留超过根目录的前导 /..，Rust path_to_key 则在根处停止向上遍历。旧实现用 /../../exit-alias 登记 deleteOnExit 后，用 /exit-alias 取消返回 false；两者实际操作同一个 Rust 文件。登记及取消现在使用同一词法路径标识，先验证文件系统归属、解析当前工作目录，再按 Rust 既有规则处理点段并限定 URI。不增加状态查询，也不改变 Rust 文件操作语义。

新增普通桶/PFS 两向回归：根路径别名登记可用规范路径取消，规范路径登记可用别名取消，关闭文件系统后内容仍保留。既有相对路径、工作目录变更和不同 URI 形式取消测试继续通过。完整 Hadoop debug 回归 **339 项、0 失败、0 错误、12 跳过**；解析 16 份 XML 确认 target/debug。此批未改 Rust/JNI，未重跑 release；上一批 release 338 为独立历史结果。exit-alias-* 日志与核验文件不上传。


### 2026-10-17：带选项重命名识别同一路径别名

带 Options.Rename 的 Hadoop 重命名要求拒绝相同源/目标。旧 Java 检查仅比较原始路径字符串，对 /../../same-file 与 /same-file 的 OVERWRITE 调用未抛错，Rust 将其识别为同一文件并返回成功 no-op。现在在已存在的源/目标状态查询之后比较 Rust 返回的规范路径，统一抛 FileAlreadyExistsException，不增加额外查询。不改变无选项布尔 rename 或 Rust 核心同文件 no-op 的既有语义。

普通桶/PFS 回归覆盖文件两向根路径别名和目录别名，内容与对象数量不变；旧实现新增测试未抛异常。完整 Hadoop release 回归 **340 项、0 失败、0 错误、12 跳过**，16 份报告确认 target/release；包含此前目录下载与退出删除修复。Rust/JNI 未修改，未重复核心构建。rename-alias-* 本地日志与核验 JSON 不上传。


### 2026-10-17：服务名使用桶身份，避免默认 DNS 解析失败

Hadoop 默认 getCanonicalServiceName 经 SecurityUtil.buildDTServiceName 将 URI host 当作网络主机。普通本地桶名 bucket 在新增测试中触发 IllegalArgumentException(UnknownHostException)，即使实际 HTTP endpoint 为本机地址。适配器现在对已初始化实例直接返回 obs://桶名，与仓库原 Java 参考实现没有 token manager 时的实际代码路径一致，不尝试解析桶名的 DNS；未初始化实例返回 null。原参考代码的旧注释测试与实际实现不同，本次以实际实现为依据，没有启用旧 getcanonicalservicename 配置开关。

普通桶/PFS 回归验证稳定服务名、getDelegationToken 仍为 null、addDelegationTokens 返回空数组且不写入凭据，并且不发对象 HEAD。该修复只是服务身份接线，不实现委托令牌、认证刷新或安全隔离。完整 Hadoop debug 回归 **341 项、0 失败、0 错误、12 跳过**；16 份报告均为 target/debug。Rust/JNI 未改，最近独立 release 结果为上一批 340，本批未重跑 release。service-name-* 日志与核验 JSON 不上传。


### 2026-10-17：无 scheme 路径仍检查桶 authority

Hadoop 默认 checkPath 对 scheme 为 null 的路径直接返回，包含 //other/file 这种 authority 非空的路径。Java 后续将 getPath 传给 Rust，因此旧实现查询 //other/authority-file 没有报错，而是落到当前桶同名文件。这不是访问了另一个真实桶；缺陷是丢失调用方给出的桶归属。

适配器覆盖 checkPath，保留 Hadoop 原有检查，并在无 scheme、authority 非空时检查是否属于当前 URI。普通桶/PFS 回归验证异桶 authority 的状态、删除、重命名两个方向及工作目录变更均在进入 JNI 前拒绝；同桶 //bucket/file 继续有效，原对象内容与数量不变。未改变普通相对路径、绝对路径和完整 URI 的既有规则。

完整 Hadoop release 回归 **342 项、0 失败、0 错误、12 跳过**，16 份 XML 确认 target/release。Rust/JNI 未改，未重复核心构建；上一批独立 debug 结果为 341。authority-path-* 日志与核验 JSON 不上传。


### 2026-10-17：Java 8 API 编译基线

仅 source/target=8 约束语法和 class 版本，不能阻止新 JDK 编译时引用 Java 8 不存在的 API。先用 JDK 17 标准 javac 显式 --release 8 干净构建并运行 release JNI Hadoop 全套：**342 项、0 失败、0 错误、12 跳过**，未发现当前源码引用较新 API 的问题。

POM 新增自动 java8-api profile，在 JDK 9 及以上设置 maven.compiler.release=8；JDK 8 的配置分支保持原 source/target=8。无显式 release 命令参数下，标准 javac 与可选 ECJ 分别 clean test-compile 成功，两个日志均确认生产/测试使用 release 8。最终 ECJ 生成的 39 个项目 class 文件逐一核验 major=52。无新增编译依赖。

这是 API/字节码兼容检查，不是实际 Java 8 JVM 运行验收；本机仍使用 JDK 17，独立 JNI 冒烟脚本仍按 README 要求使用 JDK 17+。POM 自动激活后只补编译检查，完整 342 项运行证据来自相同 release 8 参数的前置验证，未将编译成功记为再次测试通过。java8-api-* 日志和核验 JSON 不上传。


### 2026-10-17：Rust 原生 skip 避免读取丢弃的数据

Hadoop NativeInput 原先继承 InputStream.skip，逐段读取后丢弃内容。本机 HTTP 回归使用 4 字节读取块，skip(8) 旧实现触发 2 次 GET，新增测试失败。现在 Rust ObsInputStream::skip 按打开时长度限制前进量，复用 seek 的缓存、取消恢复和预读处理；JNI 新增 nativeSkip，两份 Java ABI 声明同步更新，Hadoop 仅委派并更新位置。零/负跳过在 Hadoop 层检查流状态后直接返回 0。

同一回归普通桶/PFS 的 skip(8) 均为 0 次 GET，后续读到正确字节；覆盖 Long.MAX_VALUE 到 EOF、EOF 后继续跳过、seek 超过 EOF、零/负值与 skipBytes。Rust 新增 3 项覆盖 u64 极值不溢出、取消半途读取后基于恢复位置跳过、跳至 EOF 取消预读并归还并发许可。既有 seek 版本错误及范围复用测试仍通过。独立 JVM 冒烟覆盖新的 nativeSkip，并确认对输出流调用被拒绝后仍可正常写入。

显式重建 debug/release JNI 库；Rust workspace 两种配置均 **388 通过、0 失败、16 ignored**，append-diagnostics **390 通过、0 失败、16 ignored**。Hadoop debug/release 各 **343 项、0 失败、0 错误、12 跳过**，分别核验 16 份 XML 的库路径；两种库的基础/异常 JVM 冒烟均通过。新的 Java 调用需要配套重建的 JNI 库，不能只替换 Java 类。

这是确定性的 GET 请求数对照，不是吞吐或 p99 基准。与 seek 相同，跳过区间不下载也不验证内容；后续实际读取继续遵循既有版本检查，长度使用打开时快照。skip-* 日志、汇总 JSON 和 DLL/class 仅保留在工作区，不上传。

### 2026-10-17：显式 ByteBuffer 读取

新增 Hadoop ByteBufferReadable 与 JNI nativeReadDirect。可写 heap 缓冲复用 byte[]；direct 缓冲由 Rust 写入调用方区域，省去 JNI 临时 Vec 和末次数组复制。仅声明 in:readbytebuffer，不声明 positioned/vectored/增强零拷贝能力；错误时恢复流与缓冲位置，内容可能部分覆盖。JNI 自行校验类型、只读和容量，单次最多 16 MiB。

两次独立七轮缓存对照共 84 条记录，所有测量阶段 GET=0。1 MiB 每对吞吐方向一致，但 1 B/4 KiB direct 更慢，因此不替换原 byte[] 路径。方法、线程 CPU、分位数和限制详见 [JNI_READ_BUFFER.md](JNI_READ_BUFFER.md)；可复现源文件/脚本属于项目，原始 CSV 和产物不上传。

显式重建 debug/release JNI；Rust workspace 各 **388 通过、0 失败、16 ignored**，实际 JVM 基础/异常冒烟均通过。Hadoop 两种库各 **347 项、0 失败、0 错误、12 跳过**，分别校验 16 份 XML 的库路径。新增四项 Hadoop 用例验证普通桶/PFS 的 heap/direct 切片、EOF、错误参数、关闭和中途错误恢复；JNI 冒烟另验证 16 MiB 截断与余量、非法句柄及边界。Clippy 全目标/全特性保持既有 16 项告警，无新增。append-diagnostics 本批未重跑，上次结果仍为 390/0/16。direct-buffer-* 日志、核验 JSON、DLL/class 只在工作区。

### 2026-10-17：Hadoop 缓冲池继承回退

核对实际 Hadoop 3.3.6 字节码，FSDataInputStream 的池式读取在检测到 ByteBufferReadable 后申请 direct 缓冲，并通过 ByteBufferUtil 回退，不代表支持增强零拷贝。新增两项普通桶/PFS 回归确认小池容量、部分 EOF、EOF/GET 错误/关闭文件系统时归还，成功后的显式归还、重复归还拒绝及流关闭后归还；无池仍明确拒绝。成功返回缓冲后 close 不代替 releaseBuffer，文档已明确。

完整 release Hadoop **349 项、0 失败、0 错误、12 跳过**，16 份 XML 核验 target/release。仅新增测试和文档，生产代码未改，未重复 Rust/JVM；debug 上批为 347。小块池式读取现在选择 direct，可能承担已有微基准中的固定开销，不将 byte[] 路径保持原样误写成所有入口无回退。pooled-buffer-* 日志和核验结果只留本地。

### 2026-10-17：目录上传与多源部分失败

新增两项普通桶/PFS 回归：嵌套本地目录复制后源仍存在，移动后源树移除且目标内容完整；三源上传中第二项 PUT 失败时整批抛异常，第二项本地源保留，第一/第三项复制移动完成，恢复后可单独补传失败项。这遵循 Hadoop FileUtil 非事务行为，不承诺批次原子性或回滚。

初次完整运行 351 项中目录测试报 Windows NativeIO.access0 缺失，发生在 RawLocalFileSystem 目录列举、进入 Rust 上传之前。新增测试专用 NioListingLocalFileSystem，仅用 NIO DirectoryStream 替代本地列举；文件打开、状态、删除与 Rust 上传仍走原路径。定向测试通过后完整 release **351 项、0 失败、0 错误、12 跳过**，16 份 XML 确认 target/release。不能据此声称 Windows Hadoop 原生目录权限/NativeIO 已验收。

生产代码未改，Rust/JVM 未重复；debug 仍为上一批 347 项。multi-source-hadoop-release.log 保留初次环境失败，multi-source-hadoop-release-verified.log 为最终通过记录；日志、NIO 临时目录与产物不上传。

### 2026-10-17：直接缓冲的并发与固定版本

实际 JVM 冒烟用八线程绕过 Hadoop Java 流锁，直接共享一个 JNI 输入句柄，每线程独占目标缓冲；200 个标记记录验证不重不漏、不交错、最终 EOF，等待线程完整退出。debug/release 两种库均通过基础、并发和错误分类冒烟；这是正确性测试，不是多线程性能基准。

Hadoop 新测试先通过 HEAD 固定普通桶 ETag，再覆盖相同长度对象；首次 GET 与后续 GET 两分支均验证 RemoteFileChanged、流/缓冲位置不变以及 seek 后持续失败、不再 GET。首版用例错误地把写后无 ETag 的缓存当作已有 HEAD 修订，已修正前置条件；生产仍允许缺少初始 ETag 时由首次 GET 固定版本，PFS 不承诺对象快照。

定向通过后完整 debug/release 各 **352 项、0 失败、0 错误、12 跳过**，16 份 XML 分别核验对应库目录。仅新增测试与说明，Rust/生产代码未改；上次 Rust 两配置仍为 388/0/16。direct-concurrency-hadoop-release.log 为初次前置条件失败，带 verified 后缀为最终 release 通过，其他日志与产物同样不上传。

### 2026-10-17：百万条目分页工作集与 CPU

先重跑原有分页基准，30 条默认记录的条目/请求断言通过。随后只改 ignored 基准：可配置 entries/trials/eager/paged，记录 HEAD 数，并将非整页 LIST 数断言改为向上取整；1001 条、两种模式各一轮验证两次 LIST。新增 Windows 独立进程采样脚本，先用一万条目验证，再对十万/百万条目分别正序、反序运行，八个进程、24 条正式记录，预热另计。

百万条全量/分页观测工作集分别约 465.5/110.7–111.0 MiB，分页首条约 9.6–13.1 ms，而完整扫描均约 16 秒。包含预热与析构的进程 CPU，分页约 17.3–17.6 秒、全量约 7.7–8.0 秒，不能以总墙钟接近推导 CPU 等价。完整方法、十万条结果、计时边界与复现命令见 [LISTING_SCALE.md](LISTING_SCALE.md)。模拟等待和 Windows 调度影响明显，不是云端/HTTP/Hadoop 或 Linux RSS 验收。

测量结束后 Rust release workspace **388 通过、0 失败、16 ignored**。生产逻辑未变，未重复 debug、JVM、Hadoop；它们上批结果分别仍为 Rust 388/0/16、两种 JNI 冒烟通过、Hadoop 两配置各 352/0/0/12。PowerShell 脚本已实际运行和语法解析，拒绝混用旧输出目录；最后增加重复条目规模的前置拒绝以免日志互相覆盖。listing-memory-* 原始记录、采样输出、二进制和补丁只留工作区。

### 2026-10-17：无等待与持续预热对照

新增可配置 LIST 等待和持续预热，正式记录同时包含参数，采样脚本拒绝不匹配的二进制输出。新增普通桶/PFS 页面顺序与目录别名回归，以及独立集合分配/插入/析构探针。最终同配置基线/候选的 80 条大目录记录、1600 条预热小目录记录未证明单页借用 HashSet 的稳定全路径收益，已撤回该生产优化；方法与结果见 [LISTING_SCALE.md](LISTING_SCALE.md)。一次提前复制旧二进制的预备测量已作废，不计入结果。

恢复原实现并显式构建两种 JNI DLL 后，Rust workspace debug/release 各 **389 通过、0 失败、16 ignored**，append-diagnostics **391/0/16**。实际 debug/release JVM 基础、并发和异常冒烟通过；Hadoop 两种库各 **352 项、0 失败、0 错误、12 跳过**，分别核验 16 份 XML 的原生库路径。Clippy 全目标/全特性仍为既有 16 项告警，无新增。生产 fs.rs 与已发布基线逐字节一致；只发布测试、复现工具和文档。listing-probes-* 日志、listing-page-* 冻结程序、哈希清单与原始数据仅保留本地。

### 2026-10-17：Hadoop 继承查询的服务型 404

模拟 LIST 可注入带请求 ID 的 NoSuchBucket/NoSuchUpload。新增一个普通桶/PFS 集成用例，遍历 exists、isFile/isDirectory、精确/通配 glob、getUsed、默认 getContentSummary、resolvePath、Path 版块位置、access、多路径 listStatus：错误必须保留 IOException 分类、服务码和请求 ID，每次重新到达后端；恢复服务并创建同名对象后立即可见，不被负缓存遮挡。另验证真实缺失仍返回 false/null。生产代码无需修改。

定向用例通过后，完整 release/debug 各 **353 项、0 失败、0 错误、12 跳过**，分别核验 16 份 XML 的实际 JNI 库路径。Rust 和独立 JNI 未重复，上一批 389/0/16、诊断 391/0/16、两种 JVM 冒烟通过仍有效。另核对本地 Hadoop 3.3.6 字节码，文档明确 getStatus 仍是通用容量占位值，不代表云桶配额/可用性；尚未接入真实容量服务。inherited-service404-* 日志和报告只留本地。
