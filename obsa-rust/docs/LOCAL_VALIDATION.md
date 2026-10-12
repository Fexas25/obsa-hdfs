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
