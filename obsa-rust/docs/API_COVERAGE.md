# 原始 API 与配置覆盖核对

更新：2026-10-17。依据 PROMPT_NEXT_AI_APPENDIX.md 的 A/G 两节及当前 Rust、JNI、Java 源码。这里的“接通”只表示有实现调用链；本地契约通过不等于真实服务、全部重载或原 Java 扩展全部兼容。未对第三方 Hadoop 默认实现作完整源码审计。

HEAD 响应边界：无条件对象 HEAD 仅接受 200，缺少或无法解析 Content-Length 也会报错，不把未知长度当作空文件。Rust TCP 与实际 Hadoop/JNI 回归覆盖错误传播及恢复后重新查询/读取，详见 [GET_VALIDATION.md](GET_VALIDATION.md)。

## 原始 35 项方法

1. initialize：Java 配置及凭据解析 → nativeCreate → Rust；MemArtsCC 初始化未接入。
2. getScheme：Java 常量 obs，已实现。
3. getUri / getDefaultPort：URI 已实现；defaultPort 未单独覆盖，继承行为未专项验收。
4. 工作目录：Java 解析相对路径，Rust 也有路径归一化；当前不是共享一份工作目录状态，需核对直接 Rust 调用与 JNI 一致性。
5. getHomeDirectory：继承 Hadoop 3.3.6 当前 UGI 短用户名语义；初始工作目录已改为初始化时 UGI 的短用户名。普通桶/PFS 的 remote/proxy 用户 doAs、相对路径读写、工作目录切换已专项验证；切换调用用户不重置已有工作目录，home 随当前调用用户变化。此项不是服务端权限隔离或 Kerberos 认证验收。
6. open(Path,int)：JNI → Rust 输入流接通；缓存、版本检测、预取已测。skip/skipBytes 委派 Rust 定位，按打开时长度截断且不读取被跳过区间；EOF、溢出和取消恢复已有本地回归。ByteBufferReadable 支持可写 heap/direct 缓冲，声明 in:readbytebuffer；原生定位读取 read(long,...)（不加锁、不移动游标）与 Hadoop 3.3.5+ readVectored（合并、并发，声明 in:readvectored）已接通，见 POSITIONED_VECTORED_READ.md；尚未实现 ByteBufferPositionedReadable（为兼容 Hadoop 3.3 之前版本）和增强零拷贝读取，见 JNI_READ_BUFFER.md。
7. open(Path,jobConf)：原扩展重载未接通；openFile 的 status hints 尚未下传；已验证提示不会绕过 Rust 可见性检查或覆盖真实长度/内容。
8. create 基础重载：已接通 Rust 输出流；权限/overwrite 已传入，bufferSize/replication/blockSize 参数并未全部下传。
9. create flags：递归（含 ChecksumOpt 重载）、非递归及 primitiveCreate 统一校验 CREATE/OVERWRITE/APPEND 组合与存在性，追加走 Rust append；未实现的存储标志明确拒绝。ChecksumOpt 尚未下传，不代表实现自定义校验和。createFile/appendFile builder 已验证非法组合、被文件阻断的后代路径以及正常 PFS 创建追加。
10. createNonRecursive：布尔及 flags 重载接通，父目录校验已有测试。
11. append：PFS 接通，普通桶不支持；带 newBlock 重载仅接受 false，true 明确拒绝；Java 与 JNI 都取状态，通常由缓存消除第二次 HEAD。同实例 rename/回收站移动仅等待源/目标路径范围内的在途追加，无关路径可继续追加和重命名；追加/注册与 rename 之间的冲突按登记顺序处理，取消移除等待项，成功后更新活动追加流路径（含目录后代）；覆盖目标流或结果不确定时拒绝继续写入。确定性回归及 Hadoop 文件/父目录移动回归已通过；跨实例/带外 rename 尚不支持协调。
12. truncate：Rust PFS 分支接通，普通桶不支持。
13. exists：Rust 状态/缓存路径接通。
14. getFileStatus：Rust 接通，HEAD/LIST 顺序回退、缓存及目录识别已测。
15. listStatus：Rust 全量结果；额外提供 listStatusIterator，当前跨页状态已归 Rust 管理。 disguise 模式逐条 HEAD 补齐名称/权限，自定义元数据不由 LIST 返回；none 模式保留原路径。
16. listStatus(Path,recursive)：原 Java 扩展重载未暴露；不能把 Rust 递归列表当作这个重载已经兼容。
17. listFiles：Java 已覆盖并直接使用 Rust 文件游标。recursive=true 使用平铺 LIST，false 使用 delimiter=/，目录过滤与跨页去重在 Rust。Java 包装 LocatedFileStatus；默认模式不逐条 HEAD，disguise 仍按既有规则补元数据。旧 Rust 全量 Vec API 保留。
18. listLocatedStatus：已覆盖，使用 Rust 非递归分页游标，首屏之后按需获取，可显式 Closeable.close；保留文件和目录条目，目录不伪造块位置。
19. listLocatedStatus(filter)：Java PathFilter 在迭代时执行，跨页筛选并最多额外缓存一个匹配条目；回调异常释放游标，关闭或重建文件系统后拒绝缓存条目访问。过滤器不下沉 Rust。
20. delete：Rust 接通；PFS 双键、递归顺序、普通桶批删及回退已有测试；非事务操作。
21. rename：Rust 接通；PFS 原生操作，普通桶串行 copy/delete，非原子且源并发变更风险未解决。Java Options.Rename 重载已接管，文件 OVERWRITE 直接调用 Rust，不调用 Hadoop 默认的目标预删除流程；NONE 拒绝已存在目标，已有目录覆盖和 TO_TRASH 明确拒绝。
22. mkdirs：Rust 接通，两桶模式已有契约测试。primitiveMkdir(createParent) 先按工作目录限定路径，非递归校验父目录，根路径不查询 null 父目录；实际创建仍走 Rust。
23. getContentSummary：开关开启时 Rust 平铺 LIST 逐页统计；默认关闭时使用 Rust 逐目录遍历（保留 Hadoop 默认遍历规则），桶级覆盖由 Rust 解析，已验证两种模式的结果与请求类型。
24. setPermission：Hadoop 入口现在仅 PFS + permissions.mode=disguise 写 permission 十进制字符串元数据；none/普通桶 no-op。独立 Rust set_permission 仍更新数值 mode，不能混为一谈。
25. setOwner：Java/JNI → Rust set_hadoop_owner 已接通名称元数据；PFS disguise 模式写 user/group，一个 null 保留对应字段，两个 null 拒绝；none/普通桶 no-op。当前仅非空可打印 ASCII，Unicode 编码和服务端 ACL 尚未实现。独立 Rust set_owner 保持数值 uid/gid 语义。
26. getFileBlockLocations：未单独覆盖 Java；本轮已验证单文件继承块位置的 offset/length，以及 Rust 状态与默认逻辑块大小配置一致性；完整继承语义仍待核对。
27. getFileChecksum：两种重载现在通过 Rust 校验路径，负长度拒绝；有效路径仍返回 null 表示没有 Hadoop 兼容校验和，不把 ETag 当 MD5。
28. xattrs：与参考实现一致，仅支持目录的 "AccessLabel"（值为以 ";" 分隔的标签，读取时每个标签后带 ";"）。setXAttr/getXAttr/getXAttrs 接通 Rust → PFS `?x-obs-accesslabel`（PUT JSON {"accesslabel":[...]} / GET / DELETE，线格式对照 OBS Java SDK ObsFileService）；另增 removeXAttr、getXAttrs(names)、listXAttrs（参考实现为 Unsupported）。文件拒绝，根目录无标签且不可设置，其他名称拒绝；XAttrSetFlag 与参考实现一样被忽略。本地线格式/签名与 Hadoop 往返已测，未在真实 PFS 验证。
29. copyFromLocalFile：覆盖单源/多源重载并委派 Hadoop FileUtil，false 结果转为 IOException，避免本地源删除失败仍报告移动成功；本地已验证普通桶/PFS 单文件、嵌套目录复制和移动，以及目标隐藏/禁止覆盖时保留本地源与原对象。单 PUT 提交前/后 503 均验证保留本地源，恢复后可重新移动；三源中一个失败时整批报错，失败源保留，已成功项不回滚且其他项继续尝试。目录用例仅将本地目录枚举替换为测试专用 NIO，未验收 Windows NativeIO；MPU、网络中断及完整本地权限兼容仍待验证。
30. close：Hadoop deleteOnExit 在释放原生句柄前处理，仍经过 Rust 冲突检查，注册/取消统一按调用时工作目录限定路径，并与 Rust 一致处理超过根目录的点段别名，工作目录切换不改变既有删除目标；取消注册与重复关闭已本地验证；JNI 文件系统/流清理已接通；列表游标新增 EOF/错误/显式释放/所属 FS 关闭清理。MPU 远端清理仍 best effort。
31. delegation tokens：未接入原 Java token manager。
32. checkPermission / AuthorizeProvider：未接入原 Java 扩展。
33. MemArtsCC：未接入；这是原规格允许保留 Java 的 Hybrid 能力，不能据此声称当前自动具备。
34. getCanonicalServiceName：已初始化实例直接返回 obs://桶名，避免 Hadoop 默认将桶名当作主机解析 DNS；与参考实现无 token manager 时的实际路径一致。普通桶/PFS 服务名和空委托令牌枚举已本地验证；旧配置开关和 token manager 未接入。
35. metrics / traffic / audit：Rust 有可选请求日志，Java 流有 bytesRead/bytesWritten 统计；原 metrics consumer、audit、完整 IOStatistics 未对齐。

## 其他继承入口

- checkPath：补齐无 scheme 但有 authority 的路径检查，拒绝 //其他桶/file 被误当成本桶文件；同桶 authority-only 路径仍可使用。普通桶/PFS 的状态、删除、重命名和工作目录入口已验证。

- copyToLocalFile：委派 Hadoop FileUtil 并将 false 转成 IOException，避免远端删除未完成仍报告移动成功；deleteSource=true 时先依据 Rust 状态拒绝移动根目录，普通复制根目录仍允许；本地测试通过第二段 GET 耗尽重试验证移动失败保留远端源、恢复后重试成功；目标为测试专用免 chmod 的 LocalFileSystem，未验收真实 Windows winutils/权限传播。 下载作用域主动释放中断递归复制遗留的分页游标；本地 mkdirs false 立即报错，避免 Hadoop 忽略子目录复制失败后删除未复制源。目录移动仍非事务性。

- globStatus、多路径 listStatus 与 PathFilter、getUsed：已验证普通桶隐藏后代不出现在结果/用量中，显式隐藏路径被拒绝；glob 的无匹配返回遵循 Hadoop 语义。
- 服务型 404：普通桶/PFS 下，exists、isFile/isDirectory、精确/通配 glob、getUsed、默认 getContentSummary、resolvePath、Path 版块位置、access 和多路径 listStatus 均验证 NoSuchBucket/NoSuchUpload 继续抛 IOException，并保留错误码/请求 ID。错误不写入缺失缓存，恢复后同一路径可见；真实缺失仍遵循 false/null 约定。此测试故障发生在状态回退 LIST，不代表所有 HTTP 阶段都逐项验收。
- getStatus(Path)/getStatus() 尚沿用 Hadoop 3.3.6 的通用容量占位值（capacity/remaining 为 Long.MAX_VALUE、used 为 0），不查询云桶配额或路径，也不能用来证明服务可用。它与已接通 Rust/目录遍历的 getUsed/getContentSummary 不同；真实配额/容量接口未实现。

- setReplication：校验正数参数并通过 Rust 查询路径后返回 false，不宣称改变 OBS 副本策略。
- setTimes：先通过 Rust 查询路径；两个参数均为 -1 时不修改，否则明确 UnsupportedOperationException。要求保留时间的上层复制任务应处理此能力限制。

PFS 追加底层调用的是固定 position 的 `PUT ?modify`，不是普通桶 `POST ?append`。
参照[华为云修改写接口](https://support.huaweicloud.com/intl/zh-cn/ally-visitor-1-api-obs/obs_04_0092.html)，
该接口不要求普通桶 append 的 next-append-position 响应头。断连重放保持原位置、相同正文；
truncate 重放保持相同长度，rename/MPU 初始化与完成仍不自动重试。这里的幂等性针对相同定点操作，
不保证与外部并发写入隔离，也不能把重试成功解释为远端只执行过一次。

## JNI JSON 参数边界

`nativeCall` 的参数必须是 JSON 对象。显式 path/destination 必须是字符串，
recursive/files_only/overwrite/create_parent 必须是布尔值，permission 必须是 0..=u32::MAX 整数；
不再把错误类型当作默认值，也不把更大的权限值截断。marker、owner、group 允许省略或 null，
非 null 时必须是字符串。检查发生在对应文件系统操作之前，错误映射为 IllegalArgumentException。
这是传输类型校验，不替代 Rust 的路径可见性、权限模式或操作语义校验。

JNI `open` 在登记句柄前检查 Rust 输入流打开时取得的长度，超过 Java `long` 最大值
即抛 IOException，防止后续 read/skip 让 Hadoop 的位置回绕成负数。检查复用该流的
长度快照，不额外 HEAD；纯 Rust 输入流仍保留 u64 长度范围。元数据查询的 Java 数值
投影检查继续独立生效，低层 JSON 状态值本身并不缩窄成 long。

JNI 写入也在持有流锁时，以 Rust 已接受的输出位置检查整次 Java 调用的最终位置。
超过 `Long.MAX_VALUE` 则在复制数组、分批和提交前抛 IOException；之前接受的数据保留，
后续较短的合法写入仍可继续。Rust 的位置包含待提交数据，不是持久化确认；flush/close
错误处理保持原有语义。此限制属于 Java 表示边界，纯 Rust 写入仍按 u64 检查溢出。

省略字段保留已有默认值，包括省略 path 时的根目录；setPermission 仍必须显式传 permission。
未知字段不作为新的功能声明，未被操作消费的字段不进行全面 schema 校验。
Hadoop 包装层仍显式传入路径；低层 JNI 调用方应同样显式指定目标。
独立 Java 与 Hadoop 的 NativeObs 声明保持一致，均暴露 `nativeReleaseListing`；
低层调用方可以显式释放未消费完的游标，重复释放无副作用，释放后访问报错。

Rust create/mkdirs 将权限参数归一到 `0o7777`，保留权限和特殊权限位，忽略文件类型等高位。
create 的上传 mode 再补 `S_IFREG`，写后缓存使用同一权限值，避免完整 mode 污染缓存或类型元数据。
冷 HEAD 继续按相同掩码提取权限；这是状态与请求的一致性修复，不是新增服务端 ACL 能力。

## 配置核对

核心配置读取在 obs-rust-core/src/config.rs，Java 入口在 OBSFileSystemRust.initialize / resolveCredentials。附录 G.1 中 endpoint、AK/SK/token、桶模式、连接/超时/TLS、分片、上传并发、预读、缓存、分页、PFS append/inode、trash、寻址方式和访问日志有解析路径；解析路径存在不等于每种取值均通过运行验收。

已确认需要单独跟进：

- fs.obs.buffer.total.bytes 默认 0 关闭，现覆盖真实后端共享的 HTTP 帧、合并缓冲与读写流受控分配；Vec 按 capacity 计费，包含扩容峰值，HTTP→输入流不重复计费。外部帧不可见容量、TLS 内部等不计入，见 BUFFER_BUDGET.md。
- fs.obs.upload.journal.dir 是可选本地已知 MPU 日志目录，桶级覆盖已验证。Rust recover_pending_uploads 提供有界显式清理；仅对日志中的 MPU 发 abort。Hadoop recoverPendingUploads(limit) 已通过 JNI 接入 Rust，返回不可变清理报告；Hadoop abortUpload(output) 也已接入 Rust 流级 abort，仅支持本实例的创建流，拒绝 PFS 追加流，见 UPLOAD_RECOVERY.md。

- fs.obs.content.summary.enable 已生效：默认 false 走 Rust 逐目录遍历，true 走 Rust 平铺 LIST 统计。Java 选择 Rust 入口并校验 long 表示范围，不重复实现汇总算法。配置在初始化时固定，支持桶级覆盖。
- 凭据路径已接通：参考源码标准键 fs.obs.security.credential.provider.path，兼容附录别名 fs.obs.credential.provider.path。桶级优先、同级标准键优先；所选 OBS 提供器在全局提供器前，Configuration 副本确保不污染其他文件系统。仍为初始化快照，没有 STS 自动刷新。
- fs.obs.block.size 已映射到 Rust block_size，支持桶级覆盖和正整数/k/m/g；Java 默认块大小由 Rust 解析结果提供。根目录、HEAD/LIST、写后缓存均使用同一值。非法值回退 128 MiB；它是逻辑块大小，不改变上传分片。
- Rust list_cache_ttl_ms 字段存在，但没有独立 LIST 缓存消费路径；不要将其列为有效调优项。
- 附录 G.2 的 fast.upload、multiobjectdelete、readahead.policy/range、attempts.maximum、SSE、token、authorize、metrics、buffer.dir、旧 trash/list/permissions 配置没有完成逐键兼容；不能仅因 fs.obs.* 被传入 JSON 就视为支持。
- 新增的 retry.max.attempts、retry.backoff.ms、request.timeout.ms、HTTP 响应上限、PFS append 字节额度属于本实现参数，不能与旧 SDK 同名/近义配置混淆。
- 桶级覆盖在 Rust 配置解析前合并，凭据提供器也处理桶级键；未知配置当前没有全面拒绝或告警机制。

## 推进次序

先修复会误报能力或导致错误行为的接线与配置缺口，补相应端到端用例；数字所有者与 Hadoop 用户名的映射须有明确策略，不能随意 parse 失败回退。之后补递归分页、统一内存额度与安全 copy/delete 边界。性能工作以可复现的 release 基线为起点；真实桶和原 Java 同条件对照仍未验收。

原始 35 项在此均有记录，但配置 G.2 和继承行为的全面兼容审计仍未完成。本文件不能用来计算“项目完成百分比”。

元数据安全修复：Rust 数值权限/所有者更新先 HEAD 获取最新 x-obs-meta-*，保留无关字段后 REPLACE；修改权限保留类型位，不从状态缓存补写未修改字段。失败/取消时失效缓存。该路径新增一次 HEAD，尚无 HEAD/PUT 间并发更新的原子保证，也未承诺保留所有非用户系统头。

本轮 permissions.mode 支持 none/disguise（默认 none、桶级覆盖，初始化拒绝其他值）。名称及展示权限在 HEAD、普通列表、分页列表和递归列表转换中使用；缺失显式目录对象时允许目录列表回退默认展示。列表额外 HEAD 的开销仅在 disguise 模式发生。原 AuthorizeProvider、委托令牌、完整安全模型和实际云服务行为仍未验收。


## Hadoop 数值投影与汇总溢出（2026-10-18）

Rust 文件长度和容量汇总采用 u64，Hadoop 使用有符号 long。状态查询、普通/分页列表的长度及汇总字段，现在在 Java 边界检查整数类型与 0..Long.MAX_VALUE，超界抛 IOException，不再由 JsonNode.asLong 静默变成负数。合法 Long.MAX_VALUE 仍接受，Rust 的表示范围不收窄。块大小已由 Rust 解析为正 i64，沿用原有非法值回退。

Hadoop 3.3.6 默认 getContentSummary 的 long 累加没有溢出保护。关闭 content.summary.enable 时现委派 Rust summaryDirectory：以 get_file_status/list_status 逐目录遍历、显式工作栈和 checked_add 汇总；开启时仍使用现有平铺 LIST 汇总。两者都由 Rust 执行可见性与错误检查，Java 只检查最终数值能否表示。目录遍历与平铺的请求类型开关保留；不再逐目录往返 JNI，未据此宣称性能提升。新版 Java 适配层需配套新版原生库。

仅汇总结果可表示为 long 时返回 Hadoop ContentSummary；负数回绕和正数回绕均报错。逐目录路径仍会聚合单个目录的列表并保留待访问目录，不能宣称常量内存或并发修改下的快照统计。
