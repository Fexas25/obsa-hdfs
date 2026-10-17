# 原始 API 与配置覆盖核对

更新：2026-10-17。依据 PROMPT_NEXT_AI_APPENDIX.md 的 A/G 两节及当前 Rust、JNI、Java 源码。这里的“接通”只表示有实现调用链；本地契约通过不等于真实服务、全部重载或原 Java 扩展全部兼容。未对第三方 Hadoop 默认实现作完整源码审计。

## 原始 35 项方法

1. initialize：Java 配置及凭据解析 → nativeCreate → Rust；MemArtsCC 初始化未接入。
2. getScheme：Java 常量 obs，已实现。
3. getUri / getDefaultPort：URI 已实现；defaultPort 未单独覆盖，继承行为未专项验收。
4. 工作目录：Java 解析相对路径，Rust 也有路径归一化；当前不是共享一份工作目录状态，需核对直接 Rust 调用与 JNI 一致性。
5. getHomeDirectory：继承 Hadoop 3.3.6 当前 UGI 短用户名语义；初始工作目录已改为初始化时 UGI 的短用户名。普通桶/PFS 的 remote/proxy 用户 doAs、相对路径读写、工作目录切换已专项验证；切换调用用户不重置已有工作目录，home 随当前调用用户变化。此项不是服务端权限隔离或 Kerberos 认证验收。
6. open(Path,int)：JNI → Rust 输入流接通；缓存、版本检测、预取已测。
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
23. getContentSummary：开关开启时 Rust 平铺 LIST 逐页统计；默认关闭时使用 Hadoop super 目录遍历，桶级覆盖由 Rust 解析，已验证两种模式的结果与请求类型。
24. setPermission：Hadoop 入口现在仅 PFS + permissions.mode=disguise 写 permission 十进制字符串元数据；none/普通桶 no-op。独立 Rust set_permission 仍更新数值 mode，不能混为一谈。
25. setOwner：Java/JNI → Rust set_hadoop_owner 已接通名称元数据；PFS disguise 模式写 user/group，一个 null 保留对应字段，两个 null 拒绝；none/普通桶 no-op。当前仅非空可打印 ASCII，Unicode 编码和服务端 ACL 尚未实现。独立 Rust set_owner 保持数值 uid/gid 语义。
26. getFileBlockLocations：未单独覆盖 Java；本轮已验证单文件继承块位置的 offset/length，以及 Rust 状态与默认逻辑块大小配置一致性；完整继承语义仍待核对。
27. getFileChecksum：两种重载现在通过 Rust 校验路径，负长度拒绝；有效路径仍返回 null 表示没有 Hadoop 兼容校验和，不把 ETag 当 MD5。
28. xattrs：未接通。
29. copyFromLocalFile：覆盖单源/多源重载并委派 Hadoop FileUtil，false 结果转为 IOException，避免本地源删除失败仍报告移动成功；本地已验证单文件普通桶/PFS 复制和移动，以及目标隐藏/禁止覆盖时保留本地源与原对象。单 PUT 提交前/后 503 均验证保留本地源，恢复后可重新移动；目录、多源、MPU 及网络中断的完整兼容性仍待专项验证。
30. close：Hadoop deleteOnExit 在释放原生句柄前处理，仍经过 Rust 冲突检查，注册/取消统一按调用时工作目录限定路径，并与 Rust 一致处理超过根目录的点段别名，工作目录切换不改变既有删除目标；取消注册与重复关闭已本地验证；JNI 文件系统/流清理已接通；列表游标新增 EOF/错误/显式释放/所属 FS 关闭清理。MPU 远端清理仍 best effort。
31. delegation tokens：未接入原 Java token manager。
32. checkPermission / AuthorizeProvider：未接入原 Java 扩展。
33. MemArtsCC：未接入；这是原规格允许保留 Java 的 Hybrid 能力，不能据此声称当前自动具备。
34. getCanonicalServiceName：未接入原配置开关，继承行为未专项验收。
35. metrics / traffic / audit：Rust 有可选请求日志，Java 流有 bytesRead/bytesWritten 统计；原 metrics consumer、audit、完整 IOStatistics 未对齐。

## 其他继承入口

- copyToLocalFile：委派 Hadoop FileUtil 并将 false 转成 IOException，避免远端删除未完成仍报告移动成功；deleteSource=true 时先依据 Rust 状态拒绝移动根目录，普通复制根目录仍允许；本地测试通过第二段 GET 耗尽重试验证移动失败保留远端源、恢复后重试成功；目标为测试专用免 chmod 的 LocalFileSystem，未验收真实 Windows winutils/权限传播。 下载作用域主动释放中断递归复制遗留的分页游标；本地 mkdirs false 立即报错，避免 Hadoop 忽略子目录复制失败后删除未复制源。目录移动仍非事务性。

- globStatus、多路径 listStatus 与 PathFilter、getUsed：已验证普通桶隐藏后代不出现在结果/用量中，显式隐藏路径被拒绝；glob 的无匹配返回遵循 Hadoop 语义。

- setReplication：校验正数参数并通过 Rust 查询路径后返回 false，不宣称改变 OBS 副本策略。
- setTimes：先通过 Rust 查询路径；两个参数均为 -1 时不修改，否则明确 UnsupportedOperationException。要求保留时间的上层复制任务应处理此能力限制。

## 配置核对

核心配置读取在 obs-rust-core/src/config.rs，Java 入口在 OBSFileSystemRust.initialize / resolveCredentials。附录 G.1 中 endpoint、AK/SK/token、桶模式、连接/超时/TLS、分片、上传并发、预读、缓存、分页、PFS append/inode、trash、寻址方式和访问日志有解析路径；解析路径存在不等于每种取值均通过运行验收。

已确认需要单独跟进：

- fs.obs.buffer.total.bytes 默认 0 关闭，现覆盖真实后端共享的 HTTP 帧、合并缓冲与读写流受控分配；Vec 按 capacity 计费，包含扩容峰值，HTTP→输入流不重复计费。外部帧不可见容量、TLS 内部等不计入，见 BUFFER_BUDGET.md。
- fs.obs.upload.journal.dir 是可选本地已知 MPU 日志目录，桶级覆盖已验证。Rust recover_pending_uploads 提供有界显式清理；仅对日志中的 MPU 发 abort。Hadoop recoverPendingUploads(limit) 已通过 JNI 接入 Rust，返回不可变清理报告；Hadoop abortUpload(output) 也已接入 Rust 流级 abort，仅支持本实例的创建流，拒绝 PFS 追加流，见 UPLOAD_RECOVERY.md。

- fs.obs.content.summary.enable 已生效：默认 false 走 Hadoop 默认目录遍历，true 走 Rust 平铺 LIST 统计。Java 只选择 Hadoop 默认兼容路径或委派 Rust；不重复实现汇总算法。配置在初始化时固定，支持桶级覆盖。
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
