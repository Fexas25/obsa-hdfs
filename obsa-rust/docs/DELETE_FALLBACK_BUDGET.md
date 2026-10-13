# 批量删除回退的响应预算释放

2026-10-13。批量删除收到 501 后，会回退到逐个删除。旧实现将已不用的 501 响应保留到回退结束；响应体持有共享 BufferBudget 额度，可能使后续删除响应因预算不足而失败。

修复在开始逐个删除前显式释放 501 响应。不改变何时回退、删除顺序、错误传播、重试策略或 HTTP 请求数量，也不提高预算上限。

## 本地证据

新增真实 ObsStore + 本机 HTTP 回归：共享预算 128 字节，501 响应体 128 字节，随后两个单独删除各返回 128 字节响应体。旧实现稳定返回 shared buffer budget exhausted；修复后三个请求按批量 POST、first DELETE、second DELETE 的顺序完成，无需扩大预算。

完整 Windows Rust workspace 构建与回归 **323 通过、0 失败、15 ignored**：核心 107、缓存 1、文件系统 153、HTTP 62。原有回退单项失败测试也包含在回归中。手动基准、JVM/Hadoop 和真实 OBS/PFS 本轮未运行。此证据证明无用响应体不再跨回退请求持有预算，不代表所有进程内存均被预算覆盖，也不宣称生产吞吐提升。

本地日志 delete-budget-before.log 和 delete-budget-after.log 留在工作区，不上传日志、图片或构建产物。


## 删除的 404 分类（2026-10-13）

直接普通对象删除和 501 批量回退原先会将任意 404 转成成功，包括 NoSuchBucket。现在只对明确的 404/NoSuchKey 保留幂等成功；NoSuchBucket、NoSuchUpload 和未分类 404 保留原始错误码及 request_id。PFS 直接删除仍保留 NoSuchKey 错误，批量回退允许对象在列举后已消失。

参考华为云 [DeleteObject](https://support.huaweicloud.com/api-obs/obs_04_0085.html)、[NoSuchBucket](https://support.huaweicloud.com/api-obs/obs_errorcode_0020.html) 和 [NoSuchKey](https://support.huaweicloud.com/intl/en-us/api-obs/obs_errorcode_0025.html)：桶缺失和对象缺失不能混为一谈。兼容性变化是没有 XML 错误码的 404 不再被此层默认视为成功。

两项本机 HTTP 回归覆盖直接/批量、普通桶/PFS，以及三种非对象缺失响应；验证错误码、请求 ID、请求数和正常 NoSuchKey 幂等行为。修复前失败用例得到 Ok，正常缺失对象测试已通过。修复后完整默认 debug workspace **340 通过、0 失败、15 ignored**（核心 110、缓存 1、文件系统 153、HTTP 76）。本地日志 delete-missing-before.log、delete-missing-after.log 不上传；未重跑 release/JVM/Hadoop/诊断特性或性能基准。

本轮范围是 ObsStore 的两个删除入口，没有改变 ObsError::is_not_found 的公共含义。上层文件系统仍存在按 HTTP 404 广泛分支的调用点，尚需逐项审核，不能据此宣称全链路已区分所有桶级和对象级错误。


## 上层文件系统与 PFS 的 404 分支（2026-10-13）

新增 ObsError::is_path_not_found：只接受 NotFound 且错误码为 NoSuchKey 或空值。保留 is_not_found 的既有 HTTP 状态分类含义。fs.rs 的 15 处路径控制流和 PfsOps 的 3 处直接 NotFound 匹配统一使用新判断，涉及状态/exists、创建、mkdirs、目录条目补查、父目录标记、删除及重命名。明确的 NoSuchBucket、NoSuchUpload、NoSuchCustomDomain 和其他未知非空错误码不再触发“路径不存在”分支。

空错误码的 404 仍保留历史行为：真实 HEAD 没有 XML 正文，当前传输路径不保证取得服务端细分错误码。因此本轮不能证明所有真实 HEAD 桶错误均可识别；合成根路径和已有缓存也不主动探测桶存在性。直接对象删除入口上一节的严格 404 策略保持不变。

新增 1 项分类单测与 4 项文件系统故障注入测试。覆盖六种公开操作、失败不落负缓存、目录标记回退、重命名目标/父目录/提交阶段，以及 PFS 双路径删除两阶段和 mkdirs。断言原始错误码与请求 ID、不继续复制或删除源文件、不在桶错误后递归创建目录。修复前复现桶错误被改写为 NoSuchKey、重命名误成功、PFS 删除返回 false；修复后完整默认 debug workspace **345 通过、0 失败、15 ignored**（核心 111、缓存 1、文件系统 157、HTTP 76）。正常路径缺失与既有并发/取消回归也通过。

本地日志 fs-missing-before.log、fs-missing-pfs-before.log、fs-missing-final.log 不上传。本轮未重跑 release/JVM/Hadoop、诊断特性及性能基准；没有真实 OBS/PFS 验收结论。
