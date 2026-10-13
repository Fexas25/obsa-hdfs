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
