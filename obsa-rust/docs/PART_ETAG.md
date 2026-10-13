# 分片上传成功响应的 ETag 校验

2026-10-12。本轮修复 UploadPart 收到 HTTP 200 但没有有效 ETag 时返回空字符串的缺口，不改变签名、重试策略或上传并发。

## 问题与修复

旧实现使用 resp.etag().unwrap_or_default()，缺失或空 ETag 会被包装为成功结果。空值随后可能进入 CompleteMultipartUpload 的段列表，让本应在分片阶段报告的异常推迟到合并阶段。

参考华为官方 [UploadPart API](https://support.huaweicloud.com/api-obs/obs_04_0099.html) 和[上传段说明](https://support.huaweicloud.com/sdk-java-devg-obs/obs_21_0616.html)：上传段响应返回 ETag，调用方保存它供后续合并使用。本轮在成功 HTTP 响应映射之后要求非空 ETag；缺失、空字符串、空引号或仅空白值返回 Io 错误，不能产生一个成功的空 ETag。HTTP 200 的缺失 ETag 不增加自动重试。

保留原有外层引号处理和大小写不敏感头部查询；非空 ETag 仍按不透明值传递，不假设其为固定长度 MD5，不额外改写内容。这不是所有 ETag 语法、重复头部或其它元数据的全面校验。

## 本地证据

新增三项 HTTP 回归：

- 缺失/空/空引号/仅空白 ETag 必须失败。修复前已复现 Ok(空字符串)，修复后四种响应均通过回归。
- 引号包围和未加引号的非空不透明值保持原值；覆盖混合大小写头名和特殊字符，不强加 MD5 格式。
- 通过真实 ObsStore 和 ObsOutputStream 执行初始化、上传尾分片、缺失 ETag、失败清理。断言请求顺序为初始化 POST、分片 PUT、abort DELETE，没有 complete 请求。失败结果保留 ETag 错误，上传任务名额全部可用；随后 128 字节 GET 在 128 字节共享预算下成功，验证原上传缓冲额度已释放。

修复后 release 新增三项测试通过；完整 Windows Rust workspace 构建和回归 310 通过、0 失败、15 ignored。核心单测 107、缓存 1、文件系统 153、本地 HTTP 49。JNI crate 构建通过，没有新增 JNI ABI 或 Java 代码。本轮未重跑 JVM/Hadoop、诊断特性构建或性能基准。

失败不代表服务端没有保存该分片。已知 MPU 的清理继续沿用原有 abort/日志机制；本轮只验证本地模拟的清理成功路径，不新增断点续传、提交结果确认或远端无残留保证。尚未验证真实 OBS/PFS，不宣称生产吞吐改善。

本地工作区保留 part-etag-before-tests.log、part-etag-after-tests.log 和 part-etag-full-tests.log；GitHub 仅提交源码、测试及项目文档，不提交日志、图片或构建产物。

## 2026-10-13：直接上传段的段号预检

依据同一华为 UploadPart 文档规定的 1..=10000，ObsStore::upload_part 现在在计算 MD5 和发送请求之前拒绝范围外的段号。上层上传流已有段数限制，本轮补齐直接调用入口。新增测试覆盖负数、0、10001、i32 两端以及合法端点 1/10000；合法端点仍允许空尾段。

修改前非法段号测试返回连接错误，合法端点测试通过；修改后完整默认 Rust 回归 **334 通过、0 失败、15 ignored**。本地失败日志 part-number-before.log，最终日志 part-number-after.log。后续扩展的 debug/release/JNI/Hadoop 验证见 LOCAL_VALIDATION.md 中的 2026-10-13 综合复验记录。本轮未扩展 CompleteMultipartUpload 入参校验或改变远端段大小规则。


## 2026-10-13：完成上传的分片列表预检

对照 [CompleteMultipartUpload API](https://support.huaweicloud.com/api-obs/obs_04_0102.html)，直接调用 ObsStore::complete_multipart_upload 现在在构建 XML 和发送提交请求前拒绝空列表、越界段号、重复或非递增段号，以及空白 ETag。检查仅顺序扫描一次，不排序、不克隆 ETag；允许合法的不连续段号，不要求从 1 开始，也不把不透明 ETag 强制解释为 MD5。

新增本地 HTTP 回归覆盖上述非法输入，合法的 1/10000 稀疏列表、XML 特殊字符和非空 ETag 空白保留。原有响应解析测试改为提供合法的单段请求，继续验证响应错误处理，不因前置校验而失去覆盖。空列表在修复前返回连接错误，修复后全部非法列表返回 InvalidArgument；完整默认 debug Rust workspace **336 通过、0 失败、15 ignored**（核心 110、缓存 1、文件系统 153、HTTP 72）。本次未重跑 release、诊断特性、JVM/Hadoop 和性能基准，前一轮综合结果仍是历史验证证据。

本地日志 complete-parts-before.log 与 complete-parts-after.log 不提交到仓库。此检查无法验证服务端是否存在这些段、ETag 是否匹配或实际段大小；这些仍由服务端确认。上层失败清理策略和完成请求不自动重试的语义保持不变。
