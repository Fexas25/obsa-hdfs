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
