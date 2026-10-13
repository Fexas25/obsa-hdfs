# 写入结果的对象大小

2026-10-13。本轮修复 `ObsStore` 写入结果误用 HTTP 响应 `Content-Length` 的问题。

## 行为

- 普通 PUT：返回实际请求数据的字节数，包括空对象。响应体为空不再导致非空对象返回大小 0。
- 分片合并：响应 XML 长度不是合并对象大小。直接调用返回的 `content_length` 使用与 copy 相同的 0 占位约定；这不证明对象为空。需要确切大小的直接调用方应执行 HEAD。`CompletedPart` 不携带分片长度，本轮不改公共结构或增加隐式网络请求。
- `ObsOutputStream::close` 原有逻辑继续使用 `total_written` 填充准确大小；ETag 解析、上传公平性、取消语义和重试策略不变。

## 本地验证

新增两项本地 HTTP 回归，验证非空及空 PUT、合并 XML 的长度隔离、ETag 保持以及每个操作只有一个请求。

修改生产代码前，两项新增测试均复现：13 字节 PUT 返回 0，合并结果返回 XML 的 86 字节。修复后完整 Windows Rust workspace 构建与回归：312 通过、0 失败、15 个手动基准 ignored（核心 107、缓存 1、文件系统 153、HTTP 51）。已有上传流测试也在完整回归中执行。

本地证据保存在工作区 `write-metadata-red.log` 和 `write-metadata-after.log`，不上传日志或构建产物。初次沙箱内运行受到回环网络限制；有效失败复现和最终回归均在允许本机 HTTP 的执行环境完成。本轮未执行 JVM/Hadoop、真实 OBS/PFS 或性能基准，不宣称吞吐提升。0 占位仍无法区分未知大小和空对象，未来若调整公共元数据类型应单独处理兼容性。

## 写入结果的 Content-Type

同日后续修复了响应文档类型与对象类型混用：PUT 使用调用方设置的 Content-Type；copy 和直接 complete 的 XML 响应不能证明对象类型，返回 None。未增加隐式 HEAD。分片输出流知道初始化时传入的类型，因此 close 使用该类型填充最终元数据；未指定类型时保留后端结果。

新增两项本地 HTTP 测试。第一项复现 text/plain PUT 错误返回 application/xml，修复后同时覆盖复制和合并的未知类型；第二项复现存储层修复后上传流未补回已知类型，修复后验证初始化请求及最终类型、长度和 ETag。最终完整 Windows Rust workspace 回归 **332 通过、0 失败、15 ignored**：核心 110、缓存 1、文件系统 153、HTTP 68。

本地证据为 write-content-type-before.log、stream-content-type-before.log 和 content-type-final.log。未运行真实 OBS/PFS、JVM/Hadoop 或性能基准。调用方需要 copy/直接 complete 的权威对象属性时仍应 HEAD；本轮不推测服务端默认 MIME 类型。
