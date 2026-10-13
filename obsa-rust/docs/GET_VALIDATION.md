# 完整读取与范围参数验证

2026-10-13。ObsStore 的无 Range GET 不再将 206 部分响应作为完整对象成功返回。参考 [RFC 9110 第 15.3.7 节](https://www.rfc-editor.org/rfc/rfc9110.html#section-15.3.7)，206 表示部分内容，客户端需要检查其范围；本接口没有为无 Range 请求自动拼接意外分片，因此选择报错，避免静默截断。此检查位于对象存储层，不更改通用 HttpPool 的接口行为。

同时，start > end 的倒置范围在发请求之前返回 InvalidArgument。合法范围的端点包含语义、EOF 裁剪、206/Content-Range 校验和 If-Match 行为不变。

两项新增测试分别在修复前复现无 Range 请求误收部分对象、倒置范围仍尝试连接。修复后覆盖普通/条件完整读取、4 字节精确共享预算下失败后再次读取成功，以及三组倒置范围（含 u64 边界）在本地拒绝。

完整 Windows Rust workspace 构建及回归 **330 通过、0 失败、15 ignored**：核心 110、缓存 1、文件系统 153、HTTP 66。工作区保留 full-get-before.log、get-range-before.log 与 get-validation-after.log，不上传日志或构建产物。未运行真实 OBS/PFS、JVM/Hadoop 或性能基准。

无 Range 206 的拒绝发生在响应体收集后，沿用传输层原有大小和预算限制，不宣称提前停止所有异常响应。服务端错误、已知范围读取以及其他响应码的既有处理不变。
