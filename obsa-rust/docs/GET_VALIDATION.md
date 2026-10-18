# 完整读取与范围参数验证

2026-10-13。ObsStore 的无 Range GET 不再将 206 部分响应作为完整对象成功返回。参考 [RFC 9110 第 15.3.7 节](https://www.rfc-editor.org/rfc/rfc9110.html#section-15.3.7)，206 表示部分内容，客户端需要检查其范围；本接口没有为无 Range 请求自动拼接意外分片，因此选择报错，避免静默截断。此检查位于对象存储层，不更改通用 HttpPool 的接口行为。

同时，start > end 的倒置范围在发请求之前返回 InvalidArgument。合法范围的端点包含语义、EOF 裁剪、206/Content-Range 校验和 If-Match 行为不变。

两项新增测试分别在修复前复现无 Range 请求误收部分对象、倒置范围仍尝试连接。修复后覆盖普通/条件完整读取、4 字节精确共享预算下失败后再次读取成功，以及三组倒置范围（含 u64 边界）在本地拒绝。

完整 Windows Rust workspace 构建及回归 **330 通过、0 失败、15 ignored**：核心 110、缓存 1、文件系统 153、HTTP 66。工作区保留 full-get-before.log、get-range-before.log 与 get-validation-after.log，不上传日志或构建产物。未运行真实 OBS/PFS、JVM/Hadoop 或性能基准。

无 Range 206 的拒绝发生在响应体收集后，沿用传输层原有大小和预算限制，不宣称提前停止所有异常响应。服务端错误、已知范围读取以及其他响应码的既有处理不变。


## 缺少 Content-Length 的 GET 元数据（2026-10-13）

HTTP 正文可以采用 chunked 编码或连接关闭定界，不一定提供 Content-Length（见 [RFC 9112 §6.3](https://www.rfc-editor.org/rfc/rfc9112.html#section-6.3)）。旧 ObsStore::get_with_etag 仅从头部读取长度，缺少该头时返回 0，即使实际已收到非空正文。现在从传输层已完整收集、解码并校验的 body 获取长度，无需复制或再次读取数据。

完整 GET 的长度是返回对象正文长度；范围 GET 延续已有 Content-Length 语义，报告返回范围的字节数，不将 Content-Range 中的对象总长度混入该字段。HEAD 仍使用服务端头部对象长度，不受此修复影响。响应大小、统一预算、范围校验和 ETag 一致性检查保持不变。

新增本机 HTTP 回归覆盖完整/范围条件读取，分别使用 chunked、连接关闭定界和固定长度，共六种组合；同时断言正文内容和 EOF。修复前 chunked 正文 4 字节却得到元数据长度 0；修复后完整默认 debug workspace **337 通过、0 失败、15 ignored**（核心 110、缓存 1、文件系统 153、HTTP 73）。本地日志 get-length-before.log、get-length-after.log 不上传。本轮未重跑 release/JVM/Hadoop/诊断特性，也未验证真实云环境或生产性能。


## 完整 GET 的成功状态白名单（2026-10-13）

完整对象 GET 现在仅接受 200；范围 GET 继续由传输层要求匹配的 206/Content-Range。依据 [OBS GetObject](https://support.huaweicloud.com/api-obs/obs_04_0083.html) 的完整与范围下载响应约定，不能把任意 2xx 都解释为完整对象。旧代码只排除了无 Range 的 206，201/202/204 等仍可能被当作有效内容或空对象返回。错误状态仍先走原有映射，不改变 404/412 等语义。

新增回归在修复前复现 201 被错误接受；修复后检查 201、202、203、204、205、207、226，分别覆盖有/无 If-Match。每次失败后在同一个 4 字节共享预算下读取 4 字节对象，再读取合法的 200 空对象；断言三次请求，防止异常状态造成预算泄漏或自动重试。完整默认 debug Rust workspace **338 通过、0 失败、15 ignored**，其中 HTTP 74 项。日志 get-status-before.log 和 get-status-after.log 仅保存在工作区。本轮未重跑 release、JVM/Hadoop、诊断特性和性能基准。

检查在传输层收集响应之后进行，不能据此声称异常正文完全不占用内存；现有响应大小和预算限制仍然适用。

## HEAD 必须提供对象长度（2026-10-17）

成功 HEAD 缺少或无法解析 Content-Length 时，现在拒绝该响应，不再把未知长度默认为 0。[OBS 获取对象元数据](https://support.huaweicloud.com/api-obs/obs_04_0084.html) 的响应提供对象长度；HEAD 没有正文，不能像完整 GET 一样从已收集正文补算。合法的 Content-Length: 0 仍表示空文件，HEAD 中的大对象长度也不受 HTTP 正文缓冲上限限制。

错误状态先经过原有服务错误映射，所以缺少长度的 404 仍按不存在处理。修改局限于 HEAD，不要求 PUT、复制、分片完成等共享元数据解析路径必须提供此头。新增 TCP 回归覆盖缺失、负数、非数字、u64 溢出、零长度、大长度及 open；Hadoop/JNI 用例覆盖普通桶和 PFS，检查查询/打开失败不会污染缓存，响应恢复后可读到原始内容。本地模拟不替代真实服务验收。

## HEAD 的成功状态白名单（2026-10-18）

无条件对象 HEAD 现在只接受 200；不能仅因状态属于 2xx 就把头部解释成对象元数据。官方 [HeadObject 响应示例](https://support.huaweicloud.com/api-obs/obs_04_0084.html) 使用 200，本实现不发送范围或条件 HEAD。新回归在旧实现复现 201 被接受，修复后覆盖 201、202、203、204、205、206、207、226；206 携带部分长度及 Content-Range，也不能替代完整对象状态。错误状态仍先按原逻辑分类。

Hadoop/JNI 用例在普通桶和 PFS 注入 201、202、206，确认 getFileStatus/open 报 IOException 且不发 GET；恢复 200 后可正确查询并读出全部内容。该修改不增加请求，不改变重试策略，不宣称真实云或性能验收完成。
