# 对象键与请求路径一致性

2026-10-13。RequestBuilder 不再删除对象键开头的斜杠。底层 ObjectStore 接口接收对象键，文件系统路径规范化仍由 ObsFileSystem 负责。旧实现为 /file 构造 /bucket/file，却按 /bucket//file 签名，改变对象身份并使路径和签名不一致。

新增核心测试覆盖 Path、Vhost、Cname 三种寻址方式，以及单/多个前导斜杠、仅斜杠、空键、普通键、空格和中文。旧实现已复现 /bucket//file 被缩为 /bucket/file。新增本地 HTTP 测试验证实际 HEAD 路径，并从发送路径及头部重算签名比对 Authorization。

完整 Windows Rust workspace 构建与回归 **328 通过、0 失败、15 ignored**：核心 110、缓存 1、文件系统 153、HTTP 64。文件系统既有路径测试也包含在回归中。工作区保留 key-path-before.log 与 key-path-after.log，不上传日志、图片或构建产物。

本轮未执行真实 OBS/PFS、Vhost DNS 网络验证或 JVM/Hadoop；三种寻址方式由本地 URI 测试覆盖，真实 HTTP 请求测试使用 Path。直接调用底层 API 且依赖旧隐式去斜杠行为的调用方，应显式传入其目标对象键；文件系统路径入口行为不变。
