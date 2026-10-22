# 对象键与请求路径一致性

2026-10-13。RequestBuilder 不再删除对象键开头的斜杠。底层 ObjectStore 接口接收对象键，文件系统路径规范化仍由 ObsFileSystem 负责。旧实现为 /file 构造 /bucket/file，却按 /bucket//file 签名，改变对象身份并使路径和签名不一致。

新增核心测试覆盖 Path、Vhost、Cname 三种寻址方式，以及单/多个前导斜杠、仅斜杠、空键、普通键、空格和中文。旧实现已复现 /bucket//file 被缩为 /bucket/file。新增本地 HTTP 测试验证实际 HEAD 路径，并从发送路径及头部重算签名比对 Authorization。

完整 Windows Rust workspace 构建与回归 **328 通过、0 失败、15 ignored**：核心 110、缓存 1、文件系统 153、HTTP 64。文件系统既有路径测试也包含在回归中。工作区保留 key-path-before.log 与 key-path-after.log，不上传日志、图片或构建产物。

本轮未执行真实 OBS/PFS、Vhost DNS 网络验证或 JVM/Hadoop；三种寻址方式由本地 URI 测试覆盖，真实 HTTP 请求测试使用 Path。直接调用底层 API 且依赖旧隐式去斜杠行为的调用方，应显式传入其目标对象键；文件系统路径入口行为不变。

## 2026-10-20 签名使用编码后的路径与子资源白名单

此前签名使用未编码的对象键（`/bucket/a b/文件`），而请求行发送百分号编码路径。对照华为 OBS Java SDK 源码（`RestStorageService` 以 `uri.getRawPath()` 作为 CanonicalizedResource，`AbstractAuthentication.makeServiceCanonicalString` 按 `ALLOWED_RESOURCE_PARAMTER_NAMES` 白名单不区分大小写筛选查询参数并解码其值），含空格、中文、`+`、`%`、`*` 等需要编码字符的键在真实 OBS 上会签名不一致。

- `canonical_resource` 现在与 `build_uri` 共用 `encode_object_key`，签名覆盖实际发送的编码路径；保留 `A-Za-z0-9-_.~` 不编码，与 SDK（URLEncoder 后替换 `%20`/`%2A`/`~`）一致。子资源值保持解码形式。
- 子资源从黑名单改为白名单：华为 API obs_04_0010 列表与 SDK 列表的并集（不区分大小写），以及所有 `x-obs-` 开头参数。LIST 参数、PFS mkdirs 的 `folder` 等不再参与签名。`folder` 不在官方文档或 SDK 的签名列表里；SDK 的 `newFolder` 实际发送不带查询参数的 `PUT key/`。2026-10-22 真实 PFS 实测 `?folder` 返回 404 NoSuchKey，mkdirs 已改为 SDK 的 `PUT key/`。
- 本地回归：HEAD 与带子资源的 PUT 均从实际请求行重算签名比对。仍未在真实 OBS/PFS 上验证。
