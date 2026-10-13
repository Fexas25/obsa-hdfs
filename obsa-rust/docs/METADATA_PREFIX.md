# 自定义元数据键名前缀保留

2026-10-13。本轮修复响应头解析时重复删除 `x-obs-meta-` 导致的键名损坏。

## 问题和修复

协议头 `x-obs-meta-x-obs-meta-mode` 对应用户键 `x-obs-meta-mode`。旧解析用 `trim_start_matches` 删除所有连续前缀，把它误读成 `mode`；若普通 `x-obs-meta-mode` 同时存在，两者还会发生覆盖。读取后执行 REPLACE 的权限更新和用户元数据更新会将错误的键写回。

改为 `strip_prefix`，只删除一层协议前缀。保留原有 HTTP 头名大小写归一化规则，不改变值、签名、网络请求数、并发或取消语义。支持用户键本身含一层或多层相同前缀。

## 本地证据

新增两项真实 ObsStore + 本机 HTTP 服务回归：

- HEAD 同时读取普通 mode、带重复前缀的 mode 和更深层前缀的 tag，验证三个不同键及各自值完整保留，并覆盖混合大小写头名。
- 分别执行权限更新和用户元数据更新，检查 HEAD + PUT 请求序列、原自定义键和值、文件类型与权限、新增用户字段。确保写回没有生成被错误缩短的 tag 键。

生产代码修改前，两项测试均失败：读取结果三个键变成两个；权限更新丢失原自定义键。修复后完整 Windows Rust workspace 构建与回归 **314 通过、0 失败、15 ignored**：核心 107、缓存 1、文件系统 153、HTTP 53。ignored 为手动基准，本轮未运行。JNI crate 构建通过，但未执行 JVM/Hadoop 或真实 OBS/PFS 测试。

失败复现及完整回归日志保存在工作区 `metadata-prefix-before.log` 和 `metadata-prefix-after.log`，不提交日志或产物。此修复不解决 HEAD 与 REPLACE 之间的跨客户端并发更新冲突，也不声称已全面验证所有元数据编码。
