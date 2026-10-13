# 元数据更新键的大小写

2026-10-13。`patch_user_metadata` 现在在 HEAD 前将更新键转为 ASCII 小写，再与已有元数据合并。混合大小写的更新可以正确替换既有字段，不再产生同名 HTTP 头。单批输入若包含仅大小写不同的重复键，一律返回 InvalidArgument，即使值相同也拒绝，以避免依赖 HashMap 迭代顺序。该校验不包含用户值，且发生在网络请求前。

旧实现已由两项本地 HTTP 测试复现：更新 PrOjEcT=new 后实际请求仍为 project=old；重复键输入未在 HEAD 前拒绝。修复后完整 Windows Rust workspace 构建及回归 **316 通过、0 失败、15 ignored**（核心 107、缓存 1、文件系统 153、HTTP 55）。测试覆盖只出现一次新值、无关字段保留，以及不同值和相同值的重复键均被拒绝。

本轮仅改变用户元数据补丁入口，不宣称已处理所有底层 RequestBuilder 的重复头情况。没有改变签名算法、权限语义、取消行为或增加网络请求。未执行真实 OBS/PFS、JVM/Hadoop 或手动性能基准。工作区保留 metadata-case-before.log 与 metadata-case-after.log，不上传日志、图片和构建产物。
