# 元数据更新键的大小写

2026-10-13。`patch_user_metadata` 现在在 HEAD 前将更新键转为 ASCII 小写，再与已有元数据合并。混合大小写的更新可以正确替换既有字段，不再产生同名 HTTP 头。单批输入若包含仅大小写不同的重复键，一律返回 InvalidArgument，即使值相同也拒绝，以避免依赖 HashMap 迭代顺序。该校验不包含用户值，且发生在网络请求前。

旧实现已由两项本地 HTTP 测试复现：更新 PrOjEcT=new 后实际请求仍为 project=old；重复键输入未在 HEAD 前拒绝。修复后完整 Windows Rust workspace 构建及回归 **316 通过、0 失败、15 ignored**（核心 107、缓存 1、文件系统 153、HTTP 55）。测试覆盖只出现一次新值、无关字段保留，以及不同值和相同值的重复键均被拒绝。

本轮仅改变用户元数据补丁入口，不宣称已处理所有底层 RequestBuilder 的重复头情况。没有改变签名算法、权限语义、取消行为或增加网络请求。未执行真实 OBS/PFS、JVM/Hadoop 或手动性能基准。工作区保留 metadata-case-before.log 与 metadata-case-after.log，不上传日志、图片和构建产物。

## PUT 与分片初始化补齐

同日第二批将相同校验扩展到 PUT 和分片初始化，并让三个入口共用内部归一化函数。PutHeaders 的 Vec 可以包含完全相同的键或仅大小写不同的键，过去会静默覆盖或产生歧义；现在两类重复都在发送请求前拒绝。值保持原样，键名中的额外 x-obs-meta- 前缀保留。

新增两项测试，覆盖两个创建入口、两类键碰撞、大小写及值和嵌套前缀保留。改动前：有效输入测试通过，重复键拒绝测试失败（尝试连接服务端）。改动后完整 Windows Rust 回归 **318 通过、0 失败、15 ignored**，其中 HTTP 57。工作区证据为 metadata-create-before.log 与 metadata-create-after.log。

本批仍不改变公开类型或通用 RequestBuilder 行为，也不新增元数据值编码规则；不声称已完成所有 HTTP 头校验。
