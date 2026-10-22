# 真实服务端测试（S3 兼容服务端与华为云 OBS/PFS）

2026-10-21。此前所有回归都用本项目自写的模拟服务端，它与客户端“想法一致”，发现不了协议理解偏差。
本节用独立实现的 S3 兼容服务端 [S3Proxy](https://github.com/gaul/s3proxy) 4.1.1 验证通用对象存储部分，
并提供可直接对华为云 OBS/PFS 运行的同一套测试。公开渠道没有 OBS/PFS 模拟服务端；官方 `obsa-hdfs` 的
`ITest*` 也依赖真实 OBS。PFS 专有语义（原子 rename、append、truncate、目录创建、AccessLabel、inode 提示）
及 OBS 特有头只能在华为云上验证。

## S3 兼容模式

`fs.obs.signature.type=v2`（默认 `obs`）：与 OBS SDK 的 `AuthTypeEnum.V2` 相同，`Authorization: AWS ak:sig`、
`x-amz-` 头；V2 待签字符串与 OBS 原生签名完全相同。实现上请求代码照旧构造 `x-obs-` 头，只在发送前改名
（`kms`→`aws:kms`、`...-kms-key-id`→`...-aws-kms-key-id`），响应头 `x-amz-*` 改回 `x-obs-*`、`x-amz-request-id`
作为请求 ID。OBS 同样接受此格式（内网用 s3curl 访问 OBS 即此格式），有的部署只开放 S3 兼容接口。
非法取值在初始化时报错。PFS 专有子资源（如 `x-obs-accesslabel`）在此模式下不保证可用。

## 发现并修复

- **空请求体的 PUT/POST 没有 `Content-Length`**：hyper 对空 body 不发该头，S3 服务端以
  `411 MissingContentLength` 拒绝目录标记、空文件等请求；本地模拟服务从不检查。现在 PUT/POST 始终显式发送。
  真实 OBS 很可能同样要求，属于真实环境阻断问题。
- **SSE-C 分片上传的完成请求**：S3 服务端要求 CompleteMultipartUpload 再带 SSE-C 头，OBS SDK 不带。
  S3 兼容模式下带上，OBS 原生模式保持 SDK 的请求。完成请求现在与 SDK 一样携带清单的 `Content-MD5`。
- 签名编码修复（编码后的路径）在独立服务端上得到验证：空格、`+`、`%`、中文、`~`、`!`、`*` 文件名
  的写入、列举、读取、重命名均通过。

## S3Proxy 的已知不一致（不是客户端缺陷）

S3Proxy 的 jclouds 后端按真实目录建模：在并发请求下，某个前缀下的键全部删除后，带分隔符的 LIST 仍可能
返回该前缀（不带分隔符的 LIST 与 HEAD 都显示不存在）。真实 S3/OBS 的公共前缀只由现存键推出，不会如此。
用 curl 手工签名的 PUT/DELETE、批量删除、复制单独复现均正常，只在整套测试的负载下出现；`transient`、
`transient-nio2`、`filesystem` 后端都有。客户端按不带分隔符的 LIST 删除，删不掉不存在的键，`exists` 则按
服务端返回的前缀如实判断。测试因此：每个 Hadoop 契约测试使用独立的随机目录；`renamed_tree_...` 区分
“确有键残留”（失败）与“仅带分隔符 LIST 报告的幽灵前缀”（打印警告）。尝试过“由深到浅删除”未消除该现象，已撤回。
`filesystem` 后端另外不能在 Windows 上存含 `*` 的键。

## 一键运行

```bash
scripts/live-test.sh /path/outside/repo/live.env
```

变量见 [`live-test.env.example`](live-test.env.example)：必填端点、桶、AK/SK；可选签名方式、SSE、PFS 桶、
AccessLabel、根目录契约。脚本拒绝读取仓库内的配置文件，不打印凭据；依次运行 Rust 线上测试（对象流程、
凭据刷新、SSE、PFS、目录重命名压力）与 Hadoop 官方契约 `LiveS3*`，每个测试用独立前缀并在结束时删除。
对华为云用 `OBS_LIVE_SIGNATURE=obs`、`OBS_LIVE_SECURE=true`；对 S3Proxy 等按 IP/localhost 访问的服务端
另设 `OBS_LIVE_PATH_STYLE=true`（华为云要求虚拟主机方式，默认即是）。

## 搭建 S3Proxy

1. 下载 `s3proxy`（可执行 jar），用 JDK 11+ 运行：`java -jar s3proxy --properties s3proxy.conf`。
2. 配置（凭据随机生成，放在项目之外；路径含非 ASCII 字符时用 `\uXXXX` 转义，properties 按 ISO-8859-1 读取）：

   ```
   s3proxy.authorization=aws-v2-or-v4
   s3proxy.endpoint=http://127.0.0.1:18080
   s3proxy.identity=<随机>
   s3proxy.credential=<随机>
   jclouds.provider=transient-nio2
   ```

3. 内存后端需先建桶：发一个 V2 签名的 `PUT /bucket/`（`StringToSign = "PUT\n\n\n<Date>\n/bucket/"`，
   HMAC-SHA1 后 base64）。S3Proxy 的 SSE-KMS 要求给出密钥 ID（`OBS_LIVE_SSE_KEY`），真实 OBS 可省略。

## 结果（Windows，debug JNI，S3Proxy transient-nio2）

- Rust `live_s3` 5 项：对象流程（单 PUT 与 12 MiB 分片、顺序/定位/向量化读、特殊字符名、文件与目录重命名、
  递归批删）、凭据刷新（错误密钥被服务端拒绝且不被当作路径不存在，`updateCredentials` 后下一请求成功）、
  SSE-C 与 SSE-KMS（含分片与带 SSE-C 源头的复制）、目录重命名压力 40 轮均通过；PFS 项因无 PFS 桶跳过。
- Hadoop 3.3.6 官方契约 `LiveS3*`：Create 16（跳过 2，能力声明不支持）、Delete 8、Rename 10、Open 19、
  Seek 18、GetFileStatus 20、Mkdir 8、ContentSummary 2、VectoredRead 34，共 135 项；独立测试目录后连续
  5 轮 **0 失败**。
- 未覆盖：TLS、真实网络时延与吞吐、OBS 特有头与错误码、PFS。日常回归中线上测试因无环境变量而跳过，
  会计入 Surefire 的跳过数。

## 华为云实测（2026-10-22，cn-north-4，按需计费）

普通对象桶 + 并行文件系统桶各一个，OBS 原生签名、HTTPS，Windows debug JNI。凭据只在用户本机的 env 文件中，
测试输出过滤 AK/SK；每个测试用独立前缀并在结束时删除，结束后两个桶均已核对为空。

### 发现并修复的协议偏差（本地模拟与 S3Proxy 都发现不了）

1. **寻址方式**：路径方式的桶请求被拒绝，`403 VirtualHostDomainRequired`。此前默认路径方式，等于对真实 OBS
   完全不可用。现在 Hadoop 配置默认虚拟主机方式（与 SDK `pathStyle=false` 一致），`fs.obs.path.style.access=true`
   可切回；线上测试用 `OBS_LIVE_PATH_STYLE` 选择。
2. **PFS mkdirs**：`PUT key/?folder` 返回 `404 NoSuchKey`。改为 SDK `newFolder` 的形式，即不带参数的 `PUT key/`。
   手工签名请求实测：自动创建缺失的父目录；对已有目录重复 PUT 返回 200，目录与子文件、inode 均不变；
   路径上是文件或祖先是文件时都返回 `409 FsObjectConflict`（消息不同），客户端再 HEAD 该路径区分
   “已存在文件”（FileAlreadyExists）与“父路径是文件”（ParentNotDirectory）。
3. **PFS mode 是十进制**：LIST 的 `<Mode>` 与 `x-obs-meta-mode` 均为十进制（目录 16832 = 0o40700，
   文件 33152 = 0o100600），与 SDK 的 `Integer.parseInt` 一致；原实现按八进制解析，目录被识别为文件，
   导致 PFS 目录 rename 返回 false。`-1`（从对象桶迁移的对象）视为无 mode。客户端写入的 mode 元数据也改为十进制。

另外记录：真实 PFS 的 HEAD/PUT 响应带 `x-obs-inode-no`，不返回规格里的 `x-hws-fs-inode-no` /
`x-hws-fs-inode-dentryname`（SDK 中也没有这些头）。inode 读取提示需要两者同时存在，因此在真实环境不会生效，
无副作用；是否删除留待后续。

### 结果

- Rust `live_s3` 5/5：对象流程（单 PUT 与 12 MiB 分片、顺序/定位/向量化读、中文/空格/`+`/`%`/`~`/`!`/`*` 文件名、
  文件与目录重命名、递归批删）、凭据刷新、SSE-C（随机密钥，含分片与带源密钥的复制）、PFS 语义
  （`bucket.mode=auto` 识别 PFS、中文目录 mkdirs、append、truncate、原子目录 rename、递归删除）、目录重命名压力 3 轮。
- Hadoop 3.3.6 官方契约 `LiveS3*` 对真实普通桶：Create 16（跳过 2，能力声明不支持）、Delete 8、Rename 10、
  Open 19、Seek 18、GetFileStatus 20、Mkdir 8、ContentSummary 2、VectoredRead 34，共 **135 项 0 失败**，无残留。
- 未测：SSE-KMS（会调用 KMS，可能产生费用）、AccessLabel（需额外权限）、临时 AK/SK + securitytoken
  （凭据刷新测试用的是错误密钥→正确密钥）、真实网络吞吐与性能。
