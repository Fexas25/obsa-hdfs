# 真实 S3 兼容服务端测试（S3 兼容模式）

2026-10-21。此前所有回归都用本项目自写的模拟服务端，它与客户端“想法一致”，发现不了协议理解偏差。
本节用独立实现的 S3 兼容服务端 [S3Proxy](https://github.com/gaul/s3proxy) 4.1.1 验证通用对象存储部分。
它不能验证 PFS 专有语义（原子 rename、append、truncate、`?folder`、AccessLabel、inode 提示）及 OBS 特有头，
这些仍需真实 OBS/PFS。公开渠道没有 OBS/PFS 模拟服务端；官方 `obsa-hdfs` 的 `ITest*` 也依赖真实 OBS。

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
- 签名编码修复（编码后的路径）在独立服务端上得到验证：空格、`+`、`%`、中文、`~`、`!`、`*` 文件名
  的写入、列举、读取、重命名均通过。

## 运行方法

1. 下载 `s3proxy`（可执行 jar），用 JDK 11+ 运行：`java -jar s3proxy --properties s3proxy.conf`。
2. 配置（凭据随机生成，放在项目之外；路径含非 ASCII 字符时用 `\uXXXX` 转义，properties 按 ISO-8859-1 读取）：

   ```
   s3proxy.authorization=aws-v2-or-v4
   s3proxy.endpoint=http://127.0.0.1:18080
   s3proxy.identity=<随机>
   s3proxy.credential=<随机>
   jclouds.provider=transient
   ```

   用内存后端 `transient`：`filesystem` 后端在 Windows 上不能存含 `*` 的键，且删除目录标记后残留的空目录
   会出现在带分隔符的 LIST 中（HEAD 与不带分隔符的 LIST 却显示不存在），会误导测试。
3. 内存后端需先建桶：发一个 V2 签名的 `PUT /bucket/`（`StringToSign = "PUT\n\n\n<Date>\n/bucket/"`，
   HMAC-SHA1 后 base64）。
4. 运行（缺少任一变量时测试自动跳过；凭据只经环境变量传入，不写入任何文件）：

   ```bash
   export OBS_LIVE_ENDPOINT=127.0.0.1:18080 OBS_LIVE_BUCKET=bucket \
          OBS_LIVE_ACCESS_KEY=... OBS_LIVE_SECRET_KEY=...
   cargo test -p obs-rust-core --test live_s3 -- --nocapture
   mvn -f hadoop-adapter/pom.xml test -Dtest='LiveS3*'
   ```

   可选 `OBS_LIVE_SECURE=true`、`OBS_LIVE_SIGNATURE=obs`（对真实 OBS 用原生签名）、
   `OBS_LIVE_ROOT_TESTS=true`（根目录契约会清空桶，仅限专用测试桶）。

## 结果（Windows，debug JNI）

- `live_s3`：单 PUT 与 12 MiB 分片上传、顺序/定位/向量化读、特殊字符名列举与读取、文件与目录重命名
  （copy+delete）、递归删除（批删）全部通过。
- Hadoop 3.3.6 官方契约 `LiveS3*`：Create 16（跳过 2，能力声明不支持）、Delete 8、Rename 10、Open 19、
  Seek 18、GetFileStatus 20、Mkdir 8、ContentSummary 2、VectoredRead 34，共 **135 项 0 失败**。
- 未覆盖：TLS、真实网络时延与吞吐、OBS 特有头与错误码、PFS。日常回归中 `LiveS3*` 因无环境变量而跳过，
  会计入 Surefire 的跳过数。
