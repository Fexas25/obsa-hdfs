# 与 Java 原版（Huawei OBSA）的性能对照

2026-10-22。目标：同机、同 Hadoop、同工作负载，只切换 `fs.obs.impl`，比较 Rust 适配层
`OBSFileSystemRust` 与华为发布的 `hadoop-huaweicloud-3.1.1-hw-54.10.jar`（`OBSFileSystem`，内含 OBS SDK）。
**尚无可发布的性能结论**：下面的数字来自家庭网络（北京区域，往返约 200 ms，上行一度仅约 64 KiB/s），
只用于验证工具和对比请求数；吞吐/延迟的结论必须在同区域 ECS 上得出。

## 工具

- `java/benchmarks/ObsCompareBench.java`：只经 Hadoop FileSystem API，两种实现跑同一份代码；每个工作负载一个 JVM，
  输出一行 JSON。内容用 CRC32 校验，确保两边做了相同的工作。
  - `write-read`：并发顺序写再读 `--files` × `--size-mb`
  - `random-read`：在一个 `--size-mb` 文件上做 `--reads` 次 `--read-kb` 定位读（p50/p95/p99）
  - `metadata`：`--files` 个 1 KiB 文件的 create/stat/list/delete
  - `rename-dir`：重命名含 `--files` 个小文件的目录
  - 每项都记录墙钟、进程 CPU、GC 时间、Linux 峰值 RSS（VmHWM，含 native 内存）
- `scripts/compare-bench.sh <env> <reference-jar> [out.jsonl]`：构建、按 java→rust→rust→java 交替运行多轮；
  `ROUNDS`、`THREADS`、`WORKLOADS`、`JAVA`、`JAVA_OPTS`、`RUST_CONF`/`JAVA_CONF`（只传给对应实现的配置）。
- `scripts/compare-bench-summary.py out.jsonl [--markdown]`：每项指标两边中位数、Rust/Java 比值、
  各轮方向是否一致（只有一致的比值才可引用）。
- 请求级计数：Rust 侧 `OBS_RUST_LOG=info` + `--conf fs.obs.access.log.enabled=true`（stderr 每请求一行，
  不含凭据）；Java 侧把 `com.obs` 日志调到 DEBUG（注意 SDK 每个请求打两行）。

## 公平性约定

- 同一协议：两边默认不同（Rust https、OBSA http），工具统一按 `OBS_LIVE_SECURE`（默认 https）设置。
- 两套配置都要跑：**各自默认**（用户开箱即得）与**对齐**（例如分片大小：OBSA 默认 100 MiB；写缓冲：
  OBSA 默认先落本地磁盘 `fs.obs.fast.upload.buffer=disk`）。
- 同 JDK；JDK 18+ 需 `-Djava.security.manager=allow`（脚本自动加）。基准不与编译或其他测试并行。

## 家庭网络上的初步观察（不是性能结论）

1 轮、2 线程、极小数据：

| 项目 | Java 原版 | Rust | 说明 |
|---|---|---|---|
| 进程 CPU | 基准 | 约 1/10～1/3 | 各项一致；GC 时间 Rust 侧为 0 |
| 4 KiB 随机定位读 | 基准 | 约 2～3 倍吞吐，p99 约 1/4 | 方向一致 |
| 小文件 create / stat | 基准 | 约 0.5 倍 | 由请求数决定，见下 |
| 目录 rename（10 文件） | 基准 | 约 0.6 倍 | 同上 |

请求数（与网络无关，可直接比较；普通桶，路径深 3 级）：

| 操作 | Java 原版 | Rust（修复前） | Rust（同一操作内祖先去重后） |
|---|---|---|---|
| stat 已存在文件 | 1 HEAD | 4（自身 + 3 个祖先） | 4 |
| create 新文件 | 4（HEAD、HEAD/、LIST、PUT） | 约 12 | 6～9 |
| metadata 5 文件全流程 | 约 67 | 140 | 118 |

差异来自 Rust 版普通桶的**严格文件优先语义**：每个操作都重新确认所有祖先不是文件（带外创建同名祖先文件也能立即发现），
OBSA 不做这项检查。修复前同一个 create 内同一组祖先会被重复探测 3 遍，现已在单次操作内去重（语义不变）。
祖先探测本身是并发的，但在自身 HEAD 之后，所以 stat 是 2 个往返对 OBSA 的 1 个。可选 `fs.obs.ancestor.directory.hints`
（缓存 1 s）在真实往返下几乎无效。进一步缩小差距需要语义取舍（见下），由维护者决定。

待决定的取舍：

1. 维持严格语义；或把自身 HEAD 与祖先探测并发（stat 延迟与 OBSA 同为 1 个往返，但路径不存在时多发祖先请求——
   现有测试明确要求不存在时不探测祖先）；
2. 增加与 OBSA 一致的宽松模式（不做祖先检查），默认仍严格或改为宽松。

## 在 ECS 上运行

1. 控制台在 **cn-north-4**（与测试桶同区域，内网访问 OBS 免流量费）开按需计费 ECS，建议 8 vCPU / 16 GiB 通用计算型，
   系统 EulerOS/openEuler/CentOS/Ubuntu 均可；需要能访问 `repo.huaweicloud.com` 镜像与 GitHub（或绑定按流量计费的小带宽 EIP）。
   费用以控制台为准，通常每小时一两元；用完立即释放。
2. 登录后：`git clone https://github.com/Fexas25/obsa-hdfs.git && cd obsa-hdfs/obsa-rust && bash scripts/ecs-setup.sh`
   （安装 JDK 17、Maven、Rust，经华为云镜像，构建 release `libobs_jni.so`；首次在 ECS 上尚未验证）。
3. 在 ECS 上创建 env 文件（**放在仓库外**，`chmod 600`），内容同 `docs/live-test.env.example`；更推荐用临时 AK/SK。
4. 运行（默认 3 轮、8 线程、默认工作负载约 8×256 MiB、1 GiB 随机读文件、2000 小文件）：

   ```bash
   scripts/compare-bench.sh ~/obsa.env ../release/hadoop-huaweicloud-3.1.1-hw-54.10.jar ~/defaults.jsonl
   JAVA_CONF=$'fs.obs.multipart.size=16777216\nfs.obs.fast.upload.buffer=bytebuffer' \
     scripts/compare-bench.sh ~/obsa.env ../release/hadoop-huaweicloud-3.1.1-hw-54.10.jar ~/aligned.jsonl
   python3 scripts/compare-bench-summary.py ~/defaults.jsonl --markdown
   ```

   对齐示例把 OBSA 的分片设为 Rust 默认的 16 MiB、写缓冲改为内存。预读不同：Rust 默认 8 MiB 块、
   每流最多 8 块；OBSA `fs.obs.readahead.range` 默认 1 MiB——需要时用 `RUST_CONF`/`JAVA_CONF` 另跑一组对齐预读。每个测试用独立前缀并在结束时删除。
5. 结果文件带回本地分析；不要把 env 文件或凭据带出 ECS。
