# OBS/PFS 文件系统：Rust 核心与 Hadoop 接入

本仓库同时保留原有 Java 适配器和新增的 **obsa-rust 项目**。Rust 项目位于 [`obsa-rust/`](obsa-rust/)，采用 **Rust 核心 + Rust JNI 桥接 + Java/Hadoop 适配层** 的结构。

**从 Rust 项目开始：** [项目说明与构建指南](obsa-rust/README.md) · [Rust 核心源码](obsa-rust/obs-rust-core/src/) · [Rust JNI 源码](obsa-rust/obs-jni/src/)

## Rust 项目的职责划分

- **[`obs-rust-core/`](obsa-rust/obs-rust-core/)：Rust 核心。** 实现 OBS/PFS 文件操作、HTTP 通信与签名、缓存、读写流、分片上传、资源预算和追加/重命名并发控制。
- **[`obs-jni/`](obsa-rust/obs-jni/)：Rust 编写的 JNI 桥接。** 构建原生动态库，将 Java 调用接入 Rust 核心，并处理原生句柄、缓冲区和异常边界。
- **[`java/`](obsa-rust/java/)：Java 侧 JNI 接口。** 提供调用原生库的 Java 绑定。
- **[`hadoop-adapter/`](obsa-rust/hadoop-adapter/)：Java/Hadoop 适配层。** 对接 Hadoop `FileSystem`、配置、凭据和流接口；底层存储操作通过 JNI 交给 Rust。

Hadoop 的调用路径为：**Hadoop 应用 → Java 适配层 / JNI 接口 → Rust JNI 桥接 → Rust 核心 → OBS/PFS**。Rust 核心也可由 Rust 程序直接使用。

## 目录结构

```text
.
├── obsa-rust/                 # Rust 项目入口
│   ├── Cargo.toml            # Rust workspace：obs-rust-core + obs-jni
│   ├── obs-rust-core/        # Rust 文件系统与存储核心、测试
│   ├── obs-jni/              # Rust JNI 原生动态库
│   ├── java/                 # Java 侧 JNI 绑定
│   ├── hadoop-adapter/       # Hadoop FileSystem 适配与集成测试
│   ├── scripts/              # 构建和本地验证脚本
│   ├── docs/                 # 设计、验证记录和性能基准数据
│   └── README.md             # 配置、构建、使用与能力边界
├── hadoop-huaweicloud/        # 原有 Java Hadoop OBS 项目
├── flink-release/            # 原有 Flink 发布目录
└── release/                  # 原有发布目录
```

## 当前验证阶段

Rust 项目已具备可运行的本地实现，并完成多轮 Rust、JNI、Hadoop 契约及模拟压力验证。**真实 OBS/PFS、目标集群作业和长期生产运行尚未完成验收**；本地基准不能直接代表真实云端性能，也不构成生产就绪声明。

- [构建、配置和使用说明](obsa-rust/README.md)
- [开发计划与剩余缺口](obsa-rust/docs/ADAPTER_PLAN.md)
- [本地验证记录](obsa-rust/docs/LOCAL_VALIDATION.md)
- [有延迟追加与重命名混合压力测试](obsa-rust/docs/APPEND_MIXED.md)

## 原有项目入口

- **Hadoop OBS（Java）**：位于当前分支的 [`hadoop-huaweicloud/`](hadoop-huaweicloud/)。它与 `obsa-rust/hadoop-adapter/` 是不同的项目目录。
- **Flink OBS**：原有项目说明指向 `flink-obs` 分支的 `flink-obs-fs-hadoop/`。
- **Ranger OBS**：原有项目说明指向 `ranger-obs` 分支，用于管理 OBS 权限。

## English overview

The new **Rust OBS/PFS implementation** lives in [`obsa-rust/`](obsa-rust/). Start with its [README](obsa-rust/README.md) or browse the [Rust core](obsa-rust/obs-rust-core/src/).

- `obs-rust-core` contains the storage and filesystem implementation in Rust.
- `obs-jni` is the JNI bridge, also written in Rust.
- `java` and `hadoop-adapter` provide the Java bindings and Hadoop integration that call the Rust implementation.
- `hadoop-huaweicloud` contains the original Java Hadoop adapter, which remains alongside the Rust project.

Local tests and simulated benchmarks are available. Real OBS/PFS service validation and production workload acceptance are still pending.
