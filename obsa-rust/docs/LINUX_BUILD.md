# Linux 构建与验证

2026-10-21，WSL2 Ubuntu 22.04（glibc 2.35）、Rust 1.98.1、OpenJDK 11、Maven 3.9.9。此前项目只在 Windows
上构建和测试过；生产（MRS/Spark）运行 Linux，本节记录首次 Linux 验证结论。

## 结论

- Rust workspace debug / release 各 **427 通过、0 失败、16 ignored**，与 Windows 相同。原先只在 Windows
  编译的 3 项上传日志“本地删除失败”测试改为跨平台：Unix 上把记录目录临时设为只读制造删除失败
  （root 会绕过权限，测试会拒绝以 root 运行）。
- `libobs_jni.so` debug / release：`scripts/test-jni.sh` 真实 JVM 冒烟通过；Hadoop 3.3.6 回归各
  **410 项、0 失败、0 错误、12 跳过**。
- **发现并修复一个 Linux 并发问题**：上传日志命名空间锁是 `flock`；其他线程并发 fork 子进程时，子进程在
  exec 关闭 close-on-exec 描述符前共享该锁，刚释放的锁会短暂表现为被占用。完整 release 套件 15 次中
  4 次失败，跳过会启动子进程的测试后 0 次；JVM 中 Spark Python worker、Hadoop Shell 等同样会触发。
  现在对 WouldBlock 重试约 200 ms，真实的其他持有者仍报冲突；修复后 15 次完整 release 0 失败。
- **glibc 兼容性**：在 Ubuntu 22.04 原生构建的 .so 需要 GLIBC_2.34，CentOS 7（2.17）、EulerOS 2.x 等
  较老节点无法加载。用 `cargo zigbuild --target x86_64-unknown-linux-gnu.2.17` 构建后最高需要
  **GLIBC_2.17**，只依赖 libc/libm/libpthread/libdl（无 libgcc_s）；以该库重跑 JNI 冒烟与 Hadoop release
  回归均通过（410 项 0 失败）。

## 构建

```bash
# Rust 与 Zig（不需要 root）
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --profile minimal
curl -sSfL https://ziglang.org/download/0.13.0/zig-linux-x86_64-0.13.0.tar.xz | tar -xJ -C ~/.local
export PATH="$HOME/.local/zig-linux-x86_64-0.13.0:$PATH"
cargo install --locked cargo-zigbuild

scripts/build-linux.sh --release --glibc 2.17   # 生产用：兼容 glibc >= 2.17
scripts/test-jni.sh release
mvn -f hadoop-adapter/pom.xml test -Dobs.native.profile=release
```

不带 `--glibc` 时链接构建机自身的 glibc，只适合在同等或更新的系统上运行。部署前可用
`objdump -T libobs_jni.so | grep -o 'GLIBC_[0-9.]*' | sort -uV | tail -1` 核对。

## 未验证

- 未在真实 CentOS 7 / EulerOS 节点或 MRS 集群上加载运行；glibc 2.17 结论来自符号版本检查。
- 只构建了 x86_64；aarch64（鲲鹏）需要 `--target aarch64-unknown-linux-gnu.2.17` 另行构建验证。
- Linux 上未做性能测量；WSL2 结果不代表物理机或云主机。
