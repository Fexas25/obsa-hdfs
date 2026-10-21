# Hadoop 版本兼容（类加载）

2026-10-21。适配层按 Hadoop 3.3.6 编译，但要在 Huawei MRS 常见的 Hadoop 3.1.1 等较老版本上加载。
JVM 校验字节码时，若方法把某个类型当作其父类型返回或传参，就会加载该类型；引用旧版 Hadoop 中
不存在的类会让**整个** `OBSFileSystemRust` 无法加载，而不只是对应方法不可用。

## 规则

- 只在较新 Hadoop 中存在的类型，放进单独的辅助类，仅在调用对应功能时加载：
  `NativeVectoredRead`（`FileRange`，3.3.5+）、`NativeChecksums`（`CompositeCrcFileChecksum`，3.1+）。
- 不实现只在新版本存在的接口（如 `ByteBufferPositionedReadable`，3.3+）：实现它会使输入流类无法加载。
- 覆盖新版本才有的方法（如 `readVectored`、`hasPathCapability`）是安全的：签名擦除后只含旧版已有的类型，
  旧版 Hadoop 不会调用它们。

## 检查

```bash
scripts/check-hadoop-load.sh 3.1.1 3.3.6
```

用 `-Xverify:all` 加载、校验并初始化适配层 jar 中的每个类（`NativeObs` 除外，它会加载原生库）。
2026-10-21：3.1.1 与 3.3.6 均 16/16 通过；3.0.0 上仅 `NativeChecksums` 失败（只影响 getFileChecksum）。
审查中发现此前 `getFileChecksum` 直接构造 `CompositeCrcFileChecksum`，在 3.0.0 上会让 `OBSFileSystemRust`
整体加载失败，已改为经辅助类构造。此检查只覆盖类加载，不代表对应 Hadoop 版本的行为已验收。
