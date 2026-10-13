# 发出 XML 的回车字符保留

2026-10-13。批量删除与分片合并共用的 XML 文本转义现在将回车编码为 `&#13;`，保留原有 &、<、> 转义。依据 [W3C XML 1.0 第 2.11 节](https://www.w3.org/TR/xml/#sec-line-ends)，解析器在处理字符引用之前将裸 CR 和 CRLF 归一化为 LF；旧输出因而可能改变包含回车的对象键。

两项新增核心测试模拟标准规定的行结束归一化，再解析文本，覆盖 CR、CRLF、LF、中文、制表符、引号、XML 标记字符和字面量字符引用。修改前两个测试均复现 CR 变为 LF；修改后通过。新增 HTTP 测试还验证批量删除的实际请求体及 Content-MD5 与转义后的发送字节一致。

完整 Windows Rust workspace 构建及回归 **326 通过、0 失败、15 ignored**（核心 109、缓存 1、文件系统 153、HTTP 63）。工作区保留 xml-outgoing-before.log 与 xml-outgoing-after.log，不上传日志或构建产物。本轮未运行真实 OBS/PFS、JVM/Hadoop 或性能基准，也不声称已解决 XML 1.0 无法表示的所有字符或入站 XML 的全部归一化问题。
