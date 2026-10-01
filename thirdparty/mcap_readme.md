# mcap 使用说明

## linux下 mcap v2.1.3 C++使用方法

mcap 是 Foxglove 开源的日志容器文件格式（github.com/foxglove/mcap，MIT license）。
本仓以 header-only 形式 vendoring 于 `thirdparty/mcap/include/mcap/`（13 个头文件，
无编译产物），实现随使用方源码一起编译。示例仅演示基本用法，mcap 自身质量由
上游提供者测试。

### 编译示例

1. cd thirdparty/mcap/test
2. cmake -S . -B build
3. cmake --build build

### 运行示例

1. ./build/mcap_example
2. 预期输出：写入 example.mcap（1 个无 schema 通道 rt/chatter、3 条消息），
   随后读回打印消息清单（topic / encoding / schemaId / logTime / sequence /
   字节数 / 内容 hex）

### 使用要点

1. header-only 集成：在恰好一个 .cpp 里 `#define MCAP_IMPLEMENTATION`，然后
   `#include <mcap/writer.hpp>` 与 `<mcap/reader.hpp>`；其余翻译单元只 include 头。
2. 写三步：`McapWriter::open(文件路径, McapWriterOptions)` →
   `addChannel(Channel(主题名, message_encoding, schema_id))` → `write(Message)`
   → `close()`。open / write 返回 `mcap::Status` 需检查；addSchema / addChannel
   返回 void，通道注册异常会在后续 write 时以 Status 报出。
3. 无 schema 通道（原始 CDR 字节透传落点）：`Channel("rt/chatter", "cdr", 0)`——
   schema_id=0 表示该通道无 schema，message_encoding 为任意字符串，原始 CDR
   字节可直接落盘，无需类型定义。
4. 压缩：`McapWriterOptions::compression` 可选 None / Lz4 / Zstd（chunk 级压缩，
   逐 chunk 独立）。本仓默认以编译宏 `MCAP_COMPRESSION_NO_LZ4` /
   `MCAP_COMPRESSION_NO_ZSTD` 禁用压缩依赖（零第三方硬依赖）；启用 lz4/zstd
   压缩时删除对应宏并引入依赖库。
5. 读三步：`McapReader::open(FileStreamReader 或文件路径)` → `readMessages`
   遍历 MessageView（`channel` / `message`）→ `close()`。官方另提供 mcap CLI
   （inspect / merge / split）与 C++/Python/Go/Rust 等多语言实现，可直接查看
   本仓产出的 .mcap 文件（Foxglove Studio 亦可直接打开）。
