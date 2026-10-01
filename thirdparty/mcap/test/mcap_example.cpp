// =============================================================================
// mcap 用户基本使用示例（非测试：mcap 为第三方库，测试责任在提供者）
//
// 演示三步写与读回打印：
//   写：McapWriter::open → addChannel（schema_id=0 无 schema 通道，encoding="cdr"）
//       → write（3 条消息，payload 前 4 字节为长度前缀，模拟 CDR 封装字节）→ close
//   读：McapReader::open（FileStreamReader）→ readMessages 遍历打印 → close
//
// 编译运行见同目录 CMakeLists.txt 与 thirdparty/mcap_readme.md。
// =============================================================================
#define MCAP_IMPLEMENTATION
#include <mcap/reader.hpp>
#include <mcap/writer.hpp>

#include <chrono>
#include <cstring>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <vector>

// 系统时间转 mcap::Timestamp（纳秒）
static mcap::Timestamp nowNs() {
    return mcap::Timestamp(std::chrono::duration_cast<std::chrono::nanoseconds>(
                               std::chrono::system_clock::now().time_since_epoch())
                               .count());
}

// 打印一条消息：topic / encoding / schemaId / logTime / 字节数 / 前 16 字节 hex
static void printMessage(const std::string& topic, const std::string& encoding,
                         mcap::SchemaId schemaId, const mcap::Message& msg) {
    std::cout << "  topic=" << topic << "  encoding=" << encoding
              << "  schemaId=" << schemaId << "  logTime=" << msg.logTime
              << "  sequence=" << msg.sequence << "  bytes=" << msg.dataSize << "  head=[";
    constexpr size_t kHeadMax = 16;
    const size_t head = msg.dataSize < kHeadMax ? msg.dataSize : kHeadMax;
    for (size_t i = 0; i < head; ++i) {
        std::cout << std::hex << std::setw(2) << std::setfill('0')
                  << static_cast<int>(msg.data[i]) << (i + 1 < head ? " " : "");
    }
    std::cout << std::dec << "]" << std::endl;
}

int main() {
    const char* kFilePath = "example.mcap";

    // ---------------- 写路径 ----------------
    mcap::McapWriter writer;
    mcap::McapWriterOptions options("");  // 空 profile：不绑定任何框架约定
    options.compression = mcap::Compression::None;  // 不压缩，明文可直查

    mcap::Status status = writer.open(kFilePath, options);
    if (!status.ok()) {
        std::cerr << "writer.open 失败: " << status.message << std::endl;
        return 1;
    }

    // 无 schema 通道：schemaId=0（schema_id=0 表示此通道无 schema），encoding 记 "cdr"
    // 这是"原始 CDR 字节透传落盘"的直接落点——写字节即可，无需类型定义。
    // 注：addChannel 返回 void，通道注册异常会在后续 write 时以 Status 报出
    mcap::Channel channel("rt/chatter", "cdr", 0);
    writer.addChannel(channel);

    // 写 3 条消息：payload 前 4 字节为长度前缀（模拟 CDR 封装头），其后为消息内容
    for (uint32_t seq = 0; seq < 3; ++seq) {
        const std::string content = "hello mcap #" + std::to_string(seq);
        std::vector<std::byte> payload(4 + content.size());
        const uint32_t length = static_cast<uint32_t>(content.size());
        std::memcpy(payload.data(), &length, 4);
        std::memcpy(payload.data() + 4, content.data(), content.size());

        mcap::Message msg;
        msg.channelId = channel.id;
        msg.sequence = seq;
        msg.logTime = nowNs() + seq;       // 递增纳秒时间戳
        msg.publishTime = msg.logTime;
        msg.data = payload.data();
        msg.dataSize = payload.size();

        status = writer.write(msg);
        if (!status.ok()) {
            std::cerr << "write 失败: " << status.message << std::endl;
            writer.terminate();
            return 1;
        }
    }
    writer.close();  // 收尾写 summary（索引/统计）
    std::cout << "已写入 " << kFilePath << "（1 个无 schema 通道 rt/chatter，3 条消息）" << std::endl;

    // ---------------- 读路径 ----------------
    std::ifstream in(kFilePath, std::ios::binary);
    mcap::FileStreamReader dataSource{in};
    mcap::McapReader reader;
    status = reader.open(dataSource);
    if (!status.ok()) {
        std::cerr << "reader.open 失败: " << status.message << std::endl;
        return 1;
    }

    const auto onProblem = [](const mcap::Status& problem) {
        std::cerr << "读过程问题: " << problem.message << std::endl;
    };

    std::cout << "读回消息清单:" << std::endl;
    size_t count = 0;
    for (const auto& msgView : reader.readMessages(onProblem)) {
        const mcap::Channel& channelOfMsg = *msgView.channel;
        printMessage(channelOfMsg.topic, channelOfMsg.messageEncoding, channelOfMsg.schemaId,
                     msgView.message);
        ++count;
    }
    reader.close();
    std::cout << "共读回 " << count << " 条消息" << std::endl;
    return 0;
}
