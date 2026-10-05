/**
 * @file TestFastDDSBagNodeReindex.cpp
 * @brief FastDDSBagNode 节点层 bag reindex 测试（直测静态函数，DDS-free）
 *
 * 范围：bagReindex 对 mcap::McapWriter 造的真实 bag 临时目录的重建断言（编号序收集、
 *       多分片合并、主题名升序、metadata.json 全字段与 _format 伴生键、无条件覆盖、
 *       无 summary 回退扫描）与失败路径契约；重建产物 bagInfoText 联动可读断言。
 *       服务层回执链路归 TestYomkRpcBagServiceContract。
 * 覆盖：
 *   R1 单文件重建全字段（version/storage_identifier/relative_file_paths/starting_time
 *      ns+_format/duration ns+_format/message_count/topics name+count+type 经
 *      Channel.metadata 恢复）；
 *   R2 多分片合并（bag_0/1/2/10 编号序——bag_10 排 bag_2 之后非字典序；per-topic
 *      count 合并；starting=min、end=max → duration=全局跨度）；
 *   R3 metadata.json 已存在且损坏 → 覆盖重建成功；
 *   R4 metadata.json 缺失（核心场景）→ 重建成功且 bagInfoText 联动可读（首项空行 +
 *      Files/Duration/Start/End/Messages/Topic 行与重建值一致，Type 为恢复值）；
 *   R5 空消息分片（addChannel 无 write）→ 无 topic 条目、起止/时长/计数全 0；
 *   R6 无 summary 文件（noSummary 造法）→ AllowFallbackScan 回退重建成功；
 *   R7 空目录 → "empty directory"；有文件无匹配 → "no bag files found for reindexing"；
 *   R8 路径不存在 → "bag path [..] does not exist"；传文件路径 → "must specify a bag
 *      directory"；
 *   R9 旧格式 bag（Channel 无 metadata 类型名）→ type 回退空串不虚构。
 *
 * 造文件经 mcap::McapWriter（写侧同款 open/Channel("cdr",0[,metadata])/addChannel/write/
 * close，topicType 非空时经 Channel.metadata 落类型名——新格式；空即旧格式 bag），
 * MCAP_IMPLEMENTATION 单译元在被测库 FastDDSBagNode.cpp，此处仅声明头链接库内实现）；
 * 固定 epoch 纳秒时间戳便于断言。用例目录避开 bag_ 前缀（防泄漏快照误报惯例），
 * 结束统一清理。
 *
 * 风格：纯 main() + CHECK 宏 + 失败计数（零第三方依赖），返回非 0 表示存在失败用例。
 */

#include "TestCheck.h"
#include "FastDDSBagNode.h" // 被测静态函数 bagReindex/bagInfoText（DDS-free 直测）

#include <mcap/writer.hpp> // 造真实 bag 文件（实现经链接库内单译元，仅声明头）

#include <nlohmann/json.hpp> // 重建产物 metadata.json 读回断言（vendored，header-only）

#include <cstddef> // std::byte
#include <cstdint>
#include <cstdlib> // setenv
#include <ctime>   // tzset
#include <filesystem>
#include <fstream>
#include <string>
#include <vector>

namespace
{
    constexpr std::uint64_t kSecondNs = 1000000000; // 1 秒的纳秒数（固定时间戳基础单位）
    constexpr int kLabelWidth = 19;                 // info 标签列宽（"Topic information: " 宽度）

    // 标签行期望值拼装（与节点侧 bagInfoLabeledLine 同构：标签补齐 19 列 + 内容）
    std::string labeled(const std::string &label, const std::string &content)
    {
        return label + std::string(kLabelWidth - label.size(), ' ') + content;
    }

    // 造真实 mcap 分片文件（写侧同款 open + Channel("cdr",0[,metadata]) + addChannel +
    // write + close）：messageCount 条消息 logTime 自 firstLogTimeNs 起 stepNs 递增；
    // messageCount=0 仅注册 channel 不写消息（空分片）；noSummary=true 不写 summary 段
    // （无 summary 文件造法，触发读侧 AllowFallbackScan 回退扫描）；topicType 非空经
    // Channel.metadata 落类型名（新格式），空则不写 metadata（模拟旧格式 bag）
    bool makeMcapFile(const std::string &path, const std::string &topic,
                      std::size_t messageCount, std::uint64_t firstLogTimeNs,
                      std::uint64_t stepNs, bool noSummary = false,
                      const std::string &topicType = "")
    {
        mcap::McapWriter writer;
        mcap::McapWriterOptions options("");
        options.compression = mcap::Compression::None;
        options.noSummary = noSummary;
        if (!writer.open(path, options).ok())
        {
            return false;
        }
        mcap::Channel channel(topic, "cdr", 0,
                              topicType.empty() ? mcap::KeyValueMap{}
                                                : mcap::KeyValueMap{{"type", topicType}});
        writer.addChannel(channel);
        const std::string payload = "hello"; // 任意非空透传载荷（字节内容不参与统计）
        for (std::size_t i = 0; i < messageCount; ++i)
        {
            mcap::Message msg;
            msg.channelId = channel.id;
            msg.sequence = static_cast<std::uint32_t>(i);
            msg.logTime = firstLogTimeNs + i * stepNs;
            msg.publishTime = msg.logTime;
            // uint8_t 与 std::byte 同为单字节原始存储，别名转换安全（mcap 接口要求数据指针）
            msg.data = reinterpret_cast<const std::byte *>(payload.data()); // NOLINT(cppcoreguidelines-pro-type-reinterpret-cast)
            msg.dataSize = payload.size();
            if (!writer.write(msg).ok())
            {
                return false;
            }
        }
        writer.close();
        return true;
    }

    // 读回重建产物 metadata.json 为 nlohmann json（断言前先保证可解析）
    bool readMetadataJson(const std::string &dir, nlohmann::json &out)
    {
        std::ifstream meta(dir + "/metadata.json");
        if (!meta)
        {
            return false;
        }
        try
        {
            meta >> out;
        }
        catch (const nlohmann::json::exception &)
        {
            return false;
        }
        return true;
    }

    void cleanupDir(const std::string &dir)
    {
        std::error_code ec;
        std::filesystem::remove_all(dir, ec);
    }
} // namespace

int main()
{
    // 固定时区：_format 伴生键与 bagInfoText Start/End 行人类可读段断言不随执行环境漂移
    setenv("TZ", "UTC", 1);
    tzset();

    // ---- R1：单文件重建全字段（json 读回逐键断言） ----
    {
        const std::string dir = "reindex_r1";
        cleanupDir(dir);
        CHECK(std::filesystem::create_directory(dir), "R1 前置：临时目录创建成功");
        CHECK(makeMcapFile(dir + "/bag_0.mcap", "/rt/a", 3, kSecondNs, kSecondNs, false,
                           "YomkRpc::MString"),
              "R1 前置：单分片 3 条消息（logTime 1s/2s/3s，Channel.metadata 带类型名）");
        std::string error;
        CHECK(FastDDSBagNode::bagReindex(dir, &error), "R1 bagReindex → true");
        CHECK(error.empty(), "R1 成功路径 error 为空");
        nlohmann::json info;
        CHECK(readMetadataJson(dir, info), "R1 产物 metadata.json 可解析");
        CHECK(info.at("version").get<int>() == 1, "R1 version = 1");
        CHECK(info.at("storage_identifier").get<std::string>() == "mcap", "R1 storage = mcap");
        const auto files = info.at("relative_file_paths").get<std::vector<std::string>>();
        CHECK(files.size() == 1 && files[0] == "bag_0.mcap", "R1 relative_file_paths = [bag_0.mcap]");
        CHECK(info.at("starting_time").at("nanoseconds_since_epoch").get<std::uint64_t>() ==
                  kSecondNs,
              "R1 starting_time ns = 1s");
        CHECK(info.at("starting_time").at("nanoseconds_since_epoch_format").get<std::string>() ==
                  "1970-01-01_00-00-01_000-000-000",
              "R1 starting_time _format（UTC 可读伴生键）");
        CHECK(info.at("duration").at("nanoseconds").get<std::uint64_t>() == 2 * kSecondNs,
              "R1 duration ns = 2s（全局跨度 3s-1s）");
        CHECK(info.at("duration").at("nanoseconds_format").get<std::string>() ==
                  "00-00-02_000-000-000",
              "R1 duration _format（可读伴生键）");
        CHECK(info.at("message_count").get<std::uint64_t>() == 3, "R1 message_count = 3");
        const auto &topics = info.at("topics_with_message_count");
        CHECK(topics.is_array() && topics.size() == 1, "R1 topics 数组单元素");
        if (topics.size() == 1)
        {
            CHECK(topics[0].at("topic_metadata").at("name").get<std::string>() == "/rt/a",
                  "R1 topic name = /rt/a");
            CHECK(topics[0].at("topic_metadata").at("type").get<std::string>() ==
                      "YomkRpc::MString",
                  "R1 topic type 经 Channel.metadata 恢复");
            CHECK(topics[0].at("message_count").get<std::uint64_t>() == 3, "R1 topic count = 3");
        }
        cleanupDir(dir);
    }

    // ---- R2：多分片合并（编号序 + count 合并 + 起止 min/max） ----
    {
        const std::string dir = "reindex_r2";
        cleanupDir(dir);
        CHECK(std::filesystem::create_directory(dir), "R2 前置：临时目录创建成功");
        CHECK(makeMcapFile(dir + "/bag_0.mcap", "/rt/a", 2, kSecondNs, 2 * kSecondNs, false,
                           "YomkRpc::MString"),
              "R2 前置：bag_0（/rt/a 2 条 @1s,3s）造文件成功");
        CHECK(makeMcapFile(dir + "/bag_1.mcap", "/rt/b", 1, 2 * kSecondNs, kSecondNs, false,
                           "YomkRpc::MString"),
              "R2 前置：bag_1（/rt/b 1 条 @2s）造文件成功");
        CHECK(makeMcapFile(dir + "/bag_2.mcap", "/rt/c", 0, kSecondNs, kSecondNs, false,
                           "YomkRpc::MString"),
              "R2 前置：bag_2（/rt/c 空 channel 无消息）造文件成功");
        CHECK(makeMcapFile(dir + "/bag_10.mcap", "/rt/a", 1, 10 * kSecondNs, kSecondNs, false,
                           "YomkRpc::MString"),
              "R2 前置：bag_10（/rt/a 1 条 @10s）造文件成功");
        CHECK(FastDDSBagNode::bagReindex(dir, nullptr), "R2 bagReindex → true（error 出参可空）");
        nlohmann::json info;
        CHECK(readMetadataJson(dir, info), "R2 产物 metadata.json 可解析");
        const auto files = info.at("relative_file_paths").get<std::vector<std::string>>();
        CHECK(files.size() == 4 && files[0] == "bag_0.mcap" && files[1] == "bag_1.mcap" &&
                  files[2] == "bag_2.mcap" && files[3] == "bag_10.mcap",
              "R2 编号序收集（bag_10 排 bag_2 之后，非字典序）");
        CHECK(info.at("starting_time").at("nanoseconds_since_epoch").get<std::uint64_t>() ==
                  kSecondNs,
              "R2 starting = min（1s，跨分片取最小）");
        CHECK(info.at("duration").at("nanoseconds").get<std::uint64_t>() == 9 * kSecondNs,
              "R2 duration = 全局跨度（10s-1s=9s）");
        CHECK(info.at("duration").at("nanoseconds_format").get<std::string>() ==
                  "00-00-09_000-000-000",
              "R2 duration _format");
        CHECK(info.at("message_count").get<std::uint64_t>() == 4, "R2 message_count = 4（合并）");
        const auto &topics = info.at("topics_with_message_count");
        CHECK(topics.is_array() && topics.size() == 2, "R2 topics 两项（主题名升序）");
        if (topics.size() == 2)
        {
            CHECK(topics[0].at("topic_metadata").at("name").get<std::string>() == "/rt/a" &&
                      topics[0].at("message_count").get<std::uint64_t>() == 3,
                  "R2 /rt/a count = 3（2+1 跨分片合并，升序首位）");
            CHECK(topics[0].at("topic_metadata").at("type").get<std::string>() ==
                      "YomkRpc::MString",
                  "R2 /rt/a type 恢复（跨分片同 Channel.metadata）");
            CHECK(topics[1].at("topic_metadata").at("name").get<std::string>() == "/rt/b" &&
                      topics[1].at("message_count").get<std::uint64_t>() == 1,
                  "R2 /rt/b count = 1");
            CHECK(topics[1].at("topic_metadata").at("type").get<std::string>() ==
                      "YomkRpc::MString",
                  "R2 /rt/b type 恢复");
        }
        cleanupDir(dir);
    }

    // ---- R3：metadata.json 已存在且损坏 → 覆盖重建成功 ----
    {
        const std::string dir = "reindex_r3";
        cleanupDir(dir);
        CHECK(std::filesystem::create_directory(dir), "R3 前置：临时目录创建成功");
        CHECK(makeMcapFile(dir + "/bag_0.mcap", "/rt/a", 2, 5 * kSecondNs, kSecondNs, false,
                           "YomkRpc::MString"),
              "R3 前置：单分片 2 条消息（@5s,6s）造文件成功");
        {
            std::ofstream broken(dir + "/metadata.json");
            broken << "{ not valid json";
        }
        CHECK(FastDDSBagNode::bagReindex(dir, nullptr), "R3 损坏 metadata 覆盖重建 → true");
        nlohmann::json info;
        CHECK(readMetadataJson(dir, info), "R3 重建产物为合法 JSON（覆盖成功）");
        CHECK(info.at("message_count").get<std::uint64_t>() == 2, "R3 message_count = 2");
        const auto &r3Topics = info.at("topics_with_message_count");
        CHECK(r3Topics.is_array() && r3Topics.size() == 1 &&
                  r3Topics[0].at("topic_metadata").at("type").get<std::string>() ==
                      "YomkRpc::MString",
              "R3 覆盖重建后 type 经 Channel.metadata 恢复");
        cleanupDir(dir);
    }

    // ---- R4：metadata.json 缺失（核心场景）→ 重建成功且 bagInfoText 联动可读 ----
    {
        const std::string dir = "reindex_r4";
        cleanupDir(dir);
        CHECK(std::filesystem::create_directory(dir), "R4 前置：临时目录创建成功");
        CHECK(makeMcapFile(dir + "/bag_0.mcap", "/rt/a", 3, kSecondNs, kSecondNs, false,
                           "YomkRpc::MString"),
              "R4 前置：单分片 3 条消息造文件成功（目录无 metadata.json）");
        std::string error;
        CHECK(FastDDSBagNode::bagReindex(dir, &error), "R4 缺失 metadata 重建 → true");
        CHECK(error.empty(), "R4 成功路径 error 为空");
        std::vector<std::string> lines;
        CHECK(FastDDSBagNode::bagInfoText(dir, lines, nullptr), "R4 重建后 bagInfoText 联动可读");
        CHECK(lines.size() == 9, "R4 行数 = 9（空行 + 七段标签 + 单主题行）");
        if (lines.size() == 9)
        {
            CHECK(lines[0].empty(), "R4 首行为空行");
            CHECK(lines[1] == labeled("Files:", "bag_0.mcap"), "R4 Files 行");
            CHECK(lines[4] == labeled("Duration:", "2.000000000s"), "R4 Duration 行与重建值一致");
            CHECK(lines[5] == labeled("Start:", "Jan  1 1970 00:00:01.000000000 (1.000000000)"),
                  "R4 Start 行（%e 空格填充日 + 9 位纳秒）");
            CHECK(lines[6] == labeled("End:", "Jan  1 1970 00:00:03.000000000 (3.000000000)"),
                  "R4 End 行（= Start + duration）");
            CHECK(lines[7] == labeled("Messages:", "3"), "R4 Messages 行");
            CHECK(lines[8] == labeled("Topic information:",
                              "Topic: /rt/a | Type: YomkRpc::MString | Count: 3 | "
                              "Serialization Format: cdr"),
                  "R4 Topic 行（Type 为恢复值，与重建产物一致）");
        }
        cleanupDir(dir);
    }

    // ---- R5：空消息分片（addChannel 无 write）→ 无 topic 条目、全 0 ----
    {
        const std::string dir = "reindex_r5";
        cleanupDir(dir);
        CHECK(std::filesystem::create_directory(dir), "R5 前置：临时目录创建成功");
        CHECK(makeMcapFile(dir + "/bag_0.mcap", "/rt/empty", 0, kSecondNs, kSecondNs),
              "R5 前置：空消息分片造文件成功");
        CHECK(FastDDSBagNode::bagReindex(dir, nullptr), "R5 空消息分片重建 → true");
        nlohmann::json info;
        CHECK(readMetadataJson(dir, info), "R5 产物 metadata.json 可解析");
        CHECK(info.at("message_count").get<std::uint64_t>() == 0, "R5 message_count = 0");
        CHECK(info.at("starting_time").at("nanoseconds_since_epoch").get<std::uint64_t>() == 0,
              "R5 starting_time ns = 0（无消息对齐录制侧全 0 语义）");
        CHECK(info.at("duration").at("nanoseconds").get<std::uint64_t>() == 0, "R5 duration ns = 0");
        const auto &topics = info.at("topics_with_message_count");
        CHECK(topics.is_array() && topics.empty(),
              "R5 topics 数组为空（无消息 channel 无统计条目）");
        cleanupDir(dir);
    }

    // ---- R6：无 summary 文件（noSummary 造法）→ 回退扫描重建成功 ----
    {
        const std::string dir = "reindex_r6";
        cleanupDir(dir);
        CHECK(std::filesystem::create_directory(dir), "R6 前置：临时目录创建成功");
        CHECK(makeMcapFile(dir + "/bag_0.mcap", "/rt/scan", 1, 5 * kSecondNs, kSecondNs, true),
              "R6 前置：noSummary 分片造文件成功（open+addChannel+write+close 无 summary 段）");
        std::string error;
        CHECK(FastDDSBagNode::bagReindex(dir, &error), "R6 无 summary 回退扫描重建 → true");
        CHECK(error.empty(), "R6 成功路径 error 为空");
        nlohmann::json info;
        CHECK(readMetadataJson(dir, info), "R6 产物 metadata.json 可解析");
        CHECK(info.at("message_count").get<std::uint64_t>() == 1, "R6 message_count = 1");
        CHECK(info.at("starting_time").at("nanoseconds_since_epoch").get<std::uint64_t>() ==
                  5 * kSecondNs,
              "R6 starting_time ns = 5s（扫描重建统计）");
        cleanupDir(dir);
    }

    // ---- R7：空目录 / 有文件无匹配（false + error 精确文案） ----
    {
        std::string error;
        const std::string emptyDir = "reindex_r7_empty";
        cleanupDir(emptyDir);
        CHECK(std::filesystem::create_directory(emptyDir), "R7 前置：空目录创建成功");
        CHECK(!FastDDSBagNode::bagReindex(emptyDir, &error), "R7 空目录 → false");
        CHECK(error == "empty directory", "R7 空目录 error 精确文案");
        cleanupDir(emptyDir);

        const std::string junkDir = "reindex_r7_junk";
        cleanupDir(junkDir);
        CHECK(std::filesystem::create_directory(junkDir), "R7 前置：无匹配目录创建成功");
        {
            std::ofstream junk(junkDir + "/notes.txt");
            junk << "not a bag file";
        }
        CHECK(!FastDDSBagNode::bagReindex(junkDir, &error), "R7 有文件无匹配 → false");
        CHECK(error == "no bag files found for reindexing", "R7 无匹配 error 精确文案");
        cleanupDir(junkDir);
    }

    // ---- R8：路径不存在 / 传文件路径（false + error 精确文案） ----
    {
        std::string error;
        CHECK(!FastDDSBagNode::bagReindex("reindex_no_such_dir", &error), "R8 路径不存在 → false");
        CHECK(error == "bag path [reindex_no_such_dir] does not exist", "R8 error 精确文案");

        const std::string filePath = "reindex_r8_not_dir";
        {
            std::ofstream file(filePath);
            file << "plain file";
        }
        CHECK(!FastDDSBagNode::bagReindex(filePath, &error), "R8 传文件路径 → false");
        CHECK(error == "must specify a bag directory", "R8 error 精确文案");
        std::error_code ec;
        std::filesystem::remove(filePath, ec);
    }

    // ---- R9：旧格式 bag 回退（Channel 无 metadata 类型名）→ type 空串 ----
    {
        const std::string dir = "reindex_r9";
        cleanupDir(dir);
        CHECK(std::filesystem::create_directory(dir), "R9 前置：临时目录创建成功");
        CHECK(makeMcapFile(dir + "/bag_0.mcap", "/rt/legacy", 2, kSecondNs, kSecondNs),
              "R9 前置：无 metadata 类型名的旧格式分片造文件成功");
        CHECK(FastDDSBagNode::bagReindex(dir, nullptr), "R9 旧格式 bag 重建 → true");
        nlohmann::json info;
        CHECK(readMetadataJson(dir, info), "R9 产物 metadata.json 可解析");
        const auto &topics = info.at("topics_with_message_count");
        CHECK(topics.is_array() && topics.size() == 1, "R9 topics 单元素");
        if (topics.size() == 1)
        {
            CHECK(topics[0].at("topic_metadata").at("type").get<std::string>().empty(),
                  "R9 topic type 空串（旧格式 bag 无 Channel.metadata，回退不虚构）");
        }
        cleanupDir(dir);
    }

    return testReport("TestFastDDSBagNodeReindex");
}
