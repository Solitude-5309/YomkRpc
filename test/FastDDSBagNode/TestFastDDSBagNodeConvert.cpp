/**
 * @file TestFastDDSBagNodeConvert.cpp
 * @brief FastDDSBagNode 节点层 bag convert 测试（直测静态函数，DDS-free）
 *
 * 范围：bagConvert 对 mcap::McapWriter 造的真实输入 bag 临时目录的转换断言（全缺省合并、
 *       主题展开精确/通配、时段裁剪含边界、输出分片滚动、多输入全局归并、旧格式拒转与
 *       类型冲突）与失败路径契约；产物 metadata.json 全字段 + 消息 logTime 读回升序断言 +
 *       bagInfoText 联动可读。服务层回执链路归 TestYomkRpcBagServiceContract。
 * 覆盖：
 *   C1 全缺省（单输入单条目仅 uri）→ 全主题全时段合并；metadata 对账（version/storage/
 *      relative_file_paths/starting_time+_format/duration+_format/message_count/topics
 *      name+count+type 透传）+ bagInfoText 联动 9 行；重复转换 → 输出目录已存在 fail；
 *   C2 topics 抽取：精确 + 通配混用 → 输出仅含展开集（未选主题不转换不入 metadata）；
 *      模式未命中 / 精确项不存在 / 两个 '*' → fail 精确文案；
 *   C3 时段裁剪：start/end 收窄含头含尾（2s..4s → 3 条）+ metadata 起始/时长按写入
 *      消息重算；start > end → fail；
 *   C4 输出分片：max_bagfile_size 触发滚动（大载荷跨 chunk 落盘）→ 多分片序列 +
 *      relative_file_paths 对账 + 全量不丢；max_bagfile_duration 触发滚动（步长 2s > 1s
 *      上限逐条滚）→ bag_0/1/2 确定序列；size < 1024 → fail；
 *   C5 多输入归并：两输入 logTime 交错（含多分片输入）→ 读回全局升序 + metadata 统计
 *      合并（两主题各自 type/count）；
 *   C6 旧格式拒转（入选主题无类型名）→ fail；同主题跨输入类型冲突 → fail 精确文案。
 *
 * 造文件经 mcap::McapWriter（写侧同款 open + Channel("cdr",0[,metadata]) + addChannel +
 * write + close，topicType 非空经 Channel.metadata 落类型名），MCAP_IMPLEMENTATION 单译元
 * 在被测库 FastDDSBagNode.cpp，此处仅声明头链接库内实现；读回经 mcap::McapReader 线性视图
 * （同译元）。固定 epoch 纳秒时间戳便于断言。用例目录避开 bag_ 前缀（防泄漏快照误报
 * 惯例），结束统一清理。
 *
 * 风格：纯 main() + CHECK 宏 + 失败计数（零第三方依赖），返回非 0 表示存在失败用例。
 */

#include "TestCheck.h"
#include "FastDDSBagNode.h" // 被测静态函数 bagConvert/bagInfoText（DDS-free 直测）

#include <mcap/reader.hpp> // 转换产物消息读回（实现经链接库内单译元，仅声明头）
#include <mcap/writer.hpp> // 造真实 bag 文件（同上）

#include <nlohmann/json.hpp> // 转换产物 metadata.json 读回断言（vendored，header-only）

#include <cstddef> // std::byte
#include <cstdint>
#include <limits>
#include <filesystem>
#include <fstream>
#include <string>
#include <vector>

namespace
{
    constexpr std::uint64_t kSecondNs = 1000000000; // 1 秒的纳秒数（固定时间戳基础单位）
    constexpr std::size_t kBigPayloadBytes = 800 * 1024; // C4 大载荷（单条超默认 chunk 触发落盘）

    // 造真实 mcap 分片文件（写侧同款 open + Channel("cdr",0[,metadata]) + addChannel +
    // write + close）：messageCount 条消息 logTime 自 firstLogTimeNs 起 stepNs 递增；
    // topicType 非空经 Channel.metadata 落类型名（新格式），空则不写 metadata（旧格式
    // bag）；payloadSize 控制单条载荷字节数（默认 5 字节，C4 用大载荷触发 size 分片）
    bool makeMcapFile(const std::string &path, const std::string &topic,
                      std::size_t messageCount, std::uint64_t firstLogTimeNs,
                      std::uint64_t stepNs, const std::string &topicType = "",
                      std::size_t payloadSize = 5)
    {
        mcap::McapWriter writer;
        mcap::McapWriterOptions options("");
        options.compression = mcap::Compression::None;
        if (!writer.open(path, options).ok())
        {
            return false;
        }
        mcap::Channel channel(topic, "cdr", 0,
                              topicType.empty() ? mcap::KeyValueMap{}
                                                : mcap::KeyValueMap{{"type", topicType}});
        writer.addChannel(channel);
        const std::string payload(payloadSize, 'x'); // 任意非空透传载荷（字节内容不参与断言）
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

    // 读回转换产物 metadata.json 为 nlohmann json（断言前先保证可解析）
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

    // 读回产物分片的消息 logTime 序列（线性视图顺序读，不依赖 summary）
    bool collectLogTimes(const std::string &path, std::vector<std::uint64_t> &out)
    {
        mcap::McapReader reader;
        if (!reader.open(path).ok())
        {
            return false;
        }
        // 经官方 readMessages 构造线性视图（onProblem 传忽略回调：mcap 会直调该回调，
        // 空 std::function 抛 bad_function_call）；无 summary 时 byteRange 退化全数据区
        mcap::LinearMessageView view = reader.readMessages(
            [](const mcap::Status &) {}, mcap::ReadMessageOptions{});
        for (const auto &msgView : view)
        {
            out.push_back(msgView.message.logTime);
        }
        reader.close();
        return true;
    }

    bool writeCfg(const std::string &path, const std::string &content)
    {
        std::ofstream out(path);
        out << content;
        return static_cast<bool>(out);
    }

    void cleanupDir(const std::string &dir)
    {
        std::error_code ec;
        std::filesystem::remove_all(dir, ec);
    }

    // 用例前置目录准备：error_code 兜底忽略已存在等错误（避免把 string 错误出参误传）
    void ensureDir(const std::string &dir)
    {
        std::error_code ec;
        std::filesystem::create_directories(dir, ec);
    }
} // namespace

int main()
{
    std::string err;
    std::vector<std::string> lines;

    // ---- C1：全缺省（单输入单条目仅 uri）→ 全主题全时段合并 + metadata 对账 + 联动可读 ----
    {
        const std::string inDir = "conv_in_c1";
        const std::string outDir = "conv_out_c1";
        const std::string cfg = "conv_c1.json";
        cleanupDir(inDir);
        cleanupDir(outDir);
        ensureDir(inDir);
        CHECK(makeMcapFile(inDir + "/bag_0.mcap", "/rt/c1", 3, kSecondNs, kSecondNs,
                  "YomkRpc::MString"),
              "C1 前置：输入分片造文件成功");
        CHECK(writeCfg(cfg, R"({"output_bags": [{"uri": ")" + outDir + R"("}]})"),
              "C1 前置：cfg 写入成功");

        lines.clear();
        CHECK(FastDDSBagNode::bagConvert({inDir}, cfg, lines, &err), "C1 全缺省转换成功");
        CHECK(lines.size() == 1 && lines[0].rfind("converted " + outDir + ":", 0) == 0 &&
                  lines[0].find("3 messages") != std::string::npos,
              "C1 统计行（converted <uri>: 3 messages / ...）");

        nlohmann::json info;
        CHECK(readMetadataJson(outDir, info), "C1 产物 metadata.json 可解析");
        CHECK(info.at("version") == 1 && info.at("storage_identifier") == "mcap" &&
                  info.at("relative_file_paths") == nlohmann::json::array({"bag_0.mcap"}),
              "C1 version/storage/relative_file_paths 对账");
        CHECK(info.at("starting_time").at("nanoseconds_since_epoch") == kSecondNs &&
                  !info.at("starting_time").at("nanoseconds_since_epoch_format")
                       .get<std::string>()
                       .empty(),
              "C1 starting_time ns+_format 对账（取写入消息 logTime）");
        CHECK(info.at("duration").at("nanoseconds") == 2 * kSecondNs &&
                  !info.at("duration").at("nanoseconds_format").get<std::string>().empty(),
              "C1 duration ns+_format 对账（3s..1s 全局跨度 2s）");
        CHECK(info.at("message_count") == 3, "C1 message_count 对账");
        CHECK(info.at("topics_with_message_count").size() == 1 &&
                  info.at("topics_with_message_count")[0].at("topic_metadata").at("name") ==
                      "/rt/c1" &&
                  info.at("topics_with_message_count")[0].at("topic_metadata").at("type") ==
                      "YomkRpc::MString" &&
                  info.at("topics_with_message_count")[0].at("message_count") == 3,
              "C1 topics name/type（Channel.metadata 透传）/count 对账");

        std::vector<std::uint64_t> logTimes;
        CHECK(collectLogTimes(outDir + "/bag_0.mcap", logTimes) && logTimes.size() == 3 &&
                  logTimes[0] == kSecondNs && logTimes[1] == 2 * kSecondNs &&
                  logTimes[2] == 3 * kSecondNs,
              "C1 消息读回 3 条且 logTime 保真升序");

        std::vector<std::string> infoLines;
        CHECK(FastDDSBagNode::bagInfoText(outDir, infoLines, &err), "C1 bagInfoText 联动可读");
        CHECK(infoLines.size() == 9 && infoLines[0].empty() &&
                  infoLines[3] == "Storage id:        mcap",
              "C1 info 文本 9 行对齐（首行空行 + Storage id）");

        lines.clear();
        err.clear();
        CHECK(!FastDDSBagNode::bagConvert({inDir}, cfg, lines, &err) &&
                  err.find("output dir [" + outDir + "] already exists") != std::string::npos,
              "C1 重复转换 → 输出目录已存在 fail");

        cleanupDir(inDir);
        cleanupDir(outDir);
        std::filesystem::remove(cfg);
    }

    // ---- C2：topics 抽取（精确 + 通配）→ 仅展开集；未命中/不存在/两星 fail ----
    {
        const std::string inDir = "conv_in_c2";
        const std::string outDir = "conv_out_c2";
        const std::string cfg = "conv_c2.json";
        cleanupDir(inDir);
        cleanupDir(outDir);
        ensureDir(inDir);
        CHECK(makeMcapFile(inDir + "/bag_0.mcap", "/rt/a", 1, kSecondNs, kSecondNs, "T_A") &&
                  makeMcapFile(inDir + "/bag_1.mcap", "/rt/b", 1, 2 * kSecondNs, kSecondNs,
                      "T_B") &&
                  makeMcapFile(inDir + "/bag_2.mcap", "/rt/c", 1, 3 * kSecondNs, kSecondNs,
                      "T_C"),
              "C2 前置：三主题输入分片造文件成功");

        CHECK(writeCfg(cfg,
                  R"({"output_bags": [{"uri": ")" + outDir + R"(", "topics": ["/rt/a", "/rt/b*"]}]})"),
              "C2 前置：cfg（精确 /rt/a + 通配 /rt/b*）写入成功");
        lines.clear();
        CHECK(FastDDSBagNode::bagConvert({inDir}, cfg, lines, &err), "C2 抽取转换成功");
        nlohmann::json info;
        CHECK(readMetadataJson(outDir, info), "C2 产物 metadata.json 可解析");
        const nlohmann::json topics = info.at("topics_with_message_count");
        CHECK(topics.size() == 2 && topics[0].at("topic_metadata").at("name") == "/rt/a" &&
                  topics[0].at("topic_metadata").at("type") == "T_A" &&
                  topics[1].at("topic_metadata").at("name") == "/rt/b" &&
                  topics[1].at("topic_metadata").at("type") == "T_B",
              "C2 输出仅含展开集（/rt/a ∪ /rt/b* 命中；/rt/c 不入选；type 逐主题透传）");

        cleanupDir(outDir);
        CHECK(writeCfg(cfg,
                  R"({"output_bags": [{"uri": ")" + outDir + R"(", "topics": ["/rt/nope*"]}]})"),
              "C2 前置：cfg（模式未命中）写入成功");
        lines.clear();
        err.clear();
        CHECK(!FastDDSBagNode::bagConvert({inDir}, cfg, lines, &err) &&
                  err.find("topic pattern [/rt/nope*] matched no input topic") !=
                      std::string::npos,
              "C2 模式未命中 fail 精确文案");

        CHECK(writeCfg(cfg,
                  R"({"output_bags": [{"uri": ")" + outDir + R"(", "topics": ["/rt/none"]}]})"),
              "C2 前置：cfg（精确项不存在）写入成功");
        lines.clear();
        err.clear();
        CHECK(!FastDDSBagNode::bagConvert({inDir}, cfg, lines, &err) &&
                  err.find("topic [/rt/none] not found in input bags") != std::string::npos,
              "C2 精确项不存在 fail 精确文案");

        CHECK(writeCfg(cfg,
                  R"({"output_bags": [{"uri": ")" + outDir + R"(", "topics": ["a*b*c"]}]})"),
              "C2 前置：cfg（两个 '*'）写入成功");
        lines.clear();
        err.clear();
        CHECK(!FastDDSBagNode::bagConvert({inDir}, cfg, lines, &err) &&
                  err.find("topic pattern [a*b*c] must contain at most one '*'") !=
                      std::string::npos,
              "C2 两个 '*' fail 精确文案");

        cleanupDir(inDir);
        cleanupDir(outDir);
        std::filesystem::remove(cfg);
    }

    // ---- C3：时段裁剪（含头含尾）+ 起始/时长重算；start > end fail ----
    {
        const std::string inDir = "conv_in_c3";
        const std::string outDir = "conv_out_c3";
        const std::string cfg = "conv_c3.json";
        cleanupDir(inDir);
        cleanupDir(outDir);
        ensureDir(inDir);
        CHECK(makeMcapFile(inDir + "/bag_0.mcap", "/rt/t", 5, kSecondNs, kSecondNs, "T_T"),
              "C3 前置：5 条 1s..5s 输入造文件成功");
        CHECK(writeCfg(cfg,
                  R"({"output_bags": [{"uri": ")" + outDir +
                      R"(", "start_time": 2000000000, "end_time": 4000000000}]})"),
              "C3 前置：cfg（2s..4s 收窄）写入成功");

        lines.clear();
        CHECK(FastDDSBagNode::bagConvert({inDir}, cfg, lines, &err), "C3 时段裁剪转换成功");
        nlohmann::json info;
        CHECK(readMetadataJson(outDir, info), "C3 产物 metadata.json 可解析");
        CHECK(info.at("message_count") == 3 &&
                  info.at("starting_time").at("nanoseconds_since_epoch") == 2 * kSecondNs &&
                  info.at("duration").at("nanoseconds") == 2 * kSecondNs,
              "C3 裁剪后 3 条且起始/时长按写入消息重算（2s..4s 跨度 2s）");
        std::vector<std::uint64_t> logTimes;
        CHECK(collectLogTimes(outDir + "/bag_0.mcap", logTimes) && logTimes.size() == 3 &&
                  logTimes[0] == 2 * kSecondNs && logTimes[1] == 3 * kSecondNs &&
                  logTimes[2] == 4 * kSecondNs,
              "C3 边界含头含尾（2s/3s/4s）");

        CHECK(writeCfg(cfg,
                  R"({"output_bags": [{"uri": ")" + outDir +
                      R"(", "start_time": 4000000000, "end_time": 2000000000}]})"),
              "C3 前置：cfg（start > end）写入成功");
        lines.clear();
        err.clear();
        CHECK(!FastDDSBagNode::bagConvert({inDir}, cfg, lines, &err) &&
                  err.find("start_time must not exceed end_time") != std::string::npos,
              "C3 start > end fail 精确文案");

        cleanupDir(inDir);
        cleanupDir(outDir);
        std::filesystem::remove(cfg);
    }

    // ---- C4：输出分片滚动（size 大载荷 / duration 步长）+ 下限校验 ----
    {
        const std::string inDir = "conv_in_c4";
        const std::string outDir = "conv_out_c4";
        const std::string cfg = "conv_c4.json";
        cleanupDir(inDir);
        cleanupDir(outDir);
        ensureDir(inDir);
        CHECK(makeMcapFile(inDir + "/bag_0.mcap", "/rt/s", 3, kSecondNs, kSecondNs, "T_S",
                  kBigPayloadBytes),
              "C4 前置：大载荷 3 条输入造文件成功");
        CHECK(writeCfg(cfg,
                  R"({"output_bags": [{"uri": ")" + outDir +
                      R"(", "max_bagfile_size": 1024}]})"),
              "C4 前置：cfg（size 下限 1024）写入成功");

        lines.clear();
        CHECK(FastDDSBagNode::bagConvert({inDir}, cfg, lines, &err), "C4 size 分片转换成功");
        nlohmann::json info;
        CHECK(readMetadataJson(outDir, info), "C4 产物 metadata.json 可解析");
        const nlohmann::json files = info.at("relative_file_paths");
        CHECK(files.size() >= 2 && files[0] == "bag_0.mcap" && files[1] == "bag_1.mcap",
              "C4 size 滚动多分片且序列从 bag_0 起（chunk 落盘触发写前检查）");
        CHECK(info.at("message_count") == 3, "C4 分片不丢消息（总数对账）");
        std::vector<std::uint64_t> logTimes;
        CHECK(collectLogTimes(outDir + "/bag_0.mcap", logTimes),
              "C4 首分片可读（summary 自动补写）");
        logTimes.clear();
        for (const auto &f : files)
        {
            std::vector<std::uint64_t> part;
            CHECK(collectLogTimes(outDir + "/" + f.get<std::string>(), part),
                  "C4 分片逐个可读");
            logTimes.insert(logTimes.end(), part.begin(), part.end());
        }
        CHECK(logTimes.size() == 3 && logTimes[0] == kSecondNs &&
                  logTimes[1] == 2 * kSecondNs && logTimes[2] == 3 * kSecondNs,
              "C4 跨分片读回 3 条且 logTime 保真");

        // duration 滚动：步长 2s > 1s 上限 → 每条滚一片（确定性 3 分片）
        cleanupDir(inDir);
        cleanupDir(outDir);
        ensureDir(inDir);
        CHECK(makeMcapFile(inDir + "/bag_0.mcap", "/rt/d", 3, kSecondNs, 2 * kSecondNs, "T_D"),
              "C4 前置：步长 2s 输入造文件成功");
        CHECK(writeCfg(cfg,
                  R"({"output_bags": [{"uri": ")" + outDir +
                      R"(", "max_bagfile_duration": 1}]})"),
              "C4 前置：cfg（duration 1s）写入成功");
        lines.clear();
        CHECK(FastDDSBagNode::bagConvert({inDir}, cfg, lines, &err), "C4 duration 分片转换成功");
        CHECK(readMetadataJson(outDir, info) &&
                  info.at("relative_file_paths") ==
                      nlohmann::json::array({"bag_0.mcap", "bag_1.mcap", "bag_2.mcap"}),
              "C4 duration 滚动 3 分片确定序列（严格大于：2s 差 > 1s 上限）");

        // size < 1024 fail（解析期校验，对齐 record 下限）
        CHECK(writeCfg(cfg,
                  R"({"output_bags": [{"uri": ")" + outDir +
                      R"(", "max_bagfile_size": 999}]})"),
              "C4 前置：cfg（size 999）写入成功");
        lines.clear();
        err.clear();
        CHECK(!FastDDSBagNode::bagConvert({inDir}, cfg, lines, &err) &&
                  err.find("max_bagfile_size must be at least 1024 bytes (got 999)") !=
                      std::string::npos,
              "C4 size < 1024 fail 精确文案");

        cleanupDir(inDir);
        cleanupDir(outDir);
        std::filesystem::remove(cfg);
    }

    // ---- C5：多输入归并（logTime 交错 + 多分片输入）→ 全局升序 + 统计合并 ----
    {
        const std::string inA = "conv_in_c5a";
        const std::string inB = "conv_in_c5b";
        const std::string outDir = "conv_out_c5";
        const std::string cfg = "conv_c5.json";
        cleanupDir(inA);
        cleanupDir(inB);
        cleanupDir(outDir);
        ensureDir(inA);
        ensureDir(inB);
        CHECK(makeMcapFile(inA + "/bag_0.mcap", "/rt/a", 3, kSecondNs, 2 * kSecondNs, "T_A"),
              "C5 前置：输入 A（1s/3s/5s）造文件成功");
        CHECK(makeMcapFile(inB + "/bag_0.mcap", "/rt/b", 1, 2 * kSecondNs, kSecondNs, "T_B") &&
                  makeMcapFile(inB + "/bag_1.mcap", "/rt/b", 2, 4 * kSecondNs, 2 * kSecondNs,
                      "T_B"),
              "C5 前置：输入 B（2s + 4s/6s 两分片）造文件成功");
        CHECK(writeCfg(cfg, R"({"output_bags": [{"uri": ")" + outDir + R"("}]})"),
              "C5 前置：cfg 写入成功");

        lines.clear();
        CHECK(FastDDSBagNode::bagConvert({inA, inB}, cfg, lines, &err),
              "C5 两输入归并转换成功");
        nlohmann::json info;
        CHECK(readMetadataJson(outDir, info), "C5 产物 metadata.json 可解析");
        CHECK(info.at("message_count") == 6 &&
                  info.at("starting_time").at("nanoseconds_since_epoch") == kSecondNs &&
                  info.at("duration").at("nanoseconds") == 5 * kSecondNs,
              "C5 metadata 统计合并（6 条；1s..6s 全局跨度 5s）");
        const nlohmann::json topics = info.at("topics_with_message_count");
        CHECK(topics.size() == 2 && topics[0].at("topic_metadata").at("name") == "/rt/a" &&
                  topics[0].at("message_count") == 3 &&
                  topics[1].at("topic_metadata").at("name") == "/rt/b" &&
                  topics[1].at("message_count") == 3,
              "C5 两主题各 3 条（跨输入按主题合并计数）");
        std::vector<std::uint64_t> logTimes;
        CHECK(collectLogTimes(outDir + "/bag_0.mcap", logTimes) && logTimes.size() == 6,
              "C5 归并读回 6 条");
        bool ascending = true;
        for (std::size_t i = 1; i < logTimes.size(); ++i)
        {
            ascending = ascending && logTimes[i - 1] < logTimes[i];
        }
        CHECK(ascending && logTimes.front() == kSecondNs && logTimes.back() == 6 * kSecondNs,
              "C5 全局 logTime 升序（含交错输入与跨分片续流）");

        cleanupDir(inA);
        cleanupDir(inB);
        cleanupDir(outDir);
        std::filesystem::remove(cfg);
    }

    // ---- C6：旧格式拒转（入选主题无类型名）+ 同主题跨输入类型冲突 ----
    {
        const std::string inDir = "conv_in_c6";
        const std::string inA = "conv_in_c6a";
        const std::string inB = "conv_in_c6b";
        const std::string outDir = "conv_out_c6";
        const std::string cfg = "conv_c6.json";
        cleanupDir(inDir);
        cleanupDir(inA);
        cleanupDir(inB);
        cleanupDir(outDir);

        // 旧格式（Channel 无 metadata 类型名）：入选主题类型名为空 → 拒转
        ensureDir(inDir);
        CHECK(makeMcapFile(inDir + "/bag_0.mcap", "/rt/old", 2, kSecondNs, kSecondNs),
              "C6 前置：旧格式输入（无类型名）造文件成功");
        CHECK(writeCfg(cfg, R"({"output_bags": [{"uri": ")" + outDir + R"("}]})"),
              "C6 前置：cfg 写入成功");
        lines.clear();
        err.clear();
        CHECK(!FastDDSBagNode::bagConvert({inDir}, cfg, lines, &err) &&
                  err.find("topic [/rt/old] has no type name") != std::string::npos,
              "C6 旧格式入选主题拒转 fail 精确文案");
        std::error_code ec;
        CHECK(!std::filesystem::exists(outDir, ec), "C6 拒转发生在输出目录创建之前（不落盘）");

        // 同主题跨输入类型冲突（预扫期整体报错）
        ensureDir(inA);
        ensureDir(inB);
        CHECK(makeMcapFile(inA + "/bag_0.mcap", "/rt/x", 1, kSecondNs, kSecondNs, "T1") &&
                  makeMcapFile(inB + "/bag_0.mcap", "/rt/x", 1, 2 * kSecondNs, kSecondNs, "T2"),
              "C6 前置：同主题不同类型两输入造文件成功");
        lines.clear();
        err.clear();
        CHECK(!FastDDSBagNode::bagConvert({inA, inB}, cfg, lines, &err) &&
                  err.find("topic [/rt/x] has conflicting types [T1] vs [T2]") !=
                      std::string::npos,
              "C6 跨输入类型冲突 fail 精确文案");

        cleanupDir(inDir);
        cleanupDir(inA);
        cleanupDir(inB);
        std::filesystem::remove(cfg);
    }

    return testReport("TestFastDDSBagNodeConvert");
}
