/**
 * @file TestFastDDSBagNodeRecord.cpp
 * @brief FastDDSBagNode 节点层录制收尾测试（真实 DDS 端到端：发布 → 录制 → Ctrl+C 停止 → 落盘断言）
 *
 * 范围：record 全链路——发现校验通过（远端 MString 发布者在线）、透传订阅、消息直写 mcap、
 *       bagRecordStop 触发收尾（删除 reader → writer.close 补 summary → metadata.json →
 *       stats 回填），以及收尾产物断言（bag 目录、bag_0.mcap 读回、metadata.json 字段、
 *       stats 契约）与录制定格语义（成功后再次 record 拒绝）。启动校验分支归
 *       TestFastDDSBagNodeValidation。
 * 覆盖：
 *   R1 setDomainId(202) 成功；bagRecordReset 复位残留停止标志；
 *   R2 远端 MString 发布者（rt/bag_rec_hit）周期发布，录制线程 record 阻塞；
 *   R3 ~5s 后 bagRecordStop → record 返回 true；
 *   R4 stats 契约：单主题、topic/type（YomkRpc::MString）/count/bytes 一致；
 *   R5 落盘：bag 目录（bag_<YYYY-MM-DD_HH-MM-SS_mmm>）含 bag_0.mcap 与 metadata.json；
 *   R6 mcap 读回：条数 == stats.count；Channel topic/messageEncoding=cdr/schemaId=0；
 *      每条消息字节非空且总字节 == stats.bytes；sequence 从 0 单调递增；
 *   R7 metadata.json：storage_identifier=mcap / 起始时间与时长含 _format 可读伴生键 /
 *      relative_file_paths: bag_0.mcap /
 *      topics_with_message_count 的 name/type/message_count 与 stats 一致；
 *   R8 定格：成功后再次 record → false "record already finished, recreate node to record again"；
 *   R9 通配模式录制（新节点）：精确项 + 前缀模式命中同前缀双主题 → 展开去重合并
 *      （stats 两项升序、各有条数）、metadata 列出展开后的实际主题、录制定格同样成立；
 *   R10 outputDir 指定目录名（新节点）：bagDir() == 指定名（非时间戳名）、指定目录下
 *      bag_0.mcap 与 metadata.json 落盘；
 *   R11 --start-paused 暂停态启动（新节点两段）：全程暂停零写入（消息丢弃、目录照建）；
 *      暂停后恢复开始写入（metadata starting_time 非零——暂停期不计入）；
 *   R12 -b/--max-bag-size 分片录制（新节点）：大 payload 驱动 chunk flush 触发滚动，
 *      bag_0/bag_1 分片落盘非空、metadata relative_file_paths 列全部分片、分片不丢消息
 *      （count 与跨分片读回总条数 == 发布条数）、各分片自含 Channel 声明；
 *   R13 -d/--max-bag-duration 时长分片录制（新节点）：普通 payload 25 条 @200ms（4.8s
 *      跨度）驱动时长滚动（距分片首条严格大于 2s 即切），bag_0/bag_1/bag_2 分片落盘
 *      非空、metadata relative_file_paths 列出分片、分片不丢消息（count 与跨分片读回
 *      总条数 == 发布条数）、各分片自含 Channel 声明。
 *
 * 域号 202：与 TestYomkRpcBagServiceLifecycle(200)/TestFastDDSBagNodeValidation(201) 错开，
 * ctest 串行执行互不残留。
 *
 * 风格：纯 main() + CHECK 宏 + 失败计数（零第三方依赖），返回非 0 表示存在失败用例。
 */

#include "TestCheck.h"
#include "FastDDSBagNode.h"     // 被测节点（直测，不经服务层）
#include "YomkRpcBagService.h"  // yomk::bagRecordStop/bagRecordReset（停止标志读写端）

#include <YomkRpcMsg/YomkRpcMsgPubSubTypes.hpp> // MStringPubSubType（远端发布端制造录制流量）

#include <fastdds/dds/core/status/PublicationMatchedStatus.hpp> // R12 发布端等订阅匹配（防 VOLATILE 匹配前丢消息）
#include <fastdds/dds/domain/DomainParticipant.hpp>
#include <fastdds/dds/domain/DomainParticipantFactory.hpp>
#include <fastdds/dds/publisher/DataWriter.hpp>
#include <fastdds/dds/publisher/Publisher.hpp>
#include <fastdds/dds/topic/Topic.hpp>
#include <fastdds/dds/topic/TypeSupport.hpp>

#include <mcap/reader.hpp> // 读回断言（MCAP_IMPLEMENTATION 在被测库 FastDDSBagNode.cpp 单译元内，仅声明链接实现）

#include <atomic>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

namespace
{
    constexpr uint32_t TEST_DOMAIN = 202;    // 独立域，避开其他测试用例
    constexpr const char *REC_TOPIC = "rt/bag_rec_hit";
    constexpr const char *REC_SPLIT_TOPIC = "rt/bag_rec_split_a";
    constexpr const char *REC_TIME_TOPIC = "rt/bag_rec_time_a";
    // 录制时长：校验窗（SPDP 发现发布者约 1~3s + 全端点在线即收敛）+ ~5s 录制期（150ms/条）
    constexpr int kRecordMs = 12000;
    constexpr int kPubIntervalMs = 150;
    // R9 模式录制时长：新主题 EDP 发现（PDP 已互通约 1~3s）+ 快路径两轮防抖 + ~4s 录制期
    constexpr int kPatternRecordMs = 8000;
    // R11a 全程暂停录制窗：校验收敛（1~3s）+ 暂停丢弃期（≥1s，证零写入）
    constexpr int kPausedRecordMs = 4000;
    // R11b 恢复前暂停窗（校验收敛后仍有丢弃期）与恢复后录制窗
    constexpr int kResumeDelayMs = 2000;
    constexpr int kResumeRecordMs = 3000;
    // R12 分片用例：大 payload（128KB/条）驱动 mcap chunk（默认 768KB）约每 6 条 flush；
    // -b 取下限 1024（远小于 chunkSize，每次 flush 后 size 远超上限立即切片，实际每 chunk 一片）
    constexpr uint32_t kSplitPayloadBytes = 128 * 1024;
    constexpr int kSplitMessages = 20;
    constexpr uint64_t kSplitMaxBagSize = 1024; // 分片下限（与节点层校验下限同值）
    // 录制窗：校验窗（~3~6s）+ 发布端等订阅匹配后 20 条 @150ms（3s）+ 余量
    constexpr int kSplitRecordMs = 12000;
    // R13 时长分片用例：普通小 payload（不触发 size 条件），25 条 @200ms（4.8s 跨度）
    // 驱动 -d 时长滚动（距分片首条严格大于 2s 即切）；预期 3 分片（~0-2s / ~2-4s / ~4s+ 尾片）
    constexpr int kTimeMessages = 25;
    constexpr int kTimePubIntervalMs = 200;
    constexpr uint64_t kTimeMaxBagDurationSec = 2;
    // 录制窗：校验窗（~1~3s，同 R2 口径）+ 发布端等订阅匹配（<1s）+ 25 条 @200ms（5s）+ 余量
    constexpr int kTimeRecordMs = 13000;
} // namespace

int main()
{
    namespace dds = eprosima::fastdds::dds;  // main 作用域别名：直连 FastDDS API 搭建远端发布端

    // R1：复位残留停止标志（对齐服务层 bagRecord 前置复位的直测等价动作）
    yomk::bagRecordReset();

    FastDDSBagNode node;
    CHECK(node.setDomainId(TEST_DOMAIN), "R1 setDomainId(202) 成功（真实创建 participant）");

    // ---- R2：远端发布端 + 发布线程 ----
    auto *peerParticipant = dds::DomainParticipantFactory::get_instance()->create_participant(
        TEST_DOMAIN, dds::PARTICIPANT_QOS_DEFAULT);
    CHECK(peerParticipant != nullptr, "R2 远端发布端 participant 创建成功");
    dds::TypeSupport ts(new YomkRpc::MStringPubSubType());
    ts.register_type(peerParticipant);
    auto *pub = peerParticipant->create_publisher(dds::PUBLISHER_QOS_DEFAULT);
    auto *topic = peerParticipant->create_topic(REC_TOPIC, ts.get_type_name(), dds::TOPIC_QOS_DEFAULT);
    auto *writer =
        (pub != nullptr && topic != nullptr) ? pub->create_datawriter(topic, dds::DATAWRITER_QOS_DEFAULT)
                                             : nullptr;
    CHECK(writer != nullptr, "R2 远端 DataWriter 创建成功（rt/bag_rec_hit）");

    std::atomic<bool> pubStop{false};
    std::thread pubThread([&]()
    {
        YomkRpc::MString msg;
        uint32_t seq = 0;
        while (!pubStop.load())
        {
            msg.data("bag-record-payload-" + std::to_string(seq++));
            writer->write(&msg);
            std::this_thread::sleep_for(std::chrono::milliseconds(kPubIntervalMs));
        }
    });

    // 录制线程：record 长驻阻塞至 bagRecordStop 置位收尾
    std::vector<FastDDSBagNode::BagTopicStat> stats;
    std::string error;
    std::atomic<bool> recDone{false};
    bool ok = false;
    std::thread recThread([&]()
    {
        ok = node.record({REC_TOPIC}, stats, &error);
        recDone.store(true);
    });

    // ---- R3：等待录制期后模拟 Ctrl+C（bagRecordStop 置位），等待收尾完成 ----
    std::this_thread::sleep_for(std::chrono::milliseconds(kRecordMs));
    yomk::bagRecordStop();
    for (int i = 0; i < 100 && !recDone.load(); ++i)
    {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    recThread.join();
    pubStop.store(true);
    pubThread.join();

    CHECK(ok, "R3 bagRecordStop 后 record → true（校验通过 + 录制 + 收尾完成）");
    if (!ok)
    {
        std::cerr << "record error: " << error << std::endl;
        dds::DomainParticipantFactory::get_instance()->delete_participant(peerParticipant);
        return testReport("TestFastDDSBagNodeRecord");
    }

    // ---- R4：stats 契约 ----
    CHECK(stats.size() == 1, "R4 stats 单主题一项");
    if (stats.size() == 1)
    {
        CHECK(stats[0].topic == REC_TOPIC, "R4 stats.topic == rt/bag_rec_hit（用户输入原样）");
        CHECK(stats[0].type == "YomkRpc::MString", "R4 stats.type == YomkRpc::MString（远端类型名原样）");
        CHECK(stats[0].count >= 5, "R4 录制条数 >= 5（3s 校验窗 + 2s 录制期 @150ms/条）");
        CHECK(stats[0].bytes >= stats[0].count * 4, "R4 bytes >= count×4（每条至少含 encapsulation header）");
    }
    const uint64_t recCount = stats.empty() ? 0 : stats[0].count;
    const uint64_t recBytes = stats.empty() ? 0 : stats[0].bytes;

    // ---- R5：落盘文件断言 ----
    const std::string &bagDir = node.bagDir();
    CHECK(bagDir.rfind("bag_", 0) == 0, "R5 bagDir() 以 bag_ 开头");
    CHECK(bagDir.size() == std::string("bag_YYYY-MM-DD_HH-MM-SS_mmm").size(),
          "R5 bagDir() 形如 bag_<YYYY-MM-DD_HH-MM-SS_mmm>（27 字符）");
    CHECK(std::filesystem::exists(bagDir + "/bag_0.mcap"), "R5 bag_0.mcap 存在");
    CHECK(std::filesystem::exists(bagDir + "/metadata.json"), "R5 metadata.json 存在");
    CHECK(std::filesystem::file_size(bagDir + "/bag_0.mcap") > 0, "R5 bag_0.mcap 非空");

    // ---- R6：mcap 读回断言（条数/通道契约/字节守恒/序号单调） ----
    {
        std::ifstream in(bagDir + "/bag_0.mcap", std::ios::binary);
        mcap::FileStreamReader dataSource{in};
        mcap::McapReader reader;
        const auto openStatus = reader.open(dataSource);
        CHECK(openStatus.ok(), "R6 mcap reader.open 成功（summary 索引完整）");
        if (openStatus.ok())
        {
            const auto onProblem = [](const mcap::Status &problem)
            {
                std::cerr << "读过程问题: " << problem.message << std::endl;
            };
            uint64_t count = 0;
            uint64_t totalBytes = 0;
            uint32_t lastSeq = 0;
            bool seqMonotonic = true;
            bool firstMsg = true;
            std::string channelTopic;
            std::string channelEncoding;
            mcap::SchemaId channelSchemaId = 0;
            for (const auto &msgView : reader.readMessages(onProblem))
            {
                const mcap::Channel &channelOfMsg = *msgView.channel;
                channelTopic = channelOfMsg.topic;
                channelEncoding = channelOfMsg.messageEncoding;
                channelSchemaId = channelOfMsg.schemaId;
                totalBytes += msgView.message.dataSize;
                if (msgView.message.dataSize == 0)
                {
                    CHECK(false, "R6 消息字节为空（透传 CDR 全量不应为空）");
                }
                if (firstMsg)
                {
                    firstMsg = false;
                }
                else if (msgView.message.sequence <= lastSeq)
                {
                    seqMonotonic = false;
                }
                lastSeq = msgView.message.sequence;
                ++count;
            }
            CHECK(count == recCount, "R6 mcap 读回条数 == stats.count");
            CHECK(channelTopic == REC_TOPIC, "R6 Channel topic == rt/bag_rec_hit");
            CHECK(channelEncoding == "cdr", "R6 Channel messageEncoding == cdr");
            CHECK(channelSchemaId == 0, "R6 Channel schemaId == 0（无 schema 通道）");
            CHECK(totalBytes == recBytes, "R6 读回字节总和 == stats.bytes（CDR 全量守恒）");
            CHECK(seqMonotonic, "R6 sequence 从 0 单调递增");
            reader.close();
        }
    }

    // ---- R7：metadata.json 字段断言 ----
    {
        std::ifstream meta(bagDir + "/metadata.json");
        std::stringstream buf;
        buf << meta.rdbuf();
        const std::string text = buf.str();
        CHECK(text.find("\"nanoseconds_since_epoch_format\"") != std::string::npos,
              "R7 metadata starting_time 含可读格式键");
        CHECK(text.find("\"nanoseconds_format\"") != std::string::npos,
              "R7 metadata duration 含可读格式键");
        CHECK(text.find("\"version\": 1") != std::string::npos,
              "R7 metadata version: 1（yomkrpc 自有元信息格式版本）");
        CHECK(text.find("\"storage_identifier\": \"mcap\"") != std::string::npos,
              "R7 metadata storage_identifier: mcap");
        CHECK(text.find("bag_0.mcap") != std::string::npos, "R7 metadata relative_file_paths 列出 bag_0.mcap");
        CHECK(text.find("\"name\": \"" + std::string(REC_TOPIC) + "\"") != std::string::npos,
              "R7 metadata topics_with_message_count 列出录制主题名");
        CHECK(text.find("\"type\": \"YomkRpc::MString\"") != std::string::npos,
              "R7 metadata 主题类型 == YomkRpc::MString");
        CHECK(text.find("\"message_count\": " + std::to_string(recCount)) != std::string::npos,
              "R7 metadata message_count 与 stats 一致");
        CHECK(text.find("\"nanoseconds_since_epoch\"") != std::string::npos,
              "R7 metadata starting_time 存在");
    }

    // ---- R8：录制定格语义 ----
    {
        std::vector<FastDDSBagNode::BagTopicStat> statsAgain;
        std::string errorAgain;
        const bool okAgain = node.record({REC_TOPIC}, statsAgain, &errorAgain);
        CHECK(!okAgain, "R8 定格后再次 record → false");
        CHECK(errorAgain.find("record already finished") != std::string::npos,
              "R8 error 含 record already finished");
        CHECK(errorAgain.find("recreate node") != std::string::npos,
              "R8 error 提示须重建节点");
    }

    // ---- R9：通配模式录制（新节点：精确项 + 前缀模式命中同前缀双主题，展开去重合并） ----
    {
        // 复位停止标志（R3 已置位）；同域新建 bag 节点（node 已定格不可复用）
        yomk::bagRecordReset();
        FastDDSBagNode node2;
        CHECK(node2.setDomainId(TEST_DOMAIN), "R9 新节点 setDomainId(202) 成功");

        // 远端新增两个同前缀发布者（复用 peer participant 与 MString 类型）
        auto *topicA = peerParticipant->create_topic(
            "rt/bag_rec_pre_a", ts.get_type_name(), dds::TOPIC_QOS_DEFAULT);
        auto *topicB = peerParticipant->create_topic(
            "rt/bag_rec_pre_b", ts.get_type_name(), dds::TOPIC_QOS_DEFAULT);
        auto *writerA = (pub != nullptr && topicA != nullptr)
                            ? pub->create_datawriter(topicA, dds::DATAWRITER_QOS_DEFAULT)
                            : nullptr;
        auto *writerB = (pub != nullptr && topicB != nullptr)
                            ? pub->create_datawriter(topicB, dds::DATAWRITER_QOS_DEFAULT)
                            : nullptr;
        CHECK(writerA != nullptr && writerB != nullptr, "R9 远端同前缀双 DataWriter 创建成功");

        std::atomic<bool> pubStop2{false};
        std::thread pubThread2([&]()
        {
            YomkRpc::MString msg;
            uint32_t seq = 0;
            while (!pubStop2.load())
            {
                msg.data("bag-record-pattern-" + std::to_string(seq++));
                writerA->write(&msg);
                writerB->write(&msg);
                std::this_thread::sleep_for(std::chrono::milliseconds(kPubIntervalMs));
            }
        });

        // 录制清单：精确 a + 前缀模式（命中 a/b）→ 展开去重合并为 {a, b}（升序）
        std::vector<FastDDSBagNode::BagTopicStat> statsPattern;
        std::string errorPattern;
        std::atomic<bool> recDone2{false};
        bool okPattern = false;
        std::thread recThread2([&]()
        {
            okPattern = node2.record({"rt/bag_rec_pre_a", "rt/bag_rec_pre_*"}, statsPattern, &errorPattern);
            recDone2.store(true);
        });
        std::this_thread::sleep_for(std::chrono::milliseconds(kPatternRecordMs));
        yomk::bagRecordStop();
        for (int i = 0; i < 100 && !recDone2.load(); ++i)
        {
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
        recThread2.join();
        pubStop2.store(true);
        pubThread2.join();

        CHECK(okPattern, "R9 模式录制 record → true（精确 + 模式展开去重后录制）");
        if (okPattern)
        {
            CHECK(statsPattern.size() == 2, "R9 stats 两项（精确 a 与模式命中的 a/b 去重合并）");
            if (statsPattern.size() == 2)
            {
                CHECK(statsPattern[0].topic == "rt/bag_rec_pre_a" &&
                          statsPattern[1].topic == "rt/bag_rec_pre_b",
                      "R9 展开清单去重且升序（a、b 各一项）");
                CHECK(statsPattern[0].count >= 1 && statsPattern[1].count >= 1,
                      "R9 两主题均有录制条数");
                CHECK(statsPattern[0].type == "YomkRpc::MString",
                      "R9 展开主题类型 == YomkRpc::MString");
            }
            const std::string &bagDir2 = node2.bagDir();
            CHECK(bagDir2.size() == std::string("bag_YYYY-MM-DD_HH-MM-SS_mmm").size(),
                  "R9 新 bag 目录形如 bag_<YYYY-MM-DD_HH-MM-SS_mmm>（27 字符）");
            std::ifstream meta2(bagDir2 + "/metadata.json");
            std::stringstream buf2;
            buf2 << meta2.rdbuf();
            const std::string text2 = buf2.str();
            CHECK(text2.find("\"name\": \"rt/bag_rec_pre_a\"") != std::string::npos &&
                      text2.find("\"name\": \"rt/bag_rec_pre_b\"") != std::string::npos,
                  "R9 metadata topics_with_message_count 列出展开后的两主题");
        }
        else
        {
            std::cerr << "record pattern error: " << errorPattern << std::endl;
        }

        // 定格语义对展开录制同样成立
        std::vector<FastDDSBagNode::BagTopicStat> statsAgain2;
        std::string errorAgain2;
        CHECK(!node2.record({"rt/bag_rec_pre_*"}, statsAgain2, &errorAgain2),
              "R9 模式录制后节点定格：再次 record → false");
        CHECK(errorAgain2.find("record already finished") != std::string::npos,
              "R9 定格 error 含 record already finished");
    }

    // ---- R10：outputDir 指定目录名（-o 选项节点层分流：非空用指定名而非时间戳） ----
    {
        // 复跑卫生：固定目录名上次运行残留必撞 already exists，先清
        std::error_code rmAllEc;
        std::filesystem::remove_all("bag_custom", rmAllEc);

        // 复位停止标志（R9 已置位）；同域新建 bag 节点（node2 已定格不可复用）
        yomk::bagRecordReset();
        FastDDSBagNode node3;
        CHECK(node3.setDomainId(TEST_DOMAIN), "R10 新节点 setDomainId(202) 成功");

        // 远端新增发布者（复用 peer participant 与 MString 类型；新主题避免与 R9 残留混淆）
        auto *topicOut = peerParticipant->create_topic(
            "rt/bag_rec_out", ts.get_type_name(), dds::TOPIC_QOS_DEFAULT);
        auto *writerOut = (pub != nullptr && topicOut != nullptr)
                              ? pub->create_datawriter(topicOut, dds::DATAWRITER_QOS_DEFAULT)
                              : nullptr;
        CHECK(writerOut != nullptr, "R10 远端 DataWriter 创建成功（rt/bag_rec_out）");

        std::atomic<bool> pubStop3{false};
        std::thread pubThread3([&]()
        {
            YomkRpc::MString msg;
            uint32_t seq = 0;
            while (!pubStop3.load())
            {
                msg.data("bag-record-outputdir-" + std::to_string(seq++));
                writerOut->write(&msg);
                std::this_thread::sleep_for(std::chrono::milliseconds(kPubIntervalMs));
            }
        });

        std::vector<FastDDSBagNode::BagTopicStat> statsCustom;
        std::string errorCustom;
        std::atomic<bool> recDone3{false};
        bool okCustom = false;
        std::thread recThread3([&]()
        {
            okCustom = node3.record({"rt/bag_rec_out"}, statsCustom, &errorCustom, 0, 0, "bag_custom");
            recDone3.store(true);
        });
        std::this_thread::sleep_for(std::chrono::milliseconds(kPatternRecordMs));
        yomk::bagRecordStop();
        for (int i = 0; i < 100 && !recDone3.load(); ++i)
        {
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
        recThread3.join();
        pubStop3.store(true);
        pubThread3.join();

        CHECK(okCustom, "R10 指定目录名 record → true");
        if (okCustom)
        {
            CHECK(node3.bagDir() == "bag_custom",
                  "R10 bagDir() == 指定名 bag_custom（非时间戳名）");
            CHECK(std::filesystem::is_regular_file("bag_custom/bag_0.mcap"),
                  "R10 指定目录下 bag_0.mcap 落盘");
            std::ifstream meta3("bag_custom/metadata.json");
            std::stringstream buf3;
            buf3 << meta3.rdbuf();
            const std::string text3 = buf3.str();
            CHECK(text3.find("\"name\": \"rt/bag_rec_out\"") != std::string::npos,
                  "R10 metadata 列出录制主题");
            CHECK(!statsCustom.empty() && statsCustom[0].count >= 1, "R10 stats 有录制条数");
        }
        else
        {
            std::cerr << "record outputDir error: " << errorCustom << std::endl;
        }
    }

    // ---- R11a：--start-paused 全程暂停不恢复 → 零写入 ----
    {
        // 复位双标志（R10 已置 stop；bagRecordReset 现同时复位 paused）
        yomk::bagRecordReset();
        FastDDSBagNode node4;
        CHECK(node4.setDomainId(TEST_DOMAIN), "R11a 新节点 setDomainId(202) 成功");

        // 远端新增发布者（复用 peer participant 与 MString 类型）
        auto *topicPauseA = peerParticipant->create_topic(
            "rt/bag_rec_pause_a", ts.get_type_name(), dds::TOPIC_QOS_DEFAULT);
        auto *writerPauseA = (pub != nullptr && topicPauseA != nullptr)
                                 ? pub->create_datawriter(topicPauseA, dds::DATAWRITER_QOS_DEFAULT)
                                 : nullptr;
        CHECK(writerPauseA != nullptr, "R11a 远端 DataWriter 创建成功（rt/bag_rec_pause_a）");

        std::atomic<bool> pubStop4{false};
        std::thread pubThread4([&]()
        {
            YomkRpc::MString msg;
            uint32_t seq = 0;
            while (!pubStop4.load())
            {
                msg.data("bag-record-pause-a-" + std::to_string(seq++));
                writerPauseA->write(&msg);
                std::this_thread::sleep_for(std::chrono::milliseconds(kPubIntervalMs));
            }
        });

        // 暂停态启动：bagRecordPause 先于 record（对齐 CLI --start-paused 前置置位）
        yomk::bagRecordPause();
        std::vector<FastDDSBagNode::BagTopicStat> statsPaused;
        std::string errorPaused;
        std::atomic<bool> recDone4{false};
        bool okPaused = false;
        std::thread recThread4([&]()
        {
            okPaused = node4.record({"rt/bag_rec_pause_a"}, statsPaused, &errorPaused);
            recDone4.store(true);
        });
        std::this_thread::sleep_for(std::chrono::milliseconds(kPausedRecordMs));
        yomk::bagRecordStop();
        for (int i = 0; i < 100 && !recDone4.load(); ++i)
        {
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
        recThread4.join();
        pubStop4.store(true);
        pubThread4.join();

        CHECK(okPaused, "R11a 全程暂停 record → true（暂停不影响录制会话本身）");
        CHECK(!statsPaused.empty() && statsPaused[0].count == 0,
              "R11a 全程暂停零写入（暂停期消息全部丢弃）");
        CHECK(!statsPaused.empty() && statsPaused[0].bytes == 0, "R11a 全程暂停零字节");
        CHECK(node4.bagDir().size() == std::string("bag_YYYY-MM-DD_HH-MM-SS_mmm").size(),
              "R11a 暂停态目录照建（订阅与落盘路径不受影响）");
    }

    // ---- R11b：暂停后恢复 → 恢复后开始写入 ----
    {
        yomk::bagRecordReset();
        FastDDSBagNode node5;
        CHECK(node5.setDomainId(TEST_DOMAIN), "R11b 新节点 setDomainId(202) 成功");

        auto *topicPauseB = peerParticipant->create_topic(
            "rt/bag_rec_pause_b", ts.get_type_name(), dds::TOPIC_QOS_DEFAULT);
        auto *writerPauseB = (pub != nullptr && topicPauseB != nullptr)
                                 ? pub->create_datawriter(topicPauseB, dds::DATAWRITER_QOS_DEFAULT)
                                 : nullptr;
        CHECK(writerPauseB != nullptr, "R11b 远端 DataWriter 创建成功（rt/bag_rec_pause_b）");

        std::atomic<bool> pubStop5{false};
        std::thread pubThread5([&]()
        {
            YomkRpc::MString msg;
            uint32_t seq = 0;
            while (!pubStop5.load())
            {
                msg.data("bag-record-pause-b-" + std::to_string(seq++));
                writerPauseB->write(&msg);
                std::this_thread::sleep_for(std::chrono::milliseconds(kPubIntervalMs));
            }
        });

        yomk::bagRecordPause();
        std::vector<FastDDSBagNode::BagTopicStat> statsResume;
        std::string errorResume;
        std::atomic<bool> recDone5{false};
        bool okResume = false;
        std::thread recThread5([&]()
        {
            okResume = node5.record({"rt/bag_rec_pause_b"}, statsResume, &errorResume);
            recDone5.store(true);
        });
        // 暂停一段（校验收敛后仍有丢弃期）后置恢复标志，再录一段
        std::this_thread::sleep_for(std::chrono::milliseconds(kResumeDelayMs));
        yomk::bagRecordResume();
        std::this_thread::sleep_for(std::chrono::milliseconds(kResumeRecordMs));
        yomk::bagRecordStop();
        for (int i = 0; i < 100 && !recDone5.load(); ++i)
        {
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
        recThread5.join();
        pubStop5.store(true);
        pubThread5.join();

        CHECK(okResume, "R11b 暂停后恢复 record → true");
        CHECK(!statsResume.empty() && statsResume[0].count >= 1,
              "R11b 恢复后开始写入（count >= 1）");
        // metadata starting_time 非零：firstNs 被恢复后首条写入消息置位（暂停期丢弃不置位）
        std::ifstream meta5(node5.bagDir() + "/metadata.json");
        std::stringstream buf5;
        buf5 << meta5.rdbuf();
        const std::string text5 = buf5.str();
        CHECK(text5.find("\"nanoseconds_since_epoch\": 0") == std::string::npos,
              "R11b metadata starting_time 非零（暂停期不计入）");
    }

    // ---- R12：-b/--max-bag-size 分片录制（大 payload 驱动 chunk flush 触发滚动） ----
    {
        yomk::bagRecordReset();
        FastDDSBagNode node6;
        CHECK(node6.setDomainId(TEST_DOMAIN), "R12 新节点 setDomainId(202) 成功");

        // 远端新增发布者（复用 peer participant 与 MString 类型）
        auto *topicSplit = peerParticipant->create_topic(
            REC_SPLIT_TOPIC, ts.get_type_name(), dds::TOPIC_QOS_DEFAULT);
        auto *writerSplit = (pub != nullptr && topicSplit != nullptr)
                                ? pub->create_datawriter(topicSplit, dds::DATAWRITER_QOS_DEFAULT)
                                : nullptr;
        CHECK(writerSplit != nullptr, "R12 远端 DataWriter 创建成功（rt/bag_rec_split_a）");

        // 发布端：先等订阅匹配（record 校验收敛后才建订，VOLATILE QoS 下匹配前的消息必丢）
        // 再发固定 20 条大 payload（128KB/条）；mcap chunk 默认 768KB 约每 6 条 flush 一次，
        // -b 1024 下每次 flush 后 size 远超上限立即切片——预期 bag_0..bag_3 四分片
        std::atomic<bool> pubStop6{false};
        std::thread pubThread6([&]()
        {
            YomkRpc::MString msg;
            const std::string payload(kSplitPayloadBytes, 'x');
            dds::PublicationMatchedStatus matched{};
            for (int i = 0; i < 100; ++i) // 100×100ms=10s 上限；未匹配照发（用例必失败不掩盖）
            {
                if (dds::RETCODE_OK == writerSplit->get_publication_matched_status(matched) &&
                    matched.current_count > 0)
                {
                    break;
                }
                std::this_thread::sleep_for(std::chrono::milliseconds(100));
            }
            for (int i = 0; i < kSplitMessages; ++i)
            {
                msg.data(payload);
                writerSplit->write(&msg);
                std::this_thread::sleep_for(std::chrono::milliseconds(kPubIntervalMs));
            }
        });

        std::vector<FastDDSBagNode::BagTopicStat> statsSplit;
        std::string errorSplit;
        std::atomic<bool> recDone6{false};
        bool okSplit = false;
        std::thread recThread6([&]()
        {
            okSplit = node6.record({REC_SPLIT_TOPIC}, statsSplit, &errorSplit, 0, 0, "", kSplitMaxBagSize);
            recDone6.store(true);
        });
        std::this_thread::sleep_for(std::chrono::milliseconds(kSplitRecordMs));
        yomk::bagRecordStop();
        for (int i = 0; i < 100 && !recDone6.load(); ++i)
        {
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
        recThread6.join();
        pubStop6.store(true);
        pubThread6.join();

        CHECK(okSplit, "R12 分片录制 record → true");
        if (okSplit)
        {
            CHECK(!statsSplit.empty() && statsSplit[0].count == kSplitMessages,
                  "R12 分片不丢消息（stats.count == 发布条数 20）");
            const std::string &bagDir6 = node6.bagDir();
            CHECK(std::filesystem::exists(bagDir6 + "/bag_0.mcap"), "R12 bag_0.mcap 存在");
            CHECK(std::filesystem::exists(bagDir6 + "/bag_1.mcap"),
                  "R12 bag_1.mcap 存在（达到上限已滚动分片）");
            CHECK(std::filesystem::file_size(bagDir6 + "/bag_0.mcap") > 0 &&
                      std::filesystem::file_size(bagDir6 + "/bag_1.mcap") > 0,
                  "R12 分片文件非空");

            // metadata：relative_file_paths 列全部分片，message_count 全局累计
            std::ifstream meta6(bagDir6 + "/metadata.json");
            std::stringstream buf6;
            buf6 << meta6.rdbuf();
            const std::string text6 = buf6.str();
            CHECK(text6.find("\"bag_0.mcap\"") != std::string::npos &&
                      text6.find("\"bag_1.mcap\"") != std::string::npos,
                  "R12 metadata relative_file_paths 列出 bag_0/bag_1 分片");
            CHECK(text6.find("\"message_count\": " + std::to_string(kSplitMessages)) != std::string::npos,
                  "R12 metadata message_count 全局累计 == 20");

            // 跨分片读回：各分片条数总和 == 发布条数（分片连续拼接不重复不空洞），
            // 且每片自含 Channel 声明（读回的 channel topic 一致）
            uint64_t splitTotal = 0;
            bool shardChannelOk = true;
            for (const char *shard :
                 {"bag_0.mcap", "bag_1.mcap", "bag_2.mcap", "bag_3.mcap", "bag_4.mcap"})
            {
                const std::string shardPath = bagDir6 + "/" + shard;
                if (!std::filesystem::exists(shardPath))
                {
                    continue;
                }
                std::ifstream in(shardPath, std::ios::binary);
                mcap::FileStreamReader dataSource{in};
                mcap::McapReader reader;
                if (!reader.open(dataSource).ok())
                {
                    CHECK(false, "R12 分片 mcap open 成功（每片 summary 完整可读回）");
                    continue;
                }
                for (const auto &msgView :
                     reader.readMessages([](const mcap::Status &) {}))
                {
                    if (msgView.channel->topic != REC_SPLIT_TOPIC)
                    {
                        shardChannelOk = false;
                    }
                    ++splitTotal;
                }
                reader.close();
            }
            CHECK(splitTotal == kSplitMessages,
                  "R12 跨分片读回总条数 == 20（各分片连续拼接不丢消息）");
            CHECK(shardChannelOk,
                  "R12 各分片 Channel topic 一致（每片自含通道声明）");
        }
        else
        {
            std::cerr << "record split error: " << errorSplit << std::endl;
        }
    }

    // ---- R13：-d/--max-bag-duration 时长分片录制（普通 payload 驱动时长滚动） ----
    {
        yomk::bagRecordReset();
        FastDDSBagNode node7;
        CHECK(node7.setDomainId(TEST_DOMAIN), "R13 新节点 setDomainId(202) 成功");

        // 远端新增发布者（复用 peer participant 与 MString 类型）
        auto *topicTime = peerParticipant->create_topic(
            REC_TIME_TOPIC, ts.get_type_name(), dds::TOPIC_QOS_DEFAULT);
        auto *writerTime = (pub != nullptr && topicTime != nullptr)
                               ? pub->create_datawriter(topicTime, dds::DATAWRITER_QOS_DEFAULT)
                               : nullptr;
        CHECK(writerTime != nullptr, "R13 远端 DataWriter 创建成功（rt/bag_rec_time_a）");

        // 发布端：先等订阅匹配（同 R12，VOLATILE QoS 下匹配前的消息必丢），再发
        // 固定 25 条小 payload @200ms（4.8s 跨度）；-d 2 下距分片首条严格大于 2s 即滚动
        // ——预期 bag_0/bag_1/bag_2 三分片（~0-2s / ~2-4s / ~4s+ 尾片）
        std::atomic<bool> pubStop7{false};
        std::thread pubThread7([&]()
        {
            YomkRpc::MString msg;
            dds::PublicationMatchedStatus matched{};
            for (int i = 0; i < 100; ++i) // 100×100ms=10s 上限；未匹配照发（用例必失败不掩盖）
            {
                if (dds::RETCODE_OK == writerTime->get_publication_matched_status(matched) &&
                    matched.current_count > 0)
                {
                    break;
                }
                std::this_thread::sleep_for(std::chrono::milliseconds(100));
            }
            for (int i = 0; i < kTimeMessages; ++i)
            {
                msg.data("bag-record-time-" + std::to_string(i));
                writerTime->write(&msg);
                std::this_thread::sleep_for(std::chrono::milliseconds(kTimePubIntervalMs));
            }
        });

        std::vector<FastDDSBagNode::BagTopicStat> statsTime;
        std::string errorTime;
        std::atomic<bool> recDone7{false};
        bool okTime = false;
        std::thread recThread7([&]()
        {
            okTime = node7.record({REC_TIME_TOPIC}, statsTime, &errorTime, 0, 0, "", 0,
                                  kTimeMaxBagDurationSec);
            recDone7.store(true);
        });
        std::this_thread::sleep_for(std::chrono::milliseconds(kTimeRecordMs));
        yomk::bagRecordStop();
        for (int i = 0; i < 100 && !recDone7.load(); ++i)
        {
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
        recThread7.join();
        pubStop7.store(true);
        pubThread7.join();

        CHECK(okTime, "R13 时长分片 record → true");
        if (okTime)
        {
            CHECK(!statsTime.empty() && statsTime[0].count == kTimeMessages,
                  "R13 分片不丢消息（stats.count == 发布条数 25）");
            const std::string &bagDir7 = node7.bagDir();
            CHECK(std::filesystem::exists(bagDir7 + "/bag_0.mcap"), "R13 bag_0.mcap 存在");
            CHECK(std::filesystem::exists(bagDir7 + "/bag_1.mcap"),
                  "R13 bag_1.mcap 存在（时长达限已滚动分片）");
            CHECK(std::filesystem::exists(bagDir7 + "/bag_2.mcap"),
                  "R13 bag_2.mcap 存在（4.8s 跨度超出两片 2s 上限）");
            CHECK(std::filesystem::file_size(bagDir7 + "/bag_0.mcap") > 0 &&
                      std::filesystem::file_size(bagDir7 + "/bag_1.mcap") > 0 &&
                      std::filesystem::file_size(bagDir7 + "/bag_2.mcap") > 0,
                  "R13 分片文件非空");

            // metadata：relative_file_paths 列出全部分片，message_count 全局累计
            std::ifstream meta7(bagDir7 + "/metadata.json");
            std::stringstream buf7;
            buf7 << meta7.rdbuf();
            const std::string text7 = buf7.str();
            CHECK(text7.find("\"bag_0.mcap\"") != std::string::npos &&
                      text7.find("\"bag_1.mcap\"") != std::string::npos &&
                      text7.find("\"bag_2.mcap\"") != std::string::npos,
                  "R13 metadata relative_file_paths 列出 bag_0/bag_1/bag_2 分片");
            CHECK(text7.find("\"message_count\": " + std::to_string(kTimeMessages)) != std::string::npos,
                  "R13 metadata message_count 全局累计 == 25");

            // 跨分片读回：各分片条数总和 == 发布条数（分片连续拼接不重复不空洞），
            // 且每片自含 Channel 声明（读回的 channel topic 一致）
            uint64_t timeTotal = 0;
            bool timeChannelOk = true;
            for (const char *shard :
                 {"bag_0.mcap", "bag_1.mcap", "bag_2.mcap", "bag_3.mcap", "bag_4.mcap"})
            {
                const std::string shardPath = bagDir7 + "/" + shard;
                if (!std::filesystem::exists(shardPath))
                {
                    continue;
                }
                std::ifstream in(shardPath, std::ios::binary);
                mcap::FileStreamReader dataSource{in};
                mcap::McapReader reader;
                if (!reader.open(dataSource).ok())
                {
                    CHECK(false, "R13 分片 mcap open 成功（每片 summary 完整可读回）");
                    continue;
                }
                for (const auto &msgView :
                     reader.readMessages([](const mcap::Status &) {}))
                {
                    if (msgView.channel->topic != REC_TIME_TOPIC)
                    {
                        timeChannelOk = false;
                    }
                    ++timeTotal;
                }
                reader.close();
            }
            CHECK(timeTotal == kTimeMessages,
                  "R13 跨分片读回总条数 == 25（各分片连续拼接不丢消息）");
            CHECK(timeChannelOk,
                  "R13 各分片 Channel topic 一致（每片自含通道声明）");
        }
        else
        {
            std::cerr << "record time error: " << errorTime << std::endl;
        }
    }

    // 远端发布端清理（先于 bag 节点析构亦可，二者独立参与者）
    if (peerParticipant != nullptr)
    {
        dds::DomainParticipantFactory::get_instance()->delete_participant(peerParticipant);
    }

    return testReport("TestFastDDSBagNodeRecord");
}
