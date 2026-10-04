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
 *      总条数 == 发布条数）、各分片自含 Channel 声明；
 *   R14 -c/--max-cache-size 写缓存双缓冲录制（新节点三段）：4096 字节缓存 + 定时发布
 *      曲线（同 R13）→ 停止 drain 完整（count、metadata message_count 与读回总条数均
 *      == 发布条数）；4096 缓存 + -d 2 组合 → 分片在消费线程照常滚动（断言组同 R13）；
 *      满载 1 字节缓存 + 50 条连发突发 → dropNew_ 置位期间丢新（写入 + 丢弃 == 发布
 *      条数守恒）、收尾打印丢条统计（按主题计数 + Total dropped 总计行）。
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
    constexpr const char *REC_CACHE_TOPIC_A = "rt/bag_rec_cache_a";
    constexpr const char *REC_CACHE_TOPIC_B = "rt/bag_rec_cache_b";
    constexpr const char *REC_CACHE_TOPIC_C = "rt/bag_rec_cache_c";
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
    // R14a/R14b 缓存用例：复用 R13 定时发布曲线（25 条小 payload @200ms），maxCacheSize
    // 4096（每条 CDR 编码约 40-50B，缓冲周期内远不会满）验证停止 drain 完整性；R14b 另加
    // -d 2 验证分片检查在消费线程照常滚动（三分片预期同 R13）
    constexpr int kCacheMessages = 25;
    constexpr int kCachePubIntervalMs = 200;
    constexpr uint64_t kCacheMaxCacheSize = 4096;
    constexpr uint64_t kCacheMaxBagDurationSec = 2;
    constexpr int kCacheRecordMs = 13000;
    // R14c 满载丢弃用例：1 字节缓存必满（任一条消息都超限），发布端等订阅匹配后 50 条
    // 连发（无间隔突发），dropNew_ 置位期间后续 push 全部丢弃直至消费线程 swap 复位
    constexpr int kCacheDropMessages = 50;
    constexpr uint64_t kCacheDropMaxCacheSize = 1;
    // 录制窗：校验窗（~1~3s）+ 等订阅匹配（<1s）+ 50 条连发（<1s）+ 收尾余量
    constexpr int kCacheDropRecordMs = 8000;
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

    // ---- R14a：-c/--max-cache-size 写缓存双缓冲（回调拷贝入队 + 消费线程写盘，停止 drain 完整） ----
    {
        yomk::bagRecordReset();
        FastDDSBagNode node8;
        CHECK(node8.setDomainId(TEST_DOMAIN), "R14a 新节点 setDomainId(202) 成功");

        // 远端新增发布者（复用 peer participant 与 MString 类型）
        auto *topicCache = peerParticipant->create_topic(
            REC_CACHE_TOPIC_A, ts.get_type_name(), dds::TOPIC_QOS_DEFAULT);
        auto *writerCache = (pub != nullptr && topicCache != nullptr)
                                ? pub->create_datawriter(topicCache, dds::DATAWRITER_QOS_DEFAULT)
                                : nullptr;
        CHECK(writerCache != nullptr, "R14a 远端 DataWriter 创建成功（rt/bag_rec_cache_a）");

        // 发布端：先等订阅匹配（同 R12/R13），再发固定 25 条小 payload @200ms；4096 字节
        // 缓存周期内远不会满——验证 cache 路径不丢消息且停止时 drain 完整（残余全部落盘）
        std::atomic<bool> pubStop8{false};
        std::thread pubThread8([&]()
        {
            YomkRpc::MString msg;
            dds::PublicationMatchedStatus matched{};
            for (int i = 0; i < 100; ++i) // 100×100ms=10s 上限；未匹配照发（用例必失败不掩盖）
            {
                if (dds::RETCODE_OK == writerCache->get_publication_matched_status(matched) &&
                    matched.current_count > 0)
                {
                    break;
                }
                std::this_thread::sleep_for(std::chrono::milliseconds(100));
            }
            for (int i = 0; i < kCacheMessages; ++i)
            {
                msg.data("bag-record-cache-" + std::to_string(i));
                writerCache->write(&msg);
                std::this_thread::sleep_for(std::chrono::milliseconds(kCachePubIntervalMs));
            }
        });

        std::vector<FastDDSBagNode::BagTopicStat> statsCache;
        std::string errorCache;
        std::atomic<bool> recDone8{false};
        bool okCache = false;
        std::thread recThread8([&]()
        {
            okCache = node8.record({REC_CACHE_TOPIC_A}, statsCache, &errorCache, 0, 0, "", 0, 0,
                                   kCacheMaxCacheSize);
            recDone8.store(true);
        });
        std::this_thread::sleep_for(std::chrono::milliseconds(kCacheRecordMs));
        yomk::bagRecordStop();
        for (int i = 0; i < 100 && !recDone8.load(); ++i)
        {
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
        recThread8.join();
        pubStop8.store(true);
        pubThread8.join();

        CHECK(okCache, "R14a 缓存录制 record → true");
        if (okCache)
        {
            CHECK(!statsCache.empty() && statsCache[0].count == kCacheMessages,
                  "R14a drain 完整（stats.count == 发布条数 25）");
            const std::string &bagDir8 = node8.bagDir();
            CHECK(std::filesystem::exists(bagDir8 + "/bag_0.mcap"), "R14a bag_0.mcap 存在");

            // metadata：message_count 全局累计 == 发布条数
            std::ifstream meta8(bagDir8 + "/metadata.json");
            std::stringstream buf8;
            buf8 << meta8.rdbuf();
            const std::string text8 = buf8.str();
            CHECK(text8.find("\"message_count\": " + std::to_string(kCacheMessages)) != std::string::npos,
                  "R14a metadata message_count 全局累计 == 25");

            // 读回：条数 == 发布条数（无分片参数仅 bag_0，循环复刻 R12/R13 防御多片）
            uint64_t cacheTotal = 0;
            bool cacheChannelOk = true;
            for (const char *shard :
                 {"bag_0.mcap", "bag_1.mcap", "bag_2.mcap", "bag_3.mcap", "bag_4.mcap"})
            {
                const std::string shardPath = bagDir8 + "/" + shard;
                if (!std::filesystem::exists(shardPath))
                {
                    continue;
                }
                std::ifstream in(shardPath, std::ios::binary);
                mcap::FileStreamReader dataSource{in};
                mcap::McapReader reader;
                if (!reader.open(dataSource).ok())
                {
                    CHECK(false, "R14a 分片 mcap open 成功（每片 summary 完整可读回）");
                    continue;
                }
                for (const auto &msgView :
                     reader.readMessages([](const mcap::Status &) {}))
                {
                    if (msgView.channel->topic != REC_CACHE_TOPIC_A)
                    {
                        cacheChannelOk = false;
                    }
                    ++cacheTotal;
                }
                reader.close();
            }
            CHECK(cacheTotal == kCacheMessages,
                  "R14a 读回总条数 == 25（消费线程写盘不丢消息）");
            CHECK(cacheChannelOk, "R14a 读回 Channel topic 一致");
        }
        else
        {
            std::cerr << "record cache error: " << errorCache << std::endl;
        }
    }

    // ---- R14b：缓存 + -d 2 组合（分片检查随 shardCheckAndWrite 移入消费线程后照常滚动） ----
    {
        yomk::bagRecordReset();
        FastDDSBagNode node9;
        CHECK(node9.setDomainId(TEST_DOMAIN), "R14b 新节点 setDomainId(202) 成功");

        // 远端新增发布者（复用 peer participant 与 MString 类型）
        auto *topicCacheSplit = peerParticipant->create_topic(
            REC_CACHE_TOPIC_B, ts.get_type_name(), dds::TOPIC_QOS_DEFAULT);
        auto *writerCacheSplit = (pub != nullptr && topicCacheSplit != nullptr)
                                     ? pub->create_datawriter(topicCacheSplit, dds::DATAWRITER_QOS_DEFAULT)
                                     : nullptr;
        CHECK(writerCacheSplit != nullptr, "R14b 远端 DataWriter 创建成功（rt/bag_rec_cache_b）");

        // 发布端同 R14a（等匹配后 25 条 @200ms）；-d 2 + 4096 缓存：写入走消费线程，
        // 分片检查/滚动在消费线程执行——预期三分片同 R13（~0-2s / ~2-4s / ~4s+ 尾片）
        std::atomic<bool> pubStop9{false};
        std::thread pubThread9([&]()
        {
            YomkRpc::MString msg;
            dds::PublicationMatchedStatus matched{};
            for (int i = 0; i < 100; ++i) // 100×100ms=10s 上限；未匹配照发（用例必失败不掩盖）
            {
                if (dds::RETCODE_OK == writerCacheSplit->get_publication_matched_status(matched) &&
                    matched.current_count > 0)
                {
                    break;
                }
                std::this_thread::sleep_for(std::chrono::milliseconds(100));
            }
            for (int i = 0; i < kCacheMessages; ++i)
            {
                msg.data("bag-record-cache-split-" + std::to_string(i));
                writerCacheSplit->write(&msg);
                std::this_thread::sleep_for(std::chrono::milliseconds(kCachePubIntervalMs));
            }
        });

        std::vector<FastDDSBagNode::BagTopicStat> statsCacheSplit;
        std::string errorCacheSplit;
        std::atomic<bool> recDone9{false};
        bool okCacheSplit = false;
        std::thread recThread9([&]()
        {
            okCacheSplit = node9.record({REC_CACHE_TOPIC_B}, statsCacheSplit, &errorCacheSplit, 0, 0,
                                        "", 0, kCacheMaxBagDurationSec, kCacheMaxCacheSize);
            recDone9.store(true);
        });
        std::this_thread::sleep_for(std::chrono::milliseconds(kCacheRecordMs));
        yomk::bagRecordStop();
        for (int i = 0; i < 100 && !recDone9.load(); ++i)
        {
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
        recThread9.join();
        pubStop9.store(true);
        pubThread9.join();

        CHECK(okCacheSplit, "R14b 缓存 + 时长分片组合 record → true");
        if (okCacheSplit)
        {
            CHECK(!statsCacheSplit.empty() && statsCacheSplit[0].count == kCacheMessages,
                  "R14b 分片不丢消息（stats.count == 发布条数 25）");
            const std::string &bagDir9 = node9.bagDir();
            CHECK(std::filesystem::exists(bagDir9 + "/bag_0.mcap"), "R14b bag_0.mcap 存在");
            CHECK(std::filesystem::exists(bagDir9 + "/bag_1.mcap"),
                  "R14b bag_1.mcap 存在（消费线程内时长达限已滚动分片）");
            CHECK(std::filesystem::exists(bagDir9 + "/bag_2.mcap"),
                  "R14b bag_2.mcap 存在（4.8s 跨度超出两片 2s 上限）");
            CHECK(std::filesystem::file_size(bagDir9 + "/bag_0.mcap") > 0 &&
                      std::filesystem::file_size(bagDir9 + "/bag_1.mcap") > 0 &&
                      std::filesystem::file_size(bagDir9 + "/bag_2.mcap") > 0,
                  "R14b 分片文件非空");

            // metadata：relative_file_paths 列出全部分片，message_count 全局累计
            std::ifstream meta9(bagDir9 + "/metadata.json");
            std::stringstream buf9;
            buf9 << meta9.rdbuf();
            const std::string text9 = buf9.str();
            CHECK(text9.find("\"bag_0.mcap\"") != std::string::npos &&
                      text9.find("\"bag_1.mcap\"") != std::string::npos &&
                      text9.find("\"bag_2.mcap\"") != std::string::npos,
                  "R14b metadata relative_file_paths 列出 bag_0/bag_1/bag_2 分片");
            CHECK(text9.find("\"message_count\": " + std::to_string(kCacheMessages)) != std::string::npos,
                  "R14b metadata message_count 全局累计 == 25");

            // 跨分片读回：各分片条数总和 == 发布条数，且每片自含 Channel 声明
            uint64_t cacheSplitTotal = 0;
            bool cacheSplitChannelOk = true;
            for (const char *shard :
                 {"bag_0.mcap", "bag_1.mcap", "bag_2.mcap", "bag_3.mcap", "bag_4.mcap"})
            {
                const std::string shardPath = bagDir9 + "/" + shard;
                if (!std::filesystem::exists(shardPath))
                {
                    continue;
                }
                std::ifstream in(shardPath, std::ios::binary);
                mcap::FileStreamReader dataSource{in};
                mcap::McapReader reader;
                if (!reader.open(dataSource).ok())
                {
                    CHECK(false, "R14b 分片 mcap open 成功（每片 summary 完整可读回）");
                    continue;
                }
                for (const auto &msgView :
                     reader.readMessages([](const mcap::Status &) {}))
                {
                    if (msgView.channel->topic != REC_CACHE_TOPIC_B)
                    {
                        cacheSplitChannelOk = false;
                    }
                    ++cacheSplitTotal;
                }
                reader.close();
            }
            CHECK(cacheSplitTotal == kCacheMessages,
                  "R14b 跨分片读回总条数 == 25（各分片连续拼接不丢消息）");
            CHECK(cacheSplitChannelOk,
                  "R14b 各分片 Channel topic 一致（每片自含通道声明）");
        }
        else
        {
            std::cerr << "record cache split error: " << errorCacheSplit << std::endl;
        }
    }

    // ---- R14c：满载丢弃（1 字节缓存必满，dropNew_ 置位期间丢新 + 收尾丢条统计） ----
    {
        yomk::bagRecordReset();
        FastDDSBagNode node10;
        CHECK(node10.setDomainId(TEST_DOMAIN), "R14c 新节点 setDomainId(202) 成功");

        // 远端新增发布者（复用 peer participant 与 MString 类型）
        auto *topicDrop = peerParticipant->create_topic(
            REC_CACHE_TOPIC_C, ts.get_type_name(), dds::TOPIC_QOS_DEFAULT);
        auto *writerDrop = (pub != nullptr && topicDrop != nullptr)
                               ? pub->create_datawriter(topicDrop, dds::DATAWRITER_QOS_DEFAULT)
                               : nullptr;
        CHECK(writerDrop != nullptr, "R14c 远端 DataWriter 创建成功（rt/bag_rec_cache_c）");

        // 发布端：等订阅匹配后 50 条连发（无间隔突发）；1 字节缓存下每条消息都超限，首条
        // push 置位 dropNew_（本条仍入队），置位期间后续 push 全部丢弃直至消费线程 swap 复位
        std::atomic<bool> pubStop10{false};
        std::thread pubThread10([&]()
        {
            YomkRpc::MString msg;
            dds::PublicationMatchedStatus matched{};
            for (int i = 0; i < 100; ++i) // 100×100ms=10s 上限；未匹配照发（用例必失败不掩盖）
            {
                if (dds::RETCODE_OK == writerDrop->get_publication_matched_status(matched) &&
                    matched.current_count > 0)
                {
                    break;
                }
                std::this_thread::sleep_for(std::chrono::milliseconds(100));
            }
            for (int i = 0; i < kCacheDropMessages; ++i)
            {
                msg.data("bag-record-drop-" + std::to_string(i));
                writerDrop->write(&msg);
            }
        });

        // 重定向 std::cout 捕获收尾丢条统计（logCacheDropped 输出走 stdout；重定向窗口内
        // 主线程不执行 CHECK——CHECK 宏同样写 cout）
        std::stringstream captured;
        auto *oldBuf = std::cout.rdbuf(captured.rdbuf());

        std::vector<FastDDSBagNode::BagTopicStat> statsDrop;
        std::string errorDrop;
        std::atomic<bool> recDone10{false};
        bool okDrop = false;
        std::thread recThread10([&]()
        {
            okDrop = node10.record({REC_CACHE_TOPIC_C}, statsDrop, &errorDrop, 0, 0, "", 0, 0,
                                   kCacheDropMaxCacheSize);
            recDone10.store(true);
        });
        std::this_thread::sleep_for(std::chrono::milliseconds(kCacheDropRecordMs));
        yomk::bagRecordStop();
        for (int i = 0; i < 100 && !recDone10.load(); ++i)
        {
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
        recThread10.join();
        pubStop10.store(true);
        pubThread10.join();
        std::cout.rdbuf(oldBuf); // 恢复（record 已 join，此后无并发写 captured）

        CHECK(okDrop, "R14c 满载录制 record → true");
        const uint64_t dropCount = statsDrop.empty() ? 0 : statsDrop[0].count;
        CHECK(dropCount >= 1, "R14c 首条写入（超限本条仍入队语义）");
        CHECK(dropCount < kCacheDropMessages, "R14c 写入条数 < 发布数（置位期间丢新）");

        // 收尾丢条统计断言：按主题计数 + Total dropped 总计；计数守恒（写入 + 丢弃 == 发布）
        const std::string warned = captured.str();
        CHECK(warned.find("bag cache dropped messages per topic:") != std::string::npos,
              "R14c 收尾丢条告警行出现");
        CHECK(warned.find("\n\t" + std::string(REC_CACHE_TOPIC_C) + ": ") != std::string::npos,
              "R14c 告警按主题列出丢条数");
        uint64_t droppedTotal = 0;
        const std::string totalPrefix = "Total dropped: ";
        const auto totalPos = warned.find(totalPrefix);
        CHECK(totalPos != std::string::npos, "R14c 告警含 Total dropped 总计行");
        if (totalPos != std::string::npos)
        {
            droppedTotal = std::stoull(warned.substr(totalPos + totalPrefix.size()));
        }
        CHECK(droppedTotal > 0, "R14c 满载确实丢弃（Total dropped > 0）");
        CHECK(droppedTotal + dropCount == kCacheDropMessages,
              "R14c 丢条计数守恒（写入 + 丢弃 == 发布 50）");

        // 落盘读回：丢弃的消息不落盘，bag_0 条数 == stats.count
        uint64_t dropFileCount = 0;
        std::ifstream in(node10.bagDir() + "/bag_0.mcap", std::ios::binary);
        mcap::FileStreamReader dataSource{in};
        mcap::McapReader reader;
        if (reader.open(dataSource).ok())
        {
            for (const auto &msgView : reader.readMessages([](const mcap::Status &) {}))
            {
                ++dropFileCount;
            }
            reader.close();
        }
        CHECK(dropFileCount == dropCount,
              "R14c 落盘条数 == stats.count（丢弃消息不入队不落盘）");
    }

    // 远端发布端清理（先于 bag 节点析构亦可，二者独立参与者）
    if (peerParticipant != nullptr)
    {
        dds::DomainParticipantFactory::get_instance()->delete_participant(peerParticipant);
    }

    return testReport("TestFastDDSBagNodeRecord");
}
