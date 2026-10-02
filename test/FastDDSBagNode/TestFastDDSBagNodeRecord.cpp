/**
 * @file TestFastDDSBagNodeRecord.cpp
 * @brief FastDDSBagNode 节点层录制收尾测试（真实 DDS 端到端：发布 → 录制 → Ctrl+C 停止 → 落盘断言）
 *
 * 范围：record 全链路——发现校验通过（远端 MString 发布者在线）、透传订阅、消息直写 mcap、
 *       bagRecordStop 触发收尾（删除 reader → writer.close 补 summary → metadata.yaml →
 *       stats 回填），以及收尾产物断言（bag 目录、bag_0.mcap 读回、metadata.yaml 字段、
 *       stats 契约）与录制定格语义（成功后再次 record 拒绝）。启动校验分支归
 *       TestFastDDSBagNodeValidation。
 * 覆盖：
 *   R1 setDomainId(202) 成功；bagRecordReset 复位残留停止标志；
 *   R2 远端 MString 发布者（rt/bag_rec_hit）周期发布，录制线程 record 阻塞；
 *   R3 ~5s 后 bagRecordStop → record 返回 true；
 *   R4 stats 契约：单主题、topic/type（YomkRpc::MString）/count/bytes 一致；
 *   R5 落盘：bag 目录（bag_<YYYYMMDD_HHMMSS>）含 bag_0.mcap 与 metadata.yaml；
 *   R6 mcap 读回：条数 == stats.count；Channel topic/messageEncoding=cdr/schemaId=0；
 *      每条消息字节非空且总字节 == stats.bytes；sequence 从 0 单调递增；
 *   R7 metadata.yaml：storage_identifier: mcap / relative_file_paths: bag_0.mcap /
 *      topics_with_message_count 的 name/type/message_count 与 stats 一致；
 *   R8 定格：成功后再次 record → false "record already finished, recreate node to record again"。
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
    // 录制时长：校验窗（SPDP 发现发布者约 1~3s + 全端点在线即收敛）+ ~5s 录制期（150ms/条）
    constexpr int kRecordMs = 12000;
    constexpr int kPubIntervalMs = 150;
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
    CHECK(bagDir.size() == std::string("bag_YYYYMMDD_HHMMSS").size(),
          "R5 bagDir() 形如 bag_<YYYYMMDD_HHMMSS>（19 字符）");
    CHECK(std::filesystem::exists(bagDir + "/bag_0.mcap"), "R5 bag_0.mcap 存在");
    CHECK(std::filesystem::exists(bagDir + "/metadata.yaml"), "R5 metadata.yaml 存在");
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

    // ---- R7：metadata.yaml 字段断言 ----
    {
        std::ifstream meta(bagDir + "/metadata.yaml");
        std::stringstream buf;
        buf << meta.rdbuf();
        const std::string text = buf.str();
        CHECK(text.find("yomkrpc_bagfile_information:") != std::string::npos,
              "R7 metadata 含 yomkrpc_bagfile_information 根键");
        CHECK(text.find("version: 5") != std::string::npos, "R7 metadata version: 5");
        CHECK(text.find("storage_identifier: mcap") != std::string::npos,
              "R7 metadata storage_identifier: mcap");
        CHECK(text.find("bag_0.mcap") != std::string::npos, "R7 metadata relative_file_paths 列出 bag_0.mcap");
        CHECK(text.find("name: " + std::string(REC_TOPIC)) != std::string::npos,
              "R7 metadata topics_with_message_count 列出录制主题名");
        CHECK(text.find("type: YomkRpc::MString") != std::string::npos,
              "R7 metadata 主题类型 == YomkRpc::MString");
        CHECK(text.find("message_count: " + std::to_string(recCount)) != std::string::npos,
              "R7 metadata message_count 与 stats 一致");
        CHECK(text.find("nanoseconds_since_epoch:") != std::string::npos,
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

    // 远端发布端清理（先于 bag 节点析构亦可，二者独立参与者）
    if (peerParticipant != nullptr)
    {
        dds::DomainParticipantFactory::get_instance()->delete_participant(peerParticipant);
    }

    return testReport("TestFastDDSBagNodeRecord");
}
