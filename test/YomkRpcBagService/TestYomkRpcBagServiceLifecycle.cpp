/**
 * @file TestYomkRpcBagServiceLifecycle.cpp
 * @brief YomkRpcBagService 真实 DDS 生命周期测试
 *
 * 范围：契约测试（DDS-free）刻意豁免的 /create_node 成功路径（经 setDomainId 创建真实
 *       participant）、/bag_record 完整录制链路（校验通过 → 透传订阅 → mcap 落盘 →
 *       bagRecordStop 收尾 → StringArray 统计回执）、/bag_play 完整回放链路与 /delete_node
 *       析构清理。节点层校验分支归 TestFastDDSBagNodeValidation，落盘内容与回放对账细节归
 *       TestFastDDSBagNodeRecord/Play。
 * 覆盖：
 *   L1 生命周期：创建 → 重复创建拒绝 → record 空清单 eNo → record 无端点主题 eNo（m_msg
 *      含主题名，校验失败不落盘）→ 完整录制（远端 MString 发布者 + 停止线程模拟 Ctrl+C →
 *      eOk + StringArray 首行 bag 目录 + 统计行）→ 定格后重复 record 拒绝 → 删除 →
 *      重复删除拒绝 → 删除后 record 拒绝 → 重建 → 重复创建拒绝 → delete 收尾；
 *   L2 停止标志契约：录制经 yomk::bagRecordStop 置位收尾（模拟 SIGINT 处理函数行为）；
 *   L3 /bag_play 端点（重建节点后）：负载往返保真（路径不存在 → 错误文本回显 bagDir）+
 *      最小 bag 真实回放（mcap writer + 手写 metadata.json 造 2 条 @100ms bag → eOk +
 *      StringArray 回执：首行 bagDir + 统计行，与 record 回执形态同构）；
 *   L0 解耦契约：/bag_info 纯文件读、/bag_reindex 纯文件操作与节点生命周期无关（未建/
 *      已建节点均可调，路径不存在报文件层错误而非 bag node not created）。
 *
 * 关键不变式：每个 eOk 创建的 bag 节点必须在 main 返回前经 /delete_node 显式删除，以规避
 *   YOMK 框架 atexit 服务析构晚于 FastDDS DomainParticipantFactory 单例销毁导致的静态析构
 *   顺序问题（同 TestYomkRpcDebugServiceLifecycle）。
 *
 * 域号 200（对齐调试服务生命周期先例），与 bag 直测用例(201/202)错开，ctest 串行执行互不残留。
 *
 * 风格：纯 main() + CHECK 宏 + 失败计数（零第三方依赖），返回非 0 表示存在失败用例。
 */

#include "TestCheck.h"
#include "YomkRpcBagService.h" // 服务/DDSBagNode/DDSBagRecord/DDSBagReindex/yomk::bagRecordStop

#include <YomkRpcMsg/YomkRpcMsgPubSubTypes.hpp> // MStringPubSubType（完整录制用例的远端发布端）

#include <mcap/writer.hpp>      // L3 造最小 mcap 分片（MCAP_IMPLEMENTATION 单译元在被测库内）
#include <nlohmann/json.hpp>    // L3 手写 metadata.json（record 组装同款键集）

#include <fastdds/dds/domain/DomainParticipant.hpp>
#include <fastdds/dds/domain/DomainParticipantFactory.hpp>
#include <fastdds/dds/publisher/DataWriter.hpp>
#include <fastdds/dds/publisher/Publisher.hpp>
#include <fastdds/dds/topic/Topic.hpp>
#include <fastdds/dds/topic/TypeSupport.hpp>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <string>
#include <thread>

namespace
{
    constexpr uint32_t TEST_DOMAIN = 200;
    constexpr const char *REC_TOPIC = "rt/bag_svc_rec";
    // 完整录制时长：校验窗（SPDP 发现发布者约 1~3s）+ ~5s 录制期
    constexpr int kRecordMs = 12000;
    constexpr int kPubIntervalMs = 150;
} // namespace

int main()
{
    namespace dds = eprosima::fastdds::dds;  // main 作用域别名：直连 FastDDS API 搭建远端发布端

    YOMK_INIT();

    auto *svc = new YomkRpcBagService(YOMK_SERVER_P);
    CHECK(YOMK_ADD_SERVICE(svc) == 0, "YomkRpcBagService 注册成功（所有权移交框架，init() 已内部调用）");

    // ---- L0：/bag_info 与节点生命周期解耦（纯文件读，节点状态无关） ----
    auto infoBefore = svc->invoke("/bag_info", YomkMkPtr(DDSBagInfo, DDSBagInfo{"info_svc_l0_miss"}));
    CHECK(infoBefore.m_status == YomkResponse::eNo &&
              infoBefore.m_msg.find("bag path [info_svc_l0_miss] does not exist") != std::string::npos &&
              infoBefore.m_msg.find("bag node not created") == std::string::npos,
          "未建节点时 /bag_info 可达：报文件层错误（非 bag node not created）");

    // ---- L0：/bag_reindex 与节点生命周期解耦（纯文件操作，节点状态无关） ----
    auto reindexBefore =
        svc->invoke("/bag_reindex", YomkMkPtr(DDSBagReindex, DDSBagReindex{"reindex_l0_miss"}));
    CHECK(reindexBefore.m_status == YomkResponse::eNo &&
              reindexBefore.m_msg.find("bag path [reindex_l0_miss] does not exist") != std::string::npos &&
              reindexBefore.m_msg.find("bag node not created") == std::string::npos,
          "未建节点时 /bag_reindex 可达：报文件层错误（非 bag node not created）");

    // ---- L1：创建 / 重复创建拒绝 ----
    CHECK(svc->invoke("/create_node", YomkMkPtr(DDSBagNode, DDSBagNode{TEST_DOMAIN})).m_status ==
              YomkResponse::eOk,
          "创建 bag 节点(domain 200) → eOk（真实创建 participant）");
    auto infoAfter = svc->invoke("/bag_info", YomkMkPtr(DDSBagInfo, DDSBagInfo{"info_svc_l0_miss"}));
    CHECK(infoAfter.m_status == YomkResponse::eNo &&
              infoAfter.m_msg.find("does not exist") != std::string::npos,
          "已建节点时 /bag_info 同样可达（行为与节点状态无关）");
    auto reindexAfter =
        svc->invoke("/bag_reindex", YomkMkPtr(DDSBagReindex, DDSBagReindex{"reindex_l0_miss"}));
    CHECK(reindexAfter.m_status == YomkResponse::eNo &&
              reindexAfter.m_msg.find("does not exist") != std::string::npos,
          "已建节点时 /bag_reindex 同样可达（行为与节点状态无关）");
    auto dupCreate = svc->invoke("/create_node", YomkMkPtr(DDSBagNode, DDSBagNode{TEST_DOMAIN}));
    CHECK(dupCreate.m_status == YomkResponse::eNo &&
              dupCreate.m_msg.find("bag node already exists") != std::string::npos,
          "重复创建 → eNo bag node already exists, delete it first");

    // record 空清单：服务层锁外校验先于节点触达
    auto rEmpty = svc->invoke("/bag_record", YomkMkPtr(DDSBagRecord, DDSBagRecord{{}}));
    CHECK(rEmpty.m_status == YomkResponse::eNo &&
              rEmpty.m_msg.find("no topics given") != std::string::npos,
          "record 空清单 → eNo no topics given");

    // ---- record 无端点主题：校验失败 eNo，m_msg 含主题名（默认收敛窗 ~3s，不落盘） ----
    auto miss = svc->invoke("/bag_record", YomkMkPtr(DDSBagRecord, DDSBagRecord{{"rt/bag_svc_miss"}}));
    CHECK(miss.m_status == YomkResponse::eNo, "record 无端点主题 → eNo");
    CHECK(miss.m_msg.find("主题 [rt/bag_svc_miss] 既无发布者也无订阅者") != std::string::npos,
          "record 无端点主题 m_msg 含逐主题报错文本");
    // 校验失败后节点未定格：删除并重建验证服务层后续可用（同失败后修正输入重试语义）
    CHECK(svc->invoke("/delete_node").m_status == YomkResponse::eOk, "校验失败后删除 → eOk");
    CHECK(svc->invoke("/create_node", YomkMkPtr(DDSBagNode, DDSBagNode{TEST_DOMAIN})).m_status ==
              YomkResponse::eOk,
          "重建 bag 节点 → eOk");

    // ---- 完整录制：远端 MString 发布者 + 发布线程 + 停止线程模拟 Ctrl+C ----
    auto *peerParticipant = dds::DomainParticipantFactory::get_instance()->create_participant(
        TEST_DOMAIN, dds::PARTICIPANT_QOS_DEFAULT);
    CHECK(peerParticipant != nullptr, "远端发布端 participant 创建成功（完整录制用例）");
    dds::TypeSupport ts(new YomkRpc::MStringPubSubType());
    ts.register_type(peerParticipant);
    auto *pub = peerParticipant->create_publisher(dds::PUBLISHER_QOS_DEFAULT);
    auto *topic = peerParticipant->create_topic(REC_TOPIC, ts.get_type_name(), dds::TOPIC_QOS_DEFAULT);
    auto *writer =
        (pub != nullptr && topic != nullptr) ? pub->create_datawriter(topic, dds::DATAWRITER_QOS_DEFAULT)
                                             : nullptr;
    CHECK(writer != nullptr, "远端 DataWriter 创建成功（rt/bag_svc_rec）");

    std::atomic<bool> pubStop{false};
    std::thread pubThread([&]()
    {
        YomkRpc::MString msg;
        uint32_t seq = 0;
        while (!pubStop.load())
        {
            msg.data("svc-record-payload-" + std::to_string(seq++));
            writer->write(&msg);
            std::this_thread::sleep_for(std::chrono::milliseconds(kPubIntervalMs));
        }
    });

    // 停止线程：~5s 后经 bagRecordStop 置位（模拟 CLI onSignal 的 SIGINT 路径，async-signal-safe store）
    std::thread stopThread([&]()
    {
        std::this_thread::sleep_for(std::chrono::milliseconds(kRecordMs));
        yomk::bagRecordStop();
    });

    // 主线程长驻阻塞：校验通过 → 透传订阅 → 录制 → 收尾 → eOk 回执
    auto rec = svc->invoke("/bag_record", YomkMkPtr(DDSBagRecord, DDSBagRecord{{REC_TOPIC}}));
    stopThread.join();
    pubStop.store(true);
    pubThread.join();

    CHECK(rec.m_status == YomkResponse::eOk, "完整录制（bagRecordStop 收尾）→ eOk");
    YomkUnPackPkg(rec.m_data, StringArray, lines);
    CHECK(lines != nullptr && lines->d.size() == 2, "record 回执可解包为 2 行 StringArray（bag 目录 + 统计行）");
    if (lines != nullptr && lines->d.size() == 2)
    {
        CHECK(lines->d[0].rfind("bag_", 0) == 0, "回执首行 bag 目录名（bag_ 前缀）");
        CHECK(lines->d[1].find(REC_TOPIC) != std::string::npos, "统计行含主题名");
        CHECK(lines->d[1].find(" 条 / ") != std::string::npos && lines->d[1].find(" 字节") != std::string::npos,
              "统计行形如 topic: N 条 / M 字节");
    }

    // ---- 定格语义：成功录制后重复 record 拒绝 ----
    auto again = svc->invoke("/bag_record", YomkMkPtr(DDSBagRecord, DDSBagRecord{{REC_TOPIC}}));
    CHECK(again.m_status == YomkResponse::eNo &&
              again.m_msg.find("record already finished") != std::string::npos,
          "定格式拒绝：成功录制后重复 record → eNo record already finished");

    // ---- 删除 / 重复删除拒绝 / 删除后 record 拒绝 ----
    CHECK(svc->invoke("/delete_node").m_status == YomkResponse::eOk, "删除 bag 节点 → eOk");
    auto dupDel = svc->invoke("/delete_node");
    CHECK(dupDel.m_status == YomkResponse::eNo &&
              dupDel.m_msg.find("bag node not created") != std::string::npos,
          "重复删除 → eNo bag node not created");
    auto delRec = svc->invoke("/bag_record", YomkMkPtr(DDSBagRecord, DDSBagRecord{{REC_TOPIC}}));
    CHECK(delRec.m_status == YomkResponse::eNo &&
              delRec.m_msg.find("bag node not created") != std::string::npos,
          "删除后 record → eNo bag node not created");

    // ---- 重建 / 重复创建拒绝 / 收尾删除（关键不变式：显式删除规避静态析构序问题） ----
    CHECK(svc->invoke("/create_node", YomkMkPtr(DDSBagNode, DDSBagNode{TEST_DOMAIN})).m_status ==
              YomkResponse::eOk,
          "删除后重建 bag 节点 → eOk");
    auto dupCreate2 = svc->invoke("/create_node", YomkMkPtr(DDSBagNode, DDSBagNode{TEST_DOMAIN}));
    CHECK(dupCreate2.m_status == YomkResponse::eNo &&
              dupCreate2.m_msg.find("bag node already exists") != std::string::npos,
          "重建后重复创建仍拒绝（单节点模型全程生效）");

    // ---- L3：/bag_play 端点（重建节点在场）：负载往返保真 + 最小 bag 真实回放 ----
    std::error_code ec;
    // 造最小 bag：mcap writer 单分片 + 手写 metadata.json（record 组装同款键集；2 条
    // @100ms 时间轴 ~200ms 快速播完，无订阅端——write 不依赖匹配，回执对账为准）
    const std::string playBagDir = "play_svc_l3";
    std::filesystem::remove_all(playBagDir, ec);
    CHECK(std::filesystem::create_directory(playBagDir, ec), "L3 前置：bag 目录创建成功");
    {
        mcap::McapWriter writer;
        mcap::McapWriterOptions options("");
        options.compression = mcap::Compression::None;
        CHECK(writer.open(playBagDir + "/bag_0.mcap", options).ok(), "L3 前置：mcap 分片打开成功");
        mcap::Channel channel("/rt/bag_svc_play", "cdr", 0,
            mcap::KeyValueMap{{"type", "YomkRpc::MString"}, {"qos", "RELIABLE,TRANSIENT_LOCAL"}});
        writer.addChannel(channel);
        const std::string payload(5, 'x');
        for (uint32_t i = 0; i < 2; ++i)
        {
            mcap::Message msg;
            msg.channelId = channel.id;
            msg.sequence = i;
            msg.logTime = 1700000000000000000ULL + i * 100000000ULL;
            msg.publishTime = msg.logTime;
            // uint8_t 与 std::byte 同为单字节原始存储，别名转换安全（mcap 接口要求数据指针）
            msg.data = reinterpret_cast<const std::byte *>(payload.data()); // NOLINT(cppcoreguidelines-pro-type-reinterpret-cast)
            msg.dataSize = payload.size();
            CHECK(writer.write(msg).ok(), "L3 前置：mcap 消息写入成功");
        }
        writer.close();

        nlohmann::json info;
        info["version"] = 1;
        info["storage_identifier"] = "mcap";
        info["relative_file_paths"] = nlohmann::json::array({"bag_0.mcap"});
        info["starting_time"] = {{"nanoseconds_since_epoch", 1700000000000000000ULL},
                                 {"nanoseconds_since_epoch_format", "manual"}};
        info["duration"] = {{"nanoseconds", 100000000ULL}, {"nanoseconds_format", "manual"}};
        info["message_count"] = 2;
        info["topics_with_message_count"] = nlohmann::json::array({{
            {"topic_metadata",
                {{"name", "/rt/bag_svc_play"}, {"type", "YomkRpc::MString"},
                    {"qos", "RELIABLE,TRANSIENT_LOCAL"}}},
            {"message_count", 2}}});
        std::ofstream out(playBagDir + "/metadata.json");
        out << info.dump(4) << "\n";
    }
    CHECK(std::filesystem::exists(playBagDir + "/metadata.json", ec), "L3 前置：metadata.json 落盘");

    // a) 负载往返保真：路径不存在 → eNo 且错误文本回显 bagDir（解包字段无损证明）
    auto playMiss = svc->invoke("/bag_play", YomkMkPtr(DDSBagPlay, DDSBagPlay{"play_svc_l3_miss"}));
    CHECK(playMiss.m_status == YomkResponse::eNo &&
              playMiss.m_msg.find("bag path [play_svc_l3_miss] does not exist") != std::string::npos,
          "/bag_play 负载往返：错误文本回显 bagDir（解包字段保真）");

    // b) 真实回放：eOk + StringArray 回执（首行 bagDir + 统计行，对齐 record 回执形态）
    auto playOk = svc->invoke("/bag_play", YomkMkPtr(DDSBagPlay, DDSBagPlay{playBagDir}));
    CHECK(playOk.m_status == YomkResponse::eOk, "/bag_play 真实 bag 回放 → eOk");
    YomkUnPackPkg(playOk.m_data, StringArray, playLines);
    CHECK(playLines != nullptr && playLines->d.size() == 2,
          "play 回执可解包为 2 行 StringArray（bag 目录 + 统计行）");
    if (playLines != nullptr && playLines->d.size() == 2)
    {
        CHECK(playLines->d[0] == playBagDir, "回执首行 bag 目录名");
        CHECK(playLines->d[1].find("/rt/bag_svc_play") != std::string::npos &&
                  playLines->d[1].find("2 条 / ") != std::string::npos &&
                  playLines->d[1].find(" 字节") != std::string::npos,
              "统计行形如 topic: 2 条 / N 字节");
    }
    std::filesystem::remove_all(playBagDir, ec);  // 回放输入用例收尾自清理

    CHECK(svc->invoke("/delete_node").m_status == YomkResponse::eOk, "退出前显式删除（清理不变式）");

    // 远端发布端清理
    if (peerParticipant != nullptr)
    {
        dds::DomainParticipantFactory::get_instance()->delete_participant(peerParticipant);
    }

    return testReport("TestYomkRpcBagServiceLifecycle");
}
