/**
 * @file TestYomkRpcDebugServiceLifecycle.cpp
 * @brief YomkRpcDebugService 真实 DDS 生命周期测试
 *
 * 范围：契约测试（DDS-free）刻意豁免的 /create_node 成功路径（经 setDomainId 创建真实
 *       participant）、/topic_print 登记契约与 /delete_node 析构清理，及 domainId 边界行为。
 *       节点层守卫（setDomainId 重复/未入域登记）与端到端流量（发现→动态类型→订阅→JSON 输出）
 *       已由 TestFastDDSDebugNode 覆盖，本测试不重复；topic_info 命中用例仅直连 FastDDS 建最小
 *       远端发布端触发 EDP 发现（不发布数据），覆盖服务层拼行路径。
 * 覆盖：
 *   A-1 生命周期：创建 → list_topics 收敛空列表 → topic_info 未发现主题 eNo → 重复创建拒绝 →
 *       topicPrint 空回调拒绝 → 正常登记 → 同主题重复登记拒绝 → 异主题登记 → 删除 →
 *       重复删除拒绝 → 删除后 list_topics/topic_info 拒绝 → 删除后重建 → topic_info 命中三行
 *       断言（Type/Publisher count/Subscription count）→ 退出前清理；
 *   A-2 domainId 边界：有效域 0 与上界 232（eOk；真实创建 participant 后即删）。
 *
 * 关键不变式：每个 eOk 创建的调试节点必须在 main 返回前经 /delete_node 显式删除，以规避
 *   YOMK 框架 atexit 服务析构晚于 FastDDS DomainParticipantFactory 单例销毁导致的静态析构
 *   顺序问题（参见 TestYomkRpcNodeLifecycle / ExampleYomkRpcPub）。
 *
 * 风格：纯 main() + CHECK 宏 + 失败计数（零第三方依赖），返回非 0 表示存在失败用例。
 */

#include "TestCheck.h"
#include "YomkRpcDebugService.h" // 服务/DDSDebugNode/DDSDebugTopic/DDSDebugInfo/YOMK_* 宏

#include <YomkRpcMsg/YomkRpcMsgPubSubTypes.hpp> // MStringPubSubType（仅 topic_info 命中用例的最小远端发布端）

#include <fastdds/dds/domain/DomainParticipant.hpp>        // 直连 FastDDS API 搭建远端发布端（命中用例）
#include <fastdds/dds/domain/DomainParticipantFactory.hpp> // participant 创建/删除
#include <fastdds/dds/publisher/DataWriter.hpp>
#include <fastdds/dds/publisher/Publisher.hpp>
#include <fastdds/dds/topic/Topic.hpp>
#include <fastdds/dds/topic/TypeSupport.hpp>

#include <cstdint>
#include <string>

namespace
{
    // 有效域（FastDDS 约 0-232），避开默认域 0 的潜在网络干扰
    constexpr uint32_t TEST_DOMAIN = 200;

    // 构造 /create_node 请求包
    YomkPkgPtr mkNode(uint32_t domainId)
    {
        return YomkMkPtr(DDSDebugNode, DDSDebugNode{domainId});
    }

    // 构造 /topic_print 请求包（no-op 输出回调：登记路径不产生真实流量）
    YomkPkgPtr mkPrint(const std::string &topicName)
    {
        return YomkMkPtr(DDSDebugTopic, DDSDebugTopic{topicName, [](const std::string &) {}});
    }

    // 构造 /list_topics 请求包（stableRounds=1 单次快照路径，免收敛等待）
    YomkPkgPtr mkList()
    {
        return YomkMkPtr(DDSDebugList, DDSDebugList{1, 100});
    }

    // 构造 /topic_info 请求包（未发现用例传 rounds=1 走单次快照快速路径；命中用例传收敛参数）
    YomkPkgPtr mkInfo(const std::string &topicName, uint32_t stableRounds, uint32_t intervalMs)
    {
        return YomkMkPtr(DDSDebugInfo, DDSDebugInfo{topicName, stableRounds, intervalMs});
    }
} // namespace

int main()
{
    YOMK_INIT();

    auto *svc = new YomkRpcDebugService(YOMK_SERVER_P);
    CHECK(YOMK_ADD_SERVICE(svc) == 0, "YomkRpcDebugService 注册成功（所有权移交框架，init() 已内部调用）");

    // ---- A-1：生命周期（创建/重复创建/登记契约/删除/重建） ----
    CHECK(svc->invoke("/create_node", mkNode(TEST_DOMAIN)).m_status == YomkResponse::eOk,
          "创建调试节点(domain 200) → eOk（真实创建 participant）");

    // list_topics：ctest 串行执行，此刻域 200 无任何远端 writer；stableRounds=1 单次快照
    // 即得空列表（收敛参数链路直通节点层）
    auto listed = svc->invoke("/list_topics", mkList());
    CHECK(listed.m_status == YomkResponse::eOk && listed.m_data != nullptr,
          "list_topics（入域无 writer，单次快照）→ eOk（空列表路径）");
    YomkUnPackPkg(listed.m_data, StringArray, arr);
    CHECK(arr != nullptr && arr->d.empty(), "list_topics 返回包可解包为空 StringArray");

    // topic_info 未发现主题：node_ 已建、域内无该主题，单次快照快速路径 → eNo
    auto infoAbsent = svc->invoke("/topic_info", mkInfo("t_info_absent", 1, 50));
    CHECK(infoAbsent.m_status == YomkResponse::eNo &&
              infoAbsent.m_msg.find("not found") != std::string::npos,
          "topicInfo 未发现主题（单次快照）→ eNo topic not found");

    auto dup = svc->invoke("/create_node", mkNode(TEST_DOMAIN));
    CHECK(dup.m_status == YomkResponse::eNo && dup.m_msg.find("already exists") != std::string::npos,
          "重复创建 → eNo already exists（单节点模型，须先 delete）");

    auto emptyCb = svc->invoke("/topic_print", YomkMkPtr(DDSDebugTopic, DDSDebugTopic{"t_debug_a", nullptr}));
    CHECK(emptyCb.m_status == YomkResponse::eNo &&
              emptyCb.m_msg.find("output callback is empty") != std::string::npos,
          "topicPrint 空回调 → eNo output callback is empty");

    CHECK(svc->invoke("/topic_print", mkPrint("t_debug_a")).m_status == YomkResponse::eOk,
          "topicPrint(t_debug_a) → eOk（登记待发现）");

    auto dupTopic = svc->invoke("/topic_print", mkPrint("t_debug_a"));
    CHECK(dupTopic.m_status == YomkResponse::eNo && dupTopic.m_msg.find("failed") != std::string::npos,
          "同主题重复登记 → eNo（subscribeTopic 去重拒绝）");

    CHECK(svc->invoke("/topic_print", mkPrint("t_debug_b")).m_status == YomkResponse::eOk,
          "topicPrint(t_debug_b) → eOk（异主题可并行登记）");

    CHECK(svc->invoke("/delete_node").m_status == YomkResponse::eOk,
          "删除调试节点 → eOk（析构清理 participant 与登记）");

    auto del2 = svc->invoke("/delete_node");
    CHECK(del2.m_status == YomkResponse::eNo && del2.m_msg.find("not created") != std::string::npos,
          "重复删除 → eNo debug node not created");

    auto listAfterDel = svc->invoke("/list_topics", mkList());
    CHECK(listAfterDel.m_status == YomkResponse::eNo &&
              listAfterDel.m_msg.find("not created") != std::string::npos,
          "删除后 list_topics → eNo debug node not created");

    auto infoAfterDel = svc->invoke("/topic_info", mkInfo("t_info_absent", 1, 50));
    CHECK(infoAfterDel.m_status == YomkResponse::eNo &&
              infoAfterDel.m_msg.find("not created") != std::string::npos,
          "删除后 topic_info → eNo debug node not created");

    CHECK(svc->invoke("/create_node", mkNode(TEST_DOMAIN)).m_status == YomkResponse::eOk,
          "删除后重建调试节点 → eOk（delete/create 闭环）");

    // ---- topic_info 命中用例：远端最小发布端（仅建端点触发 EDP 发现，不发布数据）----
    // 复用重建后的域 200 调试节点；时序模式同 TestFastDDSDebugNode 多端点用例：
    // list 预热（约 400ms）+ info 独立收敛窗口（500ms 起）覆盖本地 EDP 发现延迟
    {
        namespace dds = eprosima::fastdds::dds;  // 块内别名：直连 FastDDS API 搭建远端发布端
        constexpr const char *HIT_TOPIC = "t_info_hit";
        auto *hitParticipant = dds::DomainParticipantFactory::get_instance()->create_participant(
            TEST_DOMAIN, dds::PARTICIPANT_QOS_DEFAULT);
        CHECK(hitParticipant != nullptr, "远端发布端 participant 创建成功（topic_info 命中用例）");
        if (hitParticipant != nullptr)
        {
            // TypeSupport 须活过 topic/writer 使用期（声明顺序同 TestFastDDSDebugNode 计数用例）
            dds::TypeSupport ts(new YomkRpc::MStringPubSubType());
            ts.register_type(hitParticipant);
            auto *pub = hitParticipant->create_publisher(dds::PUBLISHER_QOS_DEFAULT);
            auto *topic = hitParticipant->create_topic(
                HIT_TOPIC, ts.get_type_name(), dds::TOPIC_QOS_DEFAULT);
            auto *writer = (pub != nullptr && topic != nullptr)
                ? pub->create_datawriter(topic, dds::DATAWRITER_QOS_DEFAULT)
                : nullptr;
            CHECK(writer != nullptr, "远端 DataWriter 创建成功（t_info_hit，仅建端点不发布数据）");

            if (writer != nullptr)
            {
                // list_topics 预热仅为本用例时序前置（late joiner 经 EDP 重放发现信息）；
                // topic_info 内部独立收敛查询，不依赖 list_topics
                auto warm = svc->invoke("/list_topics", YomkMkPtr(DDSDebugList, DDSDebugList{5, 100}));
                CHECK(warm.m_status == YomkResponse::eOk, "list_topics(5,100ms) 预热 → eOk（发现时序前置）");

                auto hit = svc->invoke("/topic_info", mkInfo(HIT_TOPIC, 5, 100));
                CHECK(hit.m_status == YomkResponse::eOk && hit.m_data != nullptr,
                      "topicInfo(t_info_hit,5,100ms) → eOk（独立收敛命中）");
                YomkUnPackPkg(hit.m_data, StringArray, infoArr);
                CHECK(infoArr != nullptr && infoArr->d.size() == 3,
                      "topicInfo 返回包可解包为 3 行 StringArray");
                if (infoArr != nullptr && infoArr->d.size() == 3)
                {
                    CHECK(infoArr->d[0] == "Type: YomkRpc::MString",
                          "topicInfo 第 1 行 Type 为原始 DDS 类型名（YomkRpc::MString，无转换）");
                    CHECK(infoArr->d[1] == "Publisher count: 1", "topicInfo 第 2 行 Publisher count == 1");
                    CHECK(infoArr->d[2] == "Subscription count: 0",
                          "topicInfo 第 3 行 Subscription count == 0");
                }
            }

            // 清理：writer → topic → publisher → participant（同 TestFastDDSDebugNode 计数用例顺序）
            if (writer != nullptr && pub != nullptr)
            {
                pub->delete_datawriter(writer);
            }
            if (topic != nullptr)
            {
                hitParticipant->delete_topic(topic);
            }
            if (pub != nullptr)
            {
                hitParticipant->delete_publisher(pub);
            }
            dds::DomainParticipantFactory::get_instance()->delete_participant(hitParticipant);
        }
    }

    // ---- A-2：domainId 边界（有效域下界/上界，创建后即删） ----
    CHECK(svc->invoke("/delete_node").m_status == YomkResponse::eOk, "删除重建节点 → eOk（A-2 前清理）");
    CHECK(svc->invoke("/create_node", mkNode(0)).m_status == YomkResponse::eOk,
          "domainId=0 → eOk（有效域下界）");
    CHECK(svc->invoke("/delete_node").m_status == YomkResponse::eOk, "删除 → eOk");
    CHECK(svc->invoke("/create_node", mkNode(232)).m_status == YomkResponse::eOk,
          "domainId=232 → eOk（有效域上界，FastDDS 端口算术上限）");
    CHECK(svc->invoke("/delete_node").m_status == YomkResponse::eOk, "删除 → eOk（关键不变式：退出前清理）");

    return testReport("TestYomkRpcDebugServiceLifecycle");
}
