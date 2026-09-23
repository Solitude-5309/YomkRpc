/**
 * @file TestYomkRpcDebugServiceLifecycle.cpp
 * @brief YomkRpcDebugService 真实 DDS 生命周期测试
 *
 * 范围：契约测试（DDS-free）刻意豁免的 /create_node 成功路径（经 setDomainId 创建真实
 *       participant）、/topic_print 登记契约与 /delete_node 析构清理，及 domainId 边界行为。
 *       节点层守卫（setDomainId 重复/未入域登记）与端到端流量（发现→动态类型→订阅→JSON 输出）
 *       已由 TestFastDDSDebugNode 覆盖，本测试不重复。
 * 覆盖：
 *   A-1 生命周期：创建 → list_topics 空列表 → 重复创建拒绝 → topicPrint 空回调拒绝 →
 *       正常登记 → 同主题重复登记拒绝 → 异主题登记 → 删除 → 重复删除拒绝 → 删除后
 *       list_topics 拒绝 → 删除后重建 → 退出前清理；
 *   A-2 domainId 边界：有效域 0 与上界 232（eOk；真实创建 participant 后即删）。
 *
 * 关键不变式：每个 eOk 创建的调试节点必须在 main 返回前经 /delete_node 显式删除，以规避
 *   YOMK 框架 atexit 服务析构晚于 FastDDS DomainParticipantFactory 单例销毁导致的静态析构
 *   顺序问题（参见 TestYomkRpcNodeLifecycle / ExampleYomkRpcPub）。
 *
 * 风格：纯 main() + CHECK 宏 + 失败计数（零第三方依赖），返回非 0 表示存在失败用例。
 */

#include "TestCheck.h"
#include "YomkRpcDebugService.h" // 服务/DDSDebugNode/DDSDebugTopic/YOMK_* 宏

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
} // namespace

int main()
{
    YOMK_INIT();

    auto *svc = new YomkRpcDebugService(YOMK_SERVER_P);
    CHECK(YOMK_ADD_SERVICE(svc) == 0, "YomkRpcDebugService 注册成功（所有权移交框架，init() 已内部调用）");

    // ---- A-1：生命周期（创建/重复创建/登记契约/删除/重建） ----
    CHECK(svc->invoke("/create_node", mkNode(TEST_DOMAIN)).m_status == YomkResponse::eOk,
          "创建调试节点(domain 200) → eOk（真实创建 participant）");

    // list_topics：ctest 串行执行，此刻域 200 无任何远端 writer，同步查询得空列表（正常状态）
    auto listed = svc->invoke("/list_topics");
    CHECK(listed.m_status == YomkResponse::eOk && listed.m_data != nullptr,
          "list_topics（入域无 writer）→ eOk（空列表路径）");
    YomkUnPackPkg(listed.m_data, StringArray, arr);
    CHECK(arr != nullptr && arr->d.empty(), "list_topics 返回包可解包为空 StringArray");

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

    auto listAfterDel = svc->invoke("/list_topics");
    CHECK(listAfterDel.m_status == YomkResponse::eNo &&
              listAfterDel.m_msg.find("not created") != std::string::npos,
          "删除后 list_topics → eNo debug node not created");

    CHECK(svc->invoke("/create_node", mkNode(TEST_DOMAIN)).m_status == YomkResponse::eOk,
          "删除后重建调试节点 → eOk（delete/create 闭环）");

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
