/**
 * @file TestYomkRpcNodeLifecycle.cpp
 * @brief MC2：真实 DDS 节点生命周期 + 边界值测试
 *
 * 范围：MC1（DDS-free）刻意豁免的 /create_node 成功/重名路径（经 setDomainId 创建真实
 *       participant），以及 FastDDSNode 的真实 DDS 生命周期与 domainId/nodeName 边界行为。
 * 覆盖：
 *   Part A（服务层，经 invoke）：
 *     A-1 生命周期：创建/重名/同域共存/删除/重复删除；
 *     A-2 domainId 边界：有效下界 0（eOk）、UINT32_MAX（uint32→int32 回绕为负、非法域 →
 *         setDomainId 于 participant==null 分支返回 false → createNode eNo）；
 *     A-3 nodeName 边界：空串/单字符/1024B 超长（均被服务层接受为 map key）。
 *   Part B（直接单测 FastDDSNode）：setDomainId 重复设置守卫（participant_!=nullptr → false），
 *     该分支服务层不可达（每次 createNode 都新建 FastDDSNode 并只调一次 setDomainId）。
 *
 * 关键不变式：每个 eOk 创建的节点必须在 main 返回前经 /delete_node 显式删除；Part B 的
 *   FastDDSNode 为栈对象、在 main 返回前离开作用域析构。以规避 YOMK 框架 atexit 服务析构
 *   晚于 FastDDS DomainParticipantFactory 单例销毁导致的静态析构顺序问题
 *   （参见 examples/ExampleYomkRpcPub.cpp：退出前显式销毁节点）。
 *
 * 风格：纯 main() + CHECK 宏 + 失败计数（零第三方依赖），返回非 0 表示存在失败用例。
 */

#include "TestCheck.h"
#include "YomkRpcService.h" // 服务/DDSNode/String/YOMK_* 宏
#include "FastDDSNode.h"    // Part B 直接单元测试 setDomainId

#include <cstdint>
#include <iostream>
#include <string>

namespace
{
    // 有效域（FastDDS 约 0-232），避开默认域 0 的潜在网络干扰
    constexpr uint32_t TEST_DOMAIN = 200;

    // 构造 /create_node 请求包
    YomkPkgPtr mkNode(uint32_t domainId, const std::string &nodeName)
    {
        return YomkMkPtr(DDSNode, DDSNode{domainId, nodeName});
    }

    // Part A-1：生命周期——创建/重名/同域共存/删除/重复删除
    void testLifecycle(YomkRpcService *svc)
    {
        CHECK(svc->invoke("/create_node", mkNode(TEST_DOMAIN, "node_a")).m_status == YomkResponse::eOk,
              "创建 node_a(domain 200) → eOk（真实创建 participant）");

        auto dup = svc->invoke("/create_node", mkNode(TEST_DOMAIN, "node_a"));
        CHECK(dup.m_status == YomkResponse::eNo && dup.m_msg.find("already exists") != std::string::npos,
              "重名 node_a → eNo already exists（服务层前置校验，不新建 participant）");

        CHECK(svc->invoke("/create_node", mkNode(TEST_DOMAIN, "node_b")).m_status == YomkResponse::eOk,
              "创建 node_b → eOk（与 node_a 同域共存，各自独立 participant）");

        CHECK(svc->invoke("/delete_node", YomkMkPtr(String, "node_a")).m_status == YomkResponse::eOk,
              "删除 node_a → eOk（析构清理 participant）");

        auto del2 = svc->invoke("/delete_node", YomkMkPtr(String, "node_a"));
        CHECK(del2.m_status == YomkResponse::eNo && del2.m_msg.find("not exists") != std::string::npos,
              "重复删除 node_a → eNo not exists");

        CHECK(svc->invoke("/delete_node", YomkMkPtr(String, "node_b")).m_status == YomkResponse::eOk,
              "删除 node_b → eOk");
    }

    // Part A-2：domainId 边界——有效下界 0；uint32→int32 回绕非法域触发 setDomainId participant==null 分支
    void testDomainIdBoundary(YomkRpcService *svc)
    {
        CHECK(svc->invoke("/create_node", mkNode(0, "node_d0")).m_status == YomkResponse::eOk,
              "domainId=0 → eOk（有效域下界）");
        CHECK(svc->invoke("/delete_node", YomkMkPtr(String, "node_d0")).m_status == YomkResponse::eOk,
              "删除 node_d0 → eOk");

        auto rmax = svc->invoke("/create_node", mkNode(UINT32_MAX, "node_dmax"));
        CHECK(rmax.m_status == YomkResponse::eNo && rmax.m_msg.find("setDomainId error") != std::string::npos,
              "domainId=UINT32_MAX → eNo setDomainId error（uint32→int32 回绕为负、非法域，确定性触发 participant==null 分支）");
        std::cout << "[OBSERVE] domainId=UINT32_MAX status=" << rmax.m_status
                  << " msg=\"" << rmax.m_msg << "\"" << std::endl;
    }

    // Part A-3：nodeName 边界——空串/单字符/超长（各创建后显式删除）
    void testNodeNameBoundary(YomkRpcService *svc)
    {
        CHECK(svc->invoke("/create_node", mkNode(TEST_DOMAIN, "")).m_status == YomkResponse::eOk,
              "nodeName 空串 → eOk（服务层未校验空名，作为 map key 接受）");
        CHECK(svc->invoke("/delete_node", YomkMkPtr(String, "")).m_status == YomkResponse::eOk,
              "删除空名节点 → eOk");

        CHECK(svc->invoke("/create_node", mkNode(TEST_DOMAIN, "x")).m_status == YomkResponse::eOk,
              "nodeName 单字符 x → eOk");
        CHECK(svc->invoke("/delete_node", YomkMkPtr(String, "x")).m_status == YomkResponse::eOk,
              "删除单字符节点 → eOk");

        const std::string longName(1024, 'N');
        CHECK(svc->invoke("/create_node", mkNode(TEST_DOMAIN, longName)).m_status == YomkResponse::eOk,
              "nodeName 1024B 超长 → eOk（无长度校验）");
        CHECK(svc->invoke("/delete_node", YomkMkPtr(String, longName)).m_status == YomkResponse::eOk,
              "删除超长名节点 → eOk");
    }

    // Part B：直接单测 FastDDSNode::setDomainId 重复设置守卫（服务层不可达）
    void testSetDomainIdGuard()
    {
        FastDDSNode node;
        CHECK(node.setDomainId(TEST_DOMAIN), "Part B: 首次 setDomainId(200) → true（创建 participant）");
        CHECK(!node.setDomainId(TEST_DOMAIN),
              "Part B: 重复 setDomainId → false（participant_!=nullptr 守卫，服务层每次新建 FastDDSNode 故不可达）");
        // node 栈对象在此离开作用域析构，清理 participant（DomainParticipantFactory 仍存活）
    }
} // namespace

int main()
{
    YOMK_INIT();

    auto *svc = new YomkRpcService(YOMK_SERVER_P);
    CHECK(YOMK_ADD_SERVICE(svc) == 0, "YomkRpcService 注册成功（所有权移交框架，init() 已内部调用）");

    testLifecycle(svc);
    testDomainIdBoundary(svc);
    testNodeNameBoundary(svc);
    testSetDomainIdGuard();

    return testReport("TestYomkRpcNodeLifecycle");
}
