/**
 * @file TestYomkRpcDebugServiceContract.cpp
 * @brief YomkRpcDebugService 服务层契约测试（DDS-free，白盒经 invoke 直接分发）
 *
 * 范围：仅验证调试服务 4 端点在不触发任何 DDS 运行时（不创建 participant）前提下的
 *       输入校验与错误码契约；真实 DDS 生命周期（创建/删除/登记）归
 *       TestYomkRpcDebugServiceLifecycle，节点层守卫与端到端流量归 TestFastDDSDebugNode。
 * 覆盖：
 *   T1 funcInfos 内省 4 端点齐全 + 未知端点 eNo；
 *   T2 /version 正常路径（eOk + 版本串契约 + 忽略 pkg）；
 *   T3 /create_node、/topic_print 解包双守卫（nullptr / 异类包 / 改名伪造）；
 *   T4 /delete_node 特殊契约：单节点模型无参载荷，handler (void)pkg 不走解包守卫，
 *      未建节点时无论何种载荷均 eNo "debug node not created"（与 YomkRpcService delete_node
 *      的 String 包解包守卫形态不同，属设计差异点）；
 *   T5 /create_node domainId 越界前置拦截（233 / UINT32_MAX → eNo，不触 DDS）；
 *   T6 /topic_print 未建节点早退（node_ 空检查先于 output 空检查）。
 * DDS-free 保证：T5 的越界校验在锁外返回；T4/T6 的 node_ 空检查在触达节点层前返回；
 *   /version 不碰 DDS；本测试绝不传合法 DDSDebugNode{0..232}（那会创建真实 participant）。
 *
 * 风格：纯 main() + CHECK 宏 + 失败计数（零第三方依赖），返回非 0 表示存在失败用例。
 */

#include "TestCheck.h"
#include "YomkRpcDebugService.h" // 服务/DDSDebugNode/DDSDebugTopic/String/YOMK_* 宏

#include <cstdint>
#include <string>

namespace
{
    // T2：/version 契约（不解包，忽略 pkg）
    void testVersion(YomkRpcDebugService *svc)
    {
        auto resp = svc->invoke("/version");
        CHECK(resp.m_status == YomkResponse::eOk, "/version 返回 eOk");
        CHECK(resp.m_msg == "ok", "/version m_msg == ok");

        YomkUnPackPkg(resp.m_data, String, ver);
        CHECK(ver != nullptr, "/version m_data 可解包为 String");
        if (ver != nullptr)
        {
            CHECK(ver->d.rfind("YomkRpc v", 0) == 0, "/version 版本串前缀为 YomkRpc v");
        }

        auto respNull = svc->invoke("/version", nullptr);
        CHECK(respNull.m_status == YomkResponse::eOk, "/version 传 nullptr 仍 eOk（忽略 pkg）");
    }

    // T3：/create_node 与 /topic_print 的解包双守卫（nullptr / 异类包 / 改名伪造）
    void testUnpackGuards(YomkRpcDebugService *svc)
    {
        auto checkGuards = [&](const char *ep, const char *expectName, YomkPkgPtr wrongPkg)
        {
            // (a) nullptr → 第一守卫 !pkg
            auto ra = svc->invoke(ep, nullptr);
            CHECK(ra.m_status == YomkResponse::eNo &&
                      ra.m_msg.find("pkg is null or pkg is not") != std::string::npos,
                  std::string(ep) + " : nullptr 包命中第一守卫 eNo");

            // (b) 异类包（保留自身名）→ 第一守卫 name()!=expectName
            auto rb = svc->invoke(ep, wrongPkg);
            CHECK(rb.m_status == YomkResponse::eNo &&
                      rb.m_msg.find("pkg is null or pkg is not") != std::string::npos,
                  std::string(ep) + " : 异类包命中第一守卫 eNo");

            // (c) 改名伪造：异类包改名为期望名 → 第一守卫通过、dynamic_pointer_cast 失败命中第二守卫
            wrongPkg->name(expectName);
            auto rc = svc->invoke(ep, wrongPkg);
            CHECK(rc.m_status == YomkResponse::eNo &&
                      rc.m_msg.find("dynamic_pointer_cast failed") != std::string::npos,
                  std::string(ep) + " : 改名伪造命中第二守卫(dynamic_pointer_cast 失败) eNo");
        };

        checkGuards("/create_node", "DDSDebugNode", YomkMkPtr(String, "wrong"));
        checkGuards("/topic_print", "DDSDebugTopic", YomkMkPtr(DDSDebugNode, DDSDebugNode{0}));
    }

    // T4：/delete_node 特殊契约——单节点模型无参载荷，(void)pkg 不走解包守卫
    void testDeleteNodeSpecial(YomkRpcDebugService *svc)
    {
        // 未建节点：无论载荷形态，均在 node_ 空检查处早退（不触 DDS）
        auto rNull = svc->invoke("/delete_node", nullptr);
        CHECK(rNull.m_status == YomkResponse::eNo &&
                  rNull.m_msg.find("debug node not created") != std::string::npos,
              "/delete_node nullptr 载荷 → eNo debug node not created（不走解包守卫）");

        auto rWrong = svc->invoke("/delete_node", YomkMkPtr(String, "wrong"));
        CHECK(rWrong.m_status == YomkResponse::eNo &&
                  rWrong.m_msg.find("debug node not created") != std::string::npos,
              "/delete_node 异类载荷 → eNo debug node not created（载荷被忽略）");

        auto rNoArg = svc->invoke("/delete_node");
        CHECK(rNoArg.m_status == YomkResponse::eNo &&
                  rNoArg.m_msg.find("debug node not created") != std::string::npos,
              "/delete_node 无载荷 → eNo debug node not created（对齐 YOMKRPC_DEBUG_QUIT 形态）");
    }

    // T5：/create_node domainId 越界前置拦截（锁外校验，不触 DDS）
    void testCreateDomainBoundary(YomkRpcDebugService *svc)
    {
        auto r233 = svc->invoke("/create_node", YomkMkPtr(DDSDebugNode, DDSDebugNode{233}));
        CHECK(r233.m_status == YomkResponse::eNo &&
                  r233.m_msg.find("out of valid range") != std::string::npos,
              "domainId=233 → eNo out of valid range（前置拦截，不建 participant）");

        auto rmax = svc->invoke("/create_node", YomkMkPtr(DDSDebugNode, DDSDebugNode{UINT32_MAX}));
        CHECK(rmax.m_status == YomkResponse::eNo &&
                  rmax.m_msg.find("out of valid range") != std::string::npos,
              "domainId=UINT32_MAX → eNo out of valid range（前置拦截，不再回绕深入 setDomainId）");
    }

    // T6：/topic_print 未建节点早退（node_ 空检查先于 output 空检查）
    void testTopicPrintNoNode(YomkRpcDebugService *svc)
    {
        auto rEmptyCb = svc->invoke("/topic_print", YomkMkPtr(DDSDebugTopic, DDSDebugTopic{"t_debug", nullptr}));
        CHECK(rEmptyCb.m_status == YomkResponse::eNo &&
                  rEmptyCb.m_msg.find("debug node not created") != std::string::npos,
              "/topic_print 合法包+空回调+未建节点 → eNo debug node not created（node_ 检查在前）");

        auto rValid = svc->invoke(
            "/topic_print", YomkMkPtr(DDSDebugTopic, DDSDebugTopic{"t_debug", [](const std::string &) {}}));
        CHECK(rValid.m_status == YomkResponse::eNo &&
                  rValid.m_msg.find("debug node not created") != std::string::npos,
              "/topic_print 合法包+非空回调+未建节点 → eNo debug node not created");
    }
} // namespace

int main()
{
    YOMK_INIT();

    // 注册被测服务：所有权移交框架（shared_ptr 持有），init() 内部调用后 weak_from_this() 方有效
    auto *svc = new YomkRpcDebugService(YOMK_SERVER_P);
    CHECK(YOMK_ADD_SERVICE(svc) == 0, "YomkRpcDebugService 注册成功（所有权移交框架，init() 已内部调用）");

    // T1：内省——4 端点齐全
    auto infos = svc->funcInfos();
    CHECK(infos.size() == 4 && infos.count("/version") && infos.count("/create_node") &&
              infos.count("/topic_print") && infos.count("/delete_node"),
          "funcInfos 内省 4 端点齐全");

    testVersion(svc);
    testUnpackGuards(svc);
    testDeleteNodeSpecial(svc);
    testCreateDomainBoundary(svc);
    testTopicPrintNoNode(svc);

    CHECK(svc->invoke("/no_such_endpoint").m_status == YomkResponse::eNo, "未知端点返回 eNo");

    return testReport("TestYomkRpcDebugServiceContract");
}
