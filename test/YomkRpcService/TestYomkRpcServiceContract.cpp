/**
 * @file TestYomkRpcServiceContract.cpp
 * @brief MC1：YomkRpcService 服务层契约测试（DDS-free，白盒经 invoke 直接分发）
 *
 * 范围：仅验证服务层在不触发任何 DDS 运行时（不创建 participant）前提下的输入校验与
 *       错误码契约——这是 MC2+（真实节点/主题/收发）的前置 correctness 基线。
 * 覆盖：
 *   T1 /version 正常路径（eOk + 版本串契约 + 忽略 pkg）；
 *   T2 端点 2-8 解包双守卫（nullptr / 异类包 / 改名伪造触发 dynamic_pointer_cast 失败）；
 *   T3 端点 3-8 节点不存在早退（合法包但 nodeName 不存在 → eNo，不触达 DDS）；
 *   + funcInfos 8 端点内省、未知端点 eNo。
 * DDS-free 保证：解包守卫在触达节点查找/DDS 前返回；节点不存在在 map 查找处返回；
 *   /version 不碰 DDS；本测试绝不向 /create_node 传合法 DDSNode（那会创建真实 participant，归 MC2）。
 *
 * 风格：纯 main() + CHECK 宏 + 失败计数（零第三方依赖），返回非 0 表示存在失败用例。
 */

#include "TestCheck.h"
#include "YomkRpcService.h" // 引入 YomkAPI.h 宏、DDS 结构体与 YomkMsg 声明、String 内建类型

namespace
{
    // T1：/version 契约（不解包，忽略 pkg）
    void testVersion(YomkRpcService *svc)
    {
        auto resp = svc->invoke("/version");
        CHECK(resp.m_status == YomkResponse::eOk, "/version 返回 eOk");
        CHECK(resp.m_msg == "ok", "/version m_msg == ok");

        YomkUnPackPkg(resp.m_data, String, ver);
        CHECK(ver != nullptr, "/version m_data 可解包为 String");
        if (ver != nullptr)
        {
            CHECK(ver->d.rfind("YomkRpc v", 0) == 0, "/version 版本串前缀为 YomkRpc v");
            CHECK(ver->d.find("(WIP)") != std::string::npos, "/version 版本串含 (WIP)");
        }

        auto respNull = svc->invoke("/version", nullptr);
        CHECK(respNull.m_status == YomkResponse::eOk, "/version 传 nullptr 仍 eOk（忽略 pkg）");
    }

    // T2：7 个解包端点的双守卫（nullptr / 异类包 / 改名伪造）
    void testUnpackGuards(YomkRpcService *svc)
    {
        // ep=端点；expectName=该端点期望的消息类型名；wrongPkg=调用点 freshly 构造的异类包
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

        checkGuards("/create_node", "DDSNode", YomkMkPtr(String, "wrong"));
        checkGuards("/delete_node", "String", YomkMkPtr(DDSNode, DDSNode{0, "wrong"}));
        checkGuards("/register_pub_topic", "DDSTopic", YomkMkPtr(String, "wrong"));
        checkGuards("/register_sub_topic", "DDSSubRequest", YomkMkPtr(String, "wrong"));
        checkGuards("/publish", "DDSPublish", YomkMkPtr(String, "wrong"));
        checkGuards("/loan", "DDSLoan", YomkMkPtr(String, "wrong"));
        checkGuards("/discard_loan", "DDSLoan", YomkMkPtr(String, "wrong"));
    }

    // T3：6 个查节点端点的"节点不存在"早退（合法包 + 不存在 nodeName → eNo，不触达 DDS）
    void testNodeNotFound(YomkRpcService *svc)
    {
        const std::string ghost = "ghost_node_mc1";
        auto check = [&](const char *ep, YomkPkgPtr pkg)
        {
            auto r = svc->invoke(ep, pkg);
            CHECK(r.m_status == YomkResponse::eNo &&
                      r.m_msg.find("not exists") != std::string::npos,
                  std::string(ep) + " : 节点不存在早退 eNo（不触达 DDS）");
        };

        check("/delete_node", YomkMkPtr(String, ghost));
        check("/register_pub_topic", YomkMkPtr(DDSTopic, DDSTopic{ghost, "t", nullptr}));
        check("/register_sub_topic", YomkMkPtr(DDSSubRequest, DDSSubRequest{ghost, "t", nullptr, nullptr}));
        check("/publish", YomkMkPtr(DDSPublish, DDSPublish{ghost, "t", nullptr}));
        check("/loan", YomkMkPtr(DDSLoan, DDSLoan{ghost, "t", nullptr}));
        check("/discard_loan", YomkMkPtr(DDSLoan, DDSLoan{ghost, "t", nullptr}));
    }
} // namespace

int main()
{
    YOMK_INIT();

    // 注册被测服务：所有权移交框架（shared_ptr 持有），init() 内部调用后 weak_from_this() 方有效
    auto *svc = new YomkRpcService(YOMK_SERVER_P);
    CHECK(YOMK_ADD_SERVICE(svc) == 0, "YomkRpcService 注册成功（所有权移交框架，init() 已内部调用）");

    // 内省：8 个 RPC 端点均已注册
    auto infos = svc->funcInfos();
    CHECK(infos.size() == 8 && infos.count("/version") && infos.count("/create_node") && infos.count("/delete_node") && infos.count("/register_pub_topic") && infos.count("/register_sub_topic") && infos.count("/publish") && infos.count("/loan") && infos.count("/discard_loan"),
          "funcInfos 内省 8 端点齐全");

    testVersion(svc);
    testUnpackGuards(svc);
    testNodeNotFound(svc);

    CHECK(svc->invoke("/no_such_endpoint").m_status == YomkResponse::eNo, "未知端点返回 eNo");

    return testReport("TestYomkRpcServiceContract");
}
