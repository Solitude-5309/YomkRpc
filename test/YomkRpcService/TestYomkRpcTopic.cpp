/**
 * @file TestYomkRpcTopic.cpp
 * @brief MC3：主题注册 + 端到端发布订阅（真实 DDS + YomkRpcMsg 真实类型指针）
 *
 * 范围：MC0-MC2 覆盖服务层契约（DDS-free）与节点生命周期，但主题注册与收发链路完全未测。
 *       本用例经 svc->invoke() 白盒验证 registerPubTopic/registerSubTopic/getOrCreateTopic/
 *       publish/on_data_available 的真实 DDS 行为，并首次功能触达 F1（SubResGuard）所在的
 *       registerSubTopic happy-path。测试树自此首次引入 YomkRpcMsg 真实类型依赖。
 * 覆盖：
 *   T8  register_pub_topic / register_sub_topic 注册成功；
 *   T9  getOrCreateTopic 类型名不符（t_mm 已注册 MString，再用 MInt32 订阅）→ 返回 nullptr → eNo；
 *   T10 register 守卫：重复注册同名 pub 主题（pubTopics_.count>0）、type==nullptr；
 *   T11 publish → on_data_available → callback 本地回环（同 participant 内 pub+sub 同主题）；
 *   +   publish 守卫：主题未注册（pubTopics_.find 失败）、data==nullptr；
 *   P1  type 所有权回归：register_pub/sub_topic 失败路径（节点不存在/重复注册）无条件接管并
 *       释放 caller 的 type（修复前 T10 重复注册泄漏 MStringPubSubType，asan 退出码 1）。
 *
 * 关键不变式：每个 eOk 创建的节点必须在 main 返回前经 /delete_node 显式删除（析构 reader 停止
 *   回调，测试局部捕获状态全程存活，无悬垂）；以规避 YOMK 框架 atexit 服务析构晚于 FastDDS
 *   DomainParticipantFactory 单例销毁的静态析构顺序问题（参见 examples/ExampleYomkRpcTopic.cpp）。
 *
 * 风格：纯 main() + CHECK 宏 + 失败计数（零第三方依赖），返回非 0 表示存在失败用例。
 */

#include "TestCheck.h"
#include "YomkRpcService.h" // 服务 + DDSTopic/DDSSubRequest/DDSPublish + String + YOMK_* 宏

#include <YomkRpcMsg/YomkRpcMsg.hpp>            // YomkRpc::MString / MInt32 数据类
#include <YomkRpcMsg/YomkRpcMsgPubSubTypes.hpp> // MStringPubSubType / MInt32PubSubType

#include <atomic>
#include <chrono>
#include <cstdint>
#include <iostream>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

namespace
{
    // 有效域（FastDDS 约 0-232），与 MC2 一致，避开默认域 0 的潜在网络干扰
    constexpr uint32_t TEST_DOMAIN = 200;

    // 构造 /create_node 请求包
    YomkPkgPtr mkNode(uint32_t domainId, const std::string &nodeName)
    {
        return YomkMkPtr(DDSNode, DDSNode{domainId, nodeName});
    }

    // T8/T10/T9：注册成功 + 守卫（重复注册/type==null）+ 类型名不符回滚
    void testTopicRegistration(YomkRpcService *svc)
    {
        CHECK(svc->invoke("/create_node", mkNode(TEST_DOMAIN, "topic_node")).m_status == YomkResponse::eOk,
              "创建 topic_node(domain 200) → eOk");

        CHECK(svc->invoke("/register_pub_topic",
                          YomkMkPtr(DDSTopic, DDSTopic{"topic_node", "t_str", new YomkRpc::MStringPubSubType()}))
                      .m_status == YomkResponse::eOk,
              "T8 register_pub_topic t_str(MString) → eOk");

        CHECK(svc->invoke("/register_pub_topic",
                          YomkMkPtr(DDSTopic, DDSTopic{"topic_node", "t_str", new YomkRpc::MStringPubSubType()}))
                      .m_status == YomkResponse::eNo,
              "T10 重复 register_pub_topic t_str → eNo（pubTopics_.count>0 守卫）");

        CHECK(svc->invoke("/register_pub_topic",
                          YomkMkPtr(DDSTopic, DDSTopic{"topic_node", "t_null", nullptr}))
                      .m_status == YomkResponse::eNo,
              "T10 register_pub_topic type=nullptr → eNo（type==null 守卫）");

        CHECK(svc->invoke("/register_pub_topic",
                          YomkMkPtr(DDSTopic, DDSTopic{"topic_node", "t_mm", new YomkRpc::MStringPubSubType()}))
                      .m_status == YomkResponse::eOk,
              "register_pub_topic t_mm(MString) → eOk");

        CHECK(svc->invoke("/register_sub_topic",
                          YomkMkPtr(DDSSubRequest, DDSSubRequest{"topic_node", "t_mm", new YomkRpc::MInt32PubSubType(), nullptr}))
                      .m_status == YomkResponse::eNo,
              "T9 register_sub_topic t_mm(MInt32) → eNo（getOrCreateTopic 类型名 MString≠MInt32 返回 nullptr）");

        CHECK(svc->invoke("/delete_node", YomkMkPtr(String, "topic_node")).m_status == YomkResponse::eOk,
              "删除 topic_node → eOk");
    }

    // T11：publish → on_data_available → callback 本地回环（同 participant 内 pub+sub 同主题）
    void testEndToEndPubSub(YomkRpcService *svc)
    {
        CHECK(svc->invoke("/create_node", mkNode(TEST_DOMAIN, "e2e_node")).m_status == YomkResponse::eOk,
              "创建 e2e_node → eOk");

        CHECK(svc->invoke("/register_pub_topic",
                          YomkMkPtr(DDSTopic, DDSTopic{"e2e_node", "e2e_topic", new YomkRpc::MStringPubSubType()}))
                      .m_status == YomkResponse::eOk,
              "register_pub_topic e2e_topic(MString) → eOk");

        std::atomic<int> received{0};
        std::mutex mtx;
        std::vector<std::string> got;
        auto cb = [&](const void *data)
        {
            auto *m = static_cast<const YomkRpc::MString *>(data);
            std::lock_guard<std::mutex> lk(mtx);
            got.push_back(m->data());
            received++;
        };

        CHECK(svc->invoke("/register_sub_topic",
                          YomkMkPtr(DDSSubRequest, DDSSubRequest{"e2e_node", "e2e_topic", new YomkRpc::MStringPubSubType(), cb}))
                      .m_status == YomkResponse::eOk,
              "register_sub_topic e2e_topic(callback) → eOk（首次功能触达 F1 registerSubTopic happy-path）");

        std::this_thread::sleep_for(std::chrono::seconds(1)); // 等待 discovery

        constexpr int N = 5;
        int pubOk = 0;
        for (int i = 0; i < N; ++i)
        {
            YomkRpc::MString msg;
            msg.data("e2e-" + std::to_string(i));
            if (svc->invoke("/publish", YomkMkPtr(DDSPublish, DDSPublish{"e2e_node", "e2e_topic", &msg}))
                    .m_status == YomkResponse::eOk)
            {
                pubOk++;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
        CHECK(pubOk == N, "publish 5 条 → 全部 eOk");

        std::this_thread::sleep_for(std::chrono::seconds(1)); // 等待投递

        int r = received.load();
        CHECK(r > 0 && r <= N, "回调收到消息数在 (0,5]（首条可能因 discovery 时序丢失）");
        std::string lastGot;
        {
            std::lock_guard<std::mutex> lk(mtx);
            if (!got.empty())
            {
                lastGot = got.back();
            }
        }
        std::cout << "[OBSERVE] e2e received=" << r << "/" << N << " last=\"" << lastGot << "\"" << std::endl;

        CHECK(svc->invoke("/delete_node", YomkMkPtr(String, "e2e_node")).m_status == YomkResponse::eOk,
              "删除 e2e_node → eOk（析构 reader 停止回调，捕获状态仍存活）");
    }

    // publish 守卫（节点存在但主题未注册 / 数据非法）
    void testPublishGuards(YomkRpcService *svc)
    {
        CHECK(svc->invoke("/create_node", mkNode(TEST_DOMAIN, "pg_node")).m_status == YomkResponse::eOk,
              "创建 pg_node → eOk");

        YomkRpc::MString msg;
        msg.data("x");
        CHECK(svc->invoke("/publish", YomkMkPtr(DDSPublish, DDSPublish{"pg_node", "no_topic", &msg}))
                      .m_status == YomkResponse::eNo,
              "publish 未注册主题 → eNo（pubTopics_.find 失败）");

        CHECK(svc->invoke("/register_pub_topic",
                          YomkMkPtr(DDSTopic, DDSTopic{"pg_node", "p_topic", new YomkRpc::MStringPubSubType()}))
                      .m_status == YomkResponse::eOk,
              "register_pub_topic p_topic → eOk");

        CHECK(svc->invoke("/publish", YomkMkPtr(DDSPublish, DDSPublish{"pg_node", "p_topic", nullptr}))
                      .m_status == YomkResponse::eNo,
              "publish data=nullptr → eNo（data==null 守卫）");

        CHECK(svc->invoke("/delete_node", YomkMkPtr(String, "pg_node")).m_status == YomkResponse::eOk,
              "删除 pg_node → eOk");
    }

    // P1 回归：register_pub/sub_topic 无条件接管 caller 的 type，失败路径亦须释放（否则泄漏）。
    // 覆盖修复的 4 个失败点中未被上方用例触达的 3 个：服务层节点不存在（pub/sub）、节点层
    // 重复注册 sub 守卫；节点层重复注册 pub 守卫已由 testTopicRegistration 的 T10 覆盖。
    // 断言行为 eNo；泄漏由 asan 兜底（修复前重复注册泄漏 MStringPubSubType）。
    void testTypeOwnershipOnFailure(YomkRpcService *svc)
    {
        // 服务层：节点不存在（未委托到 FastDDSNode，type 须由服务层释放）
        CHECK(svc->invoke("/register_pub_topic",
                          YomkMkPtr(DDSTopic, DDSTopic{"no_such_node", "t", new YomkRpc::MStringPubSubType()}))
                      .m_status == YomkResponse::eNo,
              "P1 register_pub_topic 节点不存在 → eNo（type 由服务层释放，不泄漏）");
        CHECK(svc->invoke("/register_sub_topic",
                          YomkMkPtr(DDSSubRequest, DDSSubRequest{"no_such_node", "t", new YomkRpc::MStringPubSubType(), nullptr}))
                      .m_status == YomkResponse::eNo,
              "P1 register_sub_topic 节点不存在 → eNo（type 由服务层释放，不泄漏）");

        // 节点层：重复注册 sub 守卫（委托到 FastDDSNode，type 须由节点层释放）
        CHECK(svc->invoke("/create_node", mkNode(TEST_DOMAIN, "own_node")).m_status == YomkResponse::eOk,
              "创建 own_node → eOk");
        CHECK(svc->invoke("/register_sub_topic",
                          YomkMkPtr(DDSSubRequest, DDSSubRequest{"own_node", "s_dup", new YomkRpc::MStringPubSubType(), nullptr}))
                      .m_status == YomkResponse::eOk,
              "register_sub_topic s_dup → eOk（首次注册成功）");
        CHECK(svc->invoke("/register_sub_topic",
                          YomkMkPtr(DDSSubRequest, DDSSubRequest{"own_node", "s_dup", new YomkRpc::MStringPubSubType(), nullptr}))
                      .m_status == YomkResponse::eNo,
              "P1 重复 register_sub_topic s_dup → eNo（subTopics_.count>0 守卫，type 由节点层释放，不泄漏）");
        CHECK(svc->invoke("/delete_node", YomkMkPtr(String, "own_node")).m_status == YomkResponse::eOk,
              "删除 own_node → eOk");
    }
} // namespace

int main()
{
    YOMK_INIT();

    // 注册被测服务：所有权移交框架（shared_ptr 持有），init() 内部调用后 weak_from_this() 方有效
    auto *svc = new YomkRpcService(YOMK_SERVER_P);
    CHECK(YOMK_ADD_SERVICE(svc) == 0, "YomkRpcService 注册成功（所有权移交框架，init() 已内部调用）");

    testTopicRegistration(svc);
    testEndToEndPubSub(svc);
    testPublishGuards(svc);
    testTypeOwnershipOnFailure(svc);

    return testReport("TestYomkRpcTopic");
}
