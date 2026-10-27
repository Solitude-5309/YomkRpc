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
 *   MC8 覆盖率补测：register_sub type==nullptr 守卫（对称 pub 侧）、空回调 sub + 数据到达
 *       （on_data_available L52 的 callback_==false 防御性跳过分支，MC3 原空回调 sub 从未收数据）；
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
#include "YomkRpcService.h" // 服务 + DDSQosProfile + DDSTopic/DDSSubRequest/DDSPublish + String + YOMK_* 宏
#include "YomkRpcDebugService.h" // YomkRpcDebugService + DDSDebugNode/DDSDebugInfo（QoS-B4 读回链路）

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
        return YomkMkPtr(DDSNode, DDSNode{nodeName, domainId});
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

        // MC8 覆盖率补测：register_sub type==nullptr 守卫（对称上方 register_pub type=null），触达
        // FastDDSNode::registerSubTopic L201 复合守卫的 type==nullptr 分支（MC3 原仅覆盖 pub 侧 type=null）。
        CHECK(svc->invoke("/register_sub_topic",
                          YomkMkPtr(DDSSubRequest, DDSSubRequest{"topic_node", "s_null", nullptr, nullptr}))
                      .m_status == YomkResponse::eNo,
              "MC8 register_sub_topic type=nullptr → eNo（type==null 守卫，delete nullptr 安全）");

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

    // MC8 覆盖率补测：空回调订阅 + 数据到达 → 触达 FastDDSNode::on_data_available L52 复合条件的
    // callback_==false 分支（防御性跳过空调用，避免 bad_function_call）。MC3 原 e2e 均带有效回调，
    // 空回调 sub（testTypeOwnershipOnFailure 的 s_dup）从未收到数据，故此分支恒 taken 0。
    // 本地回环：同 node 内 pub+sub 同 topic，发布数据触发 sub 的 on_data_available，callback_ 空则跳过。
    void testEmptyCallbackSub(YomkRpcService *svc)
    {
        CHECK(svc->invoke("/create_node", mkNode(TEST_DOMAIN, "ecb_node")).m_status == YomkResponse::eOk,
              "创建 ecb_node → eOk");
        CHECK(svc->invoke("/register_pub_topic",
                          YomkMkPtr(DDSTopic, DDSTopic{"ecb_node", "ecb_topic", new YomkRpc::MInt32PubSubType()}))
                      .m_status == YomkResponse::eOk,
              "register_pub_topic ecb_topic(MInt32) → eOk");
        // 空回调订阅：callback=nullptr，注册应成功（SubListener 持有空 std::function，收数据时防御性跳过）
        CHECK(svc->invoke("/register_sub_topic",
                          YomkMkPtr(DDSSubRequest, DDSSubRequest{"ecb_node", "ecb_topic", new YomkRpc::MInt32PubSubType(), nullptr}))
                      .m_status == YomkResponse::eOk,
              "MC8 register_sub_topic ecb_topic(空callback) → eOk");

        std::this_thread::sleep_for(std::chrono::seconds(1)); // 等待 discovery

        // 发布数据 → sub 的 on_data_available 触发：valid_data && ALIVE 真，但 callback_ 空 → 防御性跳过（不崩溃）
        constexpr int N = 5;
        int pubOk = 0;
        for (int i = 0; i < N; ++i)
        {
            YomkRpc::MInt32 msg;
            msg.data(i);
            if (svc->invoke("/publish", YomkMkPtr(DDSPublish, DDSPublish{"ecb_node", "ecb_topic", &msg}))
                    .m_status == YomkResponse::eOk)
            {
                pubOk++;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
        CHECK(pubOk == N, "MC8 publish ecb_topic 5 条 → 全部 eOk（数据到达空回调 sub）");

        std::this_thread::sleep_for(std::chrono::seconds(1)); // 等待投递触发 on_data_available（callback_ 空跳过）

        CHECK(svc->invoke("/delete_node", YomkMkPtr(String, "ecb_node")).m_status == YomkResponse::eOk,
              "删除 ecb_node → eOk（空回调路径无崩溃/UAF，析构 reader 停止回调）");
    }

    // ===== 闭环 1：端点 QoS 通道（DDSQosProfile → 翻译 → create_datawriter/reader） =====
    // B1：ros2 底座工厂组合收发——writer RELIABLE offered /
    //     reader BEST_EFFORT requested 可匹配，QoS 组合下端到端收发成功；
    // B2：reliability 生效反证——writer 显式 BEST_EFFORT + reader 显式 RELIABLE
    //     （requested > offered，DDS 不可匹配保证）→ 收 0 条；若翻译失效（QoS 未落
    //     端点），reader 底座 BEST_EFFORT 将匹配收到 → CHECK 失败侦察；
    // B4：debug 链路 QoS 直证——B2 的 writer（BEST_EFFORT+VOLATILE，与 writer 底座
    //     RELIABLE+TRANSIENT_LOCAL 双双不同）经调试节点 topic info -v 从发现数据
    //     读回断言两行 QoS，直证 profile 值落到端点；
    // 全哨兵缺省兼容：既有 T8/e2e 等用例的三参/四参聚合初始化（不带尾参）即全哨兵路径。
    void testQosProfile(YomkRpcService *svc, YomkRpcDebugService *svcDebug)
    {
        // B1：ros2 底座组合收发（invoke+四参/五参聚合尾参，与宏展开等价同链路）
        CHECK(svc->invoke("/create_node", mkNode(TEST_DOMAIN, "qos1_node")).m_status == YomkResponse::eOk,
              "QoS 创建 qos1_node → eOk");
        CHECK(svc->invoke("/register_pub_topic",
                          YomkMkPtr(DDSTopic,
                              DDSTopic{"qos1_node", "q1_topic", new YomkRpc::MStringPubSubType(), DDSQosProfile::ros2DefaultPublisher()}))
                      .m_status == YomkResponse::eOk,
              "QoS-B1 register_pub_topic 尾参 ros2DefaultPublisher → eOk（四参聚合尾参落位）");
        std::atomic<int> recv1{0};
        auto cb1 = [&recv1](const void *)
        {
            recv1++;
        };
        CHECK(svc->invoke("/register_sub_topic",
                          YomkMkPtr(DDSSubRequest,
                              DDSSubRequest{"qos1_node", "q1_topic", new YomkRpc::MStringPubSubType(), cb1,
                                  DDSQosProfile::ros2DefaultSubscriber()}))
                      .m_status == YomkResponse::eOk,
              "QoS-B1 register_sub_topic 尾参 ros2DefaultSubscriber → eOk（五参聚合尾参落位）");
        std::this_thread::sleep_for(std::chrono::seconds(1)); // 等待 discovery
        int pubOk1 = 0;
        for (int i = 0; i < 5; ++i)
        {
            YomkRpc::MString msg;
            msg.data("q1-" + std::to_string(i));
            if (svc->invoke("/publish", YomkMkPtr(DDSPublish, DDSPublish{"qos1_node", "q1_topic", &msg}))
                    .m_status == YomkResponse::eOk)
            {
                pubOk1++;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
        CHECK(pubOk1 == 5, "QoS-B1 publish 5 条 → 全部 eOk");
        std::this_thread::sleep_for(std::chrono::seconds(1)); // 等待投递
        CHECK(recv1.load() > 0, "QoS-B1 RELIABLE writer + BEST_EFFORT reader 匹配收到数据");
        CHECK(svc->invoke("/delete_node", YomkMkPtr(String, "qos1_node")).m_status == YomkResponse::eOk,
              "删除 qos1_node → eOk");

        // B2：BEST_EFFORT writer + RELIABLE reader（requested > offered 不匹配反证）
        CHECK(svc->invoke("/create_node", mkNode(TEST_DOMAIN, "qos2_node")).m_status == YomkResponse::eOk,
              "QoS 创建 qos2_node → eOk");
        DDSQosProfile be;
        be.reliability = DDSReliability::BEST_EFFORT;
        be.durability = DDSDurability::VOLATILE; // writer 底座 TRANSIENT_LOCAL，显式 VOLATILE 供 B4 直证
        CHECK(svc->invoke("/register_pub_topic",
                          YomkMkPtr(DDSTopic, DDSTopic{"qos2_node", "t_be", new YomkRpc::MStringPubSubType(), be}))
                      .m_status == YomkResponse::eOk,
              "QoS-B2 register_pub_topic t_be 显式 BEST_EFFORT+VOLATILE → eOk");
        DDSQosProfile rel;
        rel.reliability = DDSReliability::RELIABLE;
        std::atomic<int> recv2{0};
        auto cb2 = [&recv2](const void *)
        {
            recv2++;
        };
        CHECK(svc->invoke("/register_sub_topic",
                          YomkMkPtr(
                              DDSSubRequest, DDSSubRequest{"qos2_node", "t_be", new YomkRpc::MStringPubSubType(), cb2, rel}))
                      .m_status == YomkResponse::eOk,
              "QoS-B2 register_sub_topic t_be 显式 RELIABLE → eOk");
        std::this_thread::sleep_for(std::chrono::seconds(2)); // 双方 discovery + 匹配判定稳定
        int pubOk2 = 0;
        for (int i = 0; i < 5; ++i)
        {
            YomkRpc::MString msg;
            msg.data("be-" + std::to_string(i));
            if (svc->invoke("/publish", YomkMkPtr(DDSPublish, DDSPublish{"qos2_node", "t_be", &msg}))
                    .m_status == YomkResponse::eOk)
            {
                pubOk2++;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
        CHECK(pubOk2 == 5, "QoS-B2 publish 5 条 → 全部 eOk（write 不依赖匹配成功）");
        std::this_thread::sleep_for(std::chrono::seconds(1)); // 等待潜在投递
        CHECK(recv2.load() == 0,
              "QoS-B2 RELIABLE requested > BEST_EFFORT offered → 收 0 条（若翻译失效将匹配收到 → 失败侦察）");

        // B4：debug 链路 QoS 直证（发现数据读回 writer 端点实际 QoS）
        CHECK(svcDebug->invoke("/create_node", YomkMkPtr(DDSDebugNode, DDSDebugNode{TEST_DOMAIN})).m_status == YomkResponse::eOk,
              "QoS-B4 创建调试节点（域 200）→ eOk");
        std::this_thread::sleep_for(std::chrono::seconds(1)); // 调试参与者发现已在线端点
        auto hitV = svcDebug->invoke("/topic_info", YomkMkPtr(DDSDebugInfo, DDSDebugInfo{"t_be", 10, 200, true}));
        CHECK(hitV.m_status == YomkResponse::eOk && hitV.m_data != nullptr,
              "QoS-B4 topic info -v t_be → eOk（发现缓存命中）");
        YomkUnPackPkg(hitV.m_data, StringArray, infoArrV);
        CHECK(infoArrV != nullptr, "QoS-B4 verbose 返回包可解包为 StringArray");
        bool hasBe = false;
        bool hasVol = false;
        if (infoArrV != nullptr)
        {
            for (const auto &line : infoArrV->d)
            {
                if (line == "  Reliability: BEST_EFFORT")
                {
                    hasBe = true;
                }
                if (line == "  Durability: VOLATILE")
                {
                    hasVol = true;
                }
            }
        }
        CHECK(hasBe, "QoS-B4 writer QoS 读回 Reliability: BEST_EFFORT（底座 RELIABLE → profile 生效直证）");
        CHECK(hasVol, "QoS-B4 writer QoS 读回 Durability: VOLATILE（底座 TRANSIENT_LOCAL → profile 生效直证）");
        CHECK(svcDebug->invoke("/delete_node", nullptr).m_status == YomkResponse::eOk,
              "删除调试节点 → eOk");
        CHECK(svc->invoke("/delete_node", YomkMkPtr(String, "qos2_node")).m_status == YomkResponse::eOk,
              "删除 qos2_node → eOk");
    }

} // namespace

int main()
{
    YOMK_INIT();

    // 注册被测服务：所有权移交框架（shared_ptr 持有），init() 内部调用后 weak_from_this() 方有效
    auto *svc = new YomkRpcService(YOMK_SERVER_P);
    CHECK(YOMK_ADD_SERVICE(svc) == 0, "YomkRpcService 注册成功（所有权移交框架，init() 已内部调用）");
    // 调试服务（QoS-B4 从发现数据读回端点实际 QoS 用）
    auto *svcDebug = new YomkRpcDebugService(YOMK_SERVER_P);
    CHECK(YOMK_ADD_SERVICE(svcDebug) == 0, "YomkRpcDebugService 注册成功");

    testTopicRegistration(svc);
    testEndToEndPubSub(svc);
    testPublishGuards(svc);
    testTypeOwnershipOnFailure(svc);
    testEmptyCallbackSub(svc);
    testQosProfile(svc, svcDebug);

    return testReport("TestYomkRpcTopic");
}
