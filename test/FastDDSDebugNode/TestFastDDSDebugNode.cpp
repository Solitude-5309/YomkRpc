/**
 * @file TestFastDDSDebugNode.cpp
 * @brief FastDDSDebugNode：类型无关调试订阅节点（发现→动态类型→订阅→JSON 结构化输出）
 *
 * 范围：验证 FastDDSDebugNode 的公开契约与端到端链路。被测对象全程不依赖任何 IDL 生成
 *       类型——发布端经 FastDDSNode + YomkRpcMsg 真实类型（MString）制造被观察流量，
 *       FastDDSDebugNode 仅凭主题名经 DDS 发现机制取回远端 TypeInformation/TypeObject，
 *       生成 DynamicType 自动建订阅，收到消息后经内置 json_serialize 结构化输出。
 * 覆盖：
 *   守卫  未 setDomainId 时 subscribeTopic/listTopics → false；
 *         setDomainId 重复调用 → 第二次 false；
 *         subscribeTopic 重复登记同一主题 → 第二次 false；
 *         入域后立即 listTopics → true 且空（无 writer，空列表正常）；
 *   端到端 发布端持续 publish（MString "hello_debug"）→ 被测端捕获输出包含
 *         "topic=t_debug"（发现→建订阅）、"type=YomkRpc::MString"（远端类型解析成功）、
 *         "hello_debug"（反序列化 + JSON 结构化输出成功）；
 *         listTopics 含 (t_debug, YomkRpc::MString)（发现缓存同步查询）。
 *
 * 风格：纯 main() + CHECK 宏 + 失败计数（零第三方依赖），返回非 0 表示存在失败用例。
 *       不经 YomkRpcService/YOMK_INIT，直接 RAII 使用 FastDDSNode 与 FastDDSDebugNode
 *       （规避 YOMK atexit 静态析构问题，模式同 TestYomkRpcNodeLifecycle Part B）。
 */

#include "TestCheck.h"
#include "FastDDSDebugNode.h"
#include "FastDDSNode.h"

#include <YomkRpcMsg/YomkRpcMsg.hpp>            // YomkRpc::MString 数据类（仅发布端使用）
#include <YomkRpcMsg/YomkRpcMsgPubSubTypes.hpp> // MStringPubSubType（仅发布端使用）

#include <chrono>
#include <cstdint>
#include <iostream>
#include <mutex>
#include <string>
#include <thread>
#include <utility>
#include <vector>

namespace
{
    // 有效域（FastDDS 约 0-232），与 MC2/MC3 一致，避开默认域 0 的潜在网络干扰
    constexpr uint32_t TEST_DOMAIN = 200;
    constexpr const char *TEST_TOPIC = "t_debug";
    constexpr const char *TEST_PAYLOAD = "hello_debug";
} // namespace

int main()
{
    // ---- 守卫用例：未入域登记 / 重复入域 / 重复登记 ----
    {
        FastDDSDebugNode dbg;
        std::vector<std::pair<std::string, std::string>> listed;
        CHECK(!dbg.listTopics(listed), "未 setDomainId 时 listTopics → false（participant 未创建）");
        CHECK(!dbg.subscribeTopic(TEST_TOPIC), "未 setDomainId 时 subscribeTopic → false（participant 未创建）");
        CHECK(dbg.setDomainId(TEST_DOMAIN), "setDomainId(200) → true");
        CHECK(!dbg.setDomainId(TEST_DOMAIN), "重复 setDomainId(200) → false（仅可成功一次）");
        CHECK(dbg.subscribeTopic(TEST_TOPIC), "首次 subscribeTopic(t_debug) → true（登记待发现）");
        CHECK(!dbg.subscribeTopic(TEST_TOPIC), "重复 subscribeTopic(t_debug) → false（pending_ 去重）");
        // 本时刻域内无其他 participant（ctest 串行，e2e 的 pub 尚未创建），发现缓存必空；
        // 断言同步查询空列表路径正常（等待窗口由调用方负责的设计）
        listed.clear();
        CHECK(dbg.listTopics(listed) && listed.empty(),
              "入域后立即 listTopics → true 且空（无 writer，空列表正常）");
    } // dbg 析构：清理尚未命中远端 writer 的登记与已建资源

    // ---- 端到端用例：FastDDSNode 发布真实类型 → FastDDSDebugNode 自动发现订阅并结构化输出 ----
    // 捕获区声明在 dbg 作用域之外：sink 持有引用，须保证 dbg 析构（停回调）后仍存活可断言
    std::mutex mtx;
    std::string captured;
    {
        // 发布端：真实 YomkRpcMsg 类型制造被观察流量（RELIABLE 默认 QoS）
        FastDDSNode pub;
        CHECK(pub.setDomainId(TEST_DOMAIN), "发布端 setDomainId(200) → true");
        CHECK(pub.registerPubTopic(TEST_TOPIC, new YomkRpc::MStringPubSubType()),
              "发布端 registerPubTopic(t_debug, MString) → true");

        // 被测端：sink 注入（须在 subscribeTopic 之前调用，建订阅时按值捕获）
        FastDDSDebugNode dbg;
        dbg.setOutputSink([&mtx, &captured](const std::string &text)
            {
                std::lock_guard<std::mutex> lk(mtx);
                captured += text;
            });
        CHECK(dbg.setDomainId(TEST_DOMAIN), "被测端 setDomainId(200) → true");
        CHECK(dbg.subscribeTopic(TEST_TOPIC), "被测端 subscribeTopic(t_debug) → true");

        // 发布循环：每 100ms 一条（匹配建立前的样本会丢失），直至捕获到目标串或超时（约 5s）
        YomkRpc::MString msg;
        msg.data(TEST_PAYLOAD);
        int pubOk = 0;
        bool found = false;
        for (int i = 0; i < 50 && !found; ++i)
        {
            if (pub.publish(TEST_TOPIC, &msg))
            {
                ++pubOk;
            }
            {
                std::lock_guard<std::mutex> lk(mtx);
                found = captured.find(TEST_PAYLOAD) != std::string::npos;
            }
            if (!found)
            {
                std::this_thread::sleep_for(std::chrono::milliseconds(100));
            }
        }
        CHECK(pubOk > 0, "发布循环至少一次 publish 成功");
        CHECK(found, "捕获输出包含 hello_debug（发现→动态类型→订阅→反序列化→JSON 输出闭环）");

        // 输出头部结构化字段校验（快照后释放锁断言，避免在 CHECK 展开中持锁）
        std::string snap;
        {
            std::lock_guard<std::mutex> lk(mtx);
            snap = captured;
        }
        CHECK(snap.find("topic=t_debug") != std::string::npos, "输出头部含 topic=t_debug");
        CHECK(snap.find("type=YomkRpc::MString") != std::string::npos,
              "输出头部含 type=YomkRpc::MString（远端 TypeObject→DynamicType 解析成功）");
        std::cout << "[OBSERVE] captured " << snap.size() << " bytes" << std::endl;

        // listTopics：订阅建立的前提是发现事件已入 seen_ 缓存（EDP 重放覆盖 late joiner），
        // 此时同步查询必含 t_debug 与其类型名，且查询不影响既有订阅链路
        std::vector<std::pair<std::string, std::string>> listed;
        CHECK(dbg.listTopics(listed), "listTopics → true（入域状态）");
        bool listedTopic = false;
        for (const auto& entry : listed)
        {
            if (entry.first == TEST_TOPIC && entry.second == "YomkRpc::MString")
            {
                listedTopic = true;
            }
        }
        CHECK(listedTopic, "listTopics 含 (t_debug, YomkRpc::MString)（发现缓存同步查询）");
        std::cout << "[OBSERVE] listed topics=" << listed.size() << std::endl;
    }

    return testReport("TestFastDDSDebugNode");
}
