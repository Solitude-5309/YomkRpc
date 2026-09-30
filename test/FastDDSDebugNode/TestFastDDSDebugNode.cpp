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
 *         入域后 listTopics(listed,1,50) → true 且空（stableRounds=1 单次快照，无 writer 空列表正常）；
 *         入域无 writer listTopics(listed,2,100) → true 且空（快照立即稳定收敛）；
 *   端到端 发布端持续 publish（MString "hello_debug"）→ 被测端捕获输出包含
 *         "topic=t_debug"（发现→建订阅）、"type=YomkRpc::MString"（远端类型解析成功）、
 *         "hello_debug"（反序列化 + JSON 结构化输出成功）；
 *         listTopics 含 (t_debug, YomkRpc::MString)（发现缓存同步查询）；
*         listTopics(2,100ms) 收敛快照与 listTopics(1) 一致（含 t_debug 与类型名）。
 *   仅订阅者 纯订阅端仅建 DataReader（t_reader_only，无任何 writer）→ listTopics 同样
 *         列出该主题与类型名（远端 DataReader 发现缓存，writer 优先 reader 补缺）。
 *   计数    同主题 2 个 DataWriter + 1 个 DataReader → topicInfo 独立收敛查询返回
 *         typeName（原始 DDS 类型名，无转换）、publisherCount=2、subscriptionCount=1；
 *         未发现主题 topicInfo → false（单次快照快速路径）。
 *   参与者  命名 peer（FastDDSNode setDomainId 带名）+ 空名 peer + "/" 名 peer（ROS2 占位名）
 *         入域 → nodeList 独立收敛查询含命名 peer（SPDP participant_name 端到端传播）、不含
 *         调试节点自身（自身不在发现回调）、无空名/"/"名/GUID 串行（无效名参与者跳过）；
 *         未 setDomainId 时 nodeList → false。
 *   节点信息 命名 peer（发布+订阅端点）+ 匿名 peer（带端点）+ "/" 名 peer → nodeInfo 归属
 *         查询：publishers 含 pub 主题与类型、subscribers 含 sub 主题与类型，匿名 peer 端点
 *         不串入（GUID 前缀归属隔离）；未发现名 → false（单次快照快速路径）；"/" 名可查
 *         且清单为空（名称过滤不在节点层）；重复调用结果一致（收敛一致性）；
 *         未 setDomainId 时 nodeInfo → false。
 *   主题详情 verbose 同命名 peer 端点 → topicInfo verbose 模式（指针非空）填充端点详情：
 *         归属节点名（GUID 前缀归属）、FastDDS 原生 prefix|entity GUID、QoS 行集（核心组
 *         Reliability/Durability 等键行）；未发现主题 verbose 指针传入 → false 且列表为空。
 *
 * 风格：纯 main() + CHECK 宏 + 失败计数（零第三方依赖），返回非 0 表示存在失败用例。
 *       不经 YomkRpcService/YOMK_INIT，直接 RAII 使用 FastDDSNode 与 FastDDSDebugNode
 *       （规避 YOMK atexit 静态析构问题，模式同 TestYomkRpcNodeLifecycle Part B）。
 */

#include "TestCheck.h"
#include "FastDDSDebugNode.h"
#include "FastDDSNode.h"
#include "YomkRpcDebugService.h" // yomk::debugPubStop/debugPubReset（topicPub 期望建匹配中断用例）

#include <YomkRpcMsg/YomkRpcMsg.hpp>            // YomkRpc::MString 数据类（仅发布端使用）
#include <YomkRpcMsg/YomkRpcMsgPubSubTypes.hpp> // MStringPubSubType（仅发布端使用）

#include <fastdds/dds/core/policy/QosPolicies.hpp> // ReliabilityQosPolicy kind（topicPub 用例）
#include <fastdds/dds/domain/DomainParticipantFactory.hpp> // 纯订阅端 participant（仅订阅者用例）
#include <fastdds/dds/subscriber/DataReaderListener.hpp>  // topicPub 用例接收监听器
#include <fastdds/dds/subscriber/SampleInfo.hpp>
#include <fastdds/dds/subscriber/qos/DataReaderQos.hpp>

#include <algorithm>
#include <atomic>
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
    // node info 用例常量：命名 peer 名称与三个用例主题（发布/订阅/匿名 peer 发布主题）
    constexpr const char *NODEINFO_PEER_NAME = "test-nodeinfo-peer";
    constexpr const char *NODEINFO_PUB_TOPIC = "t_nodeinfo_pub";
    constexpr const char *NODEINFO_SUB_TOPIC = "t_nodeinfo_sub";
    constexpr const char *NODEINFO_ANON_TOPIC = "t_nodeinfo_anon";

    // topicPub 用例接收监听器：仅置位收到标志（送达实证），其余回调默认空实现
    class PubTestListener : public eprosima::fastdds::dds::DataReaderListener
    {
    public:
        void on_data_available(eprosima::fastdds::dds::DataReader *) override
        {
            received_.store(true);
        }
        std::atomic<bool> received_{false};
    };
} // namespace

int main()
{
    // ---- 守卫用例：未入域登记 / 重复入域 / 重复登记 ----
    {
        FastDDSDebugNode dbg;
        std::vector<std::pair<std::string, std::string>> listed;
        std::vector<std::string> nodes;
        CHECK(!dbg.listTopics(listed), "未 setDomainId 时 listTopics → false（participant 未创建）");
        CHECK(!dbg.nodeList(nodes), "未 setDomainId 时 nodeList → false（participant 未创建）");
        std::vector<std::string> ifaceGuard;
        CHECK(!dbg.interfaceShow("Any::Type", ifaceGuard),
              "未 setDomainId 时 interfaceShow → false（participant 未创建）");
        CHECK(!dbg.subscribeTopic(TEST_TOPIC), "未 setDomainId 时 subscribeTopic → false（participant 未创建）");
        CHECK(dbg.setDomainId(TEST_DOMAIN), "setDomainId(200) → true");
        CHECK(!dbg.setDomainId(TEST_DOMAIN), "重复 setDomainId(200) → false（仅可成功一次）");
        CHECK(dbg.subscribeTopic(TEST_TOPIC), "首次 subscribeTopic(t_debug) → true（登记待发现）");
        CHECK(!dbg.subscribeTopic(TEST_TOPIC), "重复 subscribeTopic(t_debug) → false（pending_ 去重）");
        // 本时刻域内无其他 participant（ctest 串行，e2e 的 pub 尚未创建），发现缓存必空；
        // 断言 stableRounds=1 单次快照路径正常（免等待的快速查询语义）
        listed.clear();
        CHECK(dbg.listTopics(listed, 1, 50) && listed.empty(),
              "入域后 listTopics(listed,1,50) → true 且空（单次快照，无 writer 空列表正常）");
        // 自适应收敛：入域无 writer 时快照立即稳定，收敛耗时为一次轮询间隔（百毫秒级）
        auto waitStart = std::chrono::steady_clock::now();
        std::vector<std::pair<std::string, std::string>> waited;
        CHECK(dbg.listTopics(waited, 2, 100) && waited.empty(),
              "入域无 writer listTopics(2,100ms) → true 且空（快照立即稳定）");
        auto waitMs = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now() - waitStart).count();
        std::cout << "[OBSERVE] listTopics stable in " << waitMs << " ms" << std::endl;
    } // dbg 析构：清理尚未命中远端 writer 的登记与已建资源

    // ---- 端到端用例：FastDDSNode 发布真实类型 → FastDDSDebugNode 自动发现订阅并结构化输出 ----
    // 捕获区声明在 dbg 作用域之外：sink 持有引用，须保证 dbg 析构（停回调）后仍存活可断言
    std::mutex mtx;
    std::string captured;
    {
        // 发布端：真实 YomkRpcMsg 类型制造被观察流量（RELIABLE 默认 QoS）
        FastDDSNode pub;
        CHECK(pub.setDomainId(TEST_DOMAIN, "test-pub"),
              "发布端 setDomainId(200, \"test-pub\") → true（test 前缀命名）");
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

        // 自适应收敛：订阅已建立说明发现已稳定，收敛查询应快速返回且与单次快照结果一致
        std::vector<std::pair<std::string, std::string>> waited;
        CHECK(dbg.listTopics(waited, 2, 100), "listTopics(2,100ms) → true（入域状态收敛查询）");
        bool waitedTopic = false;
        for (const auto& entry : waited)
        {
            if (entry.first == TEST_TOPIC && entry.second == "YomkRpc::MString")
            {
                waitedTopic = true;
            }
        }
        CHECK(waitedTopic, "listTopics(2,100ms) 含 (t_debug, YomkRpc::MString)（收敛快照一致）");
        std::cout << "[OBSERVE] waited topics=" << waited.size() << std::endl;
    }

    // ---- 仅有订阅者主题的发现用例：远端 DataReader（无任何 writer）也应被 listTopics 列出 ----
    {
        namespace dds = eprosima::fastdds::dds;  // 块内别名：直连 FastDDS API 搭建纯订阅端
        // 纯订阅端：手工建 participant + subscriber + DataReader（MString 类型），域内无 writer。
        // 被测端 dbg 只建 participant + subscriber（无本地 reader/writer），自身端点不进发现缓存
        auto* subParticipant = dds::DomainParticipantFactory::get_instance()->create_participant(
            TEST_DOMAIN, dds::PARTICIPANT_QOS_DEFAULT);
        CHECK(subParticipant != nullptr, "纯订阅端 participant 创建成功（仅有订阅者场景）");
        if (subParticipant != nullptr)
        {
            constexpr const char* READER_ONLY_TOPIC = "t_reader_only";
            auto* sub = subParticipant->create_subscriber(dds::SUBSCRIBER_QOS_DEFAULT);
            // TypeSupport 接管 PubSubType 所有权并完成 participant 内类型注册；
            // ts 须活过 reader 使用期（participant 类型表不接管所有权），块内声明于 dbg 之前
            dds::TypeSupport ts(new YomkRpc::MStringPubSubType());
            ts.register_type(subParticipant);
            auto* topic = subParticipant->create_topic(
                READER_ONLY_TOPIC, ts.get_type_name(), dds::TOPIC_QOS_DEFAULT);
            auto* reader = (sub != nullptr && topic != nullptr)
                ? sub->create_datareader(topic, dds::DATAREADER_QOS_DEFAULT) : nullptr;
            CHECK(reader != nullptr, "纯订阅端 DataReader 创建成功（t_reader_only，域内无 writer）");

            if (reader != nullptr)
            {
                FastDDSDebugNode dbg;
                CHECK(dbg.setDomainId(TEST_DOMAIN), "被测端 setDomainId(200) → true（仅订阅者场景）");
                // dbg 为 late joiner：入域时 EDP 全量重放纯订阅端 reader 发现信息
                std::vector<std::pair<std::string, std::string>> listed;
                CHECK(dbg.listTopics(listed, 5, 100), "listTopics(5,100ms) → true（仅订阅者场景收敛查询）");
                bool listedReaderOnly = false;
                std::string readerTypeName;
                for (const auto& entry : listed)
                {
                    if (entry.first == READER_ONLY_TOPIC)
                    {
                        listedReaderOnly = true;
                        readerTypeName = entry.second;
                    }
                }
                CHECK(listedReaderOnly, "listTopics 含仅有订阅者的主题 t_reader_only（reader 缓存并入快照）");
                CHECK(readerTypeName == "YomkRpc::MString", "仅有订阅者主题的类型名正确列出（YomkRpc::MString）");
                std::cout << "[OBSERVE] reader-only topic listed, type=" << readerTypeName << std::endl;
            } // dbg 析构：reader/topic/subscriber/participant 逆序清理，随后 ts 析构

            // 清理：reader → topic → subscriber → participant（顺序与 FastDDSDebugNode 析构一致）
            if (sub != nullptr && reader != nullptr)
            {
                sub->delete_datareader(reader);
            }
            if (topic != nullptr)
            {
                subParticipant->delete_topic(topic);
            }
            if (sub != nullptr)
            {
                subParticipant->delete_subscriber(sub);
            }
            dds::DomainParticipantFactory::get_instance()->delete_participant(subParticipant);
        }
    }

    // ---- 多端点计数用例：同主题 2 个 DataWriter + 1 个 DataReader → topicInfo 计数正确 ----
    {
        namespace dds = eprosima::fastdds::dds;  // 块内别名：直连 FastDDS API 搭建多端点场景
        constexpr const char* COUNT_TOPIC = "t_info_counts";
        auto* countParticipant = dds::DomainParticipantFactory::get_instance()->create_participant(
            TEST_DOMAIN, dds::PARTICIPANT_QOS_DEFAULT);
        CHECK(countParticipant != nullptr, "多端点场景 participant 创建成功（计数用例）");
        if (countParticipant != nullptr)
        {
            // TypeSupport 须活过 topic/writer/reader 使用期（声明顺序同"仅有订阅者"块）
            dds::TypeSupport ts(new YomkRpc::MStringPubSubType());
            ts.register_type(countParticipant);
            auto* pub = countParticipant->create_publisher(dds::PUBLISHER_QOS_DEFAULT);
            auto* sub = countParticipant->create_subscriber(dds::SUBSCRIBER_QOS_DEFAULT);
            auto* topic = countParticipant->create_topic(
                COUNT_TOPIC, ts.get_type_name(), dds::TOPIC_QOS_DEFAULT);
            auto* w1 = (pub != nullptr && topic != nullptr)
                ? pub->create_datawriter(topic, dds::DATAWRITER_QOS_DEFAULT) : nullptr;
            auto* w2 = (pub != nullptr && topic != nullptr)
                ? pub->create_datawriter(topic, dds::DATAWRITER_QOS_DEFAULT) : nullptr;
            auto* r1 = (sub != nullptr && topic != nullptr)
                ? sub->create_datareader(topic, dds::DATAREADER_QOS_DEFAULT) : nullptr;
            CHECK(w1 != nullptr && w2 != nullptr && r1 != nullptr,
                  "2 个 DataWriter + 1 个 DataReader 创建成功（同主题 t_info_counts）");

            if (w1 != nullptr && w2 != nullptr && r1 != nullptr)
            {
                FastDDSDebugNode dbg;
                CHECK(dbg.setDomainId(TEST_DOMAIN), "被测端 setDomainId(200) → true（计数场景）");
                // 先经 listTopics 收敛确认发现完成（late joiner 经 EDP 重放），再独立收敛查询：
                // topicInfo 不依赖 listTopics，此调用仅为本用例的时序前置
                std::vector<std::pair<std::string, std::string>> listed;
                CHECK(dbg.listTopics(listed, 5, 100), "listTopics(5,100ms) → true（计数场景收敛）");
                std::string typeName;
                size_t pubCount = 0;
                size_t subCount = 0;
                CHECK(dbg.topicInfo(COUNT_TOPIC, typeName, pubCount, subCount, 5, 100),
                      "topicInfo(t_info_counts,5,100ms) → true（独立收敛查询命中）");
                CHECK(typeName == "YomkRpc::MString",
                      "topicInfo 类型名为原始 DDS 类型名（YomkRpc::MString，无转换）");
                CHECK(pubCount == 2, "topicInfo publisherCount == 2（同主题两个远端 DataWriter）");
                CHECK(subCount == 1, "topicInfo subscriptionCount == 1（同主题一个远端 DataReader）");
                std::cout << "[OBSERVE] topicInfo pub=" << pubCount << " sub=" << subCount
                          << " type=" << typeName << std::endl;

                // 未发现主题：单次快照快速路径（stableRounds=1 免等待）→ false
                std::string missType;
                size_t missPub = 0;
                size_t missSub = 0;
                CHECK(!dbg.topicInfo("t_not_exist", missType, missPub, missSub, 1, 50),
                      "topicInfo(t_not_exist,1,50) → false（未发现主题，单次快照快速路径）");
            } // dbg 析构：reader/topic/subscriber/participant 逆序清理，随后 ts 析构

            // 清理：writer/reader → topic → publisher/subscriber → participant（同既有块模式）
            if (w1 != nullptr && pub != nullptr)
            {
                pub->delete_datawriter(w1);
            }
            if (w2 != nullptr && pub != nullptr)
            {
                pub->delete_datawriter(w2);
            }
            if (r1 != nullptr && sub != nullptr)
            {
                sub->delete_datareader(r1);
            }
            if (topic != nullptr)
            {
                countParticipant->delete_topic(topic);
            }
            if (pub != nullptr)
            {
                countParticipant->delete_publisher(pub);
            }
            if (sub != nullptr)
            {
                countParticipant->delete_subscriber(sub);
            }
            dds::DomainParticipantFactory::get_instance()->delete_participant(countParticipant);
        }
    }

    // ---- 参与者发现用例：命名 peer + 空名 peer 入域 → nodeList 收敛查询（空名跳过） ----
    {
        // peer 先于被测端入域（late joiner 经 PDP 全量重放既有 participant 发现信息）：
        // 命名 peer 复用改造后 setDomainId 带名路径，恰好端到端验证名称经 SPDP 传播
        FastDDSNode peer;
        CHECK(peer.setDomainId(TEST_DOMAIN, "test-peer-node"),
              "命名 peer setDomainId(200, \"test-peer-node\") → true（test 前缀命名）");
        namespace dds = eprosima::fastdds::dds;  // 块内别名：直连 FastDDS API 造匿名参与者
        // 匿名 peer：Fast DDS 默认参与者名为 "RTPSParticipant"（DomainParticipantQos 默认值
        // 非空），须显式 qos.name("") 才得真空名参与者，用于验证 nodeList 的空名跳过语义
        dds::DomainParticipantQos anonQos = dds::PARTICIPANT_QOS_DEFAULT;
        anonQos.name(std::string());
        auto* anonParticipant = dds::DomainParticipantFactory::get_instance()->create_participant(
            TEST_DOMAIN, anonQos);
        CHECK(anonParticipant != nullptr, "匿名 peer participant 创建成功（显式 qos.name(\"\")）");
        // "/" 名 peer：ROS2 rmw_fastrtps 把参与者名统一置为根 enclave "/"（rcl_init 兜底，
        // 节点名走另一通道），显式 qos.name("/") 复现该形态，验证 nodeList 的 "/" 过滤语义
        dds::DomainParticipantQos slashQos = dds::PARTICIPANT_QOS_DEFAULT;
        slashQos.name(std::string("/"));
        auto* slashParticipant = dds::DomainParticipantFactory::get_instance()->create_participant(
            TEST_DOMAIN, slashQos);
        CHECK(slashParticipant != nullptr, "slash peer participant 创建成功（显式 qos.name(\"/\")）");

        FastDDSDebugNode dbg;
        CHECK(dbg.setDomainId(TEST_DOMAIN), "被测端 setDomainId(200) → true（参与者发现场景）");
        std::vector<std::string> nodes;
        CHECK(dbg.nodeList(nodes, 5, 100), "nodeList(5,100ms) → true（参与者发现收敛查询）");
        bool hasNamed = false;
        bool noBadLine = true;  // 每行均非空、非 "/" 且不含 GUID 串特征 '|'（无效名参与者跳过的证据）
        for (const auto& name : nodes)
        {
            if (name == "test-peer-node")
            {
                hasNamed = true;
            }
            if (name.empty() || name == "/" || name.find('|') != std::string::npos)
            {
                noBadLine = false;
            }
        }
        CHECK(hasNamed, "nodeList 含 test-peer-node（SPDP participant_name 端到端传播）");
        CHECK(noBadLine, "nodeList 无空名/\"/\"名/GUID 串行（无效名参与者跳过，每行均为有效命名节点）");
        CHECK(std::find(nodes.begin(), nodes.end(), "yomkrpc-debug") == nodes.end(),
              "nodeList 不含调试节点自身（自身不在发现回调中）");
        std::cout << "[OBSERVE] nodeList " << nodes.size() << " nodes:" << std::endl;
        for (const auto& name : nodes)
        {
            std::cout << "[OBSERVE]   - " << name << std::endl;
        }
        if (anonParticipant != nullptr)
        {
            dds::DomainParticipantFactory::get_instance()->delete_participant(anonParticipant);
        }
        if (slashParticipant != nullptr)
        {
            dds::DomainParticipantFactory::get_instance()->delete_participant(slashParticipant);
        }
    }

    // ---- 节点信息用例：命名 peer（发布+订阅端点）+ 匿名 peer（带端点）+ "/" 名 peer → nodeInfo 归属查询 ----
    {
        namespace dds = eprosima::fastdds::dds;  // 块内别名：直连 FastDDS API 搭建匿名与 "/" 名 peer
        // 命名 peer（FastDDSNode）同时持一个发布端点与一个订阅端点：验证 nodeInfo 两份端点清单。
        // peer 先于被测端入域（late joiner 经 EDP/PDP 全量重放其参与者与端点发现信息）
        FastDDSNode peer;
        CHECK(peer.setDomainId(TEST_DOMAIN, NODEINFO_PEER_NAME),
              "命名 peer setDomainId(200, test-nodeinfo-peer) → true（test 前缀命名）");
        CHECK(peer.registerPubTopic(NODEINFO_PUB_TOPIC, new YomkRpc::MStringPubSubType()),
              "命名 peer registerPubTopic(t_nodeinfo_pub, MString) → true");
        CHECK(peer.registerSubTopic(NODEINFO_SUB_TOPIC, new YomkRpc::MStringPubSubType(),
                  [](const void *) {}),
              "命名 peer registerSubTopic(t_nodeinfo_sub, MString) → true（空回调，本用例无流量）");
        // 匿名 peer（显式 qos.name("")）带一个发布端点：验证其他参与者的端点不串入命名 peer 清单
        dds::DomainParticipantQos anonQos = dds::PARTICIPANT_QOS_DEFAULT;
        anonQos.name(std::string());
        auto* anonParticipant = dds::DomainParticipantFactory::get_instance()->create_participant(
            TEST_DOMAIN, anonQos);
        CHECK(anonParticipant != nullptr, "匿名 peer participant 创建成功（带端点，归属隔离用例）");
        // TypeSupport 须活过 writer 使用期（participant 类型表不接管所有权），块内声明于 dbg 之前
        dds::TypeSupport anonTs(new YomkRpc::MStringPubSubType());
        if (anonParticipant != nullptr)
        {
            anonTs.register_type(anonParticipant);
        }
        auto* anonTopic = (anonParticipant != nullptr)
            ? anonParticipant->create_topic(
                NODEINFO_ANON_TOPIC, anonTs.get_type_name(), dds::TOPIC_QOS_DEFAULT) : nullptr;
        auto* anonPub = (anonParticipant != nullptr)
            ? anonParticipant->create_publisher(dds::PUBLISHER_QOS_DEFAULT) : nullptr;
        auto* anonWriter = (anonPub != nullptr && anonTopic != nullptr)
            ? anonPub->create_datawriter(anonTopic, dds::DATAWRITER_QOS_DEFAULT) : nullptr;
        CHECK(anonWriter != nullptr, "匿名 peer DataWriter 创建成功（t_nodeinfo_anon）");
        // "/" 名 peer（ROS2 占位名形态，无端点）：验证 nodeInfo 的名称过滤不在节点层（"/" 可查）
        dds::DomainParticipantQos slashQos = dds::PARTICIPANT_QOS_DEFAULT;
        slashQos.name(std::string("/"));
        auto* slashParticipant = dds::DomainParticipantFactory::get_instance()->create_participant(
            TEST_DOMAIN, slashQos);
        CHECK(slashParticipant != nullptr, "slash peer participant 创建成功（显式 qos.name(\"/\")）");

        FastDDSDebugNode dbg;
        std::vector<std::pair<std::string, std::string>> pubs;
        std::vector<std::pair<std::string, std::string>> subs;
        CHECK(!dbg.nodeInfo(NODEINFO_PEER_NAME, pubs, subs),
              "未 setDomainId 时 nodeInfo → false（participant 未创建）");
        CHECK(dbg.setDomainId(TEST_DOMAIN), "被测端 setDomainId(200) → true（节点信息场景）");
        CHECK(dbg.nodeInfo(NODEINFO_PEER_NAME, pubs, subs, 5, 100),
              "nodeInfo(test-nodeinfo-peer,5,100ms) → true（peer 参与者与端点发现命中）");
        bool hasPub = false;
        bool hasSub = false;
        bool noAnon = true;  // 匿名 peer 的主题不得出现在命名 peer 的任一清单中
        for (const auto& entry : pubs)
        {
            if (entry.first == NODEINFO_PUB_TOPIC && entry.second == "YomkRpc::MString")
            {
                hasPub = true;
            }
            if (entry.first == NODEINFO_ANON_TOPIC)
            {
                noAnon = false;
            }
        }
        for (const auto& entry : subs)
        {
            if (entry.first == NODEINFO_SUB_TOPIC && entry.second == "YomkRpc::MString")
            {
                hasSub = true;
            }
            if (entry.first == NODEINFO_ANON_TOPIC)
            {
                noAnon = false;
            }
        }
        CHECK(hasPub, "nodeInfo publishers 含 (t_nodeinfo_pub, YomkRpc::MString)（writer GUID 前缀归属）");
        CHECK(hasSub, "nodeInfo subscribers 含 (t_nodeinfo_sub, YomkRpc::MString)（reader GUID 前缀归属）");
        CHECK(noAnon, "nodeInfo 不含匿名 peer 的 t_nodeinfo_anon（GUID 前缀归属隔离）");
        std::cout << "[OBSERVE] nodeInfo pubs=" << pubs.size()
                  << " subs=" << subs.size() << std::endl;

        // not-found：单次快照快速路径（stableRounds=1 免等待）→ false
        std::vector<std::pair<std::string, std::string>> missPubs;
        std::vector<std::pair<std::string, std::string>> missSubs;
        CHECK(!dbg.nodeInfo("t_no_such_node", missPubs, missSubs, 1, 50),
              "nodeInfo(t_no_such_node,1,50) → false（未发现该名参与者，单次快照快速路径）");

        // "/" 名可查：slash peer 已入域且无端点 → true 且两清单为空（名称过滤不在节点层）
        std::vector<std::pair<std::string, std::string>> slashPubs;
        std::vector<std::pair<std::string, std::string>> slashSubs;
        CHECK(dbg.nodeInfo("/", slashPubs, slashSubs, 5, 100),
              "nodeInfo(\"/\",5,100ms) → true（\"/\" 名参与者存在，可查）");
        CHECK(slashPubs.empty() && slashSubs.empty(),
              "nodeInfo(\"/\") 两端点清单为空（slash peer 无端点）");

        // 收敛一致性：重复调用与首调结果一致
        std::vector<std::pair<std::string, std::string>> pubs2;
        std::vector<std::pair<std::string, std::string>> subs2;
        CHECK(dbg.nodeInfo(NODEINFO_PEER_NAME, pubs2, subs2, 5, 100),
              "nodeInfo 重复调用 → true（收敛后查询）");
        CHECK(pubs2 == pubs && subs2 == subs, "nodeInfo 重复调用结果与首调一致（收敛快照一致性）");

        // topic info verbose：同 peer 端点详情（归属节点名 + FastDDS 原生 GUID + QoS 行集）
        std::string vTypeName;
        size_t vPubCount = 0;
        size_t vSubCount = 0;
        std::vector<FastDDSDebugNode::EndpointDetail> vPubs;
        std::vector<FastDDSDebugNode::EndpointDetail> vSubs;
        CHECK(dbg.topicInfo(NODEINFO_PUB_TOPIC, vTypeName, vPubCount, vSubCount, 5, 100,
                  &vPubs, nullptr),
            "topicInfo(t_nodeinfo_pub, verbose) → true（writer 端点详情命中）");
        CHECK(vTypeName == "YomkRpc::MString" && vPubCount == 1,
            "topicInfo(t_nodeinfo_pub, verbose) 类型名与发布者计数正确");
        CHECK(vPubs.size() == 1 && vPubs[0].nodeName == NODEINFO_PEER_NAME,
            "writer 端点详情归属节点名 == test-nodeinfo-peer（GUID 前缀归属）");
        CHECK(vPubs[0].guid.find('|') != std::string::npos,
            "writer GUID 为 FastDDS 原生 prefix|entity 形态");
        bool hasReliability = false;
        bool hasDurability = false;
        for (const auto& line : vPubs[0].qosLines)
        {
            hasReliability = hasReliability || line.rfind("  Reliability: ", 0) == 0;
            hasDurability = hasDurability || line.rfind("  Durability: ", 0) == 0;
            std::cout << "[OBSERVE] writer qos |" << line << "|" << std::endl;
        }
        CHECK(hasReliability && hasDurability,
            "writer QoS 行集含 Reliability 与 Durability 键行");
        CHECK(dbg.topicInfo(NODEINFO_SUB_TOPIC, vTypeName, vPubCount, vSubCount, 5, 100,
                  nullptr, &vSubs),
            "topicInfo(t_nodeinfo_sub, verbose) → true（reader 端点详情命中）");
        CHECK(vSubCount == 1 && vSubs.size() == 1 && vSubs[0].nodeName == NODEINFO_PEER_NAME,
            "reader 端点详情归属节点名 == test-nodeinfo-peer（GUID 前缀归属）");
        std::cout << "[OBSERVE] writer guid=" << vPubs[0].guid
                  << " reader guid=" << vSubs[0].guid << std::endl;
        // 未发现主题：verbose 指针传入 → false 且输出保持为空（单次快照快速路径）
        std::vector<FastDDSDebugNode::EndpointDetail> missDetails;
        CHECK(!dbg.topicInfo("t_not_exist_verbose", vTypeName, vPubCount, vSubCount, 1, 50,
                  &missDetails, nullptr),
            "topicInfo(t_not_exist_verbose, verbose) → false（未发现主题）");
        CHECK(missDetails.empty(), "未发现主题时 verbose 输出保持为空");

        // interface show：按类型名输出 IDL 结构（TypeObject→DynamicType 纯类型内省，
        // 不建订阅；peer 的 MString writer 在场，TypeInformation 经 TypeLookup 已就绪）
        std::vector<std::string> iface;
        CHECK(dbg.interfaceShow("YomkRpc::MString", iface, 5, 100),
              "interfaceShow(YomkRpc::MString,5,100ms) → true（writer 在场类型命中）");
        CHECK(iface.size() == 3, "IDL 行集恰好 3 行（struct 头/字段行/结尾）");
        bool ifaceOk = iface.size() == 3 &&
            iface[0] == "struct YomkRpc::MString {" &&
            iface[1] == "    string data;" &&
            iface[2] == "};";
        CHECK(ifaceOk, "IDL 行集形态正确（struct 头/四空格缩进字段行/结尾 };）");
        for (const auto& line : iface)
        {
            std::cout << "[OBSERVE] iface |" << line << "|" << std::endl;
        }
        // 未发现类型：单次快照快速路径 → false 且输出保持为空
        std::vector<std::string> ifaceMiss;
        CHECK(!dbg.interfaceShow("Not::Exist", ifaceMiss, 1, 50),
              "interfaceShow(Not::Exist,1,50ms) → false（未发现类型）");
        CHECK(ifaceMiss.empty(), "未发现类型时输出保持为空");

        // 清理：writer → topic → publisher → participant（顺序与既有块一致）
        if (anonWriter != nullptr && anonPub != nullptr)
        {
            anonPub->delete_datawriter(anonWriter);
        }
        if (anonTopic != nullptr && anonParticipant != nullptr)
        {
            anonParticipant->delete_topic(anonTopic);
        }
        if (anonPub != nullptr && anonParticipant != nullptr)
        {
            anonParticipant->delete_publisher(anonPub);
        }
        if (slashParticipant != nullptr)
        {
            dds::DomainParticipantFactory::get_instance()->delete_participant(slashParticipant);
        }
        if (anonParticipant != nullptr)
        {
            dds::DomainParticipantFactory::get_instance()->delete_participant(anonParticipant);
        }
    } // dbg/peer 析构；anonTs（TypeSupport）随块退出释放

    // ---- topicPub 发布用例：临时 writer 一次发布 + ack 自适应确认（RELIABLE 路径 + 退化路径）----
    {
        namespace dds = eprosima::fastdds::dds;  // 块内别名：直连 FastDDS API 搭建订阅端
        // RELIABLE 订阅端：显式 RELIABLE（进调试节点发现缓存 → ack 路径可用），
        // durability 默认 VOLATILE（请求 ≤ 提供 TRANSIENT_LOCAL，匹配成立）
        auto *pubParticipant = dds::DomainParticipantFactory::get_instance()->create_participant(
            TEST_DOMAIN, dds::PARTICIPANT_QOS_DEFAULT);
        CHECK(pubParticipant != nullptr, "topicPub 场景订阅端 participant 创建成功");
        if (pubParticipant != nullptr)
        {
            constexpr const char *PUB_TOPIC = "t_pub_once";
            auto *sub = pubParticipant->create_subscriber(dds::SUBSCRIBER_QOS_DEFAULT);
            dds::TypeSupport pubTs(new YomkRpc::MStringPubSubType());
            pubTs.register_type(pubParticipant);
            auto *topic = sub != nullptr ? pubParticipant->create_topic(
                PUB_TOPIC, pubTs.get_type_name(), dds::TOPIC_QOS_DEFAULT) : nullptr;
            dds::DataReaderQos rqos;
            rqos.reliability().kind = dds::RELIABLE_RELIABILITY_QOS;
            PubTestListener listener;
            auto *reader = (sub != nullptr && topic != nullptr) ?
                sub->create_datareader(topic, rqos, &listener) : nullptr;
            CHECK(reader != nullptr, "RELIABLE 订阅端 DataReader 创建成功（t_pub_once）");

            if (reader != nullptr)
            {
                FastDDSDebugNode dbg;
                CHECK(dbg.setDomainId(TEST_DOMAIN), "被测端 setDomainId(200) → true（topicPub 场景）");
                // 等 reader 发现信息进调试节点缓存（topicPub 内部再做发现/匹配收敛）
                std::this_thread::sleep_for(std::chrono::milliseconds(500));

                // 命中：RELIABLE 订阅者在场 → ack 确认路径（wait_for_acknowledgments）
                std::string pubErr;
                CHECK(dbg.topicPub(PUB_TOPIC, R"({"data":"t_pub_once_payload"})", pubErr),
                      "topicPub(t_pub_once, RELIABLE reader 在场) → true（ack 确认送达）");
                CHECK(pubErr.empty(), "topicPub 命中路径 error 为空");
                CHECK(listener.received_.load(), "订阅端 listener 收到发布消息（送达实证）");

                // 非法 JSON：解析失败不建 writer，不发布
                std::string jsonErr;
                CHECK(!dbg.topicPub(PUB_TOPIC, "{bad", jsonErr), "topicPub 非法 JSON → false");
                CHECK(jsonErr.find("invalid json") != std::string::npos,
                      "非法 JSON error 含 invalid json");

                // 未发现主题：发现收敛后仍无此主题
                std::string missErr;
                CHECK(!dbg.topicPub("t_pub_no_such", "{}", missErr),
                      "topicPub 未发现主题 → false");
                CHECK(missErr.find("not found") != std::string::npos,
                      "未发现主题 error 含 not found");
            }

            // 清理：reader → topic → subscriber → participant（与既有块一致）
            if (sub != nullptr && reader != nullptr)
            {
                sub->delete_datareader(reader);
            }
            if (topic != nullptr)
            {
                pubParticipant->delete_topic(topic);
            }
            if (sub != nullptr)
            {
                pubParticipant->delete_subscriber(sub);
            }
            dds::DomainParticipantFactory::get_instance()->delete_participant(pubParticipant);
        }

        // 退化路径：全 BEST_EFFORT 订阅者（无 RELIABLE → 不走 ack，保底窗后尽力而为成功）
        auto *beParticipant = dds::DomainParticipantFactory::get_instance()->create_participant(
            TEST_DOMAIN, dds::PARTICIPANT_QOS_DEFAULT);
        CHECK(beParticipant != nullptr, "topicPub 退化场景 participant 创建成功");
        if (beParticipant != nullptr)
        {
            constexpr const char *BE_TOPIC = "t_pub_besteff";
            auto *sub = beParticipant->create_subscriber(dds::SUBSCRIBER_QOS_DEFAULT);
            dds::TypeSupport beTs(new YomkRpc::MStringPubSubType());
            beTs.register_type(beParticipant);
            auto *topic = sub != nullptr ? beParticipant->create_topic(
                BE_TOPIC, beTs.get_type_name(), dds::TOPIC_QOS_DEFAULT) : nullptr;
            dds::DataReaderQos rqos;
            rqos.reliability().kind = dds::BEST_EFFORT_RELIABILITY_QOS;
            PubTestListener listener;
            auto *reader = (sub != nullptr && topic != nullptr) ?
                sub->create_datareader(topic, rqos, &listener) : nullptr;
            CHECK(reader != nullptr, "BEST_EFFORT 订阅端 DataReader 创建成功（t_pub_besteff）");

            if (reader != nullptr)
            {
                FastDDSDebugNode dbg;
                CHECK(dbg.setDomainId(TEST_DOMAIN), "被测端 setDomainId(200) → true（退化场景）");
                std::this_thread::sleep_for(std::chrono::milliseconds(500));
                std::string beErr;
                CHECK(dbg.topicPub(BE_TOPIC, R"({"data":"be_payload"})", beErr),
                      "topicPub(t_pub_besteff, 全 BEST_EFFORT) → true（退化尽力而为路径）");
                CHECK(listener.received_.load(), "BEST_EFFORT 订阅端也收到消息（尽力而为送达）");
            }

            if (sub != nullptr && reader != nullptr)
            {
                sub->delete_datareader(reader);
            }
            if (topic != nullptr)
            {
                beParticipant->delete_topic(topic);
            }
            if (sub != nullptr)
            {
                beParticipant->delete_subscriber(sub);
            }
            dds::DomainParticipantFactory::get_instance()->delete_participant(beParticipant);
        }
    }

    // ---- topicPub matched 校验用例：缓存有 RELIABLE 订阅者却无一与临时 writer 匹配 ----
    // 幽灵 reader 请求 RELIABLE+PERSISTENT，对测试 writer（VOLATILE）与 topicPub 临时
    // writer（TRANSIENT_LOCAL）均"请求 > 提供"被 EDP 拒绝匹配，但 DISCOVERED_READER 仍
    // 进调试节点缓存 → 集合比对发现缓存 GUID 不在匹配集 → 不发布报错（避免假成功）
    {
        namespace dds = eprosima::fastdds::dds;
        auto *gxParticipant = dds::DomainParticipantFactory::get_instance()->create_participant(
            TEST_DOMAIN, dds::PARTICIPANT_QOS_DEFAULT);
        CHECK(gxParticipant != nullptr, "matched 校验场景订阅端 participant 创建成功");
        if (gxParticipant != nullptr)
        {
            constexpr const char *GX_TOPIC = "t_pub_ghost";
            auto *sub = gxParticipant->create_subscriber(dds::SUBSCRIBER_QOS_DEFAULT);
            auto *pub = gxParticipant->create_publisher(dds::PUBLISHER_QOS_DEFAULT);
            dds::TypeSupport gxTs(new YomkRpc::MStringPubSubType());
            gxTs.register_type(gxParticipant);
            // writer 用默认 QoS（RELIABLE+VOLATILE）：仅保证主题被发现（进调试节点缓存）
            auto *topic = (sub != nullptr && pub != nullptr) ? gxParticipant->create_topic(
                GX_TOPIC, gxTs.get_type_name(), dds::TOPIC_QOS_DEFAULT) : nullptr;
            auto *writer = (pub != nullptr && topic != nullptr) ?
                pub->create_datawriter(topic, dds::DATAWRITER_QOS_DEFAULT) : nullptr;
            dds::DataReaderQos rqos;
            rqos.reliability().kind = dds::RELIABLE_RELIABILITY_QOS;
            rqos.durability().kind = dds::PERSISTENT_DURABILITY_QOS;  // 请求 > 提供，EDP 拒配
            auto *reader = (sub != nullptr && topic != nullptr) ?
                sub->create_datareader(topic, rqos, nullptr) : nullptr;
            CHECK(writer != nullptr && reader != nullptr,
                  "matched 校验场景 writer/reader 创建成功");

            if (writer != nullptr && reader != nullptr)
            {
                FastDDSDebugNode dbg;
                CHECK(dbg.setDomainId(TEST_DOMAIN), "被测端 setDomainId(200) → true（matched 校验场景）");
                // 等幽灵 reader 进调试节点缓存（topicPub 内部再做发现/匹配收敛）
                std::this_thread::sleep_for(std::chrono::milliseconds(500));
                std::string gxErr;
                CHECK(!dbg.topicPub(GX_TOPIC, R"({"data":"gx"})", gxErr),
                      "topicPub(t_pub_ghost, RELIABLE 订阅者在场却无一匹配) → false");
                CHECK(gxErr.find("not all matched") != std::string::npos,
                      "matched 校验 error 含 not all matched");
            }

            // 清理：writer → reader → topic → publisher → subscriber → participant
            if (pub != nullptr && writer != nullptr)
            {
                pub->delete_datawriter(writer);
            }
            if (sub != nullptr && reader != nullptr)
            {
                sub->delete_datareader(reader);
            }
            if (topic != nullptr)
            {
                gxParticipant->delete_topic(topic);
            }
            if (pub != nullptr)
            {
                gxParticipant->delete_publisher(pub);
            }
            if (sub != nullptr)
            {
                gxParticipant->delete_subscriber(sub);
            }
            dds::DomainParticipantFactory::get_instance()->delete_participant(gxParticipant);
        }
    }

    // ---- topicPub 离线清理用例：reader graceful 退出（dispose）后缓存同步清空，再发布
    // 不误报（若离线清理未生效，缓存残留的已退出 reader 会命中报错分支使本用例失败）----
    {
        namespace dds = eprosima::fastdds::dds;
        auto *offParticipant = dds::DomainParticipantFactory::get_instance()->create_participant(
            TEST_DOMAIN, dds::PARTICIPANT_QOS_DEFAULT);
        CHECK(offParticipant != nullptr, "离线清理场景 participant 创建成功");
        if (offParticipant != nullptr)
        {
            constexpr const char *OFF_TOPIC = "t_pub_offline";
            auto *sub = offParticipant->create_subscriber(dds::SUBSCRIBER_QOS_DEFAULT);
            auto *pub = offParticipant->create_publisher(dds::PUBLISHER_QOS_DEFAULT);
            dds::TypeSupport offTs(new YomkRpc::MStringPubSubType());
            offTs.register_type(offParticipant);
            auto *topic = (sub != nullptr && pub != nullptr) ? offParticipant->create_topic(
                OFF_TOPIC, offTs.get_type_name(), dds::TOPIC_QOS_DEFAULT) : nullptr;
            // writer（默认 RELIABLE+VOLATILE）保证 reader 删除后主题仍被发现
            auto *writer = (pub != nullptr && topic != nullptr) ?
                pub->create_datawriter(topic, dds::DATAWRITER_QOS_DEFAULT) : nullptr;
            dds::DataReaderQos rqos;
            rqos.reliability().kind = dds::RELIABLE_RELIABILITY_QOS;
            auto *reader = (sub != nullptr && topic != nullptr) ?
                sub->create_datareader(topic, rqos, nullptr) : nullptr;
            CHECK(writer != nullptr && reader != nullptr,
                  "离线清理场景 writer/reader 创建成功");

            if (writer != nullptr && reader != nullptr)
            {
                FastDDSDebugNode dbg;
                CHECK(dbg.setDomainId(TEST_DOMAIN), "被测端 setDomainId(200) → true（离线清理场景）");
                std::this_thread::sleep_for(std::chrono::milliseconds(500));
                std::string err1;
                CHECK(dbg.topicPub(OFF_TOPIC, R"({"data":"off1"})", err1),
                      "topicPub(t_pub_offline) 第一次 → true（RELIABLE reader 在场 ack 确认）");

                // graceful 退出：dispose 传播 → 调试节点 REMOVED_READER 回调同步清缓存
                sub->delete_datareader(reader);
                reader = nullptr;
                std::this_thread::sleep_for(std::chrono::milliseconds(2000));
                std::string err2;
                CHECK(dbg.topicPub(OFF_TOPIC, R"({"data":"off2"})", err2),
                      "reader 退出后再发布 → true（离线清理生效不误报）");
                CHECK(err2.empty(), "清理后再发布 error 为空");
            }

            // 清理：writer → topic → publisher → subscriber → participant（reader 已删）
            if (pub != nullptr && writer != nullptr)
            {
                pub->delete_datawriter(writer);
            }
            if (topic != nullptr)
            {
                offParticipant->delete_topic(topic);
            }
            if (pub != nullptr)
            {
                offParticipant->delete_publisher(pub);
            }
            if (sub != nullptr)
            {
                offParticipant->delete_subscriber(sub);
            }
            dds::DomainParticipantFactory::get_instance()->delete_participant(offParticipant);
        }
    }

    // ---- topicPub 期望建匹配数用例（requiredSubscribers，CLI -w N）：matched 计数达到 N
    // 才继续发布；订阅端不足时阻塞等待并打印等待进度日志，停止标志置位即中断报错，不发布
    // 不追发 ----
    {
        namespace dds = eprosima::fastdds::dds;
        // A. 达标即发：RELIABLE reader 已在场（matched=1），requiredSubscribers=1 首轮即达标
        auto *wsParticipant = dds::DomainParticipantFactory::get_instance()->create_participant(
            TEST_DOMAIN, dds::PARTICIPANT_QOS_DEFAULT);
        CHECK(wsParticipant != nullptr, "期望建匹配场景 participant 创建成功");
        if (wsParticipant != nullptr)
        {
            constexpr const char *WS_TOPIC = "t_pub_wait_sub";
            auto *sub = wsParticipant->create_subscriber(dds::SUBSCRIBER_QOS_DEFAULT);
            auto *pub = wsParticipant->create_publisher(dds::PUBLISHER_QOS_DEFAULT);
            dds::TypeSupport wsTs(new YomkRpc::MStringPubSubType());
            wsTs.register_type(wsParticipant);
            auto *topic = (sub != nullptr && pub != nullptr) ? wsParticipant->create_topic(
                WS_TOPIC, wsTs.get_type_name(), dds::TOPIC_QOS_DEFAULT) : nullptr;
            auto *writer = (pub != nullptr && topic != nullptr) ?
                pub->create_datawriter(topic, dds::DATAWRITER_QOS_DEFAULT) : nullptr;
            dds::DataReaderQos rqos;
            rqos.reliability().kind = dds::RELIABLE_RELIABILITY_QOS;
            auto *reader = (sub != nullptr && topic != nullptr) ?
                sub->create_datareader(topic, rqos, nullptr) : nullptr;
            CHECK(writer != nullptr && reader != nullptr,
                  "期望建匹配场景 writer/reader 创建成功");

            if (writer != nullptr && reader != nullptr)
            {
                FastDDSDebugNode dbg;
                CHECK(dbg.setDomainId(TEST_DOMAIN), "被测端 setDomainId(200) → true（期望建匹配场景）");
                std::this_thread::sleep_for(std::chrono::milliseconds(500));
                // 按位置传参：stableRounds=5, intervalMs=200, repeatIntervalMs=0,
                // requiredSubscribers=1（reader 已在场，等待首轮即达标进入稳定收敛与发布）
                std::string wsErr;
                CHECK(dbg.topicPub(WS_TOPIC, R"({"data":"ws1"})", wsErr, 5, 200, 0, 1),
                      "topicPub(requiredSubscribers=1, RELIABLE reader 在场) → true（达标即发）");
                CHECK(wsErr.empty(), "达标即发 error 为空");
            }

            // 清理：writer → reader → topic → publisher → subscriber → participant
            if (pub != nullptr && writer != nullptr)
            {
                pub->delete_datawriter(writer);
            }
            if (sub != nullptr && reader != nullptr)
            {
                sub->delete_datareader(reader);
            }
            if (topic != nullptr)
            {
                wsParticipant->delete_topic(topic);
            }
            if (pub != nullptr)
            {
                wsParticipant->delete_publisher(pub);
            }
            if (sub != nullptr)
            {
                wsParticipant->delete_subscriber(sub);
            }
            dds::DomainParticipantFactory::get_instance()->delete_participant(wsParticipant);
        }

        // B. 等待中断：仅 writer（无 reader，matched 恒 0）→ requiredSubscribers=1 阻塞等待，
        // 1s 后工作线程置停止标志 → topicPub 中断返回 false（error 含 interrupted），不发布
        auto *wiParticipant = dds::DomainParticipantFactory::get_instance()->create_participant(
            TEST_DOMAIN, dds::PARTICIPANT_QOS_DEFAULT);
        CHECK(wiParticipant != nullptr, "等待中断场景 participant 创建成功");
        if (wiParticipant != nullptr)
        {
            constexpr const char *WI_TOPIC = "t_pub_wait_intr";
            auto *pub = wiParticipant->create_publisher(dds::PUBLISHER_QOS_DEFAULT);
            dds::TypeSupport wiTs(new YomkRpc::MStringPubSubType());
            wiTs.register_type(wiParticipant);
            auto *topic = (pub != nullptr) ? wiParticipant->create_topic(
                WI_TOPIC, wiTs.get_type_name(), dds::TOPIC_QOS_DEFAULT) : nullptr;
            // writer 使主题被发现（无 reader：matched 恒 0，等待永不自然达标）
            auto *writer = (pub != nullptr && topic != nullptr) ?
                pub->create_datawriter(topic, dds::DATAWRITER_QOS_DEFAULT) : nullptr;
            CHECK(writer != nullptr, "等待中断场景 writer 创建成功（无 reader）");

            if (writer != nullptr)
            {
                FastDDSDebugNode dbg;
                CHECK(dbg.setDomainId(TEST_DOMAIN), "被测端 setDomainId(200) → true（等待中断场景）");
                std::this_thread::sleep_for(std::chrono::milliseconds(500));
                // 工作线程 1s 后置停止标志（topicPub 入口 debugPubReset 复位残留后才读它，
                // 故必须异步置位而非预置）
                std::atomic<bool> stopperDone{false};
                std::thread stopper([&stopperDone]() {
                    std::this_thread::sleep_for(std::chrono::milliseconds(1000));
                    yomk::debugPubStop();
                    stopperDone.store(true);
                });
                std::string wiErr;
                CHECK(!dbg.topicPub(WI_TOPIC, R"({"data":"wi"})", wiErr, 5, 200, 0, 1),
                      "topicPub(requiredSubscribers=1, 无订阅端) 阻塞等待中停止标志置位 → false");
                CHECK(wiErr.find("interrupted") != std::string::npos,
                      "等待中断 error 含 interrupted");
                stopper.join();
                CHECK(stopperDone.load(), "置位线程已执行（时序健全性）");
                yomk::debugPubReset();  // 复位停止标志，防残留影响后续调用
            }

            // 清理：writer → topic → publisher → participant
            if (pub != nullptr && writer != nullptr)
            {
                pub->delete_datawriter(writer);
            }
            if (topic != nullptr)
            {
                wiParticipant->delete_topic(topic);
            }
            if (pub != nullptr)
            {
                wiParticipant->delete_publisher(pub);
            }
            dds::DomainParticipantFactory::get_instance()->delete_participant(wiParticipant);
        }
    }

    // 主题未发现 + requiredSubscribers=1：等待主题发现前置段（域内无任何该主题端点，
    // "先发布后订阅"场景）阻塞等待中停止标志置位 → false（error 含 interrupted），不发布
    {
        FastDDSDebugNode dbg;
        CHECK(dbg.setDomainId(TEST_DOMAIN),
              "被测端 setDomainId(200) → true（主题未发现等待场景）");
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
        // 工作线程 1s 后置停止标志（入口 debugPubReset 复位残留后才读它，必须异步置位）
        std::atomic<bool> ntStopperDone{false};
        std::thread ntStopper([&ntStopperDone]() {
            std::this_thread::sleep_for(std::chrono::milliseconds(1000));
            yomk::debugPubStop();
            ntStopperDone.store(true);
        });
        std::string ntErr;
        CHECK(!dbg.topicPub("t_pub_wait_notopic", R"({"data":"nt"})", ntErr, 5, 200, 0, 1),
              "topicPub(requiredSubscribers=1, 主题未发现) 等待主题出现中置位 → false");
        CHECK(ntErr.find("interrupted") != std::string::npos,
              "主题未发现等待中断 error 含 interrupted");
        ntStopper.join();
        CHECK(ntStopperDone.load(), "置位线程已执行（时序健全性）");
        yomk::debugPubReset();  // 复位停止标志，防残留影响后续调用
    }

    // ---- topicPub 条数上限用例（maxTimes，CLI -t N）：持续循环发满 N 条（含首轮）自动停止，
    // 达标路径在销毁发布链前排空 200ms；与 requiredSubscribers（CLI -w N）正交叠加 ----
    {
        namespace dds = eprosima::fastdds::dds;
        auto *mtParticipant = dds::DomainParticipantFactory::get_instance()->create_participant(
            TEST_DOMAIN, dds::PARTICIPANT_QOS_DEFAULT);
        CHECK(mtParticipant != nullptr, "条数上限场景 participant 创建成功");
        if (mtParticipant != nullptr)
        {
            auto *sub = mtParticipant->create_subscriber(dds::SUBSCRIBER_QOS_DEFAULT);
            dds::TypeSupport mtTs(new YomkRpc::MStringPubSubType());
            mtTs.register_type(mtParticipant);
            dds::DataReaderQos rqos;
            rqos.reliability().kind = dds::RELIABLE_RELIABILITY_QOS;
            // D. 发满即停：RELIABLE reader 在场（t_pub_maxtimes），maxTimes=3 → 恰发 3 条后退出
            constexpr const char *MT_TOPIC = "t_pub_maxtimes";
            auto *mtTopic = sub != nullptr ? mtParticipant->create_topic(
                MT_TOPIC, mtTs.get_type_name(), dds::TOPIC_QOS_DEFAULT) : nullptr;
            PubTestListener mtListener;
            auto *mtReader = (sub != nullptr && mtTopic != nullptr) ?
                sub->create_datareader(mtTopic, rqos, &mtListener) : nullptr;
            CHECK(mtReader != nullptr, "条数上限场景 RELIABLE 订阅端 DataReader 创建成功");
            // E. 条数上限与期望建匹配叠加：reader 在场（t_pub_maxtimes_wait），
            // requiredSubscribers=1 + maxTimes=2 → 匹配达标后恰发 2 条退出
            constexpr const char *MW_TOPIC = "t_pub_maxtimes_wait";
            auto *mwTopic = sub != nullptr ? mtParticipant->create_topic(
                MW_TOPIC, mtTs.get_type_name(), dds::TOPIC_QOS_DEFAULT) : nullptr;
            PubTestListener mwListener;
            auto *mwReader = (sub != nullptr && mwTopic != nullptr) ?
                sub->create_datareader(mwTopic, rqos, &mwListener) : nullptr;
            CHECK(mwReader != nullptr, "条数上限叠加场景 RELIABLE 订阅端 DataReader 创建成功");

            if (mtReader != nullptr && mwReader != nullptr)
            {
                FastDDSDebugNode dbg;
                CHECK(dbg.setDomainId(TEST_DOMAIN),
                      "被测端 setDomainId(200) → true（条数上限场景）");
                std::this_thread::sleep_for(std::chrono::milliseconds(500));

                // D：位置传参 stableRounds=5, intervalMs=50（加速节拍）, repeatIntervalMs=50,
                // requiredSubscribers=0, maxTimes=3 → 发满 3 条（含首轮）即停（排空 200ms 后返回）
                std::string mtErr;
                uint32_t mtTotal = 0;
                uint32_t mtFailed = 0;
                CHECK(dbg.topicPub(MT_TOPIC, R"({"data":"mt"})", mtErr, 5, 50, 50, 0, 3,
                            &mtTotal, &mtFailed),
                      "topicPub(maxTimes=3) → true（发满即停，排空 200ms 后返回）");
                CHECK(mtErr.empty(), "发满即停 error 为空");
                CHECK(mtTotal == 3, "maxTimes=3 统计 total==3（含首轮）");
                CHECK(mtFailed == 0, "maxTimes=3 统计 failed==0");
                CHECK(mtListener.received_.load(), "订阅端收到发满即停消息（送达实证）");

                // E：requiredSubscribers=1（reader 在场等待首轮即达标）+ maxTimes=2 →
                // 等匹配达标后恰发 2 条退出（-w 与 -t 正交叠加）
                std::string mwErr;
                uint32_t mwTotal = 0;
                uint32_t mwFailed = 0;
                CHECK(dbg.topicPub(MW_TOPIC, R"({"data":"mw"})", mwErr, 5, 50, 50, 1, 2,
                            &mwTotal, &mwFailed),
                      "topicPub(requiredSubscribers=1, maxTimes=2) → true（匹配达标后发满即停）");
                CHECK(mwErr.empty(), "叠加场景 error 为空");
                CHECK(mwTotal == 2, "requiredSubscribers=1 + maxTimes=2 统计 total==2");
                CHECK(mwFailed == 0, "叠加场景统计 failed==0");
                CHECK(mwListener.received_.load(), "订阅端收到叠加场景消息（送达实证）");
            }

            // 清理：reader ×2 → topic ×2 → subscriber → participant
            if (sub != nullptr && mtReader != nullptr)
            {
                sub->delete_datareader(mtReader);
            }
            if (sub != nullptr && mwReader != nullptr)
            {
                sub->delete_datareader(mwReader);
            }
            if (mtTopic != nullptr)
            {
                mtParticipant->delete_topic(mtTopic);
            }
            if (mwTopic != nullptr)
            {
                mtParticipant->delete_topic(mwTopic);
            }
            if (sub != nullptr)
            {
                mtParticipant->delete_subscriber(sub);
            }
            dds::DomainParticipantFactory::get_instance()->delete_participant(mtParticipant);
        }
    }

    return testReport("TestFastDDSDebugNode");
}
