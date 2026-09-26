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

#include <YomkRpcMsg/YomkRpcMsg.hpp>            // YomkRpc::MString 数据类（仅发布端使用）
#include <YomkRpcMsg/YomkRpcMsgPubSubTypes.hpp> // MStringPubSubType（仅发布端使用）

#include <fastdds/dds/domain/DomainParticipantFactory.hpp> // 纯订阅端 participant（仅订阅者用例）

#include <algorithm>
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

    return testReport("TestFastDDSDebugNode");
}
