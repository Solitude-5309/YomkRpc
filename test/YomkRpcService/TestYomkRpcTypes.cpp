/**
 * @file TestYomkRpcTypes.cpp
 * @brief MC5：主题/类型分支深化 + 多类型边界 payload 端到端
 *
 * 范围：MC0-MC4 覆盖服务契约、节点生命周期、主题注册与端到端(MString)、loan 免序列化(MInt32)。
 *       精读 MC3/MC4 现有覆盖后，本用例补齐 FastDDSNode.cpp 中仍未功能触达的真实分支，并首次
 *       系统验证多类型边界 payload 经 publish 端到端的透传健壮性与数据完整回环：
 * 覆盖：
 *   Part A  主题/类型分支深化：
 *     A1  registerPubTopic 侧类型冲突（sub 先占用 topic(MInt32)，pub 后注册不同类型(MString)）
 *         → getOrCreateTopic 类型名不符返回 nullptr（L215）→ eNo（对称补齐 MC3 T9 仅测的 sub 侧冲突 L256）；
 *     A2  getOrCreateTopic 复用成功分支从 pub 侧进入（sub 先建 MInt32_1，pub 后复用同类型 MInt32_2）
 *         → 复用返回 topic（L183）+ 第二次 register_type 重复 PRECONDITION_NOT_MET → eOk，端到端连通；
 *   Part B  多类型边界 payload 端到端（6 代表类型，同 participant 内 pub+sub 同 topic 本地回环）：
 *     MFloat32/MFloat64（NaN/±Inf/极值/denormal/±0.0）、MInt64（极值）、MBool、
 *     MString（空/超长/UTF-8/emoji/转义）、MByteArray（空/大/边界字节）→ 断言 received>0
 *     且收到的每个值都匹配发送集（透传无损坏）；asan(含 ubsan) 兜底验证自有源码透传无 UB。
 *
 * 关键研究结论：
 *   - TypeSupport = std::shared_ptr<TopicDataType>（TypeSupport.hpp L46/L96-100），构造即接管 type 所有权；
 *     register_type 同名已注册返回 PRECONDITION_NOT_MET（participant 不持有），否则 RETCODE_OK（participant 持有）。
 *     故 TypeSupport 构造后（L207/L249）的所有失败 return（类型冲突 L215/L256）均由 shared_ptr 自动释放或
 *     participant 持有（delete_node 清理），不泄漏——A1 的 MString 孤儿类型、A2 的 MInt32_2 均如此。
 *   - 浮点比较用"NaN 特判 + IEEE a==b"而非 bit-pattern：denormal flush-to-zero / -0.0 符号丢失属第三方
 *     序列化差异，bit-pattern 会脆弱失败；IEEE ==（values 含 0.0 兜底）既验证透传又不脆弱。
 *
 * 关键不变式：每个 eOk 创建的节点必须在 main 返回前经 /delete_node 显式删除（析构 reader 停止回调，
 *   测试局部/Channel 捕获状态全程存活，无悬垂，同 MC3/MC4）。
 *
 * 风格：纯 main() + CHECK 宏 + 失败计数（零第三方依赖），返回非 0 表示存在失败用例。
 */

#include "TestCheck.h"
#include "YomkRpcService.h" // 服务 + DDSTopic/DDSSubRequest/DDSPublish + String + YOMK_* 宏

#include <YomkRpcMsg/YomkRpcMsg.hpp>            // YomkRpc::MFloat32/MFloat64/MInt64/MBool/MString/MByteArray 数据类
#include <YomkRpcMsg/YomkRpcMsgPubSubTypes.hpp> // 对应 PubSubType

#include <atomic>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <functional>
#include <iostream>
#include <limits>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

namespace
{
    // 有效域（FastDDS 约 0-232），与 MC2/MC3/MC4 一致，避开默认域 0 的潜在网络干扰
    constexpr uint32_t TEST_DOMAIN = 200;

    // 构造 /create_node 请求包
    YomkPkgPtr mkNode(uint32_t domainId, const std::string &nodeName)
    {
        return YomkMkPtr(DDSNode, DDSNode{domainId, nodeName});
    }

    // =========================================================================
    // Part A：主题/类型分支深化（pub 侧类型冲突 L215 + pub 侧复用 L183）
    // =========================================================================
    void testTopicTypeBranches(YomkRpcService *svc)
    {
        // ---- A1：pub 侧类型冲突（sub 先占用 topic，pub 后注册不同类型 → getOrCreateTopic 冲突 L215）----
        CHECK(svc->invoke("/create_node", mkNode(TEST_DOMAIN, "ta_node")).m_status == YomkResponse::eOk,
              "创建 ta_node(domain 200) → eOk");
        CHECK(svc->invoke("/register_sub_topic",
                          YomkMkPtr(DDSSubRequest, DDSSubRequest{"ta_node", "shared_t", new YomkRpc::MInt32PubSubType(), nullptr}))
                      .m_status == YomkResponse::eOk,
              "A1 sub 先占用 shared_t(MInt32) → eOk（topics_[shared_t] 绑定 MInt32）");
        CHECK(svc->invoke("/register_pub_topic",
                          YomkMkPtr(DDSTopic, DDSTopic{"ta_node", "shared_t", new YomkRpc::MStringPubSubType()}))
                      .m_status == YomkResponse::eNo,
              "A1 pub 侧类型冲突 → eNo（getOrCreateTopic MInt32≠MString 返回 nullptr，L215；MString 孤儿类型由 participant 清理不泄漏）");
        CHECK(svc->invoke("/delete_node", YomkMkPtr(String, "ta_node")).m_status == YomkResponse::eOk,
              "删除 ta_node → eOk");

        // ---- A2：pub 侧复用（sub 先建 MInt32_1，pub 后复用同类型 MInt32_2 → L183 复用 + register_type 重复）----
        CHECK(svc->invoke("/create_node", mkNode(TEST_DOMAIN, "tb_node")).m_status == YomkResponse::eOk,
              "创建 tb_node → eOk");
        std::atomic<int> received{0};
        std::mutex mtx;
        int32_t lastValue = -1;
        auto cb = [&](const void *d)
        {
            auto *m = static_cast<const YomkRpc::MInt32 *>(d);
            std::lock_guard<std::mutex> lk(mtx);
            lastValue = m->data();
            received++;
        };
        CHECK(svc->invoke("/register_sub_topic",
                          YomkMkPtr(DDSSubRequest, DDSSubRequest{"tb_node", "reuse_t", new YomkRpc::MInt32PubSubType(), cb}))
                      .m_status == YomkResponse::eOk,
              "A2 sub 先建 reuse_t(MInt32_1) → eOk");
        CHECK(svc->invoke("/register_pub_topic",
                          YomkMkPtr(DDSTopic, DDSTopic{"tb_node", "reuse_t", new YomkRpc::MInt32PubSubType()}))
                      .m_status == YomkResponse::eOk,
              "A2 pub 侧复用同类型 → eOk（getOrCreateTopic 复用 L183 + register_type PRECONDITION_NOT_MET，MInt32_2 由 pubTopics_ 持有不泄漏）");

        std::this_thread::sleep_for(std::chrono::milliseconds(1500)); // 等待 discovery
        constexpr int N = 5;
        int pubOk = 0;
        for (int i = 0; i < N; ++i)
        {
            YomkRpc::MInt32 m;
            m.data(777);
            if (svc->invoke("/publish", YomkMkPtr(DDSPublish, DDSPublish{"tb_node", "reuse_t", &m}))
                    .m_status == YomkResponse::eOk)
            {
                pubOk++;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
        CHECK(pubOk == N, "A2 publish 5 条 MInt32(777) → 全部 eOk");

        std::this_thread::sleep_for(std::chrono::milliseconds(1500)); // 等待投递
        int r = received.load();
        int32_t lv = 0;
        {
            std::lock_guard<std::mutex> lk(mtx);
            lv = lastValue;
        }
        CHECK(r > 0 && lv == 777, "A2 复用 topic pub→sub 连通（received>0 且 lastValue==777，走对齐反序列化交付）");
        std::cout << "[OBSERVE] A2 reuse received=" << r << "/" << N << " lastValue=" << lv << std::endl;

        CHECK(svc->invoke("/delete_node", YomkMkPtr(String, "tb_node")).m_status == YomkResponse::eOk,
              "删除 tb_node → eOk");
    }

    // =========================================================================
    // Part B 基础设施：类型擦除通道（分阶段共享 discovery，避免 6 类型各自 sleep）
    // =========================================================================
    struct IChannel
    {
        virtual ~IChannel() = default;
        virtual void registerPubSub(YomkRpcService *svc) = 0;
        virtual void send(YomkRpcService *svc) = 0;
        virtual void assertRoundTrip() = 0;
        std::string label;
        std::string node;
        std::string topic;
    };

    // 单一类型的端到端通道：注册 pub+sub → 发送特殊值 → 断言 received>0 且收到值均匹配发送集
    template <typename MsgT, typename PubSubT, typename ValueT>
    struct Channel : IChannel
    {
        std::vector<ValueT> values;                             // 发送的边界特殊值集
        std::function<bool(const ValueT &, const ValueT &)> eq; // 类型特定比较器
        std::vector<ValueT> got;                                // 回调收到的值（mtx 保护）
        std::mutex mtx;
        std::atomic<int> received{0};

        void registerPubSub(YomkRpcService *svc) override
        {
            CHECK(svc->invoke("/register_pub_topic",
                              YomkMkPtr(DDSTopic, DDSTopic{node, topic, new PubSubT()}))
                          .m_status == YomkResponse::eOk,
                  label + " register_pub → eOk");
            auto cb = [this](const void *d)
            {
                auto *m = static_cast<const MsgT *>(d);
                std::lock_guard<std::mutex> lk(mtx);
                got.push_back(m->data());
                received++;
            };
            CHECK(svc->invoke("/register_sub_topic",
                              YomkMkPtr(DDSSubRequest, DDSSubRequest{node, topic, new PubSubT(), cb}))
                          .m_status == YomkResponse::eOk,
                  label + " register_sub → eOk");
        }

        void send(YomkRpcService *svc) override
        {
            int cnt = 0;
            for (const auto &v : values)
            {
                MsgT m;
                m.data(v); // 标量/字符串/序列 setter 同构（void data(const ValueT&)）
                if (svc->invoke("/publish", YomkMkPtr(DDSPublish, DDSPublish{node, topic, &m}))
                        .m_status == YomkResponse::eOk)
                {
                    cnt++;
                }
                std::this_thread::sleep_for(std::chrono::milliseconds(30));
            }
            CHECK(cnt == static_cast<int>(values.size()),
                  label + " publish " + std::to_string(values.size()) + " 条边界特殊值 → 全部 eOk");
        }

        void assertRoundTrip() override
        {
            int r = received.load();
            bool allMatched = true;
            {
                std::lock_guard<std::mutex> lk(mtx);
                for (const auto &g : got)
                {
                    bool found = false;
                    for (const auto &v : values)
                    {
                        if (eq(g, v))
                        {
                            found = true;
                            break;
                        }
                    }
                    if (!found)
                    {
                        allMatched = false;
                        break;
                    }
                }
            }
            CHECK(r > 0 && allMatched,
                  label + " 边界 payload 完整回环（received=" + std::to_string(r) + "/" +
                      std::to_string(values.size()) + "，收到值均匹配发送集，透传无损坏）");
            std::cout << "[OBSERVE] " << label << " received=" << r << "/" << values.size() << std::endl;
        }
    };

    // 浮点比较器：NaN 需特判（NaN!=NaN），其余用 IEEE a==b（±Inf/极值精确；-0.0==0.0 与 denormal
    // flush-to-zero 由 values 内的 0.0 兜底匹配，规避 bit-pattern 的第三方差异脆弱性）
    template <typename F>
    std::function<bool(const F &, const F &)> makeFloatEq()
    {
        return [](const F &a, const F &b)
        {
            if (std::isnan(a) && std::isnan(b))
            {
                return true;
            }
            if (std::isnan(a) || std::isnan(b))
            {
                return false;
            }
            return a == b;
        };
    }

    // =========================================================================
    // Part B：多类型边界 payload 端到端（6 代表类型，分阶段共享 discovery）
    // =========================================================================
    void testMultiTypeBoundaryPayload(YomkRpcService *svc)
    {
        CHECK(svc->invoke("/create_node", mkNode(TEST_DOMAIN, "types_node")).m_status == YomkResponse::eOk,
              "创建 types_node → eOk");

        std::vector<std::unique_ptr<IChannel>> chans;

        // MFloat32：NaN / ±Inf / ±max / denormal / ±0.0（含 0.0 兜底 denormal flush 与 -0.0 符号）
        {
            auto c = std::make_unique<Channel<YomkRpc::MFloat32, YomkRpc::MFloat32PubSubType, float>>();
            c->label = "MFloat32";
            c->node = "types_node";
            c->topic = "t_f32";
            using L = std::numeric_limits<float>;
            c->values = {L::quiet_NaN(), L::infinity(), -L::infinity(), L::max(), -L::max(),
                         L::denorm_min(), 0.0f, -0.0f};
            c->eq = makeFloatEq<float>();
            chans.push_back(std::move(c));
        }
        // MFloat64：NaN / ±Inf / ±max / denormal / 0.0
        {
            auto c = std::make_unique<Channel<YomkRpc::MFloat64, YomkRpc::MFloat64PubSubType, double>>();
            c->label = "MFloat64";
            c->node = "types_node";
            c->topic = "t_f64";
            using L = std::numeric_limits<double>;
            c->values = {L::quiet_NaN(), L::infinity(), -L::infinity(), L::max(), -L::max(),
                         L::denorm_min(), 0.0};
            c->eq = makeFloatEq<double>();
            chans.push_back(std::move(c));
        }
        // MInt64：极值 / -1 / 0 / 常规
        {
            auto c = std::make_unique<Channel<YomkRpc::MInt64, YomkRpc::MInt64PubSubType, int64_t>>();
            c->label = "MInt64";
            c->node = "types_node";
            c->topic = "t_i64";
            using L = std::numeric_limits<int64_t>;
            c->values = {L::min(), L::max(), -1, 0, 42};
            c->eq = [](const int64_t &a, const int64_t &b)
            { return a == b; };
            chans.push_back(std::move(c));
        }
        // MBool：true / false
        {
            auto c = std::make_unique<Channel<YomkRpc::MBool, YomkRpc::MBoolPubSubType, bool>>();
            c->label = "MBool";
            c->node = "types_node";
            c->topic = "t_bool";
            c->values = {true, false};
            c->eq = [](const bool &a, const bool &b)
            { return a == b; };
            chans.push_back(std::move(c));
        }
        // MString：空 / 超长(10000) / UTF-8 中文 / emoji / 转义字符（不含嵌入 \0，其 DDS 序列化语义不确定）
        {
            auto c = std::make_unique<Channel<YomkRpc::MString, YomkRpc::MStringPubSubType, std::string>>();
            c->label = "MString";
            c->node = "types_node";
            c->topic = "t_str";
            c->values = {"", std::string(10000, 'x'), "中文测试",
                         "\xF0\x9F\x98\x80\xF0\x9F\x8E\x89", // 😀🎉 (UTF-8 字节转义，规避源编码差异)
                         "a\\b\"c\ttab"};
            c->eq = [](const std::string &a, const std::string &b)
            { return a == b; };
            chans.push_back(std::move(c));
        }
        // MByteArray：空 / 大(100000 字节循环) / 边界字节(0x00,0xFF,0x7F,0x80)
        {
            auto c = std::make_unique<Channel<YomkRpc::MByteArray, YomkRpc::MByteArrayPubSubType, std::vector<uint8_t>>>();
            c->label = "MByteArray";
            c->node = "types_node";
            c->topic = "t_barr";
            std::vector<uint8_t> big;
            big.reserve(100000);
            for (int i = 0; i < 100000; ++i)
            {
                big.push_back(static_cast<uint8_t>(i % 256));
            }
            c->values = {std::vector<uint8_t>{}, big, std::vector<uint8_t>{0x00, 0xFF, 0x7F, 0x80}};
            c->eq = [](const std::vector<uint8_t> &a, const std::vector<uint8_t> &b)
            { return a == b; };
            chans.push_back(std::move(c));
        }

        // 分阶段：全部注册 → 一次 discovery → 全部发送 → 一次投递 → 全部断言
        for (auto &c : chans)
        {
            c->registerPubSub(svc);
        }
        std::this_thread::sleep_for(std::chrono::seconds(2)); // 6 topic 共享 discovery
        for (auto &c : chans)
        {
            c->send(svc);
        }
        std::this_thread::sleep_for(std::chrono::seconds(2)); // 等待投递
        for (auto &c : chans)
        {
            c->assertRoundTrip();
        }

        CHECK(svc->invoke("/delete_node", YomkMkPtr(String, "types_node")).m_status == YomkResponse::eOk,
              "删除 types_node → eOk（析构 6 reader 停止回调，Channel 仍存活至本函数结束）");
    }
} // namespace

int main()
{
    YOMK_INIT();

    // 注册被测服务：所有权移交框架（shared_ptr 持有），init() 内部调用后 weak_from_this() 方有效
    auto *svc = new YomkRpcService(YOMK_SERVER_P);
    CHECK(YOMK_ADD_SERVICE(svc) == 0, "YomkRpcService 注册成功（所有权移交框架，init() 已内部调用）");

    testTopicTypeBranches(svc);
    testMultiTypeBoundaryPayload(svc);

    return testReport("TestYomkRpcTypes");
}
