/**
 * @file TestYomkRpcConcurrency.cpp
 * @brief MC6：并发安全 + TSan 基线（多线程 invoke 锁正确性 / 并发收发 / teardown 竞态）
 *
 * 范围：MC0-MC5 均由单线程顺序驱动（回调虽在 FastDDS reader 线程触发，但主逻辑串行），双层锁
 *       （YomkRpcService::mtx_ 序列化所有 invoke；FastDDSNode::mtx_ 序列化节点内 3 张 map）与通用
 *       take() 的 reader 回调路径（on_data_available 在 reader 线程 take()+return_loan 借出交付）
 *       在真并发下的正确性从未被系统验证。本用例经 svc->invoke() 从多线程并发驱动，配合 off/asan/tsan
 *       三模式，实证锁正确性、并发收发数据完整性、以及 delete_node 与活跃回调的 teardown 竞态安全。
 * 覆盖：
 *   Part A 并发 invoke 锁正确性（确定性"唯一赢家"断言，不依赖投递时序故不抖动）：
 *     A1 N=8 线程 × R=20 轮并发 create_node 同名 → 每轮恰 1 eOk（service mtx_ 序列化 find-then-insert）；
 *     A2 单节点上 M=8 线程 × R=20 轮并发 register_pub_topic 同 topic 名 → 每轮恰 1 eOk（node mtx_ +
 *        dup 守卫 count()>0）；asan 交叉验证 MC3 泄漏修复在并发下仍成立（每轮 M-1 个 loser 的 caller-new
 *        type 由 registerPubTopic 首守卫在锁内 delete，20 轮共 140 个 loser type 零泄漏）；
 *     A3 T=8 线程 × K=200 轮混合并发（唯一 topic 注册 / publish / loan / discard_loan / version）→
 *        各操作计数全部 eOk、终态一致（验证 node mtx_ 序列化写；version 只读端点无锁与写并发安全）。
 *   Part B 并发发布+订阅端到端（通用 take() reader 路径 + 回调契约并发下）：P=4 发布线程各发 K=250 条
 *     MInt32（值编码 tid*1000+seq，全局唯一）→ 同一 writer 并发 write 经 node mtx_ 序列化；reader 单
 *     线程顺序 drain 借出缓冲交付回调；抗抖动断言 received∈(0,P*K] 且所有收到值可解码（检测借出缓冲撕裂/串值）。
 *   Part C delete_node 与活跃回调竞态（最高风险 teardown）：R=10 轮，高频发布流进行中 delete_node，
 *     实证 ~FastDDSNode "先 delete_datareader（drain 回调）后 delete_data" 的顺序安全（asan 无 UAF、
 *     tsan 无 race、无死锁）。
 *
 * 关键设计约束：
 *   - sub 回调绝不回调 svc->invoke()：delete_node 持 service mtx_ 期间 ~FastDDSNode 阻塞在
 *     delete_datareader 等待 reader 线程回调 drain，若回调再取 service mtx_ 即死锁。回调只碰测试自有
 *     mutex 守护的状态，且该状态为函数作用域，outlive 所有节点删除（teardown drain 后无回调再触碰）。
 *   - 每线程 publish 的 MInt32 为线程局部对象（writer->write 在 node mtx_ 内于发布线程同步序列化读取，
 *     线程局部故无跨线程数据竞争）；A3 loan 写池样本用 MInt32（alignof=4，loan 指针 base+4 天然满足
 *     4 对齐，规避 8 字节 plain 类型严格对齐告诫——与 MC4 一致），且写发生在 loan/discard 的 node mtx_
 *     括号之间，池样本复用的 happens-before 由 mtx_ 建立，tsan 干净。
 *   - 测试自有共享态（计数器/收集 vector）一律用测试 mutex 或 atomic 守护并在锁内快照读取，避免污染
 *     tsan 对 YomkRpc 自有源码的 race 判定、亦避免对测试自有变量的误报。
 *
 * 关键不变式：每个 eOk 创建的节点必须在 main 返回前经 /delete_node 显式删除（析构 reader 停止回调，
 *   测试局部捕获状态全程存活，无悬垂）；不遗留未归还的 loan（A3 借出即 discard）。
 *
 * 风格：纯 main() + CHECK 宏 + 失败计数（零第三方依赖），返回非 0 表示存在失败用例。
 */

#include "TestCheck.h"
#include "YomkRpcService.h" // 服务 + DDSNode/DDSTopic/DDSSubRequest/DDSPublish/DDSLoan/DDSLoanResult + String + YOMK_* 宏

#include <YomkRpcMsg/YomkRpcMsg.hpp>            // YomkRpc::MInt32
#include <YomkRpcMsg/YomkRpcMsgPubSubTypes.hpp> // MInt32PubSubType

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
    // MC6 专用域，与 MC2-MC5(200) 隔离，避免并行/残留 participant 干扰 discovery
    constexpr uint32_t TEST_DOMAIN = 210;

    // 构造 /create_node 请求包
    YomkPkgPtr mkNode(uint32_t domainId, const std::string &nodeName)
    {
        return YomkMkPtr(DDSNode, DDSNode{domainId, nodeName});
    }

    // ---- Part A1：并发 create_node 同名 → 每轮唯一赢家（service mtx_ 序列化 find-then-insert）----
    void partA1_concurrentCreateNode(YomkRpcService *svc)
    {
        constexpr int N = 8;
        constexpr int R = 20;
        int oneWinnerRounds = 0;
        int delOk = 0;
        for (int r = 0; r < R; ++r)
        {
            std::atomic<bool> go{false};
            std::atomic<int> okCount{0};
            std::vector<std::thread> ths;
            ths.reserve(N);
            for (int t = 0; t < N; ++t)
            {
                ths.emplace_back([&]
                                 {
                    while (!go.load(std::memory_order_acquire)) { std::this_thread::yield(); }
                    if (svc->invoke("/create_node", mkNode(TEST_DOMAIN, "a1_race_node")).m_status == YomkResponse::eOk)
                    {
                        okCount.fetch_add(1);
                    } });
            }
            go.store(true, std::memory_order_release);
            for (auto &th : ths)
            {
                th.join();
            }
            if (okCount.load() == 1)
            {
                ++oneWinnerRounds;
            }
            // 删除本轮唯一赢家创建的节点，为下一轮重置（否则下一轮全员命中 already-exists → eNo）
            if (svc->invoke("/delete_node", YomkMkPtr(String, "a1_race_node")).m_status == YomkResponse::eOk)
            {
                ++delOk;
            }
        }
        CHECK(oneWinnerRounds == R,
              "A1 并发 create_node 同名：20 轮每轮恰 1 eOk、7 eNo（service mtx_ 序列化 find-then-insert）");
        CHECK(delOk == R, "A1 每轮竞胜节点均可删除 → 20/20 eOk（终态一致，无重复插入残留）");
    }

    // ---- Part A2：并发 register_pub_topic 同名 → 每轮唯一赢家（node mtx_ + dup 守卫 count()>0）----
    void partA2_concurrentRegister(YomkRpcService *svc)
    {
        CHECK(svc->invoke("/create_node", mkNode(TEST_DOMAIN, "a2_node")).m_status == YomkResponse::eOk,
              "A2 创建 a2_node（供 20 轮并发注册竞争，避免每轮重建 participant）→ eOk");
        constexpr int M = 8;
        constexpr int R = 20;
        int oneWinnerRounds = 0;
        for (int r = 0; r < R; ++r)
        {
            const std::string topic = "a2_race_" + std::to_string(r); // 每轮唯一 topic，避免跨轮恒 dup
            std::atomic<bool> go{false};
            std::atomic<int> okCount{0};
            std::vector<std::thread> ths;
            ths.reserve(M);
            for (int t = 0; t < M; ++t)
            {
                ths.emplace_back([&, topic]
                                 {
                    while (!go.load(std::memory_order_acquire)) { std::this_thread::yield(); }
                    // 每线程 new 一个 type；loser 的 type 由 registerPubTopic 首守卫在锁内 delete（P1 契约）
                    if (svc->invoke("/register_pub_topic",
                            YomkMkPtr(DDSTopic, DDSTopic{"a2_node", topic, new YomkRpc::MInt32PubSubType()}))
                            .m_status == YomkResponse::eOk)
                    {
                        okCount.fetch_add(1);
                    } });
            }
            go.store(true, std::memory_order_release);
            for (auto &th : ths)
            {
                th.join();
            }
            if (okCount.load() == 1)
            {
                ++oneWinnerRounds;
            }
        }
        CHECK(oneWinnerRounds == R,
              "A2 并发 register_pub_topic 同名：20 轮每轮恰 1 eOk、7 eNo（node mtx_ + dup 守卫 count()>0）");
        CHECK(svc->invoke("/delete_node", YomkMkPtr(String, "a2_node")).m_status == YomkResponse::eOk,
              "A2 删除 a2_node → eOk（清理 20 个竞胜 topic 的 writer；asan 验证 140 个 loser type 零泄漏）");
    }

    // ---- Part A3：混合并发（唯一 topic 注册 / publish / loan / discard / version）→ 计数全 eOk ----
    void partA3_mixedConcurrent(YomkRpcService *svc)
    {
        CHECK(svc->invoke("/create_node", mkNode(TEST_DOMAIN, "a3_node")).m_status == YomkResponse::eOk,
              "A3 创建 a3_node → eOk");
        constexpr int T = 8;
        constexpr int K = 200;
        std::atomic<bool> go{false};
        std::atomic<int> regOk{0}, pubOk{0}, loanOk{0}, discOk{0}, verOk{0};
        std::vector<std::thread> ths;
        ths.reserve(T);
        for (int t = 0; t < T; ++t)
        {
            ths.emplace_back([&, t]
                             {
                const std::string topic = "a3_pub_" + std::to_string(t); // 每线程唯一主题
                while (!go.load(std::memory_order_acquire)) { std::this_thread::yield(); }
                // 并发注册各自唯一主题（node mtx_ 序列化 pubTopics_/topics_ map 插入与 register_type）
                if (svc->invoke("/register_pub_topic",
                        YomkMkPtr(DDSTopic, DDSTopic{"a3_node", topic, new YomkRpc::MInt32PubSubType()}))
                        .m_status == YomkResponse::eOk)
                {
                    regOk.fetch_add(1);
                }
                YomkRpc::MInt32 msg; // 线程局部：publish 于 node mtx_ 内在本线程同步序列化读取，无跨线程竞争
                for (int i = 0; i < K; ++i)
                {
                    const int32_t v = t * 1000 + i;
                    msg.data(v);
                    if (svc->invoke("/publish", YomkMkPtr(DDSPublish, DDSPublish{"a3_node", topic, &msg}))
                            .m_status == YomkResponse::eOk)
                    {
                        pubOk.fetch_add(1);
                    }
                    // loan(MInt32,alignof=4)→池内填值→discard：并发借出各得独立池样本，复用的 happens-before 由 mtx_ 建立
                    auto lresp = svc->invoke("/loan", YomkMkPtr(DDSLoan, DDSLoan{"a3_node", topic, nullptr}));
                    YomkUnPackPkg(lresp.m_data, DDSLoanResult, lres);
                    void *s = (lres != nullptr) ? lres->msg.sample : nullptr;
                    if (lresp.m_status == YomkResponse::eOk && s != nullptr)
                    {
                        loanOk.fetch_add(1);
                        static_cast<YomkRpc::MInt32 *>(s)->data(v);
                        if (svc->invoke("/discard_loan", YomkMkPtr(DDSLoan, DDSLoan{"a3_node", topic, s}))
                                .m_status == YomkResponse::eOk)
                        {
                            discOk.fetch_add(1);
                        }
                    }
                    // version：只读端点不取 mtx_，与并发写共存（验证 getVersion 无锁读线程安全）
                    if (svc->invoke("/version", YomkMkPtr(String, "")).m_status == YomkResponse::eOk)
                    {
                        verOk.fetch_add(1);
                    }
                } });
        }
        go.store(true, std::memory_order_release);
        for (auto &th : ths)
        {
            th.join();
        }
        CHECK(regOk.load() == T, "A3 并发注册 8 个唯一 pub 主题全部 eOk（node mtx_ 序列化 map 插入）");
        CHECK(pubOk.load() == T * K, "A3 并发 publish 8*200=1600 全部 eOk（同一 node mtx_ 序列化 write）");
        CHECK(loanOk.load() == T * K, "A3 并发 loan 1600 全部 eOk（plain MInt32 池借出，8 线程并发不耗尽）");
        CHECK(discOk.load() == T * K, "A3 并发 discard_loan 1600 全部 eOk（归还池样本，无残留借出）");
        CHECK(verOk.load() == T * K, "A3 并发 version 1600 全部 eOk（只读无锁端点与写并发安全）");
        CHECK(svc->invoke("/delete_node", YomkMkPtr(String, "a3_node")).m_status == YomkResponse::eOk,
              "A3 删除 a3_node → eOk（终态一致）");
    }

    // ---- Part B：并发发布 + 订阅端到端（通用 take() reader 路径 + 回调契约在并发下）----
    void partB_concurrentPubSub(YomkRpcService *svc)
    {
        CHECK(svc->invoke("/create_node", mkNode(TEST_DOMAIN, "b_node")).m_status == YomkResponse::eOk,
              "B 创建 b_node → eOk");
        CHECK(svc->invoke("/register_pub_topic",
                          YomkMkPtr(DDSTopic, DDSTopic{"b_node", "b_topic", new YomkRpc::MInt32PubSubType()}))
                      .m_status == YomkResponse::eOk,
              "B register_pub_topic b_topic(MInt32) → eOk");

        std::mutex mtx;           // 测试自有 mutex：隔离 tsan 对被测源码的判定
        std::vector<int32_t> got; // 收集所有收到值，供事后解码校验
        auto cb = [&](const void *d)
        {
            // 通用 take()：reader 线程 take()+return_loan 借出交付；MInt32 alignof≤4，base+4 仍 4 字节对齐，可安全 deref
            auto *m = static_cast<const YomkRpc::MInt32 *>(d);
            const int32_t v = m->data();
            std::lock_guard<std::mutex> lk(mtx);
            got.push_back(v);
        };
        CHECK(svc->invoke("/register_sub_topic",
                          YomkMkPtr(DDSSubRequest, DDSSubRequest{"b_node", "b_topic", new YomkRpc::MInt32PubSubType(), cb}))
                      .m_status == YomkResponse::eOk,
              "B register_sub_topic b_topic(callback) → eOk");

        std::this_thread::sleep_for(std::chrono::milliseconds(1500)); // 等待 discovery

        constexpr int P = 4;
        constexpr int K = 250;
        std::atomic<bool> go{false};
        std::atomic<int> pubOk{0};
        std::vector<std::thread> ths;
        ths.reserve(P);
        for (int t = 0; t < P; ++t)
        {
            ths.emplace_back([&, t]
                             {
                YomkRpc::MInt32 msg; // 线程局部
                while (!go.load(std::memory_order_acquire)) { std::this_thread::yield(); }
                for (int i = 0; i < K; ++i)
                {
                    msg.data(t * 1000 + i); // 全局唯一编码：tid*1000+seq
                    if (svc->invoke("/publish", YomkMkPtr(DDSPublish, DDSPublish{"b_node", "b_topic", &msg}))
                            .m_status == YomkResponse::eOk)
                    {
                        pubOk.fetch_add(1);
                    }
                } });
        }
        go.store(true, std::memory_order_release);
        for (auto &th : ths)
        {
            th.join();
        }
        CHECK(pubOk.load() == P * K, "B 4 线程并发 publish 同一 writer 4*250=1000 全部 eOk（node mtx_ 序列化 write）");

        std::this_thread::sleep_for(std::chrono::milliseconds(1500)); // 等待投递 drain

        int r = 0;
        bool allValid = true;
        {
            std::lock_guard<std::mutex> lk(mtx); // 锁内快照：与 reader 线程回调建立 happens-before，tsan 干净
            r = static_cast<int>(got.size());
            for (int32_t v : got)
            {
                const int tid = v / 1000;
                const int seq = v % 1000;
                if (tid < 0 || tid >= P || seq < 0 || seq >= K)
                {
                    allValid = false;
                    break;
                }
            }
        }
        CHECK(r > 0 && r <= P * K, "B 回调收到数 ∈ (0,1000]（BEST_EFFORT 高量并发可能丢样，抗抖动不假设全达）");
        CHECK(allValid, "B 所有收到值可解码(tid∈[0,4),seq∈[0,250))→ 单 reader 线程顺序 drain data_ 无并发撕裂/串值");
        std::cout << "[OBSERVE] B received=" << r << "/" << (P * K) << std::endl;

        CHECK(svc->invoke("/delete_node", YomkMkPtr(String, "b_node")).m_status == YomkResponse::eOk,
              "B 删除 b_node → eOk（析构 reader 停止回调，捕获状态仍存活）");
    }

    // ---- Part C：delete_node 与活跃回调竞态（最高风险 teardown）----
    void partC_teardownRace(YomkRpcService *svc)
    {
        constexpr int R = 10;
        // cb 捕获的状态为函数作用域，outlive 所有轮次与节点删除（teardown drain 后无回调再触碰）
        std::mutex mtx;
        long totalReceived = 0;
        int createOk = 0, regSubOk = 0, regPubOk = 0, delOk = 0;

        auto cb = [&](const void *d)
        {
            auto *m = static_cast<const YomkRpc::MInt32 *>(d);
            (void)m->data();
            std::lock_guard<std::mutex> lk(mtx);
            ++totalReceived;
        };

        for (int r = 0; r < R; ++r)
        {
            if (svc->invoke("/create_node", mkNode(TEST_DOMAIN, "c_node")).m_status == YomkResponse::eOk)
            {
                ++createOk;
            }
            if (svc->invoke("/register_sub_topic",
                            YomkMkPtr(DDSSubRequest, DDSSubRequest{"c_node", "c_topic", new YomkRpc::MInt32PubSubType(), cb}))
                    .m_status == YomkResponse::eOk)
            {
                ++regSubOk;
            }
            if (svc->invoke("/register_pub_topic",
                            YomkMkPtr(DDSTopic, DDSTopic{"c_node", "c_topic", new YomkRpc::MInt32PubSubType()}))
                    .m_status == YomkResponse::eOk)
            {
                ++regPubOk;
            }

            std::this_thread::sleep_for(std::chrono::milliseconds(1000)); // 等待 discovery，使回调可被触发

            std::atomic<bool> stop{false};
            std::thread pub([&] // 按值捕获 r（轮内 join，r 稳定）；svc/stop 按引用
                            {
                YomkRpc::MInt32 msg;
                while (!stop.load(std::memory_order_acquire))
                {
                    msg.data(r);
                    svc->invoke("/publish", YomkMkPtr(DDSPublish, DDSPublish{"c_node", "c_topic", &msg}));
                    std::this_thread::sleep_for(std::chrono::milliseconds(2));
                } });

            std::this_thread::sleep_for(std::chrono::milliseconds(300)); // 让回调处于活跃触发中

            // teardown 竞态核心：发布流 + 回调进行中删除节点
            // （~FastDDSNode 持 service mtx_，先 delete_datareader 阻塞 drain 回调，后 delete_data）
            if (svc->invoke("/delete_node", YomkMkPtr(String, "c_node")).m_status == YomkResponse::eOk)
            {
                ++delOk;
            }

            stop.store(true, std::memory_order_release);
            pub.join(); // delete 后 publisher 若再 invoke 命中 nodes_.find 失败 → eNo（无害）
        }

        CHECK(createOk == R && regSubOk == R && regPubOk == R,
              "C 10 轮 create/register_sub/register_pub 全部 eOk（每轮重建竞态现场）");
        CHECK(delOk == R, "C delete_node 与活跃回调并发：10 轮全部 eOk（teardown 无崩溃/无死锁）");

        long receivedSnapshot = 0;
        {
            std::lock_guard<std::mutex> lk(mtx); // 锁内快照，tsan 干净
            receivedSnapshot = totalReceived;
        }
        CHECK(receivedSnapshot > 0, "C 回调在 teardown 前确有触发（竞态窗口被实际命中，非空跑）");
        std::cout << "[OBSERVE] C totalReceived=" << receivedSnapshot << " over " << R << " rounds" << std::endl;
        // asan：无 UAF（reader 未在 delete_data 后触碰 data_）；tsan：无 race（delete_datareader drain 建立 happens-before）
    }
} // namespace

int main()
{
    YOMK_INIT();

    // 注册被测服务：所有权移交框架（shared_ptr 持有），init() 内部调用后 weak_from_this() 方有效
    auto *svc = new YomkRpcService(YOMK_SERVER_P);
    CHECK(YOMK_ADD_SERVICE(svc) == 0, "YomkRpcService 注册成功（所有权移交框架，init() 已内部调用）");

    partA1_concurrentCreateNode(svc);
    partA2_concurrentRegister(svc);
    partA3_mixedConcurrent(svc);
    partB_concurrentPubSub(svc);
    partC_teardownRace(svc);

    return testReport("TestYomkRpcConcurrency");
}
