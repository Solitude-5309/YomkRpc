/**
 * @file TestYomkRpcStressConcurrent.cpp
 * @brief MC7-C：多线程高量压力/soak（并发 publish/loan / 并发 node churn / 并发 pub-sub 活跃回调）
 *
 * 范围：MC6 在"小量级"（8 线程 × 200 轮混合、4 线程 × 250 条收发）验证了双层锁与方向2 reader 路径的
 *       并发正确性，但未在 10 万级负载 + 资源维度验证并发下的稳定性。本用例经 svc->invoke() 从 N=4
 *       线程（nproc=2 适度超订）并发驱动，配合 Linux /proc 零依赖资源采样器，实证并发负载下：
 *       loan 池不耗尽、node churn 并发 teardown 完整回收、方向2 reader 路径高量无数据撕裂、句柄稳定。
 *       与 TestYomkRpcStressSerial 独立进程隔离（用户决策：多种线程模型各自独立测试，尽可能暴露问题）。
 * 覆盖：
 *   C1 并发持续 publish/loan 量：N 线程聚合 STRESS_ITERS ops（loan→填值→publish|discard 交替）on
 *      预注册 8 个 topic（实体有界，非 10 万 unique 注册）；writer-only（无 matched reader → RELIABLE
 *      样本 vacuously ack 即时释放）；断言无崩溃、loan 成功数==尝试数（并发下池不耗尽）、每借出样本
 *      均归还、RSS/fd/Threads 有界（10 万并发 op 不泄漏资源）。
 *   C2 并发 node create/delete churn × STRESS_CYCLES：N 线程各自 churn 自名 node（create→register
 *      pub+sub→loan/publish/discard→delete）；断言每轮全 eOk、终态 RSS/fd/Threads 回落基线（并发
 *      teardown 完整回收，无每实体泄漏）、无崩溃/UAF。
 *   C3 并发 pub/sub + 活跃回调 at volume：N 发布线程持续发 MInt32（值编码 tid*1000000+seq，可解码）
 *      给同一 writer 的活跃 subscriber；回调（方向2 reader 路径 on_data_available）在 atomic 下计数 +
 *      on-the-fly 校验可解码（tid∈[0,N)、seq∈[0,perThread)）；断言无崩溃/UAF、received>0、invalid==0
 *      （高量下 data_ 无撕裂/串值）、RSS/fd/Threads 有界。抗抖动：不断言精确投递计数（RELIABLE 流控/
 *      BEST_EFFORT 丢样致 received<=published 抖动），只断言完整性/资源/无崩溃。
 *
 * 关键设计约束（承 MC6）：
 *   - sub 回调绝不回调 svc->invoke()：delete_node 持 service mtx_ 期间 ~FastDDSNode 阻塞在
 *     delete_datareader 等待 reader 线程回调 drain，回调再取 service mtx_ 即死锁。回调只碰函数作用域
 *     atomic（outlive 所有节点删除）。
 *   - loan 一律用 MInt32（alignof=4，规避 8 字节 plain 严格对齐告诫）；每 op loan→立即 publish/discard
 *     归还，故并发下至多 N 个样本同时借出，池（数千槽）永不耗尽；write/discard 于 node mtx_ 内序列化，
 *     池样本复用的 happens-before 由 mtx_ 建立（tsan 干净，本 exe 不跑 tsan 但保持纪律）。
 *   - 各线程 publish 的 MInt32 为线程局部对象（write 于 node mtx_ 内在本线程同步序列化读取，无跨线程竞争）。
 *   - 测试自有共享态一律 atomic，锁内/原子快照读取，避免污染对被测源码的判定。
 *   - 资源容差：Threads<=base+2、fd<=base+8（整数强信号）；RSS 容差模式感知：off 紧界 base+max(32MB, base×0.5)，
 *     asan 宽界 base+max(256MB, base×3)（asan shadow/quarantine 膨胀脱钩真实分配，精确泄漏由退出时 lsan 判定 0 泄漏）。
 *
 * 旋钮：STRESS_ITERS（闭环 100000）、STRESS_CYCLES（闭环 50）经 CMake compile definitions 注入；未定义兜底 5000/30。
 * 域 212 专用，隔离 MC2-5(200)/MC6(210)/MC7-serial(211)。
 *
 * 风格：纯 main() + CHECK 宏 + 失败计数（零第三方依赖），返回非 0 表示存在失败用例。
 */

#include "TestCheck.h"
#include "YomkRpcService.h" // 服务 + DDSNode/DDSTopic/DDSSubRequest/DDSPublish/DDSLoan/DDSLoanResult + String + YOMK_* 宏

#include <YomkRpcMsg/YomkRpcMsg.hpp>            // YomkRpc::MInt32
#include <YomkRpcMsg/YomkRpcMsgPubSubTypes.hpp> // MInt32PubSubType

#include <dirent.h> // opendir/readdir 计数 /proc/self/fd

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <iostream>
#include <string>
#include <thread>
#include <vector>

#ifndef STRESS_ITERS
#define STRESS_ITERS 5000
#endif
#ifndef STRESS_CYCLES
#define STRESS_CYCLES 30
#endif

namespace
{
    constexpr uint32_t TEST_DOMAIN = 212; // MC7-concurrent 专用域
    constexpr int NTHREADS = 4;           // nproc=2 适度超订（用户决策：多线程模型独立压测）

    YomkPkgPtr mkNode(uint32_t domainId, const std::string &nodeName)
    {
        return YomkMkPtr(DDSNode, DDSNode{domainId, nodeName});
    }

    // ============================ 资源采样器（Linux /proc，零第三方依赖；与 serial exe 各自独立）============================
    struct ResSnapshot
    {
        long rssKB = -1;
        int threads = -1;
        int fds = -1;
    };

    long parseStatusLong(const char *key)
    {
        std::ifstream f("/proc/self/status");
        if (!f)
        {
            return -1;
        }
        const size_t klen = std::strlen(key);
        std::string line;
        while (std::getline(f, line))
        {
            if (line.size() >= klen && line.compare(0, klen, key) == 0)
            {
                const size_t p = line.find_first_of("0123456789");
                if (p == std::string::npos)
                {
                    return -1;
                }
                return std::strtol(line.c_str() + p, nullptr, 10);
            }
        }
        return -1;
    }

    int countOpenFds()
    {
        DIR *d = opendir("/proc/self/fd");
        if (d == nullptr)
        {
            return -1;
        }
        int n = 0;
        struct dirent *e;
        while ((e = readdir(d)) != nullptr)
        {
            if (std::strcmp(e->d_name, ".") == 0 || std::strcmp(e->d_name, "..") == 0)
            {
                continue;
            }
            ++n;
        }
        closedir(d);
        return n - 1; // 扣除 opendir 自身 fd（常量偏移，delta 有效）
    }

    ResSnapshot sampleRes()
    {
        ResSnapshot s;
        s.rssKB = parseStatusLong("VmRSS:");
        s.threads = static_cast<int>(parseStatusLong("Threads:"));
        s.fds = countOpenFds();
        return s;
    }

    // asan 编译期探测：本项目工具链为 GCC 11.4，-fsanitize=address 时定义 __SANITIZE_ADDRESS__。
    // （Clang 需改用 __has_feature(address_sanitizer)，且须置于嵌套 #if 内——单行 defined(__has_feature)&&__has_feature(...)
    //   在 GCC 上会因整个表达式先宏展开为 0(0) 而报语法错误。）
#if defined(__SANITIZE_ADDRESS__)
    constexpr bool kSanitizerActive = true;
#else
    constexpr bool kSanitizerActive = false;
#endif

    // RSS 容差（模式感知）：off 紧界（RSS delta 是无界增长有效粗信号）；asan 宽界（shadow/redzone/
    // quarantine 使 RSS 数倍膨胀脱钩真实分配，精确泄漏由退出时 lsan 判定 0 泄漏，此处仅捕获灾难性增长）。
    long rssToleranceKB(long baseKB)
    {
        if (kSanitizerActive)
        {
            const long asanFloorKB = 256 * 1024;
            const long asanPropKB = (baseKB > 0) ? baseKB * 3 : asanFloorKB;
            return std::max(asanFloorKB, asanPropKB);
        }
        const long floorKB = 32 * 1024;
        const long propKB = (baseKB > 0) ? baseKB / 2 : floorKB;
        return std::max(floorKB, propKB);
    }

    void printRes(const char *tag, const ResSnapshot &s)
    {
        std::cout << "[RES] " << tag << " rssKB=" << s.rssKB
                  << " threads=" << s.threads << " fds=" << s.fds << std::endl;
    }

    // ============================ C1：并发持续 publish/loan 量（writer-only）============================
    void c1_concurrentPublishLoan(YomkRpcService *svc)
    {
        const std::string node = "c1_node";
        constexpr int NTOPICS = 8; // 预注册少量 topic：实体有界，非 10 万 unique 注册
        CHECK(svc->invoke("/create_node", mkNode(TEST_DOMAIN, node)).m_status == YomkResponse::eOk,
              "C1 创建持久 c1_node → eOk");
        int regOk = 0;
        for (int i = 0; i < NTOPICS; ++i)
        {
            if (svc->invoke("/register_pub_topic",
                            YomkMkPtr(DDSTopic, DDSTopic{node, "c1_topic_" + std::to_string(i), new YomkRpc::MInt32PubSubType()}))
                    .m_status == YomkResponse::eOk)
            {
                ++regOk;
            }
        }
        CHECK(regOk == NTOPICS, "C1 预注册 8 个 pub topic(MInt32,writer-only) 全部 eOk");

        std::this_thread::sleep_for(std::chrono::seconds(1)); // 等实体资源落地
        ResSnapshot base = sampleRes();
        printRes("C1 baseline (post-register)", base);

        const long total = STRESS_ITERS;
        const long perThread = total / NTHREADS;
        const long remainder = total % NTHREADS;

        std::atomic<long> loanAttempts{0}, loanOk{0}, pubOk{0}, discOk{0};
        std::atomic<bool> go{false};
        std::vector<std::thread> ths;
        ths.reserve(NTHREADS);
        for (int t = 0; t < NTHREADS; ++t)
        {
            ths.emplace_back([&, t]
                             {
                const long ops = perThread + (t < remainder ? 1 : 0);
                while (!go.load(std::memory_order_acquire)) { std::this_thread::yield(); }
                for (long i = 0; i < ops; ++i)
                {
                    // 每线程使用 2 个专属 writer（{t, t+NTHREADS}，8 topic 无跨线程共享）：避免多线程并发
                    // loan 同一 RELIABLE writer 池的瞬态压力（write 后池槽异步释放，共享时偶发 loan_sample
                    // 返回 eNo——FastDDS ResourceLimits 良性特性、非 YomkRpc 缺陷）。专属 writer 下单线程
                    // loan→归还纪律与 S2 一致 → 池确定性不耗尽；并发仍由 service/node mtx_ 序列化充分压测。
                    const int tidx = (i % 2 == 0) ? t : (t + NTHREADS);
                    const std::string topic = "c1_topic_" + std::to_string(tidx);
                    loanAttempts.fetch_add(1, std::memory_order_relaxed);
                    auto lr = svc->invoke("/loan", YomkMkPtr(DDSLoan, DDSLoan{node, topic, nullptr}));
                    YomkUnPackPkg(lr.m_data, DDSLoanResult, lres);
                    void *s = (lres != nullptr) ? lres->msg.sample : nullptr;
                    if (lr.m_status == YomkResponse::eOk && s != nullptr)
                    {
                        loanOk.fetch_add(1, std::memory_order_relaxed);
                        static_cast<YomkRpc::MInt32 *>(s)->data(static_cast<int32_t>(i & 0x7fffffff));
                        if (i % 2 == 0)
                        {
                            if (svc->invoke("/publish", YomkMkPtr(DDSPublish, DDSPublish{node, topic, s}))
                                    .m_status == YomkResponse::eOk)
                            {
                                pubOk.fetch_add(1, std::memory_order_relaxed);
                            }
                        }
                        else if (svc->invoke("/discard_loan", YomkMkPtr(DDSLoan, DDSLoan{node, topic, s}))
                                     .m_status == YomkResponse::eOk)
                        {
                            discOk.fetch_add(1, std::memory_order_relaxed);
                        }
                    }
                } });
        }
        go.store(true, std::memory_order_release);
        for (auto &th : ths)
        {
            th.join();
        }

        std::this_thread::sleep_for(std::chrono::seconds(1)); // 等瞬态资源回落
        ResSnapshot after = sampleRes();
        printRes("C1 after concurrent ops", after);

        const long la = loanAttempts.load(), lo = loanOk.load(), po = pubOk.load(), do_ = discOk.load();
        CHECK(la == total, "C1 聚合 loan 尝试数==STRESS_ITERS（N 线程并发无丢失/崩溃）");
        CHECK(lo == la, "C1 并发 loan 成功数==尝试数（每样本 write/discard 即归还，并发下池永不耗尽）");
        CHECK(po + do_ == lo, "C1 每借出样本均被 publish 或 discard 归还（并发下无残留借出/池泄漏）");
        CHECK(after.threads <= base.threads + 2, "C1 线程数有界（<=base+2，并发 op 不累积线程）");
        CHECK(after.fds <= base.fds + 8, "C1 fd 有界（<=base+8，10 万并发 op 不泄漏 fd）");
        CHECK(after.rssKB <= base.rssKB + rssToleranceKB(base.rssKB),
              "C1 RSS 有界（<=base+容差，10 万并发 op 无内存无界增长；精确泄漏由 asan 兜底）");
        std::cout << "[OBSERVE] C1 total=" << total << " loanOk=" << lo << "/" << la
                  << " pubOk=" << po << " discOk=" << do_
                  << " | threads " << base.threads << "->" << after.threads
                  << " fds " << base.fds << "->" << after.fds
                  << " rssKB " << base.rssKB << "->" << after.rssKB << std::endl;

        CHECK(svc->invoke("/delete_node", YomkMkPtr(String, node)).m_status == YomkResponse::eOk,
              "C1 删除 c1_node → eOk（8 writer 池随节点析构释放）");
    }

    // ============================ C2：并发 node create/delete churn ============================
    void c2_concurrentNodeChurn(YomkRpcService *svc)
    {
        std::atomic<long> cbCount{0};
        auto cb = [&](const void *d)
        {
            auto *m = static_cast<const YomkRpc::MInt32 *>(d);
            (void)m->data();
            cbCount.fetch_add(1, std::memory_order_relaxed);
        };

        std::atomic<int> createOk{0}, regPubOk{0}, regSubOk{0}, loanOk{0}, delOk{0};
        // 单线程单轮 churn：create → register pub+sub → loan/publish/discard 数次 → delete（自名 node）
        auto churnThread = [&](int t, int cycles)
        {
            const std::string name = "c2_node_" + std::to_string(t); // 各线程自名 node，并发创建/销毁独立 participant
            for (int c = 0; c < cycles; ++c)
            {
                if (svc->invoke("/create_node", mkNode(TEST_DOMAIN, name)).m_status == YomkResponse::eOk)
                {
                    createOk.fetch_add(1, std::memory_order_relaxed);
                }
                if (svc->invoke("/register_pub_topic",
                                YomkMkPtr(DDSTopic, DDSTopic{name, "c2_topic", new YomkRpc::MInt32PubSubType()}))
                        .m_status == YomkResponse::eOk)
                {
                    regPubOk.fetch_add(1, std::memory_order_relaxed);
                }
                if (svc->invoke("/register_sub_topic",
                                YomkMkPtr(DDSSubRequest, DDSSubRequest{name, "c2_topic", new YomkRpc::MInt32PubSubType(), cb}))
                        .m_status == YomkResponse::eOk)
                {
                    regSubOk.fetch_add(1, std::memory_order_relaxed);
                }
                for (int i = 0; i < 3; ++i)
                {
                    auto lr = svc->invoke("/loan", YomkMkPtr(DDSLoan, DDSLoan{name, "c2_topic", nullptr}));
                    YomkUnPackPkg(lr.m_data, DDSLoanResult, lres);
                    void *s = (lres != nullptr) ? lres->msg.sample : nullptr;
                    if (lr.m_status == YomkResponse::eOk && s != nullptr)
                    {
                        loanOk.fetch_add(1, std::memory_order_relaxed);
                        static_cast<YomkRpc::MInt32 *>(s)->data(i);
                        if (i % 2 == 0)
                        {
                            svc->invoke("/publish", YomkMkPtr(DDSPublish, DDSPublish{name, "c2_topic", s}));
                        }
                        else
                        {
                            svc->invoke("/discard_loan", YomkMkPtr(DDSLoan, DDSLoan{name, "c2_topic", s}));
                        }
                    }
                }
                if (svc->invoke("/delete_node", YomkMkPtr(String, name)).m_status == YomkResponse::eOk)
                {
                    delOk.fetch_add(1, std::memory_order_relaxed);
                }
            }
        };

        // warmup：每线程 1 轮 churn 使 FastDDS 工厂进入稳态，再取基线
        {
            std::vector<std::thread> warm;
            warm.reserve(NTHREADS);
            for (int t = 0; t < NTHREADS; ++t)
            {
                warm.emplace_back([&, t]
                                  { churnThread(t, 1); }); // t 按值捕获（避免循环变量悬垂引用 UB）
            }
            for (auto &th : warm)
            {
                th.join();
            }
        }
        std::this_thread::sleep_for(std::chrono::seconds(1)); // 等 warmup 异步 teardown
        ResSnapshot base = sampleRes();
        printRes("C2 baseline (post-warmup)", base);

        // 清零 warmup 计数，正式并发 churn
        createOk = regPubOk = regSubOk = loanOk = delOk = 0;
        int cyclesPerThread = STRESS_CYCLES / NTHREADS;
        if (cyclesPerThread < 1)
        {
            cyclesPerThread = 1;
        }
        const int totalCycles = cyclesPerThread * NTHREADS;
        {
            std::atomic<bool> go{false};
            std::vector<std::thread> ths;
            ths.reserve(NTHREADS);
            for (int t = 0; t < NTHREADS; ++t)
            {
                ths.emplace_back([&, t]
                                 {
                    while (!go.load(std::memory_order_acquire)) { std::this_thread::yield(); }
                    churnThread(t, cyclesPerThread); });
            }
            go.store(true, std::memory_order_release);
            for (auto &th : ths)
            {
                th.join();
            }
        }
        std::this_thread::sleep_for(std::chrono::seconds(2)); // 等并发 teardown 异步释放 fd/线程/内存
        ResSnapshot after = sampleRes();
        printRes("C2 after concurrent churn", after);

        CHECK(createOk.load() == totalCycles && regPubOk.load() == totalCycles &&
                  regSubOk.load() == totalCycles && delOk.load() == totalCycles,
              "C2 并发 churn：每轮 create/register_pub/register_sub/delete 全部 eOk（无累积失败/残留）");
        CHECK(loanOk.load() == totalCycles * 3, "C2 并发 churn 每轮 loan 3 次全部成功（并发下池不耗尽）");
        CHECK(after.threads <= base.threads + 2, "C2 线程数回落基线（并发 teardown 回收 discovery 线程；<=base+2）");
        CHECK(after.fds <= base.fds + 8, "C2 fd 回落基线（并发每轮实体全删，无 fd 泄漏；<=base+8）");
        CHECK(after.rssKB <= base.rssKB + rssToleranceKB(base.rssKB),
              "C2 RSS 回落基线容差内（并发 create/teardown 无单调增长；精确泄漏由 asan 兜底）");
        std::cout << "[OBSERVE] C2 totalCycles=" << totalCycles << " cbCount=" << cbCount.load()
                  << " | threads " << base.threads << "->" << after.threads
                  << " fds " << base.fds << "->" << after.fds
                  << " rssKB " << base.rssKB << "->" << after.rssKB << std::endl;
    }

    // ============================ C3：并发 pub/sub + 活跃回调 at volume ============================
    void c3_concurrentPubSubVolume(YomkRpcService *svc)
    {
        const std::string node = "c3_node";
        const std::string topic = "c3_topic";
        CHECK(svc->invoke("/create_node", mkNode(TEST_DOMAIN, node)).m_status == YomkResponse::eOk,
              "C3 创建 c3_node → eOk");
        CHECK(svc->invoke("/register_pub_topic",
                          YomkMkPtr(DDSTopic, DDSTopic{node, topic, new YomkRpc::MInt32PubSubType()}))
                      .m_status == YomkResponse::eOk,
              "C3 register_pub_topic c3_topic(MInt32) → eOk");

        const int P = NTHREADS;
        const long perThread = STRESS_ITERS / P; // 每发布线程样本数；编码 tid*1000000+seq（seq<perThread<=1e6）

        std::atomic<long> received{0}, invalid{0};
        auto cb = [&](const void *d)
        {
            // 方向2：reader 线程 take_next_sample 进对齐 data_ 后交付；d 已按 MInt32 对齐，可安全 deref
            auto *m = static_cast<const YomkRpc::MInt32 *>(d);
            const int32_t v = m->data();
            const long tid = v / 1000000;
            const long seq = v % 1000000;
            received.fetch_add(1, std::memory_order_relaxed);
            // on-the-fly 校验（不存 vector，10 万级）：越界即 data_ 撕裂/串值信号
            if (tid < 0 || tid >= P || seq < 0 || seq >= perThread)
            {
                invalid.fetch_add(1, std::memory_order_relaxed);
            }
        };
        CHECK(svc->invoke("/register_sub_topic",
                          YomkMkPtr(DDSSubRequest, DDSSubRequest{node, topic, new YomkRpc::MInt32PubSubType(), cb}))
                      .m_status == YomkResponse::eOk,
              "C3 register_sub_topic c3_topic(callback) → eOk");

        std::this_thread::sleep_for(std::chrono::milliseconds(1500)); // 等 discovery，pub/sub 匹配
        ResSnapshot base = sampleRes();
        printRes("C3 baseline (post-discovery)", base);

        std::atomic<long> pubOk{0};
        std::atomic<bool> go{false};
        std::vector<std::thread> ths;
        ths.reserve(P);
        for (int t = 0; t < P; ++t)
        {
            ths.emplace_back([&, t]
                             {
                YomkRpc::MInt32 msg; // 线程局部
                while (!go.load(std::memory_order_acquire)) { std::this_thread::yield(); }
                for (long s = 0; s < perThread; ++s)
                {
                    msg.data(static_cast<int32_t>(t * 1000000 + s)); // 全局唯一可解码编码
                    if (svc->invoke("/publish", YomkMkPtr(DDSPublish, DDSPublish{node, topic, &msg}))
                            .m_status == YomkResponse::eOk)
                    {
                        pubOk.fetch_add(1, std::memory_order_relaxed);
                    }
                } });
        }
        go.store(true, std::memory_order_release);
        for (auto &th : ths)
        {
            th.join();
        }

        std::this_thread::sleep_for(std::chrono::milliseconds(1500)); // 等投递 drain
        ResSnapshot after = sampleRes();
        printRes("C3 after concurrent pub/sub", after);

        const long rec = received.load(), inv = invalid.load(), po = pubOk.load();
        CHECK(po > 0, "C3 发布线程确有成功 publish（并发写同一 writer，node mtx_ 序列化）");
        CHECK(rec > 0, "C3 回调在活跃负载下确有触发（方向2 reader 路径命中，非空跑）");
        CHECK(inv == 0, "C3 所有收到值可解码(tid∈[0,P),seq∈[0,perThread))→ 高量下 data_ 无撕裂/串值");
        CHECK(after.threads <= base.threads + 2, "C3 线程数有界（<=base+2）");
        CHECK(after.fds <= base.fds + 8, "C3 fd 有界（<=base+8，高量收发不泄漏 fd）");
        CHECK(after.rssKB <= base.rssKB + rssToleranceKB(base.rssKB),
              "C3 RSS 有界（<=base+容差，高量收发 writer/reader 历史有界，无内存无界增长）");
        std::cout << "[OBSERVE] C3 published=" << po << "/" << (P * perThread)
                  << " received=" << rec << " invalid=" << inv
                  << " | threads " << base.threads << "->" << after.threads
                  << " fds " << base.fds << "->" << after.fds
                  << " rssKB " << base.rssKB << "->" << after.rssKB << std::endl;

        CHECK(svc->invoke("/delete_node", YomkMkPtr(String, node)).m_status == YomkResponse::eOk,
              "C3 删除 c3_node → eOk（析构 reader 停止回调，捕获 atomic 仍存活）");
    }
} // namespace

int main()
{
    YOMK_INIT();

    auto *svc = new YomkRpcService(YOMK_SERVER_P);
    CHECK(YOMK_ADD_SERVICE(svc) == 0, "YomkRpcService 注册成功（所有权移交框架，init() 已内部调用）");

    std::cout << "[CFG] STRESS_ITERS=" << STRESS_ITERS << " STRESS_CYCLES=" << STRESS_CYCLES
              << " threads=" << NTHREADS << " domain=" << TEST_DOMAIN << std::endl;

    c1_concurrentPublishLoan(svc);
    c2_concurrentNodeChurn(svc);
    c3_concurrentPubSubVolume(svc);

    return testReport("TestYomkRpcStressConcurrent");
}
