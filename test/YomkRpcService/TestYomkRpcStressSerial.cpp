/**
 * @file TestYomkRpcStressSerial.cpp
 * @brief MC7-S：单线程压力/soak（node 生命周期 churn / 持续 publish+loan / 实体增长完整回收）
 *
 * 范围：MC0-MC6 均在"小量级"下验证功能/契约/并发正确性，但负载下的资源稳定性（无泄漏、
 *       无内存无界增长、loan 池不耗尽、吞吐不退化、fd/thread 句柄稳定）从未被系统验证。
 *       本用例经 svc->invoke() 白盒，在单线程下对 YomkRpc 自有源码做 soak：以 Linux /proc 零依赖
 *       资源采样器（VmRSS/Threads/fd）量化"负载前基线 vs 负载后终态"，实证 create/teardown 无泄漏、
 *       持续 publish/loan 池不耗尽且吞吐稳定、大量实体注册后 ~FastDDSNode 完整回收。
 * 覆盖：
 *   S1 node 生命周期 soak × STRESS_CYCLES：warmup 数轮进入稳态后快照基线，再跑
 *      [create_node → register 1 pub(MInt32)+1 sub(回调计数) → loan/publish/discard 数次 → delete_node]
 *      循环；断言终态 Threads/fd/RSS 回落基线容差内（每轮销毁全部实体 → 对 DDS writer 历史 QoS 免疫，
 *      资源回收是核心证明；node churn 是 FastDDSNode 无单实体注销 API 下唯一可整体回收的路径）。
 *   S2 持续 publish + loan/discard（持久 node，writer-only 预注册 1 topic）× STRESS_ITERS：
 *      warmup→计时 early 窗口→bulk→计时 late 窗口；每 op = loan→填值→(publish 或 discard 交替)。
 *      断言：主环无崩溃、loan 成功数==尝试数（每样本 write/discard 即归还，池永不耗尽）、
 *      每借出样本均被归还、吞吐 late>=early×0.5（无严重退化）；RSS 仅信息记录不硬断言
 *      （writer-only 无 matched reader → RELIABLE 样本被vacuously ack 即时释放，历史有界；
 *      精确泄漏由退出时 asan/lsan + S1/S3 回收断言覆盖）。
 *   S3 实体增长 + 完整回收：单 node 注册 K=200 unique pub + K sub（共 400 实体），快照 RSS/fd（随 K
 *      增长），delete_node 后再快照；断言 RSS/fd/Threads 回落注册前基线容差内（~FastDDSNode 完整回收
 *      topics/writers/readers/data，无每实体泄漏）。
 *
 * 关键设计约束：
 *   - sub 回调绝不回调 svc->invoke()：delete_node 持 service mtx_ 期间 ~FastDDSNode 阻塞在
 *     delete_datareader 等待 reader 线程回调 drain，若回调再取 service mtx_ 即死锁。回调只碰
 *     函数作用域的 atomic 计数（outlive 所有节点删除）。
 *   - loan 一律用 MInt32（alignof=4，loan 指针 base+4 天然满足 4 对齐，规避 8 字节 plain 类型严格
 *     对齐告诫——与 MC4/MC6 一致）。
 *   - 资源容差：Threads<=base+2、fd<=base+8（整数强信号，无碎片噪声，捕获单调增长）；RSS 容差模式感知：
 *     off 紧界 base+max(32MB, base×0.5)（RSS delta 是无界增长有效粗信号）；asan 宽界 base+max(256MB, base×3)
 *     （asan shadow/redzone/quarantine 使 RSS 数倍膨胀脱钩真实分配，精确泄漏由退出时 lsan 判定 0 泄漏）。
 *   - churn/回收后 settle sleep（1-2s）待 FastDDS 异步释放 fd/线程/内存再采样。
 *
 * 旋钮：STRESS_ITERS（廉价 op 量级，闭环设 100000）、STRESS_CYCLES（churn 轮数，discovery-bound，
 *   闭环设 50）经 CMake compile definitions 注入；未定义时兜底默认（5000/30）。
 *
 * 关键不变式：每个 eOk 创建的节点必须在函数返回前经 /delete_node 显式删除；不遗留未归还的 loan
 *   （借出即 publish 或 discard）。域 211 专用，隔离 MC2-5(200)/MC6(210)/MC7-concurrent(212)。
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

#ifndef STRESS_ITERS
#define STRESS_ITERS 5000
#endif
#ifndef STRESS_CYCLES
#define STRESS_CYCLES 30
#endif

namespace
{
    // MC7-serial 专用域，隔离 MC2-5(200)/MC6(210)/MC7-concurrent(212)，避免残留 participant 干扰 discovery
    constexpr uint32_t TEST_DOMAIN = 211;

    // 构造 /create_node 请求包
    YomkPkgPtr mkNode(uint32_t domainId, const std::string &nodeName)
    {
        return YomkMkPtr(DDSNode, DDSNode{domainId, nodeName});
    }

    // ============================ 资源采样器（Linux /proc，零第三方依赖）============================
    struct ResSnapshot
    {
        long rssKB = -1;  // /proc/self/status VmRSS:（kB）
        int threads = -1; // /proc/self/status Threads:
        int fds = -1;     // /proc/self/fd 目录项计数（扣除 opendir 自身 fd）
    };

    // 解析 /proc/self/status 中以 key 前缀开头的行的首个整数（如 "VmRSS:"→kB，"Threads:"→个数）
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

    // 计数当前打开的 fd：遍历 /proc/self/fd 目录项，扣除 opendir 自身占用的 1 个 fd（常量偏移，delta 有效）。
    // 不用 status 的 FDSize（其为 fd 表容量、只增不减，无法反映实际释放）。
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
        return n - 1;
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

    // RSS 容差（模式感知）：
    //  - off：RSS delta 是“无内存无界增长”的有效粗信号，用紧界（32MB 地板 / 基线 50%）捕获单调增长；
    //  - asan：shadow memory + redzone + 释放隔离区(quarantine)使 RSS 数倍膨胀且随分配累计增长，与真实
    //    存活分配脱钩；精确泄漏改由退出时 lsan 判定（0 泄漏即无缺陷），此处仅以宽松界（256MB 地板 /
    //    基线 3×）捕获灾难性无界增长，避免 asan 内存模型导致 RSS 假阳性。
    long rssToleranceKB(long baseKB)
    {
        if (kSanitizerActive)
        {
            const long asanFloorKB = 256 * 1024;
            const long asanPropKB = (baseKB > 0) ? baseKB * 3 : asanFloorKB;
            return std::max(asanFloorKB, asanPropKB);
        }
        const long floorKB = 32 * 1024;                          // off：32MB 地板
        const long propKB = (baseKB > 0) ? baseKB / 2 : floorKB; // 或基线 50%
        return std::max(floorKB, propKB);
    }

    void printRes(const char *tag, const ResSnapshot &s)
    {
        std::cout << "[RES] " << tag << " rssKB=" << s.rssKB
                  << " threads=" << s.threads << " fds=" << s.fds << std::endl;
    }

    // ============================ S1：node 生命周期 soak ============================
    void s1_nodeLifecycleSoak(YomkRpcService *svc)
    {
        std::atomic<long> cbCount{0};
        auto cb = [&](const void *d)
        {
            // 回调在 reader 线程触发：只碰 atomic 计数，绝不 invoke（规避与持 service mtx_ 的 delete_node 死锁）
            auto *m = static_cast<const YomkRpc::MInt32 *>(d);
            (void)m->data();
            cbCount.fetch_add(1, std::memory_order_relaxed);
        };

        int createOk = 0, regPubOk = 0, regSubOk = 0, loanOk = 0, pubOk = 0, discOk = 0, delOk = 0;

        // 单轮 churn：create → register 1 pub(MInt32)+1 sub(callback) → loan/publish/discard 数次 → delete
        auto oneCycle = [&](const std::string &name)
        {
            if (svc->invoke("/create_node", mkNode(TEST_DOMAIN, name)).m_status == YomkResponse::eOk)
            {
                ++createOk;
            }
            if (svc->invoke("/register_pub_topic",
                            YomkMkPtr(DDSTopic, DDSTopic{name, "s1_topic", new YomkRpc::MInt32PubSubType()}))
                    .m_status == YomkResponse::eOk)
            {
                ++regPubOk;
            }
            if (svc->invoke("/register_sub_topic",
                            YomkMkPtr(DDSSubRequest, DDSSubRequest{name, "s1_topic", new YomkRpc::MInt32PubSubType(), cb}))
                    .m_status == YomkResponse::eOk)
            {
                ++regSubOk;
            }
            // 少量 loan→填值→(publish|discard)：每轮固定小量，聚焦 create/teardown 资源回收
            for (int i = 0; i < 5; ++i)
            {
                auto lr = svc->invoke("/loan", YomkMkPtr(DDSLoan, DDSLoan{name, "s1_topic", nullptr}));
                YomkUnPackPkg(lr.m_data, DDSLoanResult, lres);
                void *s = (lres != nullptr) ? lres->msg.sample : nullptr;
                if (lr.m_status == YomkResponse::eOk && s != nullptr)
                {
                    ++loanOk;
                    static_cast<YomkRpc::MInt32 *>(s)->data(i);
                    if (i % 2 == 0)
                    {
                        if (svc->invoke("/publish", YomkMkPtr(DDSPublish, DDSPublish{name, "s1_topic", s}))
                                .m_status == YomkResponse::eOk)
                        {
                            ++pubOk;
                        }
                    }
                    else if (svc->invoke("/discard_loan", YomkMkPtr(DDSLoan, DDSLoan{name, "s1_topic", s}))
                                 .m_status == YomkResponse::eOk)
                    {
                        ++discOk;
                    }
                }
            }
            if (svc->invoke("/delete_node", YomkMkPtr(String, name)).m_status == YomkResponse::eOk)
            {
                ++delOk;
            }
        };

        // warmup：数轮 churn 使 FastDDS 工厂线程/分配器进入稳态，再取基线（避免冷启动噪声污染基线）
        constexpr int WARMUP = 3;
        for (int i = 0; i < WARMUP; ++i)
        {
            oneCycle("s1_warm_node");
        }
        std::this_thread::sleep_for(std::chrono::seconds(1)); // 等 warmup 异步 teardown 释放 fd/线程
        ResSnapshot base = sampleRes();
        printRes("S1 baseline (post-warmup)", base);

        // 清零 warmup 计数，正式计量 STRESS_CYCLES 轮
        createOk = regPubOk = regSubOk = loanOk = pubOk = discOk = delOk = 0;
        const int CYCLES = STRESS_CYCLES;
        for (int c = 0; c < CYCLES; ++c)
        {
            oneCycle("s1_churn_node");
        }
        std::this_thread::sleep_for(std::chrono::seconds(2)); // 等最后一轮异步 teardown 完成
        ResSnapshot after = sampleRes();
        printRes("S1 after churn", after);

        CHECK(createOk == CYCLES && regPubOk == CYCLES && regSubOk == CYCLES && delOk == CYCLES,
              "S1 node churn：每轮 create/register_pub/register_sub/delete 全部 eOk（无累积失败/残留）");
        CHECK(loanOk == CYCLES * 5, "S1 每轮 loan 5 次全部成功（churn 下 plain MInt32 池不耗尽）");
        CHECK(pubOk + discOk == loanOk, "S1 每借出样本均经 publish 或 discard 归还（无残留借出）");
        CHECK(after.threads <= base.threads + 2,
              "S1 线程数回落基线（participant 销毁回收 discovery 线程；<=base+2）");
        CHECK(after.fds <= base.fds + 8,
              "S1 fd 回落基线（每轮实体全删，无 fd 泄漏；<=base+8）");
        CHECK(after.rssKB <= base.rssKB + rssToleranceKB(base.rssKB),
              "S1 RSS 回落基线容差内（create/teardown 无单调增长；精确泄漏由 asan 兜底）");
        std::cout << "[OBSERVE] S1 cycles=" << CYCLES << " cbCount=" << cbCount.load()
                  << " | threads " << base.threads << "->" << after.threads
                  << " fds " << base.fds << "->" << after.fds
                  << " rssKB " << base.rssKB << "->" << after.rssKB << std::endl;
    }

    // ============================ S2：持续 publish + loan/discard（writer-only）============================
    void s2_continuousPublishLoan(YomkRpcService *svc)
    {
        const std::string node = "s2_node";
        const std::string topic = "s2_topic";
        CHECK(svc->invoke("/create_node", mkNode(TEST_DOMAIN, node)).m_status == YomkResponse::eOk,
              "S2 创建持久 s2_node → eOk");
        CHECK(svc->invoke("/register_pub_topic",
                          YomkMkPtr(DDSTopic, DDSTopic{node, topic, new YomkRpc::MInt32PubSubType()}))
                      .m_status == YomkResponse::eOk,
              "S2 register_pub_topic s2_topic(MInt32,writer-only) → eOk");

        const long N = STRESS_ITERS;
        const long W = (N >= 10) ? N / 10 : 1; // 窗口大小：warmup/early/late 各 W，bulk=N-3W

        long loanAttempts = 0, loanOk = 0, pubOk = 0, discOk = 0;

        // 单 op：loan → 填值 → (publish 或 discard 交替)；每次 loan 前一样本必已归还，池只需 1 槽
        auto oneOp = [&](long i)
        {
            ++loanAttempts;
            auto lr = svc->invoke("/loan", YomkMkPtr(DDSLoan, DDSLoan{node, topic, nullptr}));
            YomkUnPackPkg(lr.m_data, DDSLoanResult, lres);
            void *s = (lres != nullptr) ? lres->msg.sample : nullptr;
            if (lr.m_status == YomkResponse::eOk && s != nullptr)
            {
                ++loanOk;
                static_cast<YomkRpc::MInt32 *>(s)->data(static_cast<int32_t>(i & 0x7fffffff));
                if (i % 2 == 0)
                {
                    if (svc->invoke("/publish", YomkMkPtr(DDSPublish, DDSPublish{node, topic, s}))
                            .m_status == YomkResponse::eOk)
                    {
                        ++pubOk;
                    }
                }
                else if (svc->invoke("/discard_loan", YomkMkPtr(DDSLoan, DDSLoan{node, topic, s}))
                             .m_status == YomkResponse::eOk)
                {
                    ++discOk;
                }
            }
        };

        ResSnapshot pre = sampleRes();
        printRes("S2 pre-loop", pre);

        long i = 0;
        for (long w = 0; w < W; ++w, ++i)
        {
            oneOp(i);
        } // warmup（不计时）
        auto t0 = std::chrono::steady_clock::now();
        for (long w = 0; w < W; ++w, ++i)
        {
            oneOp(i);
        } // early 窗口（计时）
        auto t1 = std::chrono::steady_clock::now();
        const long bulk = (N - 3 * W > 0) ? (N - 3 * W) : 0;
        for (long b = 0; b < bulk; ++b, ++i)
        {
            oneOp(i);
        } // bulk（不计时）
        auto t2 = std::chrono::steady_clock::now();
        for (long w = 0; w < W; ++w, ++i)
        {
            oneOp(i);
        } // late 窗口（计时）
        auto t3 = std::chrono::steady_clock::now();

        ResSnapshot post = sampleRes();
        printRes("S2 post-loop", post);

        const double earlyMs = std::chrono::duration<double, std::milli>(t1 - t0).count();
        const double lateMs = std::chrono::duration<double, std::milli>(t3 - t2).count();
        const double earlyOps = (earlyMs > 0) ? (W / (earlyMs / 1000.0)) : 0;
        const double lateOps = (lateMs > 0) ? (W / (lateMs / 1000.0)) : 0;

        CHECK(i == N, "S2 主环执行 STRESS_ITERS 次 op 无崩溃（loan+publish/discard 交替）");
        CHECK(loanOk == loanAttempts, "S2 loan 成功数==尝试数（每样本 write/discard 即归还，池永不耗尽）");
        CHECK(pubOk + discOk == loanOk, "S2 每借出样本均被 publish 或 discard 归还（无残留借出/池泄漏）");
        CHECK(lateMs > 0 && earlyMs > 0 && lateMs <= earlyMs * 2.0,
              "S2 吞吐无严重退化（late 窗口耗时 <= early×2，即 late ops/s >= early×0.5）");
        std::cout << "[OBSERVE] S2 N=" << N << " loanOk=" << loanOk << "/" << loanAttempts
                  << " pubOk=" << pubOk << " discOk=" << discOk
                  << " | early=" << static_cast<long>(earlyOps) << " ops/s late="
                  << static_cast<long>(lateOps) << " ops/s"
                  << " | rssKB " << pre.rssKB << "->" << post.rssKB << " (informational)" << std::endl;

        CHECK(svc->invoke("/delete_node", YomkMkPtr(String, node)).m_status == YomkResponse::eOk,
              "S2 删除 s2_node → eOk（writer 池随节点析构释放）");
    }

    // ============================ S3：实体增长 + 完整回收 ============================
    void s3_entityGrowthReclaim(YomkRpcService *svc)
    {
        const std::string node = "s3_node";
        constexpr int K = 200; // pub+sub 各 K，共 2K=400 实体（平衡运行时/discovery 与可测资源增长）

        std::this_thread::sleep_for(std::chrono::seconds(1)); // 确保 S1/S2 节点异步 teardown 完全落地
        ResSnapshot base = sampleRes();                       // 注册前基线
        printRes("S3 pre-register baseline", base);

        CHECK(svc->invoke("/create_node", mkNode(TEST_DOMAIN, node)).m_status == YomkResponse::eOk,
              "S3 创建 s3_node → eOk");

        std::atomic<long> cbCount{0};
        auto cb = [&](const void *d)
        {
            auto *m = static_cast<const YomkRpc::MInt32 *>(d);
            (void)m->data();
            cbCount.fetch_add(1, std::memory_order_relaxed);
        };

        int regPubOk = 0, regSubOk = 0;
        for (int i = 0; i < K; ++i)
        {
            const std::string t = "s3_topic_" + std::to_string(i);
            if (svc->invoke("/register_pub_topic",
                            YomkMkPtr(DDSTopic, DDSTopic{node, t, new YomkRpc::MInt32PubSubType()}))
                    .m_status == YomkResponse::eOk)
            {
                ++regPubOk;
            }
            if (svc->invoke("/register_sub_topic",
                            YomkMkPtr(DDSSubRequest, DDSSubRequest{node, t, new YomkRpc::MInt32PubSubType(), cb}))
                    .m_status == YomkResponse::eOk)
            {
                ++regSubOk;
            }
        }
        CHECK(regPubOk == K && regSubOk == K,
              "S3 注册 K=200 pub + 200 sub 唯一主题全部 eOk（共 400 实体）");

        std::this_thread::sleep_for(std::chrono::seconds(1)); // 等实体资源（fd/缓冲）落地
        ResSnapshot grown = sampleRes();
        printRes("S3 after register (grown)", grown);

        // sanity：注册 400 实体后资源确有增长（否则"回落基线"断言无意义——空跑也能过）
        CHECK(grown.fds > base.fds || grown.rssKB > base.rssKB,
              "S3 实体增长可观测（fd 或 RSS 较基线上升，证明注册确分配了资源）");

        CHECK(svc->invoke("/delete_node", YomkMkPtr(String, node)).m_status == YomkResponse::eOk,
              "S3 删除 s3_node → eOk（~FastDDSNode 整体回收 400 实体）");

        std::this_thread::sleep_for(std::chrono::seconds(2)); // 等异步 teardown 释放 fd/线程/内存
        ResSnapshot after = sampleRes();
        printRes("S3 after delete (reclaimed)", after);

        CHECK(after.threads <= base.threads + 2, "S3 线程数回落注册前基线（<=base+2）");
        CHECK(after.fds <= base.fds + 8,
              "S3 fd 回落注册前基线（400 实体 fd 全释放，无每实体 fd 泄漏；<=base+8）");
        CHECK(after.rssKB <= base.rssKB + rssToleranceKB(base.rssKB),
              "S3 RSS 回落注册前基线容差内（~FastDDSNode 完整回收 topics/writers/readers/data，无每实体内存泄漏）");
        std::cout << "[OBSERVE] S3 K=" << K
                  << " | fds " << base.fds << "->" << grown.fds << "->" << after.fds
                  << " rssKB " << base.rssKB << "->" << grown.rssKB << "->" << after.rssKB
                  << " threads " << base.threads << "->" << grown.threads << "->" << after.threads
                  << " cbCount=" << cbCount.load() << std::endl;
    }
} // namespace

int main()
{
    YOMK_INIT();

    // 注册被测服务：所有权移交框架（shared_ptr 持有），init() 内部调用后 weak_from_this() 方有效
    auto *svc = new YomkRpcService(YOMK_SERVER_P);
    CHECK(YOMK_ADD_SERVICE(svc) == 0, "YomkRpcService 注册成功（所有权移交框架，init() 已内部调用）");

    std::cout << "[CFG] STRESS_ITERS=" << STRESS_ITERS << " STRESS_CYCLES=" << STRESS_CYCLES
              << " domain=" << TEST_DOMAIN << std::endl;

    s1_nodeLifecycleSoak(svc);
    s2_continuousPublishLoan(svc);
    s3_entityGrowthReclaim(svc);

    return testReport("TestYomkRpcStressSerial");
}
