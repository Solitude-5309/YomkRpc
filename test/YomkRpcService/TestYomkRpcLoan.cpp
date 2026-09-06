/**
 * @file TestYomkRpcLoan.cpp
 * @brief MC4：loan 借出/归还（plain 类型免序列化路径）
 *
 * 范围：MC0-MC3 覆盖服务契约、节点生命周期、主题注册与普通发布订阅，但 loan 免序列化路径
 *       完全未测。本用例经 svc->invoke() 白盒验证 FastDDSNode::loan/discardLoan 与
 *       YomkRpcService::loan/discardLoan 的真实行为：plain(MInt32) 借出成功、非 plain(MString)
 *       回退、loan→池内填值→publish 免序列化端到端、discard_loan 归还、loan/discard 守卫。
 * 覆盖：
 *   Part A  loan 类型差异：plain MInt32 → eOk + sample 非空；池内填值后 discard_loan 归还 → eOk；
 *           非 plain MString → eNo（loan_sample RETCODE_ILLEGAL_OPERATION，回退普通发布）；
 *   Part B  loan 端到端：MInt32 loan→池内填值→publish（免序列化）→ sub callback 收到 (0,5]；
 *   Part C  loan/discard 守卫：loan 未注册主题 → eNo；discard_loan sample==null → eNo；
 *           discard_loan 未注册主题 → eNo（pubTopics_.find 失败短路，不触达 writer）。
 *
 * 关键研究结论：
 *   - is_plain 实证：MInt32PubSubType → true（loan_sample RETCODE_OK）；MStringPubSubType → false
 *     （loan_sample RETCODE_ILLEGAL_OPERATION，sample 保持 null → 服务层 eNo）。
 *   - loan 所有权 = writer 内部池借出，中间件始终拥有：write 后自动收回，未发布须 discard_loan 归还，
 *     writer 销毁（~FastDDSNode→delete_datawriter）时池释放。sample 从不是 caller new，服务层/节点层
 *     都不 delete 它——与 MC3 register_*_topic 的 caller-new type（所有权转移）语义完全不同，
 *     故 MC4 无 caller-owned 裸指针泄漏路径，asan 预期零未抑制泄漏。
 *
 * 关键不变式：每个 eOk 创建的节点必须在 main 返回前经 /delete_node 显式删除；不遗留未归还的 loan
 *   （Part A 借出即 discard、Part B 每次 publish 由中间件收回、Part C 不实际借出有效样本）。
 *
 * 风格：纯 main() + CHECK 宏 + 失败计数（零第三方依赖），返回非 0 表示存在失败用例。
 */

#include "TestCheck.h"
#include "YomkRpcService.h" // DDSLoan/DDSLoanResult/DDSTopic/DDSSubRequest/DDSPublish + String + YOMK_* 宏

#include <YomkRpcMsg/YomkRpcMsg.hpp>            // YomkRpc::MInt32 / MString
#include <YomkRpcMsg/YomkRpcMsgPubSubTypes.hpp> // MInt32PubSubType / MStringPubSubType

#include <atomic>
#include <chrono>
#include <cstdint>
#include <iostream>
#include <mutex>
#include <string>
#include <thread>

namespace
{
    // 有效域（FastDDS 约 0-232），与 MC2/MC3 一致，避开默认域 0 的潜在网络干扰
    constexpr uint32_t TEST_DOMAIN = 200;

    // 构造 /create_node 请求包
    YomkPkgPtr mkNode(uint32_t domainId, const std::string &nodeName)
    {
        return YomkMkPtr(DDSNode, DDSNode{domainId, nodeName});
    }

    // Part A：loan 类型差异（plain MInt32 借出成功 / 非 plain MString 回退）+ discard 归还
    void testLoanTypeAndDiscard(YomkRpcService *svc)
    {
        CHECK(svc->invoke("/create_node", mkNode(TEST_DOMAIN, "loan_node")).m_status == YomkResponse::eOk,
              "创建 loan_node(domain 200) → eOk");

        CHECK(svc->invoke("/register_pub_topic",
                          YomkMkPtr(DDSTopic, DDSTopic{"loan_node", "loan_plain", new YomkRpc::MInt32PubSubType()}))
                      .m_status == YomkResponse::eOk,
              "register_pub_topic loan_plain(MInt32,plain) → eOk");

        // plain 类型借出：eOk + 解包 DDSLoanResult 取 sample（writer 池指针，中间件所有权）
        auto r = svc->invoke("/loan", YomkMkPtr(DDSLoan, DDSLoan{"loan_node", "loan_plain", nullptr}));
        CHECK(r.m_status == YomkResponse::eOk, "loan plain MInt32 → eOk");
        YomkUnPackPkg(r.m_data, DDSLoanResult, lr);
        void *s = (lr != nullptr) ? lr->msg.sample : nullptr;
        CHECK(s != nullptr, "loan 返回 sample 非空（writer 池借出，中间件所有权）");
        if (s != nullptr)
        {
            static_cast<YomkRpc::MInt32 *>(s)->data(42); // 池内直接填值（免序列化）
            CHECK(svc->invoke("/discard_loan", YomkMkPtr(DDSLoan, DDSLoan{"loan_node", "loan_plain", s}))
                          .m_status == YomkResponse::eOk,
                  "discard_loan 归还未发布样本 → eOk");
        }

        // 非 plain 类型借出：loan_sample RETCODE_ILLEGAL_OPERATION → sample 保持 null → 服务层 eNo
        CHECK(svc->invoke("/register_pub_topic",
                          YomkMkPtr(DDSTopic, DDSTopic{"loan_node", "loan_nonplain", new YomkRpc::MStringPubSubType()}))
                      .m_status == YomkResponse::eOk,
              "register_pub_topic loan_nonplain(MString,非plain) → eOk");
        CHECK(svc->invoke("/loan", YomkMkPtr(DDSLoan, DDSLoan{"loan_node", "loan_nonplain", nullptr}))
                      .m_status == YomkResponse::eNo,
              "loan 非 plain MString → eNo（loan_sample 不支持，回退普通发布）");

        CHECK(svc->invoke("/delete_node", YomkMkPtr(String, "loan_node")).m_status == YomkResponse::eOk,
              "删除 loan_node → eOk");
    }

    // Part B：loan 端到端（plain MInt32：loan→池内填值→publish→sub callback 本地回环）
    void testLoanEndToEnd(YomkRpcService *svc)
    {
        CHECK(svc->invoke("/create_node", mkNode(TEST_DOMAIN, "e2e_loan_node")).m_status == YomkResponse::eOk,
              "创建 e2e_loan_node → eOk");
        CHECK(svc->invoke("/register_pub_topic",
                          YomkMkPtr(DDSTopic, DDSTopic{"e2e_loan_node", "loan_e2e", new YomkRpc::MInt32PubSubType()}))
                      .m_status == YomkResponse::eOk,
              "register_pub_topic loan_e2e(MInt32) → eOk");

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
                          YomkMkPtr(DDSSubRequest, DDSSubRequest{"e2e_loan_node", "loan_e2e", new YomkRpc::MInt32PubSubType(), cb}))
                      .m_status == YomkResponse::eOk,
              "register_sub_topic loan_e2e(callback) → eOk");

        std::this_thread::sleep_for(std::chrono::seconds(1)); // 等待 discovery

        constexpr int N = 5;
        int loanOk = 0;
        int pubOk = 0;
        for (int i = 0; i < N; ++i)
        {
            auto lresp = svc->invoke("/loan", YomkMkPtr(DDSLoan, DDSLoan{"e2e_loan_node", "loan_e2e", nullptr}));
            YomkUnPackPkg(lresp.m_data, DDSLoanResult, lres);
            void *s = (lres != nullptr) ? lres->msg.sample : nullptr;
            if (lresp.m_status == YomkResponse::eOk && s != nullptr)
            {
                loanOk++;
                static_cast<YomkRpc::MInt32 *>(s)->data(100 + i); // 池内填值
                // write 后中间件收回 s（caller 不应再访问），下一轮重新 loan
                if (svc->invoke("/publish", YomkMkPtr(DDSPublish, DDSPublish{"e2e_loan_node", "loan_e2e", s}))
                        .m_status == YomkResponse::eOk)
                {
                    pubOk++;
                }
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
        CHECK(loanOk == N, "loan 5 次全部成功（plain 池借出，每次 write 后重新借出）");
        CHECK(pubOk == N, "loan→publish 5 条全部 eOk（免序列化路径）");

        std::this_thread::sleep_for(std::chrono::seconds(1)); // 等待投递

        int r = received.load();
        CHECK(r > 0 && r <= N, "回调收到消息数在 (0,5]（首条或因 discovery 时序丢失）");
        int32_t lv = 0;
        {
            std::lock_guard<std::mutex> lk(mtx);
            lv = lastValue;
        }
        std::cout << "[OBSERVE] loan e2e received=" << r << "/" << N << " lastValue=" << lv << std::endl;

        CHECK(svc->invoke("/delete_node", YomkMkPtr(String, "e2e_loan_node")).m_status == YomkResponse::eOk,
              "删除 e2e_loan_node → eOk（析构 reader 停止回调，捕获状态仍存活）");
    }

    // Part C：loan/discard 守卫（未注册主题 / sample==null）
    void testLoanGuards(YomkRpcService *svc)
    {
        CHECK(svc->invoke("/create_node", mkNode(TEST_DOMAIN, "lg_node")).m_status == YomkResponse::eOk,
              "创建 lg_node → eOk");

        CHECK(svc->invoke("/loan", YomkMkPtr(DDSLoan, DDSLoan{"lg_node", "no_topic", nullptr}))
                      .m_status == YomkResponse::eNo,
              "loan 未注册主题 → eNo（pubTopics_.find 失败）");

        CHECK(svc->invoke("/register_pub_topic",
                          YomkMkPtr(DDSTopic, DDSTopic{"lg_node", "lg_topic", new YomkRpc::MInt32PubSubType()}))
                      .m_status == YomkResponse::eOk,
              "register_pub_topic lg_topic(MInt32) → eOk");

        CHECK(svc->invoke("/discard_loan", YomkMkPtr(DDSLoan, DDSLoan{"lg_node", "lg_topic", nullptr}))
                      .m_status == YomkResponse::eNo,
              "discard_loan sample=nullptr → eNo（FastDDSNode::discardLoan 守卫）");

        // 未注册主题：FastDDSNode::discardLoan 在 pubTopics_.find 失败处短路返回，绝不解引用 sample，
        // 故传任意非空指针安全（即便触达 writer，discard_loan 对非法指针返回 BAD_PARAMETER 亦不崩溃）
        int dummy = 0;
        CHECK(svc->invoke("/discard_loan", YomkMkPtr(DDSLoan, DDSLoan{"lg_node", "no_topic", static_cast<void *>(&dummy)}))
                      .m_status == YomkResponse::eNo,
              "discard_loan 未注册主题 → eNo（pubTopics_.find 失败短路，不触达 writer）");

        CHECK(svc->invoke("/delete_node", YomkMkPtr(String, "lg_node")).m_status == YomkResponse::eOk,
              "删除 lg_node → eOk");
    }
} // namespace

int main()
{
    YOMK_INIT();

    // 注册被测服务：所有权移交框架（shared_ptr 持有），init() 内部调用后 weak_from_this() 方有效
    auto *svc = new YomkRpcService(YOMK_SERVER_P);
    CHECK(YOMK_ADD_SERVICE(svc) == 0, "YomkRpcService 注册成功（所有权移交框架，init() 已内部调用）");

    testLoanTypeAndDiscard(svc);
    testLoanEndToEnd(svc);
    testLoanGuards(svc);

    return testReport("TestYomkRpcLoan");
}
