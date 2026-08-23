#include <YomkServer/YomkAPI.h>
#include <YomkRpc/YomkRpcAPI.h>
#include <YomkRpcMsg/YomkRpcMsg.hpp>
#include <YomkRpcMsg/YomkRpcMsgPubSubTypes.hpp>
#include <atomic>
#include <chrono>
#include <mutex>
#include <string>
#include <thread>

using namespace yomk;

static int g_pass = 0;
static int g_fail = 0;

void check(const std::string &name, bool condition)
{
    if (condition)
    {
        YOMK_INFO_TAG("TestRpcTopicLoan", "[PASS] ", name);
        g_pass++;
    }
    else
    {
        YOMK_ERROR_TAG("TestRpcTopicLoan", "[FAIL] ", name);
        g_fail++;
    }
}

// 本测试专注验证 loan 借出机制：
// - 发布端借出池内样本免序列化发布（仅 plain 类型可用，非 plain 自动回退）
// - 订阅端透明走 reader loan 路径（回调期间指针有效）
int main(int argc, char *argv[])
{
    YOMK_INIT();
    YOMK_NEW_SERVICE(YomkRpcService);

    // 测试创建节点
    YOMK_INFO_TAG("TestRpcTopicLoan", "=== Test YomkRpcService::createNode ===");
    auto resp = YOMKRPC_NODE(0, "node0");
    check("createNode node0: status=" + std::to_string(static_cast<int>(resp.m_status)), resp.m_status == YomkResponse::eOk);

    // 测试非 plain 类型 loan 回退：MString 不可 loan，outPtr 应保持 nullptr
    YOMK_INFO_TAG("TestRpcTopicLoan", "=== Test loan fallback (non-plain MString) ===");
    resp = YOMKRPC_PUB_TOPIC("node0", "fallback_topic", new YomkRpc::MStringPubSubType());
    check("registerPubTopic fallback_topic: status=" + std::to_string(static_cast<int>(resp.m_status)), resp.m_status == YomkResponse::eOk);
    void *loanPtr = nullptr;
    YOMKRPC_LOAN("node0", "fallback_topic", loanPtr);
    check("loan fallback: non-plain MString 借出返回 nullptr（回退普通发布）", loanPtr == nullptr);

    // 测试 plain 类型 loan 全链路（MInt32 为纯基础类型，FINAL 后 is_plain 为 true）
    YOMK_INFO_TAG("TestRpcTopicLoan", "=== Test loan publish (plain MInt32) ===");
    resp = YOMKRPC_PUB_TOPIC("node0", "loan_topic", new YomkRpc::MInt32PubSubType());
    check("registerPubTopic loan_topic: status=" + std::to_string(static_cast<int>(resp.m_status)), resp.m_status == YomkResponse::eOk);

    std::atomic<int> loanReceived{0};
    std::mutex loanMtx;
    int32_t lastLoanValue = -1;
    auto onLoan = [&](const void *data)
    {
        auto *msg = static_cast<const YomkRpc::MInt32 *>(data);
        std::lock_guard<std::mutex> lock(loanMtx);
        lastLoanValue = msg->data();
        loanReceived++;
        YOMK_INFO_TAG("TestRpcTopicLoan", "[RECV] 收到借出消息: ", msg->data());
    };
    resp = YOMKRPC_SUB_TOPIC("node0", "loan_topic", new YomkRpc::MInt32PubSubType(), onLoan);
    check("registerSubTopic loan_topic: status=" + std::to_string(static_cast<int>(resp.m_status)), resp.m_status == YomkResponse::eOk);

    // 等待 DDS discovery 完成
    std::this_thread::sleep_for(std::chrono::seconds(1));

    // 借出 → 池内直接填值 → 发布（每次 write 后中间件收回指针，须重新借出）
    bool loanPublished = true;
    for (int i = 0; i < 5; ++i)
    {
        YOMKRPC_LOAN("node0", "loan_topic", loanPtr);
        if (loanPtr == nullptr)
        {
            loanPublished = false;
            YOMK_ERROR_TAG("TestRpcTopicLoan", "[SEND] 借出失败: 第 ", i, " 次 loan 返回 nullptr");
            break;
        }
        static_cast<YomkRpc::MInt32 *>(loanPtr)->data(100 + i);
        resp = YOMKRPC_PUB_MSG("node0", "loan_topic", loanPtr);
        if (resp.m_status != YomkResponse::eOk)
        {
            loanPublished = false;
            YOMK_ERROR_TAG("TestRpcTopicLoan", "[SEND] 借出发布失败: ", 100 + i, " status=", static_cast<int>(resp.m_status));
            break;
        }
        YOMK_INFO_TAG("TestRpcTopicLoan", "[SEND] 借出发布: ", 100 + i);
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    check("loan sample publish 5 messages: 发送=5", loanPublished);

    // 等待接收完成，首条消息可能因 discovery 时序丢失，收到 4 条以上视为通过
    std::this_thread::sleep_for(std::chrono::seconds(1));
    const int recvCount = loanReceived.load();
    int32_t lastValueCopy = 0;
    {
        std::lock_guard<std::mutex> lock(loanMtx);
        lastValueCopy = lastLoanValue;
    }
    YOMK_INFO_TAG("TestRpcTopicLoan", "[RESULT] 接收=", recvCount, "/5 最后值=", lastValueCopy);
    check("loan subscriber received >= 4 messages: 接收=" + std::to_string(recvCount) + "/5", recvCount >= 4);
    {
        std::lock_guard<std::mutex> lock(loanMtx);
        check("loan last value is 104: 最后值=" + std::to_string(lastLoanValue), lastLoanValue == 104);
    }

    // 测试 discard：借出后放弃归还
    YOMK_INFO_TAG("TestRpcTopicLoan", "=== Test discard loan ===");
    YOMKRPC_LOAN("node0", "loan_topic", loanPtr);
    check("loan for discard: 借出指针=" + std::string(loanPtr != nullptr ? "非空" : "空"), loanPtr != nullptr);
    if (loanPtr != nullptr)
    {
        resp = YOMKRPC_DISCARD_LOAN("node0", "loan_topic", loanPtr);
        check("discard loan: status=" + std::to_string(static_cast<int>(resp.m_status)), resp.m_status == YomkResponse::eOk);
    }

    // 退出前销毁节点，确保 DDS 实体在 FastDDS 静态资源销毁前清理
    YOMK_INFO_TAG("TestRpcTopicLoan", "=== Test YomkRpcService::deleteNode ===");
    resp = YOMKRPC_DEL_NODE("node0");
    check("deleteNode node0: status=" + std::to_string(static_cast<int>(resp.m_status)), resp.m_status == YomkResponse::eOk);

    YOMK_INFO_TAG("TestRpcTopicLoan", "========== Test Summary ==========");
    YOMK_INFO_TAG("TestRpcTopicLoan", "PASS: ", g_pass);
    YOMK_INFO_TAG("TestRpcTopicLoan", "FAIL: ", g_fail);

    return g_fail > 0 ? 1 : 0;
}
