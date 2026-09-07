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

// loan 借出机制示例：演示 借出池内样本免序列化发布（仅 plain 类型可用，非 plain 回退普通发布）与 discard 归还，
// 并对比 plain(MInt32) 与非 plain(MString) 的借出差异；订阅端回调透明接收（指针仅回调期间有效）。
int main(int argc, char *argv[])
{
    YOMK_INIT();
    YOMK_NEW_SERVICE(YomkRpcService);

    // 1. 创建节点
    YOMK_INFO_TAG("ExampleYomkRpcTopicLoan", "=== YomkRpcService::createNode ===");
    auto resp = YOMKRPC_NODE(0, "node0");
    if (resp.m_status != YomkResponse::eOk)
    {
        YOMK_ERROR_TAG("ExampleYomkRpcTopicLoan", "create node failed: ", resp.m_msg);
        return 1;
    }

    // 2. 非 plain 类型 loan 回退演示：MString 含 string 成员不可 loan，借出返回 nullptr
    YOMK_INFO_TAG("ExampleYomkRpcTopicLoan", "=== loan fallback (non-plain MString) ===");
    resp = YOMKRPC_PUB_TOPIC("node0", "fallback_topic", new YomkRpc::MStringPubSubType());
    if (resp.m_status != YomkResponse::eOk)
    {
        YOMK_ERROR_TAG("ExampleYomkRpcTopicLoan", "register pub topic failed: ", resp.m_msg);
        YOMKRPC_DEL_NODE("node0");
        return 1;
    }
    void *loanPtr = nullptr;
    YOMKRPC_LOAN("node0", "fallback_topic", loanPtr);
    YOMK_INFO_TAG("ExampleYomkRpcTopicLoan", "非 plain MString 借出指针=", (loanPtr != nullptr ? "非空" : "空"), "（预期为空，回退普通发布）");

    // 3. plain 类型 loan 全链路演示：MInt32 为纯基础类型，FINAL 后 is_plain 为 true
    YOMK_INFO_TAG("ExampleYomkRpcTopicLoan", "=== loan publish (plain MInt32) ===");
    resp = YOMKRPC_PUB_TOPIC("node0", "loan_topic", new YomkRpc::MInt32PubSubType());
    if (resp.m_status != YomkResponse::eOk)
    {
        YOMK_ERROR_TAG("ExampleYomkRpcTopicLoan", "register pub topic failed: ", resp.m_msg);
        YOMKRPC_DEL_NODE("node0");
        return 1;
    }

    // 订阅端透明走 reader loan：回调中打印收到的值并累计计数
    std::atomic<int> loanReceived{0};
    std::mutex loanMtx;
    int32_t lastLoanValue = -1;
    auto onLoan = [&](const void *data)
    {
        // data 为交付的消息实例指针；MInt32 是 alignof≤4 的 plain 类型，可直接转型解引用（对齐契约见 src/FastDDSNode.cpp）
        auto *msg = static_cast<const YomkRpc::MInt32 *>(data);
        std::lock_guard<std::mutex> lock(loanMtx);
        lastLoanValue = msg->data();
        loanReceived++;
        YOMK_INFO_TAG("ExampleYomkRpcTopicLoan", "[RECV] 收到借出消息: ", msg->data());
    };
    resp = YOMKRPC_SUB_TOPIC("node0", "loan_topic", new YomkRpc::MInt32PubSubType(), onLoan);
    if (resp.m_status != YomkResponse::eOk)
    {
        YOMK_ERROR_TAG("ExampleYomkRpcTopicLoan", "register sub topic failed: ", resp.m_msg);
        YOMKRPC_DEL_NODE("node0");
        return 1;
    }

    // 4. 等待 DDS discovery 完成
    std::this_thread::sleep_for(std::chrono::seconds(1));

    // 借出 → 池内直接填值 → 发布（每次 write 后中间件收回指针，须重新借出）
    for (int i = 0; i < 5; ++i)
    {
        YOMKRPC_LOAN("node0", "loan_topic", loanPtr);
        if (loanPtr == nullptr)
        {
            YOMK_ERROR_TAG("ExampleYomkRpcTopicLoan", "[SEND] 借出失败: 第 ", i, " 次 loan 返回 nullptr");
            break;
        }
        static_cast<YomkRpc::MInt32 *>(loanPtr)->data(100 + i);
        resp = YOMKRPC_PUB_MSG("node0", "loan_topic", loanPtr);
        if (resp.m_status != YomkResponse::eOk)
        {
            YOMK_ERROR_TAG("ExampleYomkRpcTopicLoan", "[SEND] 借出发布失败: ", 100 + i, " status=", static_cast<int>(resp.m_status));
            break;
        }
        YOMK_INFO_TAG("ExampleYomkRpcTopicLoan", "[SEND] 借出发布: ", 100 + i);
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    // 5. 等待接收完成（首条消息可能因 discovery 时序丢失），打印接收统计
    std::this_thread::sleep_for(std::chrono::seconds(1));
    int32_t lastValueCopy = 0;
    {
        std::lock_guard<std::mutex> lock(loanMtx);
        lastValueCopy = lastLoanValue;
    }
    YOMK_INFO_TAG("ExampleYomkRpcTopicLoan", "[RESULT] 接收=", loanReceived.load(), "/5 最后值=", lastValueCopy);

    // 6. discard 演示：借出后不发布须归还缓冲（否则池泄漏）
    YOMK_INFO_TAG("ExampleYomkRpcTopicLoan", "=== discard loan ===");
    YOMKRPC_LOAN("node0", "loan_topic", loanPtr);
    YOMK_INFO_TAG("ExampleYomkRpcTopicLoan", "借出指针=", (loanPtr != nullptr ? "非空" : "空"));
    if (loanPtr != nullptr)
    {
        resp = YOMKRPC_DISCARD_LOAN("node0", "loan_topic", loanPtr);
        if (resp.m_status != YomkResponse::eOk)
        {
            YOMK_ERROR_TAG("ExampleYomkRpcTopicLoan", "discard loan failed: ", resp.m_msg);
        }
    }

    // 7. 退出前销毁节点，确保 DDS 实体在 FastDDS 静态资源销毁前清理
    YOMK_INFO_TAG("ExampleYomkRpcTopicLoan", "=== YomkRpcService::deleteNode ===");
    resp = YOMKRPC_DEL_NODE("node0");
    if (resp.m_status != YomkResponse::eOk)
    {
        YOMK_ERROR_TAG("ExampleYomkRpcTopicLoan", "delete node failed: ", resp.m_msg);
        return 1;
    }
    return 0;
}
