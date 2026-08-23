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
        YOMK_INFO_TAG("TestRpcTopic", "[PASS] ", name);
        g_pass++;
    }
    else
    {
        YOMK_ERROR_TAG("TestRpcTopic", "[FAIL] ", name);
        g_fail++;
    }
}

int main(int argc, char *argv[])
{
    YOMK_INIT();
    YOMK_NEW_SERVICE(YomkRpcService);

    // 测试版本查询（宏内部解包并打印）
    YOMK_INFO_TAG("TestRpcTopic", "=== Test YomkRpcService::getVersion ===");
    YOMKRPC_VERSION();

    // 测试创建节点
    YOMK_INFO_TAG("TestRpcTopic", "=== Test YomkRpcService::createNode ===");
    auto resp = YOMKRPC_NODE(0, "node0");
    check("createNode node0: status=" + std::to_string(static_cast<int>(resp.m_status)), resp.m_status == YomkResponse::eOk);

    // 测试注册发布主题
    YOMK_INFO_TAG("TestRpcTopic", "=== Test YomkRpcService::registerPubTopic ===");
    resp = YOMKRPC_PUB_TOPIC("node0", "rpc_test_topic", new YomkRpc::MStringPubSubType());
    check("registerPubTopic rpc_test_topic: status=" + std::to_string(static_cast<int>(resp.m_status)), resp.m_status == YomkResponse::eOk);

    // 测试注册订阅主题
    YOMK_INFO_TAG("TestRpcTopic", "=== Test YomkRpcService::registerSubTopic ===");
    std::atomic<int> received{0};
    std::mutex msgMtx;
    std::string lastMsg;

    // 订阅回调：收到消息时累加计数并记录最后一条消息
    auto onMessage = [&](const void *data)
    {
        auto *msg = static_cast<const YomkRpc::MString *>(data);
        std::lock_guard<std::mutex> lock(msgMtx);
        lastMsg = msg->data();
        received++;
        YOMK_INFO_TAG("TestRpcTopic", "[RECV] 收到消息: ", msg->data());
    };

    resp = YOMKRPC_SUB_TOPIC("node0", "rpc_test_topic", new YomkRpc::MStringPubSubType(), onMessage);
    check("registerSubTopic rpc_test_topic: status=" + std::to_string(static_cast<int>(resp.m_status)), resp.m_status == YomkResponse::eOk);

    // 等待 DDS discovery 完成后发布数据
    YOMK_INFO_TAG("TestRpcTopic", "=== Test YomkRpcService::publish ===");
    std::this_thread::sleep_for(std::chrono::seconds(1));
    bool allPublished = true;
    for (int i = 0; i < 5; ++i)
    {
        YomkRpc::MString msg;
        msg.data("Hello YomkRpcService " + std::to_string(i));
        resp = YOMKRPC_PUB_MSG("node0", "rpc_test_topic", &msg);
        if (resp.m_status != YomkResponse::eOk)
        {
            allPublished = false;
            YOMK_ERROR_TAG("TestRpcTopic", "[SEND] 发布失败: ", msg.data(), " status=", static_cast<int>(resp.m_status));
            break;
        }
        YOMK_INFO_TAG("TestRpcTopic", "[SEND] 发布消息: ", msg.data());
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    check("publish 5 messages: 发送=5", allPublished);

    // 等待接收完成，首条消息可能因 discovery 时序丢失，收到 4 条以上视为通过
    std::this_thread::sleep_for(std::chrono::seconds(1));
    const int recvCount = received.load();
    std::string lastMsgCopy;
    {
        std::lock_guard<std::mutex> lock(msgMtx);
        lastMsgCopy = lastMsg;
    }
    YOMK_INFO_TAG("TestRpcTopic", "[RESULT] 接收=", recvCount, "/5 最后一条=", lastMsgCopy);
    check("subscriber received >= 4 messages: 接收=" + std::to_string(recvCount) + "/5", recvCount >= 4);
    check("last message content matches: 最后一条=" + lastMsgCopy, lastMsgCopy == "Hello YomkRpcService 4");

    // 退出前销毁节点，确保 DDS 实体在 FastDDS 静态资源销毁前清理
    YOMK_INFO_TAG("TestRpcTopic", "=== Test YomkRpcService::deleteNode ===");
    resp = YOMKRPC_DEL_NODE("node0");
    check("deleteNode node0: status=" + std::to_string(static_cast<int>(resp.m_status)), resp.m_status == YomkResponse::eOk);

    YOMK_INFO_TAG("TestRpcTopic", "========== Test Summary ==========");
    YOMK_INFO_TAG("TestRpcTopic", "PASS: ", g_pass);
    YOMK_INFO_TAG("TestRpcTopic", "FAIL: ", g_fail);

    return g_fail > 0 ? 1 : 0;
}
