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

// 单进程完整流程示例：同一节点内注册发布与订阅主题，发布 5 条 MString 并在订阅回调中接收打印，最后销毁节点。
// 覆盖 create_node / register_pub_topic / register_sub_topic / publish / delete_node 全流程。
int main(int argc, char *argv[])
{
    YOMK_INIT();
    YOMK_NEW_SERVICE(YomkRpcService);

    // 查看版本（宏内部自动解包并打印）
    YOMK_INFO_TAG("ExampleYomkRpcTopic", "=== YomkRpcService::getVersion ===");
    YOMKRPC_VERSION();

    // 1. 创建节点（每节点一个独立 DDS 参与者）
    YOMK_INFO_TAG("ExampleYomkRpcTopic", "=== YomkRpcService::createNode ===");
    auto resp = YOMKRPC_NODE(0, "node0");
    if (resp.m_status != YomkResponse::eOk)
    {
        YOMK_ERROR_TAG("ExampleYomkRpcTopic", "create node failed: ", resp.m_msg);
        return 1;
    }

    // 2. 注册发布主题（type 所有权移交 FastDDSNode）
    YOMK_INFO_TAG("ExampleYomkRpcTopic", "=== YomkRpcService::registerPubTopic ===");
    resp = YOMKRPC_PUB_TOPIC("node0", "rpc_demo_topic", new YomkRpc::MStringPubSubType());
    if (resp.m_status != YomkResponse::eOk)
    {
        YOMK_ERROR_TAG("ExampleYomkRpcTopic", "register pub topic failed: ", resp.m_msg);
        YOMKRPC_DEL_NODE("node0");
        return 1;
    }

    // 3. 注册订阅主题：回调中累加计数并记录最后一条消息
    YOMK_INFO_TAG("ExampleYomkRpcTopic", "=== YomkRpcService::registerSubTopic ===");
    std::atomic<int> received{0};
    std::mutex msgMtx;
    std::string lastMsg;
    auto onMessage = [&](const void *data)
    {
        // data 为交付的消息实例指针（仅回调期间有效），转型为具体消息类型后读取
        auto *msg = static_cast<const YomkRpc::MString *>(data);
        std::lock_guard<std::mutex> lock(msgMtx);
        lastMsg = msg->data();
        received++;
        YOMK_INFO_TAG("ExampleYomkRpcTopic", "[RECV] 收到消息: ", msg->data());
    };
    resp = YOMKRPC_SUB_TOPIC("node0", "rpc_demo_topic", new YomkRpc::MStringPubSubType(), onMessage);
    if (resp.m_status != YomkResponse::eOk)
    {
        YOMK_ERROR_TAG("ExampleYomkRpcTopic", "register sub topic failed: ", resp.m_msg);
        YOMKRPC_DEL_NODE("node0");
        return 1;
    }

    // 4. 等待 DDS discovery 完成后发布 5 条消息
    YOMK_INFO_TAG("ExampleYomkRpcTopic", "=== YomkRpcService::publish ===");
    std::this_thread::sleep_for(std::chrono::seconds(1));
    for (int i = 0; i < 5; ++i)
    {
        YomkRpc::MString msg;
        msg.data("Hello YomkRpcService " + std::to_string(i));
        resp = YOMKRPC_PUB_MSG("node0", "rpc_demo_topic", &msg);
        if (resp.m_status != YomkResponse::eOk)
        {
            YOMK_ERROR_TAG("ExampleYomkRpcTopic", "[SEND] 发布失败: ", msg.data(), " status=", static_cast<int>(resp.m_status));
            break;
        }
        YOMK_INFO_TAG("ExampleYomkRpcTopic", "[SEND] 发布消息: ", msg.data());
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    // 5. 等待接收完成（首条消息可能因 discovery 时序丢失），打印接收统计
    std::this_thread::sleep_for(std::chrono::seconds(1));
    std::string lastMsgCopy;
    {
        std::lock_guard<std::mutex> lock(msgMtx);
        lastMsgCopy = lastMsg;
    }
    YOMK_INFO_TAG("ExampleYomkRpcTopic", "[RESULT] 接收=", received.load(), "/5 最后一条=", lastMsgCopy);

    // 6. 退出前销毁节点，确保 DDS 实体在 FastDDS 静态资源销毁前清理
    YOMK_INFO_TAG("ExampleYomkRpcTopic", "=== YomkRpcService::deleteNode ===");
    resp = YOMKRPC_DEL_NODE("node0");
    if (resp.m_status != YomkResponse::eOk)
    {
        YOMK_ERROR_TAG("ExampleYomkRpcTopic", "delete node failed: ", resp.m_msg);
        return 1;
    }
    return 0;
}
