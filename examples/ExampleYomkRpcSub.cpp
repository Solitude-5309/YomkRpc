#include <YomkRpc/YomkRpcAPI.h>
#include <YomkServer/YomkAPI.h>

#include <YomkRpcMsg/YomkRpcMsg.hpp>
#include <YomkRpcMsg/YomkRpcMsgPubSubTypes.hpp>
#include <atomic>
#include <chrono>
#include <csignal>
#include <thread>

using namespace yomk;

static std::atomic<bool> g_stop{false};  // SIGINT（Ctrl+C）退出标志

static void onSignal(int)
{
    g_stop.store(true);
}

// 订阅端示例：演示 创建节点 → 注册 MString 订阅主题（回调打印每条消息）→ 等待 Ctrl+C 退出 → 销毁节点 的代码流程。
// 跨进程运行方式（与 ExampleYomkRpcPub 配合、启动顺序）见 README。
int main(int argc, char* argv[])
{
    YOMK_INIT();
    YOMK_NEW_SERVICE(YomkRpcService);

    // 1. 创建节点：domainId=0，须与发布端同域方可互通
    auto resp = YOMKRPC_NODE(0, "sub_node");
    if (resp.m_status != YomkResponse::eOk)
    {
        YOMK_ERROR_TAG("ExampleYomkRpcSub", "create node failed: ", resp.m_msg);
        return 1;
    }

    // 2. 注册订阅主题：type 所有权移交服务端；收到消息时回调 onMessage
    std::atomic<int> received{0};
    auto onMessage = [&](const void* data)
    {
        // data 为交付的消息实例指针（仅回调期间有效），转型为具体消息类型后读取
        auto* msg = static_cast<const YomkRpc::MString*>(data);
        received++;
        YOMK_INFO_TAG("ExampleYomkRpcSub", "[RECV] ", msg->data());
    };
    resp = YOMKRPC_SUB_TOPIC("sub_node", "hello_world", new YomkRpc::MStringPubSubType(), onMessage);
    if (resp.m_status != YomkResponse::eOk)
    {
        YOMK_ERROR_TAG("ExampleYomkRpcSub", "register sub topic failed: ", resp.m_msg);
        YOMKRPC_DEL_NODE("sub_node");
        return 1;
    }

    // 3. 监听 Ctrl+C，主循环等待退出信号
    std::signal(SIGINT, onSignal);
    YOMK_INFO_TAG("ExampleYomkRpcSub", "subscribing hello_world, press Ctrl+C to exit");
    while (!g_stop.load())
    {
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }

    // 4. 退出前显式销毁节点，确保 DDS 实体在 FastDDS 静态资源销毁前清理
    YOMK_INFO_TAG("ExampleYomkRpcSub", "received total=", received.load(), " messages");
    resp = YOMKRPC_DEL_NODE("sub_node");
    if (resp.m_status != YomkResponse::eOk)
    {
        YOMK_ERROR_TAG("ExampleYomkRpcSub", "delete node failed: ", resp.m_msg);
        return 1;
    }
    return 0;
}
