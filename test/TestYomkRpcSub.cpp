#include <YomkServer/YomkAPI.h>
#include <YomkRpc/YomkRpcAPI.h>
#include <YomkRpcMsg/YomkRpcMsg.hpp>
#include <YomkRpcMsg/YomkRpcMsgPubSubTypes.hpp>
#include <atomic>
#include <chrono>
#include <csignal>
#include <thread>

using namespace yomk;

static std::atomic<bool> g_stop{false}; // SIGINT（Ctrl+C）退出标志

static void onSignal(int)
{
    g_stop.store(true);
}

// YomkRpc 订阅端示例程序（用户参考）：订阅 hello_world 主题，
// 收到每条消息打印内容；Ctrl+C 干净退出并打印累计接收条数；
// 与 RpcPubHelloWorld（发布端示例）配合可演示真实跨进程发布/订阅通信
int main(int argc, char *argv[])
{
    YOMK_INIT();
    YOMK_NEW_SERVICE(YomkRpcService);

    // 1. 创建节点
    auto resp = YOMKRPC_NODE(0, "sub_node");
    if (resp.m_status != YomkResponse::eOk)
    {
        YOMK_ERROR_TAG("RpcSubHelloWorld", "create node failed: ", resp.m_msg);
        return 1;
    }

    // 2. 注册订阅主题：回调中打印消息内容并累计计数
    std::atomic<int> received{0};
    auto onMessage = [&](const void *data)
    {
        auto *msg = static_cast<const YomkRpc::MString *>(data);
        received++;
        YOMK_INFO_TAG("RpcSubHelloWorld", "[RECV] ", msg->data());
    };
    resp = YOMKRPC_SUB_TOPIC("sub_node", "hello_world", new YomkRpc::MStringPubSubType(), onMessage);
    if (resp.m_status != YomkResponse::eOk)
    {
        YOMK_ERROR_TAG("RpcSubHelloWorld", "register sub topic failed: ", resp.m_msg);
        YOMKRPC_DEL_NODE("sub_node");
        return 1;
    }

    // 3. 监听 Ctrl+C，主循环等待退出信号
    std::signal(SIGINT, onSignal);
    YOMK_INFO_TAG("RpcSubHelloWorld", "subscribing hello_world, press Ctrl+C to exit");
    while (!g_stop.load())
    {
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }

    // 4. 退出前显式销毁节点，确保 DDS 实体在 FastDDS 静态资源销毁前清理
    YOMK_INFO_TAG("RpcSubHelloWorld", "received total=", received.load(), " messages");
    resp = YOMKRPC_DEL_NODE("sub_node");
    if (resp.m_status != YomkResponse::eOk)
    {
        YOMK_ERROR_TAG("RpcSubHelloWorld", "delete node failed: ", resp.m_msg);
        return 1;
    }
    return 0;
}
