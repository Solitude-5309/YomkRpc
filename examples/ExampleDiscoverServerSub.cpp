#include <YomkRpc/YomkRpcAPI.h>
#include <YomkServer/YomkAPI.h>

#include <YomkRpcMsg/YomkRpcMsg.hpp>
#include <YomkRpcMsg/YomkRpcMsgPubSubTypes.hpp>
#include <atomic>
#include <chrono>
#include <csignal>
#include <string>
#include <thread>

using namespace yomk;

static std::atomic<bool> g_stop{false};  // SIGINT（Ctrl+C）退出标志

static void onSignal(int)
{
    g_stop.store(true);
}

// 发现服务器示例·订阅端：演示 跨域经发现服务器互通 的订阅侧代码流程——节点域 id 固定 200，
// 以 CLIENT 角色直配发现服务器（YOMKRPC_NODE 形态 3 尾参 serverLocator），与域 id 100 的
// ExampleDiscoverServerPub 互通（DS 模式下 server 不校验域 id，原理见 README 5.7/5.8）。
// 用法: ExampleDiscoverServerSub [serverLocator]  // "ip[:port]"，缺省 127.0.0.1:11811，
//                                                // 须与 LaunchDiscoverServer.sh 启动的 server 一致
// 三终端运行方式（先启动发现服务器，再 Sub/Pub 任意顺序）见 README 5.8。
int main(int argc, char* argv[])
{
    // 发现服务器直配串：可选首参覆盖，缺省 127.0.0.1:11811（fastdds discovery 缺省端口）
    const std::string serverLocator = argc > 1 ? argv[1] : "127.0.0.1:11811";

    YOMK_INIT();
    YOMK_NEW_SERVICE(YomkRpcService);

    // 1. 创建节点：域 id 显式 200（DS 模式下可与对端异域），尾参直配发现服务器
    //    （"ip[:port]" 聚合串，participant 以 CLIENT 发现协议运行）
    auto resp = YOMKRPC_NODE("ds_sub_node", 200, serverLocator);
    if (resp.m_status != YomkResponse::eOk)
    {
        YOMK_ERROR_TAG("ExampleDiscoverServerSub", "create node failed: ", resp.m_msg);
        return 1;
    }

    // 2. 注册订阅主题：type 所有权移交服务端；收到消息时回调 onMessage
    std::atomic<int> received{0};
    auto onMessage = [&](const void* data)
    {
        // data 为交付的消息实例指针（仅回调期间有效），转型为具体消息类型后读取
        auto* msg = static_cast<const YomkRpc::MString*>(data);
        received++;
        YOMK_INFO_TAG("ExampleDiscoverServerSub", "[RECV] ", msg->data());
    };
    resp = YOMKRPC_SUB_TOPIC("ds_sub_node", "ds_hello_world", new YomkRpc::MStringPubSubType(), onMessage);
    if (resp.m_status != YomkResponse::eOk)
    {
        YOMK_ERROR_TAG("ExampleDiscoverServerSub", "register sub topic failed: ", resp.m_msg);
        YOMKRPC_DEL_NODE("ds_sub_node");
        return 1;
    }

    // 3. 监听 Ctrl+C，主循环等待退出信号
    std::signal(SIGINT, onSignal);
    YOMK_INFO_TAG("ExampleDiscoverServerSub", "subscribing ds_hello_world, press Ctrl+C to exit");
    while (!g_stop.load())
    {
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }

    // 4. 退出前显式销毁节点，确保 DDS 实体在 FastDDS 静态资源销毁前清理
    YOMK_INFO_TAG("ExampleDiscoverServerSub", "received total=", received.load(), " messages");
    resp = YOMKRPC_DEL_NODE("ds_sub_node");
    if (resp.m_status != YomkResponse::eOk)
    {
        YOMK_ERROR_TAG("ExampleDiscoverServerSub", "delete node failed: ", resp.m_msg);
        return 1;
    }
    return 0;
}
