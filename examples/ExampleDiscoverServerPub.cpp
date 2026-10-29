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

// 发现服务器示例·发布端：演示 跨域经发现服务器互通 的发布侧代码流程——节点域 id 固定 100，
// 以 CLIENT 角色直配发现服务器（YOMKRPC_NODE 形态 3 尾参 serverLocator），与域 id 200 的
// ExampleDiscoverServerSub 互通（DS 模式下 server 不校验域 id，原理见 README 5.7/5.8）。
// 用法: ExampleDiscoverServerPub [serverLocator]  // "ip[:port]"，缺省 127.0.0.1:11811，
//                                                // 须与 LaunchDiscoverServer.sh 启动的 server 一致
// 三终端运行方式（先启动发现服务器，再 Sub/Pub 任意顺序）见 README 5.8。
int main(int argc, char* argv[])
{
    // 发现服务器直配串：可选首参覆盖，缺省 127.0.0.1:11811（fastdds discovery 缺省端口）
    const std::string serverLocator = argc > 1 ? argv[1] : "127.0.0.1:11811";

    YOMK_INIT();
    YOMK_NEW_SERVICE(YomkRpcService);

    // 1. 创建节点：域 id 显式 100（DS 模式下可与对端异域），尾参直配发现服务器
    //    （"ip[:port]" 聚合串，participant 以 CLIENT 发现协议运行）
    auto resp = YOMKRPC_NODE("ds_pub_node", 100, serverLocator);
    if (resp.m_status != YomkResponse::eOk)
    {
        YOMK_ERROR_TAG("ExampleDiscoverServerPub", "create node failed: ", resp.m_msg);
        return 1;
    }

    // 2. 注册发布主题：type 所有权移交服务端，new 后无需自行 delete
    resp = YOMKRPC_PUB_TOPIC("ds_pub_node", "ds_hello_world", new YomkRpc::MStringPubSubType());
    if (resp.m_status != YomkResponse::eOk)
    {
        YOMK_ERROR_TAG("ExampleDiscoverServerPub", "register pub topic failed: ", resp.m_msg);
        YOMKRPC_DEL_NODE("ds_pub_node");
        return 1;
    }

    // 3. 等待经发现服务器完成 discovery 后，每隔 1s 发布一次，直到收到 Ctrl+C 退出信号
    std::signal(SIGINT, onSignal);
    std::this_thread::sleep_for(std::chrono::seconds(1));
    int total = 0;  // 已发布条数（含失败，作消息序号）
    int failCount = 0;
    while (!g_stop.load())
    {
        YomkRpc::MString msg;
        msg.data("hello world " + std::to_string(total));
        resp = YOMKRPC_PUB_MSG("ds_pub_node", "ds_hello_world", &msg);
        if (resp.m_status != YomkResponse::eOk)
        {
            failCount++;
            YOMK_ERROR_TAG(
                "ExampleDiscoverServerPub",
                "[SEND] publish failed: ",
                msg.data(),
                " status=",
                static_cast<int>(resp.m_status));
        }
        else
        {
            YOMK_INFO_TAG("ExampleDiscoverServerPub", "[SEND] ", msg.data());
        }
        total++;
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }

    // 4. 退出前显式销毁节点，确保 DDS 实体在 FastDDS 静态资源销毁前清理
    YOMK_INFO_TAG("ExampleDiscoverServerPub", "publish done: total=", total, " failed=", failCount);
    resp = YOMKRPC_DEL_NODE("ds_pub_node");
    if (resp.m_status != YomkResponse::eOk)
    {
        YOMK_ERROR_TAG("ExampleDiscoverServerPub", "delete node failed: ", resp.m_msg);
        return 1;
    }
    return failCount > 0 ? 1 : 0;
}
