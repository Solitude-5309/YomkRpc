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

// 发布端示例：演示 创建节点 → 注册 MString 发布主题 → 每隔 1s 发布一条 → 收到 Ctrl+C 退出信号后
// 停止发布并销毁节点 的代码流程。
// 跨进程运行方式（与 ExampleYomkRpcSub 配合、启动顺序）见 README。
int main(int argc, char* argv[])
{
    YOMK_INIT();
    YOMK_NEW_SERVICE(YomkRpcService);

    // 1. 创建节点：domainId=0（仅同域节点互通），nodeName 唯一
    auto resp = YOMKRPC_NODE(0, "pub_node");
    if (resp.m_status != YomkResponse::eOk)
    {
        YOMK_ERROR_TAG("ExampleYomkRpcPub", "create node failed: ", resp.m_msg);
        return 1;
    }

    // 2. 注册发布主题：type 所有权移交服务端，new 后无需自行 delete
    resp = YOMKRPC_PUB_TOPIC("pub_node", "hello_world", new YomkRpc::MStringPubSubType());
    if (resp.m_status != YomkResponse::eOk)
    {
        YOMK_ERROR_TAG("ExampleYomkRpcPub", "register pub topic failed: ", resp.m_msg);
        YOMKRPC_DEL_NODE("pub_node");
        return 1;
    }

    // 3. 等待 DDS discovery 完成后，每隔 1s 发布一次，直到收到 Ctrl+C 退出信号
    std::signal(SIGINT, onSignal);
    std::this_thread::sleep_for(std::chrono::seconds(1));
    int total = 0;  // 已发布条数（含失败，作消息序号）
    int failCount = 0;
    while (!g_stop.load())
    {
        YomkRpc::MString msg;
        msg.data("hello world " + std::to_string(total));
        resp = YOMKRPC_PUB_MSG("pub_node", "hello_world", &msg);
        if (resp.m_status != YomkResponse::eOk)
        {
            failCount++;
            YOMK_ERROR_TAG(
                "ExampleYomkRpcPub",
                "[SEND] publish failed: ",
                msg.data(),
                " status=",
                static_cast<int>(resp.m_status));
        }
        else
        {
            YOMK_INFO_TAG("ExampleYomkRpcPub", "[SEND] ", msg.data());
        }
        total++;
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }

    // 4. 退出前显式销毁节点，确保 DDS 实体在 FastDDS 静态资源销毁前清理
    YOMK_INFO_TAG("ExampleYomkRpcPub", "publish done: total=", total, " failed=", failCount);
    resp = YOMKRPC_DEL_NODE("pub_node");
    if (resp.m_status != YomkResponse::eOk)
    {
        YOMK_ERROR_TAG("ExampleYomkRpcPub", "delete node failed: ", resp.m_msg);
        return 1;
    }
    return failCount > 0 ? 1 : 0;
}
