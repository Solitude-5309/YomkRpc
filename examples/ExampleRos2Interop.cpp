/**
 * @file ExampleRos2Interop.cpp
 * @brief 闭环 6 用例 4b：set_name 类型名直改与 ROS2 互通的双模式示例（pub/sub）
 *
 * 机制（闭环 5 README 5.6 同源）：MStringPubSubType 构造后、移交服务端前 set_name 成 ROS2
 * 约定名（std_msgs::msg::dds_::String_），链路内 register_type 以新名注册、发现数据
 * type_name 即新名，ROS2 侧同名匹配即通（Humble 无 XTypes 类型匹配，退化为 type_name
 * 字符串比较）。CDR 布局前提成立：MString 与 std_msgs/msg/String 同为单 string 字段。
 *
 * 双模式（test/run_ros2_interop.sh 真机脚本驱动）：
 *   pub 模式：域 0 创建节点 + 新名注册 rt/chatter_interop，1Hz 发布 30 条后正常退出；
 *   sub 模式：域 0 创建节点 + 新名订阅 rt/chatter_interop_rev，收到即打印 "[RECV] ..." 并退出
 *   （最长等待 30s，超时退出码 1）。
 *
 * 话题名口径（互通实测三要素之一，逐字节一致）：rmw_fastrtps 对 ROS2 语义话题名加
 *   "rt/" 前缀生成实际 DDS 话题名（ros2 topic echo/pub /chatter_interop 的 DDS 名为
 *   rt/chatter_interop），YomkRpc 端直接用 rt/ 形态注册——无前缀名与 ROS2 端永不匹配。
 *
 * QoS 底座（对齐 ROS2 端点默认）：pub 端 DDSQosProfile::ros2DefaultPublisher()
 *   （KEEP_LAST(10)+RELIABLE offered，等价 ROS2 publisher 默认）、sub 端
 *   ros2DefaultSubscriber()（KEEP_LAST(10)+BEST_EFFORT requested，等价 ROS2
 *   subscription 默认）——requested≤offered 对脚本两侧 ros2 topic echo/pub 全兼容；
 *   缺省 Fast DDS 默认底座与 ROS2 端点匹配行为存在差异，互通示例显式选底座。
 *
 * ROS2 侧验证（脚本步骤）：
 *   ros2 topic echo /chatter_interop std_msgs/msg/String --once   （YomkRpc pub → ROS2）
 *   ros2 topic pub /chatter_interop_rev std_msgs/msg/String ...   （ROS2 → YomkRpc sub）
 */

#include <YomkRpc/YomkRpcAPI.h>
#include <YomkServer/YomkAPI.h>

#include <YomkRpcMsg/YomkRpcMsg.hpp>
#include <YomkRpcMsg/YomkRpcMsgPubSubTypes.hpp>

#include <atomic>
#include <chrono>
#include <csignal>
#include <iostream>
#include <string>
#include <thread>

using namespace yomk;

static std::atomic<bool> g_stop{false}; // SIGINT/SIGTERM 退出标志

static void onSignal(int)
{
    g_stop.store(true);
}

int main(int argc, char *argv[])
{
    if (argc < 2 || (std::string(argv[1]) != "pub" && std::string(argv[1]) != "sub"))
    {
        std::cerr << "usage: ExampleRos2Interop <pub|sub>  (ROS2 Humble 互通双模式)" << std::endl;
        return 2;
    }
    const std::string mode = argv[1];

    YOMK_INIT();
    YOMK_NEW_SERVICE(YomkRpcService);

    if (mode == "pub")
    {
        // 1. 创建节点（域 0 对齐 ROS2 默认域）
        auto resp = YOMKRPC_NODE("interop_pub", 0);
        if (resp.m_status != YomkResponse::eOk)
        {
            YOMK_ERROR_TAG("ExampleRos2Interop", "create node failed: ", resp.m_msg);
            return 1;
        }

        // 2. 类型对象 new 后、移交前 set_name 成 ROS2 约定名（宏实参位内联 new 无法先改名）
        auto *type = new YomkRpc::MStringPubSubType();
        type->set_name("std_msgs::msg::dds_::String_");
        // QoS 用 ROS2 发布端默认底座：RELIABLE offered 对 ros2 topic echo 任意 requested 可匹配
        resp = YOMKRPC_PUB_TOPIC("interop_pub", "rt/chatter_interop", type,
                                 DDSQosProfile::ros2DefaultPublisher());
        if (resp.m_status != YomkResponse::eOk)
        {
            YOMK_ERROR_TAG("ExampleRos2Interop", "register pub topic failed: ", resp.m_msg);
            YOMKRPC_DEL_NODE("interop_pub");
            return 1;
        }

        // 3. 1Hz 发布 30 条后正常退出（脚本 timeout 兜底）
        std::signal(SIGINT, onSignal);
        std::this_thread::sleep_for(std::chrono::seconds(1)); // 等待 discovery
        for (int i = 0; i < 30 && !g_stop.load(); ++i)
        {
            YomkRpc::MString msg;
            msg.data("interop hello " + std::to_string(i));
            resp = YOMKRPC_PUB_MSG("interop_pub", "rt/chatter_interop", &msg);
            if (resp.m_status != YomkResponse::eOk)
            {
                YOMK_ERROR_TAG("ExampleRos2Interop", "[SEND] publish failed");
            }
            else
            {
                YOMK_INFO_TAG("ExampleRos2Interop", "[SEND] ", msg.data());
            }
            std::this_thread::sleep_for(std::chrono::seconds(1));
        }

        // 4. 退出前显式销毁节点
        resp = YOMKRPC_DEL_NODE("interop_pub");
        if (resp.m_status != YomkResponse::eOk)
        {
            YOMK_ERROR_TAG("ExampleRos2Interop", "delete node failed: ", resp.m_msg);
            return 1;
        }
        return 0;
    }

    // ---- sub 模式：同名 set_name 订阅，收到即打印 RECV 并退出 ----
    auto resp = YOMKRPC_NODE("interop_sub", 0);
    if (resp.m_status != YomkResponse::eOk)
    {
        YOMK_ERROR_TAG("ExampleRos2Interop", "create node failed: ", resp.m_msg);
        return 1;
    }

    std::atomic<int> received{0};
    auto *type = new YomkRpc::MStringPubSubType();
    type->set_name("std_msgs::msg::dds_::String_");
    auto onMessage = [&](const void *data)
    {
        auto *msg = static_cast<const YomkRpc::MString *>(data);
        received++;
        std::cout << "[RECV] " << msg->data() << std::endl;
    };
    // QoS 用 ROS2 订阅端默认底座：BEST_EFFORT requested 对任意 offered（含 ros2 topic
    // pub 的 RELIABLE）均可匹配（头文件 YOMKRPC_SUB_TOPIC 注释同口径）
    resp = YOMKRPC_SUB_TOPIC("interop_sub", "rt/chatter_interop_rev", type, onMessage,
                             DDSQosProfile::ros2DefaultSubscriber());
    if (resp.m_status != YomkResponse::eOk)
    {
        YOMK_ERROR_TAG("ExampleRos2Interop", "register sub topic failed: ", resp.m_msg);
        YOMKRPC_DEL_NODE("interop_sub");
        return 1;
    }

    std::signal(SIGINT, onSignal);
    for (int i = 0; i < 30 && received.load() == 0 && !g_stop.load(); ++i)
    {
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }

    resp = YOMKRPC_DEL_NODE("interop_sub");
    if (resp.m_status != YomkResponse::eOk)
    {
        YOMK_ERROR_TAG("ExampleRos2Interop", "delete node failed: ", resp.m_msg);
        return 1;
    }
    return received.load() > 0 ? 0 : 1;
}
