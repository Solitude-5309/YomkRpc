/**
 * @file ExampleRos2InteropSub.cpp
 * @brief 闭环 6：set_name 类型名直改与 ROS2 互通的订阅端示例（run_ros2_interop.sh
 *        方向 2/3：YomkRpc↔YomkRpc 同主题互通与 ROS2→YomkRpc）
 *
 * 机制（README 5.6 同源）：MStringPubSubType 构造后、移交服务端前 set_name 成 ROS2
 * 约定名（std_msgs::msg::dds_::String_），链路内 register_type 以新名注册，ROS2 侧
 * 同名匹配即通（Humble 无 XTypes 类型匹配，退化为 type_name 字符串比较）。
 *
 * 行为：域 0 创建节点 + set_name 新名订阅 rt/chatter_interop（ros2DefaultSubscriber
 * 底座，BEST_EFFORT requested 对任意 offered——含 ros2 topic pub 与 YomkRpc pub 的
 * RELIABLE——均可匹配），逐条打印 "[RECV] ..." 持续监听，Ctrl+C/SIGTERM 优雅退出
 * （退出时打印总收条数）。
 *
 * 话题名口径（互通三要素之一，逐字节一致）：rmw_fastrtps 对 ROS2 语义话题名加
 *   "rt/" 前缀生成实际 DDS 话题名（ros2 topic pub /chatter_interop 的 DDS 名为
 *   rt/chatter_interop），YomkRpc 端直接用 rt/ 形态注册——无前缀名与 ROS2 端永不匹配。
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

static std::atomic<bool> g_stop{false};  // SIGINT/SIGTERM 退出标志

static void onSignal(int)
{
    g_stop.store(true);
}

int main()
{
    YOMK_INIT();
    YOMK_NEW_SERVICE(YomkRpcService);

    // 创建节点（域 0 对齐 ROS2 默认域）
    auto resp = YOMKRPC_NODE("interop_sub", 0);
    if (resp.m_status != YomkResponse::eOk)
    {
        YOMK_ERROR_TAG("ExampleRos2InteropSub", "create node failed: ", resp.m_msg);
        return 1;
    }

    // 类型对象 new 后、移交前 set_name 成 ROS2 约定名（宏实参位内联 new 无法先改名）
    std::atomic<int> received{0};
    auto* type = new YomkRpc::MStringPubSubType();
    type->set_name("std_msgs::msg::dds_::String_");
    auto onMessage = [&](const void* data)
    {
        auto* msg = static_cast<const YomkRpc::MString*>(data);
        received++;
        YOMK_INFO_TAG("ExampleRos2InteropSub", "[RECV] ", msg->data());
    };
    // QoS 用 ROS2 订阅端默认底座：BEST_EFFORT requested 对任意 offered（含 ros2 topic
    // pub 与 YomkRpc pub 的 RELIABLE）均可匹配（头文件 YOMKRPC_SUB_TOPIC 注释同口径）
    resp =
        YOMKRPC_SUB_TOPIC("interop_sub", "rt/chatter_interop", type, onMessage, DDSQosProfile::ros2DefaultSubscriber());
    if (resp.m_status != YomkResponse::eOk)
    {
        YOMK_ERROR_TAG("ExampleRos2InteropSub", "register sub topic failed: ", resp.m_msg);
        YOMKRPC_DEL_NODE("interop_sub");
        return 1;
    }

    // 持续监听：收流回调逐条打印 [RECV]（不退出），Ctrl+C/SIGTERM 置位 g_stop 后收尾
    std::signal(SIGINT, onSignal);
    std::signal(SIGTERM, onSignal);
    while (!g_stop.load())
    {
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
    }

    // 退出前显式销毁节点，打印总收条数
    resp = YOMKRPC_DEL_NODE("interop_sub");
    if (resp.m_status != YomkResponse::eOk)
    {
        YOMK_ERROR_TAG("ExampleRos2InteropSub", "delete node failed: ", resp.m_msg);
        return 1;
    }
    std::cout << "[QUIT] received total=" << received.load() << std::endl;
    return 0;
}
