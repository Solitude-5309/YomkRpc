/**
 * @file ExampleRos2InteropPub.cpp
 * @brief ROS2 互通发布端示例（run_ros2_interop.sh 方向 1，ros2 topic echo 验收）：set_name
 *       直改类型名为 ROS2 约定名 std_msgs::msg::dds_::String_ + "rt/" 话题前缀 +
 *       ros2DefaultPublisher 底座 1Hz 发布；机制与互通三要素详见
 *       docs/manual/ros2-interop-tutorial.md。
 */

#include <YomkRpc/YomkRpcAPI.h>
#include <YomkServer/YomkAPI.h>

#include <YomkRpcMsg/YomkRpcMsg.hpp>
#include <YomkRpcMsg/YomkRpcMsgPubSubTypes.hpp>

#include <atomic>
#include <chrono>
#include <csignal>
#include <cstdint>
#include <iostream>
#include <string>
#include <thread>

using namespace yomk;

static std::atomic<bool> g_stop{false}; // SIGINT/SIGTERM 退出标志

static void onSignal(int)
{
    g_stop.store(true);
}

int main()
{
    YOMK_INIT();
    YOMK_NEW_SERVICE(YomkRpcService);

    // 1. 创建节点（域 0 对齐 ROS2 默认域）
    auto resp = YOMKRPC_NODE("interop_pub", 0);
    if (resp.m_status != YomkResponse::eOk)
    {
        YOMK_ERROR_TAG("ExampleRos2InteropPub", "create node failed: ", resp.m_msg);
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
        YOMK_ERROR_TAG("ExampleRos2InteropPub", "register pub topic failed: ", resp.m_msg);
        YOMKRPC_DEL_NODE("interop_pub");
        return 1;
    }

    // 3. 1Hz 持续发布（Ctrl+C/SIGTERM 置位 g_stop 后跳出，清理退出）
    std::signal(SIGINT, onSignal);
    std::signal(SIGTERM, onSignal);
    std::this_thread::sleep_for(std::chrono::seconds(1)); // 等待 discovery
    std::int64_t seq = 0;
    while (!g_stop.load())
    {
        YomkRpc::MString msg;
        msg.data("interop hello " + std::to_string(seq++));
        resp = YOMKRPC_PUB_MSG("interop_pub", "rt/chatter_interop", &msg);
        if (resp.m_status != YomkResponse::eOk)
        {
            YOMK_ERROR_TAG("ExampleRos2InteropPub", "[SEND] publish failed");
        }
        else
        {
            YOMK_INFO_TAG("ExampleRos2InteropPub", "[SEND] ", msg.data());
        }
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }

    // 4. 退出前显式销毁节点
    resp = YOMKRPC_DEL_NODE("interop_pub");
    if (resp.m_status != YomkResponse::eOk)
    {
        YOMK_ERROR_TAG("ExampleRos2InteropPub", "delete node failed: ", resp.m_msg);
        return 1;
    }
    return 0;
}
