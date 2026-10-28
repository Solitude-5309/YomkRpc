/**
 * @file ExampleRos2InteropPub.cpp
 * @brief 闭环 6：set_name 类型名直改与 ROS2 互通的发布端示例（run_ros2_interop.sh 方向 1）
 *
 * 机制（README 5.6 同源）：MStringPubSubType 构造后、移交服务端前 set_name 成 ROS2
 * 约定名（std_msgs::msg::dds_::String_），链路内 register_type 以新名注册、发现数据
 * type_name 即新名，ROS2 侧同名匹配即通（Humble 无 XTypes 类型匹配，退化为 type_name
 * 字符串比较）。CDR 布局前提成立：MString 与 std_msgs/msg/String 同为单 string 字段。
 *
 * 行为：域 0 创建节点 + set_name 新名注册 rt/chatter_interop（ros2DefaultPublisher
 * 底座，RELIABLE offered 对 ros2 topic echo 任意 requested 可匹配），1Hz 持续发布
 * 直至 Ctrl+C/SIGTERM（g_stop 置位后清理退出）。
 *
 * 话题名口径（互通三要素之一，逐字节一致）：rmw_fastrtps 对 ROS2 语义话题名加
 *   "rt/" 前缀生成实际 DDS 话题名（ros2 topic echo /chatter_interop 的 DDS 名为
 *   rt/chatter_interop），YomkRpc 端直接用 rt/ 形态注册——无前缀名与 ROS2 端永不匹配。
 *
 * ROS2 侧验证（test/run_ros2_interop.sh 方向 1）：
 *   ros2 topic echo /chatter_interop std_msgs/msg/String --once
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
