/**
 * @file TestRos2InteropPub.cpp
 * @brief 真机互通回归判定程序·发布端（run_ros2_interop.sh 驱动，ctest 树不含）
 *
 * 测试设施定位（examples 的 ExampleRos2InteropPub 为面向用户的教学示例，判定职责归本目录）：
 * 域 0 建节点 + MStringPubSubType set_name 成 ROS2 约定名（std_msgs::msg::dds_::String_）
 * 注册 rt/chatter_interop（ros2DefaultPublisher 底座），发布内容 "hello ros2 N"（N 递增），
 * 1Hz 发布 30 条后正常退出（脚本 timeout 兜底）。
 *
 * 对跑协议（run_ros2_interop.sh）：
 *   方向 1：本程序 → ros2 topic echo /chatter_interop --once（判定 ROS2 收到）
 *
 * 话题名口径：rmw_fastrtps 对 ROS2 语义话题名加 rt/ 前缀生成实际 DDS 话题名，
 * YomkRpc 端直接用 rt/ 形态注册（README 5.6 互通三要素之一）。
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

int main()
{
    YOMK_INIT();
    YOMK_NEW_SERVICE(YomkRpcService);

    // 创建节点（域 0 对齐 ROS2 默认域）
    auto resp = YOMKRPC_NODE("interop_pub", 0);
    if (resp.m_status != YomkResponse::eOk)
    {
        YOMK_ERROR_TAG("TestRos2InteropPub", "create node failed: ", resp.m_msg);
        return 1;
    }

    // 类型对象 new 后、移交前 set_name 成 ROS2 约定名（宏实参位内联 new 无法先改名）
    auto *type = new YomkRpc::MStringPubSubType();
    type->set_name("std_msgs::msg::dds_::String_");
    // QoS 用 ROS2 发布端默认底座：RELIABLE offered 对 ros2 topic echo 任意 requested 可匹配
    resp = YOMKRPC_PUB_TOPIC("interop_pub", "rt/chatter_interop", type,
                             DDSQosProfile::ros2DefaultPublisher());
    if (resp.m_status != YomkResponse::eOk)
    {
        YOMK_ERROR_TAG("TestRos2InteropPub", "register pub topic failed: ", resp.m_msg);
        YOMKRPC_DEL_NODE("interop_pub");
        return 1;
    }

    // 1Hz 发布 30 条后正常退出（Ctrl+C 提前中断）
    std::signal(SIGINT, onSignal);
    std::this_thread::sleep_for(std::chrono::seconds(1)); // 等待 discovery
    for (int i = 0; i < 30 && !g_stop.load(); ++i)
    {
        YomkRpc::MString msg;
        msg.data("hello ros2 " + std::to_string(i));
        resp = YOMKRPC_PUB_MSG("interop_pub", "rt/chatter_interop", &msg);
        if (resp.m_status != YomkResponse::eOk)
        {
            YOMK_ERROR_TAG("TestRos2InteropPub", "[SEND] publish failed");
        }
        else
        {
            YOMK_INFO_TAG("TestRos2InteropPub", "[SEND] ", msg.data());
        }
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }

    // 退出前显式销毁节点
    resp = YOMKRPC_DEL_NODE("interop_pub");
    if (resp.m_status != YomkResponse::eOk)
    {
        YOMK_ERROR_TAG("TestRos2InteropPub", "delete node failed: ", resp.m_msg);
        return 1;
    }
    return 0;
}
