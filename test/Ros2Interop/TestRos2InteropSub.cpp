/**
 * @file TestRos2InteropSub.cpp
 * @brief 真机互通回归判定程序·订阅端（run_ros2_interop.sh 驱动，ctest 树不含）
 *
 * 测试设施定位（examples 的 ExampleRos2InteropSub 为面向用户的教学示例，判定职责归本目录）：
 * 域 0 建节点 + MStringPubSubType set_name 成 ROS2 约定名（std_msgs::msg::dds_::String_）
 * 订阅 rt/chatter_interop_rev（ros2DefaultSubscriber 底座），逐条打印 "[RECV] ..."，
 * 收满 3 条即退出（回调置位退出标志立即生效，延迟窗内多余消息不打印；未收满由脚本
 * 兜底 SIGINT 后以退出码 1 反映，单独运行时 Ctrl+C 退出）。
 *
 * 对跑协议（run_ros2_interop.sh）：
 *   方向 2：ros2 topic pub /chatter_interop_rev -r 1 → 本程序（判定 ROS2→YomkRpc）
 *   （与 TestRos2InteropPub 不互通：两者分别只与 ROS2 CLI 对跑，判定口径见脚本头注释）
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
    auto resp = YOMKRPC_NODE("interop_sub", 0);
    if (resp.m_status != YomkResponse::eOk)
    {
        YOMK_ERROR_TAG("TestRos2InteropSub", "create node failed: ", resp.m_msg);
        return 1;
    }

    // 类型对象 new 后、移交前 set_name 成 ROS2 约定名（宏实参位内联 new 无法先改名）
    std::atomic<int> received{0};
    auto *type = new YomkRpc::MStringPubSubType();
    type->set_name("std_msgs::msg::dds_::String_");
    auto onMessage = [&](const void *data)
    {
        if (received.load() >= 3)
        {
            return; // 退出延迟窗内的多余消息（三路 pub 仍在发）不再打印/计数
        }
        auto *msg = static_cast<const YomkRpc::MString *>(data);
        received++;
        std::cout << "[RECV] " << msg->data() << std::endl;
        if (received.load() >= 3)
        {
            g_stop.store(true); // 收满即置退出标志，不等轮询周期，避免多收
        }
    };
    // QoS 用 ROS2 订阅端默认底座：BEST_EFFORT requested 对任意 offered（含 ros2 topic
    // pub 的 RELIABLE）均可匹配（头文件 YOMKRPC_SUB_TOPIC 注释同口径）
    resp = YOMKRPC_SUB_TOPIC("interop_sub", "rt/chatter_interop_rev", type, onMessage,
                             DDSQosProfile::ros2DefaultSubscriber());
    if (resp.m_status != YomkResponse::eOk)
    {
        YOMK_ERROR_TAG("TestRos2InteropSub", "register sub topic failed: ", resp.m_msg);
        YOMKRPC_DEL_NODE("interop_sub");
        return 1;
    }

    // 收满 3 条即退出（回调置位 g_stop 立即生效；最长等待 30s，未收满退出码 1 供脚本判定）
    std::signal(SIGINT, onSignal);
    while (!g_stop.load())
    {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    // 退出前显式销毁节点；收满 3 条返回 0，未收满返回 1
    resp = YOMKRPC_DEL_NODE("interop_sub");
    if (resp.m_status != YomkResponse::eOk)
    {
        YOMK_ERROR_TAG("TestRos2InteropSub", "delete node failed: ", resp.m_msg);
        return 1;
    }
    return received.load() >= 3 ? 0 : 1;
}
