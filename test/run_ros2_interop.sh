#!/bin/bash
# =============================================================================
# 闭环 6 用例 4b：ROS2 Humble 真机互通手动回归脚本（ctest 树不含，用户裁定）
#
# 前置：
#   1. /opt/ros/humble 环境可用（ros2 CLI 在 PATH）
#   2. 本仓编译安装后的 examples 二进制 ExampleRos2Interop（examples/CMakeLists 注册）
#
# 用法：
#   ./run_ros2_interop.sh /path/to/ExampleRos2Interop
#
# 流程（双向对跑，退出码即互通结果）：
#   方向 1：ExampleRos2Interop pub（set_name 新名 + 1Hz 发布）→ ros2 topic echo --once 收到
#   方向 2：ros2 topic pub -r 1 → ExampleRos2Interop sub（同名 set_name 订阅）打印 [RECV]
#
# 退出码：0 = 双向互通成功；1 = 某方向失败；2 = 参数错误
# =============================================================================

set -u

EXE="${1:?usage: run_ros2_interop.sh /path/to/ExampleRos2Interop}"
if [ ! -x "$EXE" ]; then
    echo "[FAIL] 二进制不存在或不可执行: $EXE"
    exit 2
fi

# ROS2 setup.bash 内部引用未定义变量（AMENT_TRACE_SETUP_FILES 等），与 set -u 不兼容，
# source 前后临时关闭/恢复（ROS2 环境脚本与 strict mode 的经典冲突）
set +u
source /opt/ros/humble/setup.bash
set -u

# 清理收尾：杀全部本脚本后台进程并等待（防孤儿持有管道/残留 DDS 实体）
cleanup() {
    local jobs_pids=""
    jobs_pids=$(jobs -p)
    if [ -n "$jobs_pids" ]; then
        kill $jobs_pids 2>/dev/null
        wait $jobs_pids 2>/dev/null
    fi
}
trap cleanup EXIT

echo "==== 方向 1：YomkRpc pub → ROS2 topic echo ===="
"$EXE" pub &
PUB_PID=$!
sleep 2 # discovery 建立窗
if timeout 15 ros2 topic echo /chatter_interop std_msgs/msg/String --once 2>/dev/null | grep -q "data:"; then
    echo "[PASS] YomkRpc pub → ros2 topic echo 收到（set_name 新名互通）"
else
    echo "[FAIL] ROS2 侧未收到 /chatter_interop"
    exit 1
fi
kill "$PUB_PID" 2>/dev/null
wait "$PUB_PID" 2>/dev/null

echo "==== 方向 2：ROS2 topic pub → YomkRpc sub ===="
ros2 topic pub /chatter_interop_rev std_msgs/msg/String "{data: hello}" -r 1 &
ROS_PID=$!
sleep 2 # discovery 建立窗
# sub 模式收到即退出（内部最长等 30s）；timeout 40 兜底
SUB_OUT=$(timeout 40 "$EXE" sub 2>&1)
SUB_RC=$?
if echo "$SUB_OUT" | grep -q "\[RECV\]"; then
    echo "[PASS] ros2 topic pub → YomkRpc sub 收到: $(echo "$SUB_OUT" | grep '\[RECV\]' | head -1)"
else
    echo "[FAIL] YomkRpc 侧未收到 /chatter_interop_rev（sub exit=$SUB_RC）"
    echo "$SUB_OUT" | tail -5
    exit 1
fi

echo "==== 真机互通双向 PASS ===="
exit 0
