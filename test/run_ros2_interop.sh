#!/bin/bash
# =============================================================================
# 闭环 6 用例 4b：ROS2 真机互通手动回归脚本（ctest 树不含，用户裁定）
#
# 前置：
#   1. ROS2 环境（脚本自动探测版本：优先继承当前终端已 source 的 ROS_DISTRO，
#      否则取 /opt/ros/* 字典序最后一个发行版；缺失时 exit 2 并提示安装后重跑）
#   2. test 树构建后的判定二进制（test/build/Ros2Interop/TestRos2InteropPub|Sub，
#      examples 的 ExampleRos2InteropPub|Sub 为用户教学示例，不承担判定职责）
#
# 用法：
#   ./test/run_ros2_interop.sh                # 无参：默认取 test 树构建产物
#   ./test/run_ros2_interop.sh <Pub> [<Sub>]  # 可选：显式覆盖判定程序路径
#   （默认：本脚本同目录下 build/Ros2Interop/TestRos2InteropPub 与同目录 TestRos2InteropSub；
#     未构建时先执行 cmake -S test -B test/build -DCMAKE_PREFIX_PATH=/opt/yomk &&
#     cmake --build test/build --target TestRos2InteropPub TestRos2InteropSub）
#
# 两项判定（以数据到达为准；两侧收发消息流逐条一行展示再判 PASS/FAIL）：
#   方向 1：TestRos2InteropPub（set_name 新名 + 1Hz 发 30 条 "hello ros2 N" 后自退）→ ros2 topic echo（6s 窗口）收到多条
#   方向 2：ros2 topic pub 三路各 -r 1 发 "hello yomkrpc N"（N=1..3，VOLATILE 不回放故 Sub 先行匹配）→ TestRos2InteropSub 收满 3 条 [RECV] 即退
#
# 退出码：0 = 两项全过；1 = 某方向失败；2 = 参数错误/ROS2 环境不可用
# =============================================================================

set -u

# 判定程序路径：无参默认取本脚本同目录 test 树构建产物，可选传参覆盖（路径固定非必输）
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PUB_EXE="${1:-${SCRIPT_DIR}/build/Ros2Interop/TestRos2InteropPub}"
SUB_EXE="${2:-$(dirname "$PUB_EXE")/TestRos2InteropSub}"
if [ ! -x "$PUB_EXE" ]; then
    echo "[FAIL] 发布端不存在或不可执行: $PUB_EXE"
    echo "       先构建 test 树判定程序："
    echo "         cmake -S $(dirname "$SCRIPT_DIR")/test -B $SCRIPT_DIR/build -DCMAKE_PREFIX_PATH=/opt/yomk"
    echo "         cmake --build $SCRIPT_DIR/build --target TestRos2InteropPub TestRos2InteropSub"
    exit 2
fi
if [ ! -x "$SUB_EXE" ]; then
    echo "[FAIL] 订阅端不存在或不可执行: $SUB_EXE"
    exit 2
fi

# ROS2 预检：自动搜索可用发行版（/opt/ros/*/setup.bash）。优先继承当前终端已 source 的
# ROS_DISTRO（对应 setup.bash 存在时），否则取字典序最后一个发行版（foxy < galactic <
# humble < iron < jazzy ... 按字母序近似发布序）；全部缺失时告知用户并提示安装后重跑。
ROS_SETUP=""
if [ -n "${ROS_DISTRO:-}" ] && [ -f "/opt/ros/${ROS_DISTRO}/setup.bash" ]; then
    ROS_SETUP="/opt/ros/${ROS_DISTRO}/setup.bash" # 已 source 的发行版优先
else
    for setup in /opt/ros/*/setup.bash; do # 无匹配时 glob 保持字面量，由 [ -f ] 过滤
        [ -f "$setup" ] && ROS_SETUP="$setup"
    done
fi
if [ -z "$ROS_SETUP" ]; then
    echo "[FAIL] 未检测到 ROS2 环境（/opt/ros/*/setup.bash 均不存在）"
    echo "       本脚本验证 YomkRpc 与 ROS2 的 DDS 层互通，须先安装 ROS2"
    echo "       （Ubuntu 22.04 示例：sudo apt install ros-humble-desktop），"
    echo "       安装完成后重跑本脚本验证：./test/run_ros2_interop.sh"
    exit 2
fi

# setup.bash 内部引用未定义变量（AMENT_TRACE_SETUP_FILES 等），与 set -u 不兼容，
# source 前后临时关闭/恢复（ROS2 环境脚本与 strict mode 的经典冲突）
set +u
source "$ROS_SETUP"
set -u
# source 成功 != 可用：再验 ros2 CLI 已进 PATH（残缺安装兜底拦截）
if ! command -v ros2 >/dev/null 2>&1; then
    echo "[FAIL] 已 source $ROS_SETUP 但 ros2 CLI 不可用，请检查 ROS2 安装完整性"
    echo "       （重装/补装后重跑本脚本验证：./test/run_ros2_interop.sh）"
    exit 2
fi

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
PUB_LOG=/tmp/yomk_interop_pub.log
"$PUB_EXE" >"$PUB_LOG" 2>&1 &
PUB_PID=$!
sleep 2 # discovery 建立窗
ECHO_OUT=$(timeout 6 ros2 topic echo /chatter_interop std_msgs/msg/String 2>/dev/null)
kill -INT "$PUB_PID" 2>/dev/null
wait "$PUB_PID" 2>/dev/null
# 收发消息流逐条一行对照：发布侧 [SEND] 序列，接收侧 echo data: 序列
grep "\[SEND\]" "$PUB_LOG" | sed "s/.*\[SEND\] */[YomkRpc pub] /"
PUB_TOTAL=$(grep -c "\[SEND\]" "$PUB_LOG")
rm -f "$PUB_LOG"
ECHO_N=$(echo "$ECHO_OUT" | grep -c "^data:")
if [ "$ECHO_N" -gt 0 ]; then
    echo "$ECHO_OUT" | grep "^data:" | sed "s/^data: */[ros2 echo ] /"
    echo "[PASS] 方向 1：YomkRpc pub → ros2 topic echo 收到（set_name 新名互通，pub ${PUB_TOTAL} 条 / echo ${ECHO_N} 条）"
else
    echo "[ros2 echo ] （6s 窗口无输出）"
    echo "[FAIL] 方向 1：ROS2 侧未收到 /chatter_interop（pub ${PUB_TOTAL} 条）"
    exit 1
fi

echo "==== 方向 2：ROS2 topic pub → YomkRpc sub ===="
SUB_LOG=/tmp/yomk_interop_sub.log
"$SUB_EXE" >"$SUB_LOG" 2>&1 &
SUB_PID=$!
sleep 2 # Sub discovery 建立窗（VOLATILE 不回放：Sub 先行匹配后再发保证送达）
# 三路发布：各 -r 1 持续发固定序号内容，每路一行展示
for i in 1 2 3; do
    echo "[ros2 pub  ] hello yomkrpc $i（-r 1 持续发布，直到 Sub 收满 3 条）"
    timeout 8 ros2 topic pub /chatter_interop_rev std_msgs/msg/String "{data: hello yomkrpc $i}" -r 1 >/dev/null 2>&1 &
    sleep 0.2
done
# 等 Sub 收满 3 条自退（最长 30s），等不到自退则 SIGINT 收尾
for _ in $(seq 1 35); do
    kill -0 "$SUB_PID" 2>/dev/null || break
    sleep 1
done
kill -INT "$SUB_PID" 2>/dev/null
wait "$SUB_PID" 2>/dev/null
RECV_N=$(grep -c "\[RECV\]" "$SUB_LOG")
if [ "$RECV_N" -ge 3 ]; then
    grep "\[RECV\]" "$SUB_LOG" | sed "s/.*\[RECV\] */[YomkRpc sub] /"
    echo "[PASS] 方向 2：ros2 topic pub → YomkRpc sub 收到"
else
    grep "\[RECV\]" "$SUB_LOG" | sed "s/.*\[RECV\] */[YomkRpc sub] /"
    echo "[FAIL] 方向 2：YomkRpc 侧未收满 3 条 /chatter_interop_rev（共 ${RECV_N} 条）"
    tail -5 "$SUB_LOG"
    rm -f "$SUB_LOG"
    exit 1
fi
rm -f "$SUB_LOG"

echo "==== 真机互通两项判定全 PASS ===="
exit 0
