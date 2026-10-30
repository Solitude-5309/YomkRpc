#!/bin/bash
# LaunchDiscoverServer.sh —— 启动 Fast DDS 发现服务器（Discovery Server）
#
# 用法: LaunchDiscoverServer.sh [--ip <监听IP>] [--port <监听端口>] [--type <udp|tcp>]
#   --ip   监听地址，缺省 127.0.0.1
#   --port 监听端口，缺省随 --type（udp→11811，tcp→42100）
#   --type 监听协议，缺省 udp；tcp 模式 client 走 YOMKRPC_TCP_DISCOVER_SERVERS 直配
# 前台运行，Ctrl+C 停止；server id 固定 -i 0。
#
# 二进制定位：按序探测 YomkServer/Fast DDS 安装（thirdparty/Fast-DDS 编译安装产物）中的
# fast-discovery-server 工具；若安装产物缺执行位，拷贝到临时目录补执行位运行（不动系统文件、
# 免 sudo），退出时自动清理。动态库依赖经 ldconfig 缓存解析（build_ubuntu.sh 已注册），
# 与示例程序运行条件一致。
#
# 完整演示流程（另开两个终端，先本脚本再 Pub/Sub，顺序任意）见 README 5.8。

set -u

IP="127.0.0.1"
TYPE="udp"
PORT=""  # 缺省随 TYPE：udp→11811 / tcp→42100，参数解析后回填

# 端口缺省值：与发现服务器工具缺省同源（udp: -p 11811；tcp: -q 42100）
default_port() {
    [ "$1" = "tcp" ] && echo "42100" || echo "11811"
}

usage() {
    echo "用法: $(basename "$0") [--ip <监听IP>] [--port <监听端口>] [--type <udp|tcp>]"
    echo "  --ip   监听地址，缺省 ${IP}"
    echo "  --port 监听端口，缺省 $(default_port "${TYPE}")（随 --type：udp→11811 / tcp→42100）"
    echo "  --type 监听协议，缺省 ${TYPE}"
}

# ===== 参数解析 =====
while [ $# -gt 0 ]; do
    case "$1" in
        --ip)
            [ $# -ge 2 ] || { echo "错误: --ip 缺少参数值"; usage; exit 1; }
            IP="$2"; shift 2 ;;
        --port)
            [ $# -ge 2 ] || { echo "错误: --port 缺少参数值"; usage; exit 1; }
            PORT="$2"; shift 2 ;;
        --type)
            [ $# -ge 2 ] || { echo "错误: --type 缺少参数值"; usage; exit 1; }
            TYPE="$2"; shift 2 ;;
        -h|--help)
            usage; exit 0 ;;
        *)
            echo "错误: 未知参数 '$1'"; usage; exit 1 ;;
    esac
done

if [ "${TYPE}" != "udp" ] && [ "${TYPE}" != "tcp" ]; then
    echo "错误: --type 仅支持 udp 或 tcp，当前为 '${TYPE}'"
    exit 1
fi
if [ -z "${PORT}" ]; then
    PORT="$(default_port "${TYPE}")"
fi

if [ -z "${IP}" ]; then
    echo "错误: --ip 不能为空"
    exit 1
fi
if ! echo "${PORT}" | grep -Eq '^[0-9]+$' || [ "${PORT}" -lt 1 ] || [ "${PORT}" -gt 65535 ]; then
    echo "错误: --port 须为 [1,65535] 内整数，当前为 '${PORT}'"
    exit 1
fi

# ===== 定位发现服务器二进制 =====
DS_BIN=""
for _CAND in \
    "${YOMK_PREFIX_PATH:-}/bin/fast-discovery-server" \
    "${YOMK_PREFIX_PATH:-}/bin/fast-discovery-server-1.0.1" \
    "/opt/yomk/bin/fast-discovery-server" \
    "/opt/yomk/bin/fast-discovery-server-1.0.1"
do
    if [ -n "${_CAND}" ] && [ -f "${_CAND}" ]; then
        DS_BIN="${_CAND}"
        break
    fi
done
if [ -z "${DS_BIN}" ]; then
    for _NAME in fast-discovery-server fast-discovery-server-1.0.1; do
        _FOUND="$(command -v "${_NAME}" 2>/dev/null || true)"
        if [ -n "${_FOUND}" ]; then
            DS_BIN="${_FOUND}"
            break
        fi
    done
fi
if [ -z "${DS_BIN}" ]; then
    echo "错误: 未找到 fast-discovery-server 工具（YomkServer/Fast DDS 安装产物）"
    echo "提示: 设置 YOMK_PREFIX_PATH 指向安装路径，或将其 bin 目录加入 PATH"
    exit 1
fi

# ===== 执行位兜底：缺执行位时拷贝到临时目录补执行位运行，退出自动清理 =====
TMP_DIR=""
if [ ! -x "${DS_BIN}" ]; then
    TMP_DIR="$(mktemp -d)"
    trap '[ -n "${TMP_DIR}" ] && rm -rf "${TMP_DIR}"' EXIT INT TERM
    cp "${DS_BIN}" "${TMP_DIR}/fast-discovery-server"
    chmod +x "${TMP_DIR}/fast-discovery-server"
    DS_BIN="${TMP_DIR}/fast-discovery-server"
fi

# ===== 启动（前台运行，Ctrl+C 停止） =====
if [ "${TYPE}" = "tcp" ]; then
    DS_TYPE_DESC="TCP（-t 监听地址 / -q 监听端口）"
    DS_ENV_HINT="export YOMKRPC_TCP_DISCOVER_SERVERS=${IP}:${PORT}"
else
    DS_TYPE_DESC="UDP（-l 监听地址 / -p 监听端口）"
    DS_ENV_HINT="export YOMKRPC_UDP_DISCOVER_SERVERS=${IP}:${PORT}"
fi
echo "============================================================"
echo " Fast DDS Discovery Server"
echo "   监听协议: ${DS_TYPE_DESC}"
echo "   监听地址: ${IP}:${PORT}"
echo "   client 等价直配串: ${IP}:${PORT}"
echo "   client 环境变量自动直配（另开终端执行）："
echo "     export YOMKRPC_USE_DISCOVER_SERVER=true"
echo "     ${DS_ENV_HINT}"
echo "   （另开两个终端运行 ExampleDiscoverServerPub/Sub，"
echo "     域 id 100/200 跨域互通，详见 README 5.8）"
echo "============================================================"

# Fast DDS 3.x 的 fast-discovery-server 要求首个参数为整数命令索引（42=SERVER，
# 见安装包 tools/fastdds/discovery/parser.py 的 command_to_int 映射），2.x 旧版工具
# 则直接解析 -i/-l/-p；以 "42 -h" 探测协议后附加对应前缀。
# udp 模式起 -l/-p（UDP 通道）；tcp 模式起 -t/-q（TCP 监听口）。
if [ "${TYPE}" = "tcp" ]; then
    DS_ARGS=(-i 0 -t "${IP}" -q "${PORT}")
else
    DS_ARGS=(-i 0 -l "${IP}" -p "${PORT}")
fi
if "${DS_BIN}" 42 -h > /dev/null 2>&1; then
    DS_ARGS=(42 "${DS_ARGS[@]}")
fi
"${DS_BIN}" "${DS_ARGS[@]}"
