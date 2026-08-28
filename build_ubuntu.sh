#!/bin/bash
# 一键编译脚本（交互式）
# 用法: source build_ubuntu.sh
# 依次交互询问 YomkServer 安装路径（前置路径）与扩展安装路径，默认均取环境变量 YOMK_PREFIX_PATH，可修改
# 扩展库与 YomkServer 安装到一起（头文件由 YomkServer::YomkServer 的 INTERFACE include 统一提供）
# 安装后将扩展 lib 注册到系统动态库搜索路径（复用 yomk.conf）并刷新 ldconfig 缓存，新开任意终端即可找到扩展 so

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_NAME="YomkRpc"
BUILD_DIR="${SCRIPT_DIR}/build"
TEST_DIR="${SCRIPT_DIR}/test"
TEST_BUILD_DIR="${TEST_DIR}/build"
MSG_DIR="${SCRIPT_DIR}/msg"
MSG_BUILD_DIR="${MSG_DIR}/build"
_ORIG_DIR="$(pwd)"

# 路径规范化：展开 ~ 、相对路径补全
_normalize_path() {
    local p="$1"
    p="${p/#\~/$HOME}"
    if [[ -n "${p}" && "${p}" != /* ]]; then
        p="$(pwd)/${p}"
    fi
    echo "${p}"
}

if [ -z "${YOMK_PREFIX_PATH}" ]; then
    echo "警告: 未检测到环境变量 YOMK_PREFIX_PATH，可能未通过 build_ubuntu.sh 安装 YomkServer，请手动输入安装路径"
fi

# 交互询问 YomkServer 安装路径（前置路径），默认取环境变量 YOMK_PREFIX_PATH
read -r -p "请输入 YomkServer 安装路径 [默认: ${YOMK_PREFIX_PATH:-无}]: " _INPUT_PREFIX
YOMK_SERVER_PATH="$(_normalize_path "${_INPUT_PREFIX:-${YOMK_PREFIX_PATH}}")"
if [ -z "${YOMK_SERVER_PATH}" ]; then
    echo "错误: 未指定 YomkServer 安装路径"
    return 1
fi
echo "-- YomkServer 安装路径: ${YOMK_SERVER_PATH}"

# 交互询问扩展安装路径，默认装入 YomkServer 安装目录（与 YomkServer 安装到一起）
read -r -p "请输入扩展安装路径 [默认: ${YOMK_PREFIX_PATH:-无}]: " _INPUT_INSTALL
INSTALL_DIR="$(_normalize_path "${_INPUT_INSTALL:-${YOMK_PREFIX_PATH}}")"
unset _INPUT_PREFIX _INPUT_INSTALL
if [ -z "${INSTALL_DIR}" ]; then
    echo "错误: 未指定扩展安装路径"
    return 1
fi
echo "-- 扩展安装路径: ${INSTALL_DIR}"

# 安装目录不可写时（如 /opt/yomk）使用 sudo 执行安装
# 目录不存在时向上找最近的已存在父目录判断；已存在目录用真实写入探测（避免新建目录属 root 导致 -w 误判）
SUDO=""
_PROBE_DIR="${INSTALL_DIR}"
while [ -n "${_PROBE_DIR}" ] && [ ! -e "${_PROBE_DIR}" ]; do
    _PROBE_DIR="$(dirname "${_PROBE_DIR}")"
done
if [ -e "${_PROBE_DIR}" ] && [ ! -w "${_PROBE_DIR}" ]; then
    SUDO="sudo"
elif [ -e "${INSTALL_DIR}" ] && ! touch "${INSTALL_DIR}/.yomk_write_test" 2>/dev/null; then
    SUDO="sudo"
else
    rm -f "${INSTALL_DIR}/.yomk_write_test" 2>/dev/null
fi
unset _PROBE_DIR

# ========== 环境检查 ==========
info() {
    echo "[INFO] $*"
}

warn() {
    echo "[WARN] $*"
}

check_and_install_deps() {
    local DEPS=(
        gcc
        swig
        python3-dev
        build-essential
        cmake
    )

    local MISSING=()

    for pkg in "${DEPS[@]}"; do
        if ! dpkg -l | grep -q "^ii  $pkg "; then
            MISSING+=("$pkg")
        fi
    done

    if [ ${#MISSING[@]} -eq 0 ]; then
        info "所有依赖已安装，跳过"
        return
    fi

    warn "检测到以下依赖未安装：${MISSING[*]}"

    read -p "是否现在安装这些依赖？(y/n，默认 y): " ANSWER
    ANSWER=${ANSWER:-y}

    if [[ "$ANSWER" =~ ^[Yy]$ ]]; then
        info "开始安装依赖..."
        sudo apt update
        sudo apt install -y "${MISSING[@]}"
        info "依赖安装完成"
    else
        warn "依赖不完整，脚本终止"
        return 1
    fi
}

# 执行环境检查
check_and_install_deps
if [ $? -ne 0 ]; then
    cd "${_ORIG_DIR}"
    return 1
fi

# 询问是否编译 test
read -p "编译测试程序? [Y/n]: " BUILD_TEST
BUILD_TEST=${BUILD_TEST:-y}
if [[ "${BUILD_TEST}" =~ ^[Yy]$ ]]; then
    BUILD_TEST="ON"
else
    BUILD_TEST="OFF"
fi

# 步骤 1/3: 编译安装主库
echo ""
echo "步骤 1/3: 编译 ${PROJECT_NAME} 主库"
mkdir -p "${BUILD_DIR}"
cd "${BUILD_DIR}" || return 1

cmake "${SCRIPT_DIR}" -DCMAKE_INSTALL_PREFIX="${INSTALL_DIR}" -DCMAKE_PREFIX_PATH="${YOMK_SERVER_PATH}"
if [ $? -ne 0 ]; then
    echo "cmake 配置失败"
    cd "${_ORIG_DIR}"
    return 1
fi

${SUDO} cmake --build . --config Release --target install
if [ $? -ne 0 ]; then
    echo "编译失败"
    cd "${_ORIG_DIR}"
    return 1
fi

cd "${_ORIG_DIR}"

# 步骤 2/3: 编译 msg 类型库（fastddsgen 生成工程，含 SWIG Python 绑定）
# FastDDS 已随主库装入 INSTALL_DIR，msg 库同样安装到扩展安装路径
echo ""
echo "步骤 2/3: 编译 msg 类型库"
mkdir -p "${MSG_BUILD_DIR}"
cd "${MSG_BUILD_DIR}" || return 1

cmake "${MSG_DIR}" -DCMAKE_PREFIX_PATH="${INSTALL_DIR}" -DCMAKE_INSTALL_PREFIX="${INSTALL_DIR}" -DCMAKE_BUILD_TYPE=Release
if [ $? -ne 0 ]; then
    echo "msg cmake 配置失败"
    cd "${_ORIG_DIR}"
    return 1
fi

${SUDO} cmake --build . --config Release --target install
if [ $? -ne 0 ]; then
    echo "msg 编译安装失败"
    cd "${_ORIG_DIR}"
    return 1
fi

cd "${_ORIG_DIR}"

# 注册扩展库路径到系统动态库搜索路径（扩展属于 yomk，复用 yomk.conf，幂等追加）
YOMK_LDCONF_FILE="/etc/ld.so.conf.d/yomk.conf"
if ! grep -qxF "${INSTALL_DIR}/lib" "${YOMK_LDCONF_FILE}" 2>/dev/null; then
    echo "-- 注册动态库搜索路径: ${YOMK_LDCONF_FILE}"
    echo "${INSTALL_DIR}/lib" | sudo tee -a "${YOMK_LDCONF_FILE}" >/dev/null
fi
# 刷新动态库缓存：新增的 so 不会自动进入 ld.so.cache，必须重新执行 ldconfig
echo "-- 刷新动态库缓存 (ldconfig)..."
sudo ldconfig
if [ $? -ne 0 ]; then
    echo "ldconfig 执行失败"
    cd "${_ORIG_DIR}"
    return 1
fi

# 步骤 3/3: 编译测试程序（随扩展一并安装到 ${INSTALL_DIR}/bin）
if [ "${BUILD_TEST}" = "ON" ]; then
    echo ""
    echo "步骤 3/3: 编译测试程序"
    mkdir -p "${TEST_BUILD_DIR}"
    cd "${TEST_BUILD_DIR}" || return 1

    cmake "${TEST_DIR}" -DCMAKE_PREFIX_PATH="${INSTALL_DIR};${YOMK_SERVER_PATH}" -DCMAKE_INSTALL_PREFIX="${INSTALL_DIR}"
    if [ $? -ne 0 ]; then
        echo "测试程序 cmake 配置失败"
        cd "${_ORIG_DIR}"
        return 1
    fi

    ${SUDO} cmake --build . --config Release --target install
    if [ $? -ne 0 ]; then
        echo "测试程序编译失败"
        cd "${_ORIG_DIR}"
        return 1
    fi
fi

cd "${_ORIG_DIR}"
unset _ORIG_DIR

# ========== 安装结果 ==========
echo "==========================================="
echo " ${PROJECT_NAME} 扩展安装成功!"
echo "-------------------------------------------"
echo " 安装路径:       ${INSTALL_DIR}"
echo " YomkServer 路径: ${YOMK_SERVER_PATH}"
echo " 动态库缓存:"
ldconfig -p | grep -i "${PROJECT_NAME}" || true
if [ "${BUILD_TEST}" = "ON" ]; then
    echo " 测试程序列表（安装于 ${INSTALL_DIR}/bin）:"
    for _BIN in "${INSTALL_DIR}"/bin/TestYomkRpc*; do
        [ -x "${_BIN}" ] && echo "   - $(basename "${_BIN}")"
    done
    unset _BIN
    echo " 可直接运行 TestYomkRpcTopic / TestYomkRpcTopicLoan 验证；"
    echo " 示例程序 TestYomkRpcPub/TestYomkRpcSub 可另开两个终端分别运行观察跨进程发布/订阅"
fi
echo "==========================================="
echo "编译完成，扩展库已注册到系统动态库缓存，新开任意终端即可使用"
