#!/bin/bash
# 用法: source build.sh [额外的cmake参数...]
# 示例: source build.sh -DCMAKE_PREFIX_PATH=/path/to/YomkServer/install

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_NAME="YomkRpc"
BUILD_DIR="${SCRIPT_DIR}/build"
INSTALL_DIR="${SCRIPT_DIR}/install"
TEST_DIR="${SCRIPT_DIR}/test"
TEST_BUILD_DIR="${TEST_DIR}/build"
MSG_DIR="${SCRIPT_DIR}/msg"
MSG_BUILD_DIR="${MSG_DIR}/build"
_ORIG_DIR="$(pwd)"

# 解析用户传入的 cmake 参数
YOMK_SERVER_PATH=""
USER_INSTALL_PREFIX=""
for arg in "$@"; do
    if [[ "${arg}" == -DCMAKE_PREFIX_PATH=* ]]; then
        YOMK_SERVER_PATH="${arg#-DCMAKE_PREFIX_PATH=}"
    elif [[ "${arg}" == -DCMAKE_INSTALL_PREFIX=* ]]; then
        USER_INSTALL_PREFIX="${arg#-DCMAKE_INSTALL_PREFIX=}"
    fi
done

# 展开路径开头的 ~（bash 不会展开 -DXXX=~/path 中的 ~）
if [[ "${USER_INSTALL_PREFIX}" == "~"* ]]; then
    USER_INSTALL_PREFIX="${HOME}${USER_INSTALL_PREFIX#\~}"
fi
if [[ "${YOMK_SERVER_PATH}" == "~"* ]]; then
    YOMK_SERVER_PATH="${HOME}${YOMK_SERVER_PATH#\~}"
fi

# 用户指定了安装目录时，以用户指定的为准
if [ -n "${USER_INSTALL_PREFIX}" ]; then
    INSTALL_DIR="${USER_INSTALL_PREFIX}"
fi
echo "安装目录: ${INSTALL_DIR}"

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

# 编译安装主库
echo "步骤 1/3: 编译 YomkRpc 主库"
mkdir -p "${BUILD_DIR}"
cd "${BUILD_DIR}" || return 1

cmake "${SCRIPT_DIR}" -DCMAKE_INSTALL_PREFIX="${INSTALL_DIR}" "$@"
if [ $? -ne 0 ]; then
    echo "cmake 配置失败"
    cd "${_ORIG_DIR}"
    return 1
fi

cmake --build . --config Release --target install
if [ $? -ne 0 ]; then
    echo "编译失败"
    cd "${_ORIG_DIR}"
    return 1
fi

cd "${_ORIG_DIR}"

# 编译 msg 类型库（fastddsgen 生成工程，含 SWIG Python 绑定）
echo ""
echo "步骤 2/3: 编译 msg 类型库"
mkdir -p "${MSG_BUILD_DIR}"
cd "${MSG_BUILD_DIR}" || return 1

cmake "${MSG_DIR}" -DCMAKE_PREFIX_PATH=${HOME}/YomkServer/install -DCMAKE_INSTALL_PREFIX=${HOME}/YomkServer/install -DCMAKE_BUILD_TYPE=Release
if [ $? -ne 0 ]; then
    echo "msg cmake 配置失败"
    cd "${_ORIG_DIR}"
    return 1
fi

cmake --build . --config Release --target install
if [ $? -ne 0 ]; then
    echo "msg 编译安装失败"
    cd "${_ORIG_DIR}"
    return 1
fi

cd "${_ORIG_DIR}"

# 编译测试程序
if [ "${BUILD_TEST}" = "ON" ]; then
    echo ""
    echo "步骤 3/3: 编译测试程序"
    mkdir -p "${TEST_BUILD_DIR}"
    cd "${TEST_BUILD_DIR}" || return 1

    CMAKE_PREFIX_ARGS="-DCMAKE_PREFIX_PATH=${INSTALL_DIR}"
    if [ -n "${YOMK_SERVER_PATH}" ]; then
        CMAKE_PREFIX_ARGS="-DCMAKE_PREFIX_PATH=${INSTALL_DIR};${YOMK_SERVER_PATH}"
    fi

    cmake "${TEST_DIR}" ${CMAKE_PREFIX_ARGS}
    if [ $? -ne 0 ]; then
        echo "测试程序 cmake 配置失败"
        cd "${_ORIG_DIR}"
        return 1
    fi

    cmake --build . --config Release
    if [ $? -ne 0 ]; then
        echo "测试程序编译失败"
        cd "${_ORIG_DIR}"
        return 1
    fi

    # 设置临时环境变量：安装目录 lib + YomkServer lib（含 fastdds 等间接依赖）
    LD_LIBRARY_PATH="${INSTALL_DIR}/lib:${LD_LIBRARY_PATH}"
    if [ -n "${YOMK_SERVER_PATH}" ]; then
        LD_LIBRARY_PATH="${YOMK_SERVER_PATH}/lib:${LD_LIBRARY_PATH}"
    fi
    export LD_LIBRARY_PATH
    export PATH="${TEST_BUILD_DIR}:${PATH}"

    # 运行测试
    echo ""
    echo "========== 运行服务接口测试 =========="
    "./TestRpcTopic"
    TEST_SERVICE_RESULT=$?
    echo ""
    echo "========== 运行 loan 机制测试 =========="
    "./TestRpcTopicLoan"
    TEST_LOAN_RESULT=$?
    if [ ${TEST_SERVICE_RESULT} -ne 0 ] || [ ${TEST_LOAN_RESULT} -ne 0 ]; then
        echo ""
        echo "存在测试失败：TestRpcTopic=${TEST_SERVICE_RESULT} TestRpcTopicLoan=${TEST_LOAN_RESULT}"
    fi
    echo ""
    echo "示例程序 RpcPubHelloWorld/RpcSubHelloWorld 已编译（不参与自动测试）："
    echo "可另开两个终端分别运行 RpcSubHelloWorld 与 RpcPubHelloWorld 观察发布/订阅"
fi

# 返回原目录
cd "${_ORIG_DIR}" || { echo "Warning: cd failed"; exit 1; }
unset _ORIG_DIR

echo "======================================="
echo "✅ 所有编译已完成"
echo "======================================="
