#!/bin/bash
# =============================================================================
# YomkRpc 全量测试运行器
#
# 用法:
#   ./run_tests.sh               全量运行（含 2 个 stress，编译期规模 5000/30）
#   ./run_tests.sh --skip-stress 仅运行 7 个非 stress 测试（快速冒烟）
#   ./run_tests.sh --full        以闭环规模(STRESS_ITERS=100000, STRESS_CYCLES=50)
#                                 重配+重编 stress 目标后全量运行
#   ./run_tests.sh --bin DIR     指定测试可执行根目录（默认 <test>/build，自动定位子目录）
#   ./run_tests.sh --timeout N   覆盖单测试超时秒数（默认 300；stress 默认 1800）
#   ./run_tests.sh -h/--help     显示本帮助
#
# 行为:
#   1. 运行前清理 /dev/shm 中 FastDDS 上次遗留的共享内存残留（崩溃/超时时 SHM 段不会自清）
#   2. 逐个串行运行测试（快→慢，stress 压尾），每个测试在独立临时工作目录运行
#   3. 任一测试失败（非 0 退出 / 超时 / 正常退出但 /dev/shm 残留未自清理）→ 立即停止，
#      输出 [FAIL] 行摘要与日志路径
#   4. 日志落盘: test/test_logs/<时间戳>/<测试名>.log + summary.log
#   5. 全部结束后复查一次 /dev/shm 残留，发现则清理并计为失败
#
# 写磁盘操作审计（2026-09 逐文件核对）:
#   - 9 个测试程序与被测源码 src/*.cpp 均无写盘操作；唯一文件 IO 是 stress 两例
#     只读 /proc/self/status 与 /proc/self/fd 的资源采样器（VmRSS/Threads/fd）。
#   - 唯一运行时"磁盘"产物来自 FastDDS 3.6.1 默认 SHM 传输: /dev/shm 下
#     fastdds_* / fastdds_port* / sem.fastdds_port*_mutex / fast_datasharing_*，
#     正常销毁 participant 时自清；进程被 kill/崩溃/超时则遗留（本脚本接管清理）。
#   - 测试程序 main() 返回 0=全部通过，非 0=存在失败用例（[FAIL] 行输出）。
# =============================================================================

set -u

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BUILD_DIR="${SCRIPT_DIR}/build"   # --full 重编 stress 的默认构建目录
BIN_DIR="${BUILD_DIR}"            # 默认从 test/build 定位测试可执行
TIMEOUT_SECS=300
USER_TIMEOUT=0
SKIP_STRESS=0
FULL_MODE=0

# 测试清单：快→慢，stress 压尾（与 test/CMakeLists.txt 注册的目标一一对应）
ALL_TESTS=(
    TestHarnessSmoke
    TestYomkRpcServiceContract
    TestYomkRpcNodeLifecycle
    TestYomkRpcTopic
    TestYomkRpcLoan
    TestYomkRpcTypes
    TestYomkRpcConcurrency
    TestYomkRpcStressSerial
    TestYomkRpcStressConcurrent
)
STRESS_TESTS=(TestYomkRpcStressSerial TestYomkRpcStressConcurrent)

usage() {
    sed -n '2,12p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'
    exit 0
}

parse_args() {
    while [ $# -gt 0 ]; do
        case "$1" in
            --skip-stress) SKIP_STRESS=1 ;;
            --full)        FULL_MODE=1 ;;
            --bin)         [ $# -ge 2 ] || { echo "错误: --bin 需要目录参数"; exit 1; }
                           BIN_DIR="$2"; shift ;;
            --timeout)     [ $# -ge 2 ] || { echo "错误: --timeout 需要秒数参数"; exit 1; }
                           TIMEOUT_SECS="$2"; USER_TIMEOUT=1; shift ;;
            -h|--help)     usage ;;
            *) echo "错误: 未知参数 $1"; usage ;;
        esac
        shift
    done
}

# EXIT/INT/TERM 兜底清理临时工作目录（幂等）
CURRENT_WORKDIR=""
cleanup_workdir() {
    [ -n "${CURRENT_WORKDIR}" ] && rm -rf "${CURRENT_WORKDIR}"
    CURRENT_WORKDIR=""
}
trap cleanup_workdir EXIT
trap 'echo ""; echo "被用户中断"; exit 130' INT TERM

# 定位每个测试可执行（兼容 test/build/{Harness,YomkRpcService}/ 与单 bin 目录两种布局）
declare -A TEST_BIN
locate_tests() {
    local t found
    for t in "${TESTS[@]}"; do
        found="$(find "${BIN_DIR}" -maxdepth 2 -type f -name "${t}" -perm -u+x -print -quit 2>/dev/null)"
        if [ -n "${found}" ]; then
            TEST_BIN["${t}"]="${found}"
        fi
    done
}

BUILD_HINT="cmake -S ${SCRIPT_DIR} -B ${SCRIPT_DIR}/build -DCMAKE_PREFIX_PATH=\"\${YOMK_PREFIX_PATH:-/opt/yomk}\" && cmake --build ${SCRIPT_DIR}/build -j"

precheck_bin() {
    if [ ! -d "${BIN_DIR}" ]; then
        echo "错误: 测试可执行根目录不存在: ${BIN_DIR}"
        echo "请先构建: ${BUILD_HINT}"
        exit 1
    fi
    locate_tests
    local missing=() t
    for t in "${TESTS[@]}"; do
        if [ -z "${TEST_BIN[${t}]+set}" ]; then
            missing+=("${t}")
        fi
    done
    if [ ${#missing[@]} -gt 0 ]; then
        echo "错误: ${BIN_DIR} 缺少以下测试可执行（共 ${#missing[@]} 个）:"
        for t in "${missing[@]}"; do echo "   - ${t}"; done
        echo "请先构建: ${BUILD_HINT}"
        exit 1
    fi
}

# 清理 /dev/shm 中 FastDDS 遗留的共享内存（段/端口/锁/互斥量/数据共享）
clean_residue() {
    shopt -s nullglob
    local -a residue=(/dev/shm/fastdds_* /dev/shm/sem.fastdds_port*_mutex /dev/shm/fast_datasharing_*)
    shopt -u nullglob
    if [ ${#residue[@]} -gt 0 ]; then
        echo "-- 清理 FastDDS 残留: ${#residue[@]} 个 /dev/shm 共享内存条目"
        rm -f -- "${residue[@]}"
    fi
}

# 复查 /dev/shm 残留：列出并清理，返回非 0 表示存在残留
check_residue() {
    shopt -s nullglob
    local -a residue=(/dev/shm/fastdds_* /dev/shm/sem.fastdds_port*_mutex /dev/shm/fast_datasharing_*)
    shopt -u nullglob
    if [ ${#residue[@]} -gt 0 ]; then
        for p in "${residue[@]}"; do echo "   - ${p}"; done
        rm -f -- "${residue[@]}"
        return 1
    fi
    return 0
}

# 单测试超时：用户显式 --timeout 优先；stress 默认放宽到 1800s（与 CTest TIMEOUT 对齐）
timeout_for() {
    local t="$1"
    if [ "${USER_TIMEOUT}" -eq 1 ]; then
        echo "${TIMEOUT_SECS}"
    elif [ "${t}" = "TestYomkRpcStressSerial" ] || [ "${t}" = "TestYomkRpcStressConcurrent" ]; then
        echo 1800
    else
        echo "${TIMEOUT_SECS}"
    fi
}

# --full：以闭环规模重配+重编 2 个 stress 目标（规模持久写入 test/build cache）
rebuild_stress_full() {
    echo "-- full 模式: 重配 ${BUILD_DIR} 为闭环规模 STRESS_ITERS=100000 STRESS_CYCLES=50"
    cmake -S "${SCRIPT_DIR}" -B "${BUILD_DIR}" -DSTRESS_ITERS=100000 -DSTRESS_CYCLES=50 \
        || { echo "错误: stress 重配失败"; exit 1; }
    cmake --build "${BUILD_DIR}" \
        --target TestYomkRpcStressSerial TestYomkRpcStressConcurrent -j"$(nproc)" \
        || { echo "错误: stress 重编失败"; exit 1; }
    echo "-- 闭环规模已持久写入 ${BUILD_DIR}/CMakeCache.txt（后续默认运行沿用）"
}

main() {
    parse_args "$@"

    TESTS=("${ALL_TESTS[@]}")
    if [ "${SKIP_STRESS}" -eq 1 ]; then
        local keep=() t
        for t in "${TESTS[@]}"; do
            case " ${STRESS_TESTS[*]} " in
                *" ${t} "*) ;;
                *) keep+=("${t}") ;;
            esac
        done
        TESTS=("${keep[@]}")
        echo "-- 模式: 快速冒烟（--skip-stress，跳过 ${#STRESS_TESTS[@]} 个 stress）"
    fi

    clean_residue

    if [ "${FULL_MODE}" -eq 1 ]; then
        rebuild_stress_full
    else
        echo "-- 模式: 默认（stress 编译期规模 5000/30，--full 可切闭环规模）"
    fi

    precheck_bin

    local log_root="${SCRIPT_DIR}/test_logs/$(date +%Y%m%d_%H%M%S)"
    mkdir -p "${log_root}"
    local summary="${log_root}/summary.log"

    echo "-- 测试可执行目录: ${BIN_DIR}"
    echo "-- 日志目录:       ${log_root}"
    echo "-- 测试总数:       ${#TESTS[@]}"
    echo ""

    local total=${#TESTS[@]} passed=0 idx=0 t log rc start elapsed to
    for t in "${TESTS[@]}"; do
        idx=$((idx + 1))
        log="${log_root}/${t}.log"
        to="$(timeout_for "${t}")"
        CURRENT_WORKDIR="$(mktemp -d)"
        printf "[%2d/%2d] %-32s " "${idx}" "${total}" "${t}"
        start=${SECONDS}
        ( cd "${CURRENT_WORKDIR}" && timeout "${to}" "${TEST_BIN[${t}]}" ) > "${log}" 2>&1
        rc=$?
        elapsed=$((SECONDS - start))
        cleanup_workdir
        if [ ${rc} -eq 0 ]; then
            # 正常退出但遗留 /dev/shm → 测试未自清理，按失败处理
            local -a res
            shopt -s nullglob
            res=(/dev/shm/fastdds_* /dev/shm/sem.fastdds_port*_mutex /dev/shm/fast_datasharing_*)
            shopt -u nullglob
            if [ ${#res[@]} -gt 0 ]; then
                echo "FAIL (残留未清理, ${elapsed}s)"
                printf "[FAIL] %-32s 残留未清理: %s\n" "${t}" "${res[*]}" >> "${summary}"
                echo "-------------------------------------------"
                echo "测试 ${t} 正常退出但 /dev/shm 残留共享内存未自清理:"
                for p in "${res[@]}"; do echo "   - ${p}"; done
                rm -f -- "${res[@]}"
                echo "       已清理，请排查对应测试的 teardown"
                echo "完整日志: ${log}"
                echo "-------------------------------------------"
                exit 1
            fi
            echo "PASS (${elapsed}s)"
            printf "[PASS] %-32s %ds\n" "${t}" "${elapsed}" >> "${summary}"
            passed=$((passed + 1))
        else
            local reason="退出码 ${rc}"
            [ ${rc} -eq 124 ] && reason="超时(>${to}s)"
            echo "FAIL (${reason}, ${elapsed}s)"
            printf "[FAIL] %-32s %s\n" "${t}" "${reason}" >> "${summary}"
            echo "-------------------------------------------"
            echo "测试 ${t} 失败: ${reason}"
            echo "完整日志: ${log}"
            local fail_count
            fail_count=$(grep -c "\[FAIL\]" "${log}" 2>/dev/null || true)
            if [ -n "${fail_count}" ] && [ "${fail_count}" -gt 0 ]; then
                echo "失败用例（共 ${fail_count} 行 [FAIL]，摘要如下）:"
                grep "\[FAIL\]" "${log}" | head -20
            else
                echo "（无 [FAIL] 行，可能为崩溃/超时，请查看完整日志）"
                tail -30 "${log}"
            fi
            echo "-------------------------------------------"
            clean_residue   # 崩溃/超时遗留的 SHM 立即清掉，保证机器现场干净
            exit 1
        fi
    done

    # 现场残留验收（安全网；正常情况下逐测试检查已覆盖）
    local residue_out residue_rc
    residue_out="$(check_residue 2>&1)"
    residue_rc=$?
    if [ -n "${residue_out}" ]; then
        echo "${residue_out}" | tee -a "${summary}"
    fi
    if [ ${residue_rc} -ne 0 ]; then
        echo ""
        echo "==========================================="
        echo " 测试后现场残留检查未通过，整体判定失败"
        echo "==========================================="
        exit 1
    fi

    echo ""
    echo "==========================================="
    echo " YomkRpc 全量测试通过: ${passed}/${total}"
    echo " 总耗时: ${SECONDS}s"
    echo " 日志目录: ${log_root}"
    echo "==========================================="
    echo "YomkRpc 全量测试通过: ${passed}/${total}" >> "${summary}"
    exit 0
}

main "$@"
