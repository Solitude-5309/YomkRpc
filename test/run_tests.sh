#!/bin/bash
# =============================================================================
# YomkRpc 全量测试运行器
#
# 用法:
#   ./run_tests.sh               全量运行（含 2 个 stress，编译期规模 5000/30）
#   ./run_tests.sh --skip-stress 仅运行 17 个非 stress 测试（快速冒烟）
#   ./run_tests.sh --full        以闭环规模(STRESS_ITERS=100000, STRESS_CYCLES=50)
#                                 重配+重编 stress 目标后全量运行
#   ./run_tests.sh --bin DIR     指定测试可执行根目录（默认 <test>/build，自动定位子目录）
#   ./run_tests.sh --timeout N   覆盖单测试超时秒数（默认 300；stress 默认 1800）
#   ./run_tests.sh -h/--help     显示本帮助
#
# 行为:
#   1. 运行前清理 /dev/shm 中 FastDDS 上次遗留的共享内存残留（崩溃/超时时 SHM 段不会自清；
#      被活跃进程持有的条目跳过并警告——误删活体会使其锁孤儿化，触发 FastDDS is_zombie
#      误判导致 SHM 永久幽灵，须重启业务进程才能恢复）
#   2. 逐个串行运行测试（快→慢，stress 压尾），每个测试在独立临时工作目录运行
#      （bag 诸例的 bag 目录/mcap/metadata.json 产物即落在该目录，随用例后清理）
#   3. 任一测试失败（非 0 退出 / 超时 / 正常退出但 /dev/shm 残留未自清理）→ 立即停止，
#      输出 [FAIL] 行摘要与日志路径
#   4. 日志落盘: test/test_logs/<时间戳>/<测试名>.log + summary.log
#   5. 全部结束后复查一次 /dev/shm 残留：无进程持有的残留清理并计为失败，
#      被活跃进程持有的条目仅提示跳过（非本次测试泄漏），不计失败
#
# 写磁盘操作审计（2026-09 逐文件核对，bag 七例新增时更新）：
#   - 除 bag 七例外，其余测试程序与被测源码 src/*.cpp 均无写盘操作；唯一文件 IO 是 stress
#     两例只读 /proc/self/status 与 /proc/self/fd 的资源采样器（VmRSS/Threads/fd）。
#   - bag 七例的写盘（均落在运行时临时工作目录）：TestYomkRpcBagServiceLifecycle 与
#     TestFastDDSBagNodeRecord 经真实 DDS 录制产 bag 目录；TestYomkRpcBagServiceContract
#     （T7-T9）与 TestFastDDSBagNodeInfo/Reindex/Convert 经 mcap::McapWriter 造输入分片与
#     info/重建/转换产物；仅 TestFastDDSBagNodeValidation 校验失败路径不建任何文件。
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
# bag 七例：契约（DDS-free 快）→ 生命周期（含 ~3s 校验窗与 ~5s 录制）→ 节点层三快测
# （info/reindex/convert：McapWriter 造输入直测静态函数，DDS-free 毫秒级）→ 节点校验
# （3 个 ~3s 收敛窗）→ 节点录制收尾（~5s 录制 + mcap 读回）；bag 产物写在各自临时工作
# 目录，随 cleanup_workdir 清理
ALL_TESTS=(
    TestHarnessSmoke
    TestYomkRpcServiceContract
    TestYomkRpcDebugServiceContract
    TestYomkRpcBagServiceContract
    TestYomkRpcDebugServiceLifecycle
    TestYomkRpcBagServiceLifecycle
    TestFastDDSDebugNode
    TestFastDDSBagNodeInfo
    TestFastDDSBagNodeReindex
    TestFastDDSBagNodeConvert
    TestFastDDSBagNodeValidation
    TestFastDDSBagNodeRecord
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

# ---------------------------------------------------------------------------
# 持有者感知检测（防止误删活跃进程的 SHM 文件）:
#   背景: 活体文件的锁（_el）被删后 flock 孤儿化，持有者零感知；此后任意进程
#   open_port 会触发 FastDDS is_zombie 误判 → 删健康段同名重建 → 该进程沦为
#   SHM 永久幽灵（SHM 零收而 UDP 正常，重启业务进程才恢复）。
#   判据一（fd + maps 扫描）: 活跃进程持有的 /dev/shm 文件出现在两类位置：
#           锁文件 _el/_sl 持 open fd（/proc/<pid>/fd 链接目标，需保持 open+flock）；
#           段文件是 mmap 持有——映射建立后 fd 即被关闭（实测 boost
#           managed_shared_memory 如此），只在 /proc/<pid>/maps 的映射路径中可见。
#           fd 扫描对段文件全盲（实测误删引发幽灵），maps 扫描补上：映射在
#           进程存活期间必然保留。
#   判据二（inode 匹配）: POSIX named mutex（sem.fastdds_port<N>_mutex）在
#           glibc 2.35 下按名双盲——sem_open 映射的是混淆临时名 sem.<随机>（fd
#           即刻关闭，且临时名随即被 unlink），语义名只是同一 inode 的别名（实测）。
#           /proc/<pid>/maps 行自带 inode 列，收集活体映射的 inode 集合，候选文件
#           stat 其 inode 比对命中即活体——本体证据，与显示路径名无关。
#   判据三（试锁）: 对 _el/_sl 用 flock -n 试锁。flock 绑 inode：持有者活着
#           试锁必失败；持有者已死锁自动释放、试锁成功可删。不依赖 /proc 读
#           权限（兜住 root 进程持有场景）。
#   性能: fd 链接经 xargs 分批批量 readlink（每批 1000 个），fork 次数与 fd 总数
#         解耦；桌面环境数千 fd 下扫描耗时毫秒级，避免逐 fd fork 的秒级卡顿。
# ---------------------------------------------------------------------------
declare -A SHM_HELD=()      # key: /dev/shm 文件名（fd 链接目标 / maps 路径名）
declare -A SHM_INO_HELD=()  # key: 活体映射的 inode（maps 行自带，路径名混淆时唯一证据）

# 一次扫描建立持有集合：SHM_HELD 按名（fd 链接目标 + maps 路径名），
# SHM_INO_HELD 按映射 inode（覆盖 glibc named semaphore 混淆临时名）
scan_shm_holders() {
    SHM_HELD=()
    SHM_INO_HELD=()
    local link
    local -a fds=(/proc/[0-9]*/fd/*)
    if [ -e "${fds[0]}" ]; then
        while IFS= read -r link; do
            case "${link}" in
                /dev/shm/*) SHM_HELD["${link#/dev/shm/}"]=1 ;;
            esac
        done < <(printf '%s\0' "${fds[@]}" | xargs -0 -r -n 1000 readlink 2>/dev/null)
    fi
    # maps 行: <addr> <perms> <offset> <dev> <inode> <path> [(deleted)]。
    # 路径按名收集（段文件等）；inode 收集覆盖路径名被混淆/已 unlink 的映射。
    # (deleted) 旧映射只贡献 inode——若同名文件已被重建（新 inode），按名收集会
    # 把幽灵受害者的旧映射误判为活体持有，导致新文件永不清理。
    local -a w
    while IFS= read -r link; do
        case "${link}" in
            */dev/shm/*)
                read -r -a w <<< "${link}"
                SHM_INO_HELD["${w[4]}"]=1
                case "${link}" in
                    *"(deleted)") ;;
                    *)
                        link="${link##*/dev/shm/}"
                        SHM_HELD["${link%% *}"]=1 ;;
                esac ;;
        esac
    done < <(grep -h -- "/dev/shm/" /proc/[0-9]*/maps 2>/dev/null)
    return 0
}

# _el/_sl 专属：试锁失败即判定被活进程持有；文件不存在/试锁成功=不在用。
# 入参为裸文件名（/dev/shm 下的 basename），函数内补全绝对路径——否则会相对
# 脚本 cwd 找文件（不存在即误判“不在用”，试锁判据将永远失明）
_el_locked_by_live() {
    local f="/dev/shm/${1}"
    case "${f}" in *_el|*_sl) ;; *) return 1 ;; esac
    [ -e "${f}" ] || return 1
    if ( exec 3<>"${f}" && flock -n 3 ) 2>/dev/null; then
        return 1
    fi
    return 0
}

file_in_use() {
    [ -n "${SHM_HELD[${1}]+set}" ] && return 0
    # inode 本体判据：glibc named semaphore 映射混淆临时名，按名双盲时唯一证据
    local ino
    ino=$(stat -c %i "/dev/shm/${1}" 2>/dev/null) || return 1
    [ -n "${SHM_INO_HELD[${ino}]+set}" ] && return 0
    _el_locked_by_live "$1"
}

# 清理 /dev/shm 中 FastDDS 遗留的共享内存（段/端口/锁/互斥量/数据共享）；
# 被活跃进程持有的条目跳过并警告（见上方持有者感知检测说明）
clean_residue() {
    shopt -s nullglob
    local -a candidates=(/dev/shm/fastdds_* /dev/shm/sem.fastdds_port*_mutex /dev/shm/fast_datasharing_*)
    shopt -u nullglob
    if [ ${#candidates[@]} -eq 0 ]; then
        return 0    # /dev/shm 干净，无需扫描持有者
    fi
    echo "-- 清理前扫描 /dev/shm 持有者（活跃进程检测，防误删）..."
    scan_shm_holders
    local -a doomed=() held=() f
    for f in "${candidates[@]}"; do
        if file_in_use "$(basename "${f}")"; then
            held+=("${f}")
        else
            doomed+=("${f}")
        fi
    done
    local h
    if [ ${#held[@]} -gt 0 ]; then
        echo "警告: 检测到活跃 FastDDS 进程持有 ${#held[@]} 个 /dev/shm 条目，已跳过清理（防止误删导致 SHM 永久幽灵）:"
        for h in "${held[@]}"; do echo "   - ${h}"; done
    fi
    if [ ${#doomed[@]} -gt 0 ]; then
        echo "-- 清理 FastDDS 残留: ${#doomed[@]} 个 /dev/shm 共享内存条目"
        rm -f -- "${doomed[@]}"
    fi
}

# 复查 /dev/shm 残留：列出并清理，返回非 0 表示存在残留。
# Fast DDS SHM 段的回收存在异步窗口：participant 全部销毁后个别段（尤其默认域 0 的
# fastdds_port7415 及其信号量）可能延迟数秒才被清理，进程正常退出后立即复查会误报
# （测试自身 79/102 checks 全绿、participant 均经 API 显式销毁的场景亦会命中）。
# 因此检出残留后等待 RESIDUE_WAIT_SECS 再复查一次，仍在才判失败——真实崩溃/kill
# 泄漏不会自清，依然会被抓到，检测能力不变。
# 首轮与复查均先经持有者感知过滤：被活跃进程持有的条目仅提示跳过（非本次测试
# 泄漏），不清理、不计失败；仅对“无持有者且等待后仍在”的文件清理并判失败。
RESIDUE_WAIT_SECS=6

_residue_files() {
    shopt -s nullglob
    local -a residue=(/dev/shm/fastdds_* /dev/shm/sem.fastdds_port*_mutex /dev/shm/fast_datasharing_*)
    shopt -u nullglob
    echo "${residue[@]}"
}

check_residue() {
    local -a residue=() held=() leaked=() f
    read -r -a residue <<< "$(_residue_files)"
    if [ ${#residue[@]} -eq 0 ]; then
        return 0
    fi
    # 先分离“活体持有”与“疑似泄漏”：活体（常驻业务进程在用）不算残留，
    # 提示跳过即可，绝不能 rm（会触发 SHM 永久幽灵）也不能判 FAIL
    echo "-- 残留复查: 发现 ${#residue[@]} 项，扫描持有者..."
    scan_shm_holders
    for f in "${residue[@]}"; do
        if file_in_use "$(basename "${f}")"; then
            held+=("${f}")
        else
            leaked+=("${f}")
        fi
    done
    local h
    for h in "${held[@]}"; do
        echo "   跳过被活跃进程持有的条目（非本次测试泄漏）: ${h}"
    done
    if [ ${#leaked[@]} -eq 0 ]; then
        return 0
    fi
    echo "-- 无持有者残留 ${#leaked[@]} 项，等待 ${RESIDUE_WAIT_SECS}s 复查（SHM 自清异步窗口）..."
    sleep "${RESIDUE_WAIT_SECS}"
    read -r -a residue <<< "$(_residue_files)"
    if [ ${#residue[@]} -eq 0 ]; then
        return 0    # 延迟窗口内已自清，非泄漏
    fi
    # 复查窗口内活体可能退出/新泄漏可能出现，重新过滤一轮持有者
    echo "-- 复查: 重新扫描持有者..."
    scan_shm_holders
    local -a still=()
    for f in "${residue[@]}"; do
        file_in_use "$(basename "${f}")" || still+=("${f}")
    done
    if [ ${#still[@]} -eq 0 ]; then
        return 0    # 复查时仅剩活体持有文件，无真泄漏
    fi
    echo "   等待 ${RESIDUE_WAIT_SECS}s 后仍存在且无持有者（崩溃/kill 类泄漏）:"
    local p
    for p in "${still[@]}"; do echo "   - ${p}"; done
    rm -f -- "${still[@]}"
    return 1
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
            # 正常退出但遗留 /dev/shm → 测试未自清理，按失败处理。SHM 回收存在异步窗口，
            # 且相邻测试的段清理活动会进一步延迟自清，统一走 check_residue 的等待-复查
            # 窗口：窗口内自清视为 PASS，复查仍在才判 FAIL（真实泄漏仍会被抓到）
            local res_out res_rc
            res_out="$(check_residue 2>&1)"
            res_rc=$?
            if [ ${res_rc} -ne 0 ]; then
                echo "FAIL (残留未清理, ${elapsed}s)"
                printf "[FAIL] %-32s 残留未清理:\n%s\n" "${t}" "${res_out}" >> "${summary}"
                echo "-------------------------------------------"
                echo "测试 ${t} 正常退出但 /dev/shm 残留共享内存未自清理:"
                echo "${res_out}"
                echo "完整日志: ${log}"
                echo "-------------------------------------------"
                exit 1
            fi
            echo "PASS (${elapsed}s)"
            if [ -n "${res_out}" ]; then
                # 复查中有持有者跳过等提示时回显（check_residue 的输出被捕获，
                # PASS 路径默认静默丢弃，不回显则用户看不到活体文件被正确跳过）
                echo "${res_out}"
            fi
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
