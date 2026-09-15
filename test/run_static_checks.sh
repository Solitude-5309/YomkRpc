#!/bin/bash
# YomkRpc 静态代码检查运行器（镜像 YomkPluginSystem/test/run_static_checks.sh 形态）
# 用法:
#   ./run_static_checks.sh              默认双跑 cppcheck + clang-tidy
#   ./run_static_checks.sh --cppcheck   仅跑 cppcheck
#   ./run_static_checks.sh --tidy       仅跑 clang-tidy
#   ./run_static_checks.sh -h|--help    显示本帮助
# 行为:
#   1. cppcheck 档: 只检测自有源码 src/ include/ examples/，零告警验收（--error-exitcode=1），
#      先打印检测文件清单，扫描时过滤显示逐文件 Checking 进度行与告警行（完整 verbose
#      输出落盘日志）；thirdparty/ 与 msg/（含安装树中的第三方/生成头）
#      仅参与 include 解析，路径级抑制不检测
#   2. clang-tidy 档: 检查集读仓库根 .clang-tidy（WarningsAsErrors=* 使告警即非零退出），
#      分析 src/*.cpp（compile_commands.json 经 -p 指向仓库根，主库构建自动导出），
#      header-filter 限本仓库 include/src 头
#   3. 任一工具告警 → 输出 [FAIL] 摘要并以非零码退出；全部零告警 → 输出 [PASS]
#   4. 日志落盘: test/test_logs/<时间戳>/cppcheck.log + clang-tidy.log + summary.log
# 依赖: cppcheck、clang-tidy（缺失时报错并给出安装提示）

set -u

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

usage() {
    sed -n '2,18p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'
    exit 0
}

RUN_TIDY=0
RUN_CPPCHECK=0
while [ $# -gt 0 ]; do
    case "$1" in
        --cppcheck) RUN_CPPCHECK=1 ;;
        --tidy)     RUN_TIDY=1 ;;
        -h|--help)  usage ;;
        *) echo "错误: 未知参数 $1"; usage ;;
    esac
    shift
done
if [ ${RUN_TIDY} -eq 0 ] && [ ${RUN_CPPCHECK} -eq 0 ]; then
    RUN_TIDY=1
    RUN_CPPCHECK=1
fi

if [ ${RUN_CPPCHECK} -eq 1 ] && ! command -v cppcheck >/dev/null 2>&1; then
    echo "错误: 未找到 cppcheck，请先安装: sudo apt-get install cppcheck"
    exit 1
fi
if [ ${RUN_TIDY} -eq 1 ] && ! command -v clang-tidy >/dev/null 2>&1; then
    echo "错误: 未找到 clang-tidy，请先安装: sudo apt-get install clang-tidy"
    exit 1
fi
if [ ${RUN_TIDY} -eq 1 ] && [ ! -f "${REPO_DIR}/compile_commands.json" ]; then
    echo "错误: ${REPO_DIR}/compile_commands.json 不存在，请先配置构建主库生成（cmake export_compile_commands 自动导出）"
    exit 1
fi

FAILED=0

# 日志落盘（与 run_tests.sh 同形态）：test_logs/<时间戳>/ 下 cppcheck.log / clang-tidy.log / summary.log
LOG_ROOT="${SCRIPT_DIR}/test_logs/$(date +%Y%m%d_%H%M%S)"
mkdir -p "${LOG_ROOT}"
SUMMARY="${LOG_ROOT}/summary.log"
echo "-- 日志目录: ${LOG_ROOT}"

# ---------- cppcheck：自有源码零告警验收（不检测第三方/生成代码） ----------
# -I 三条仅用于解析依赖头（<fastdds/...>、<YomkServer/...>、<YomkRpc/...>、<YomkRpcMsg/...>，
# 自有 API 头按安装树路径 include，源码树无 YomkRpc/ 目录层级，须依赖安装前缀解析）；
# thirdparty/ 与 msg/ 全部路径级忽略——cppcheck 会沿 include 链把告警归到被包含的头文件，
# 抑制后告警只可能来自 src/include/examples 自有源码。
if [ ${RUN_CPPCHECK} -eq 1 ]; then
    # 检测文件清单：先明示扫什么、共多少个，再进入扫描
    echo "-- cppcheck 检测文件清单（src include examples，排除 build/ 生成物）:"
    mapfile -t CPPCHECK_FILES < <(find "${REPO_DIR}/src" "${REPO_DIR}/include" "${REPO_DIR}/examples" \
        -type f \( -name '*.cpp' -o -name '*.h' -o -name '*.hpp' \) ! -path '*/build/*' | sort)
    for f in "${CPPCHECK_FILES[@]}"; do
        echo "--   ${f#"${REPO_DIR}/"}"
    done
    echo "-- cppcheck 开始扫描（共 ${#CPPCHECK_FILES[@]} 个文件，Checking 行即逐文件进度，完整 verbose 日志落盘）..."
    CPPCHECK_OUT="${LOG_ROOT}/cppcheck.log"
    cppcheck --enable=warning,performance,portability --std=c++17 --language=c++ \
        --verbose -j"$(nproc)" \
        --inline-suppr --suppress=missingIncludeSystem --suppress=toomanyconfigs \
        -I "${REPO_DIR}/include" \
        -I "${REPO_DIR}/thirdparty/Fast-DDS-3.6.1/install/include" \
        -I "${YOMK_PREFIX_PATH:-/opt/yomk}/include" \
        --suppress='*:*/thirdparty/*' \
        --suppress="*:${YOMK_PREFIX_PATH:-/opt/yomk}/include/*" \
        -i"${REPO_DIR}/examples/build" \
        --error-exitcode=1 \
        "${REPO_DIR}/src" "${REPO_DIR}/include" "${REPO_DIR}/examples" 2>&1 \
        | tee "${CPPCHECK_OUT}" \
        | grep --line-buffered -E '^Checking [^:]*\.\.\.|warning:|performance:|portability:|error:'
    rc=${PIPESTATUS[0]}
    if [ ${rc} -ne 0 ]; then
        echo "[FAIL] cppcheck 检出告警（退出码 ${rc}）:"
        grep -E "warning:|performance:|portability:|error:" "${CPPCHECK_OUT}" | head -20
        printf "[FAIL] cppcheck 退出码 %s\n" "${rc}" >> "${SUMMARY}"
        FAILED=1
    else
        echo "[PASS] cppcheck 零告警"
        printf "[PASS] cppcheck 零告警\n" >> "${SUMMARY}"
    fi
fi

# ---------- clang-tidy：src 编译单元 + 本仓库头，零告警验收（逐文件输出进度） ----------
if [ ${RUN_TIDY} -eq 1 ]; then
    TIDY_FILES=("${REPO_DIR}"/src/*.cpp)
    TIDY_TOTAL=${#TIDY_FILES[@]}
    echo "-- clang-tidy 扫描 src/*.cpp（共 ${TIDY_TOTAL} 个，检查集: 仓库根 .clang-tidy）..."
    TIDY_OUT="${LOG_ROOT}/clang-tidy.log"
    TIDY_FAIL=0
    TIDY_IDX=0
    for f in "${TIDY_FILES[@]}"; do
        TIDY_IDX=$((TIDY_IDX + 1))
        echo "-- [${TIDY_IDX}/${TIDY_TOTAL}] $(basename "${f}")"
        if ! clang-tidy -p "${REPO_DIR}" \
            --header-filter='.*/YomkRpc/(include|src)/.*' \
            "${f}" >> "${TIDY_OUT}" 2>&1; then
            echo "    [FAIL] $(basename "${f}") 检出告警或编译错误"
            TIDY_FAIL=1
        fi
    done
    if [ ${TIDY_FAIL} -ne 0 ]; then
        echo "[FAIL] clang-tidy 检出告警:"
        grep -E "warning:|error:" "${TIDY_OUT}" | head -20
        printf "[FAIL] clang-tidy 告警\n" >> "${SUMMARY}"
        FAILED=1
    else
        echo "[PASS] clang-tidy 零告警"
        printf "[PASS] clang-tidy 零告警\n" >> "${SUMMARY}"
    fi
fi

if [ ${FAILED} -ne 0 ]; then
    echo "==========================================="
    echo " 静态代码检查未通过（日志目录: ${LOG_ROOT}）"
    echo "==========================================="
    echo "YomkRpc 静态代码检查未通过" >> "${SUMMARY}"
    exit 1
fi
echo "==========================================="
echo " 静态代码检查全部通过（日志目录: ${LOG_ROOT}）"
echo "==========================================="
echo "YomkRpc 静态代码检查全部通过" >> "${SUMMARY}"
exit 0
