# =============================================================================
# YomkRpc 测试树（独立 CMake 工程，与 examples/ 同构；测试程序仅供开发验证，不 install）
#
# 设计原则（用户决策）：
#   1. 只测 YomkRpc 自有源码（../src、../include）；第三方（FastDDS/YomkServer/YomkRpcMsg）
#      默认正确，只链接、不插桩、不修改。
#   2. 为仅对自有源码插桩 sanitizer/coverage，测试树从源码重新编译 ../src/*.cpp 为被测目标
#      yomkrpc_under_test，而非链接已安装的 libYomkRpc.so（后者无法单独插桩）。
#   3. 每个被测主题一个独立可执行程序（单一 TU + 纯 main + CHECK），进程隔离，
#      规避 YomkAPI 单进程单次初始化约束。
#
# 构建（在 test/ 下独立配置，勿污染主库构建）：
#   cmake -S test -B test/build -DCMAKE_PREFIX_PATH="/opt/yomk"
#   cmake --build test/build -j
# sanitizer / coverage 开关见下方 CACHE 选项；静态检查用 test/run_static_checks.sh（独立运行器，读仓库根 .clang-tidy）。
# =============================================================================
cmake_minimum_required(VERSION 3.14)
project(YomkRpcTest LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
# 导出 compile_commands.json 供 clang-tidy 使用
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# YomkRpc 源码根（test/ 的上一级）
get_filename_component(YOMKRPC_ROOT "${CMAKE_CURRENT_SOURCE_DIR}/.." ABSOLUTE)

# ----------------------------- 依赖定位 -----------------------------
find_package(YomkServer REQUIRED)
message(STATUS "[test] YomkServer version: ${YomkServer_VERSION}")

# FastDDS 随主库 vendored 于 thirdparty/，测试树直接复用，不依赖系统安装
set(THIRD_PARTY_DIR "${YOMKRPC_ROOT}/thirdparty")
list(APPEND CMAKE_PREFIX_PATH "${THIRD_PARTY_DIR}/Fast-DDS-3.6.1/install")
find_package(fastdds 3 REQUIRED)
message(STATUS "[test] fastdds found")

# MC3+ 端到端收发需 YomkRpcMsg 真实类型（MString/MInt32 等 PubSubType）；
# YomkRpcMsg 无导出包，沿用 examples 已验证方式：find_package(YomkRpc) 取 lib 目录后裸库名链接。
# 置于 vendored fastdds 之后：仅取 ${YomkRpc_LIB_DIR} 供 link_directories，不链接 YomkRpc::YomkRpc
# 导入目标（被测目标从源码重编），避免其 interface 依赖干扰已解析的 vendored fastdds。
find_package(YomkRpc REQUIRED)
link_directories(${YomkRpc_LIB_DIR})
message(STATUS "[test] YomkRpc lib dir: ${YomkRpc_LIB_DIR}")

# ----------------------------- 被测目标 -----------------------------
# 仅编译自有源码两个 TU；第三方以 IMPORTED/已安装库形式链接，不参与插桩
add_library(yomkrpc_under_test STATIC
    ${YOMKRPC_ROOT}/src/YomkRpcService.cpp
    ${YOMKRPC_ROOT}/src/FastDDSNode.cpp
)
target_include_directories(yomkrpc_under_test PUBLIC
    ${YOMKRPC_ROOT}/include
    ${YOMKRPC_ROOT}/src
    ${CMAKE_CURRENT_SOURCE_DIR}   # TestCheck.h
)
# 与主库一致的版本宏（getVersion 拼接 "YomkRpc v" YOMKRPC_VERSION_STRING）
# MC9：随主库改名 YOMKRPC_VERSION→YOMKRPC_VERSION_STRING，避免与 YomkRpcAPI.h 的 YOMKRPC_VERSION() API 宏冲突
# #5(b) drift-free：从根 ../CMakeLists.txt 的 project(YomkRpc VERSION x.y.z) 提取版本，消除硬编码 0.1.0 与根版本脱节
file(STRINGS "${YOMKRPC_ROOT}/CMakeLists.txt" _yomkrpc_ver_line
     REGEX "^project\\(YomkRpc[ \t]+VERSION[ \t]+[0-9]")
string(REGEX REPLACE "^project\\(YomkRpc[ \t]+VERSION[ \t]+([0-9]+\\.[0-9]+\\.[0-9]+).*$" "\\1"
       YOMKRPC_SRC_VERSION "${_yomkrpc_ver_line}")
target_compile_definitions(yomkrpc_under_test PUBLIC YOMKRPC_VERSION_STRING="${YOMKRPC_SRC_VERSION}")
target_link_libraries(yomkrpc_under_test PUBLIC YomkServer::YomkServer fastdds)

# ----------------------------- Sanitizer 开关 -----------------------------
# off/asan/tsan；asan 合并 address+undefined（覆盖需求 #3 ASan 与 #4 UBSan），tsan 覆盖 #5。
# 标志以 PUBLIC 附加到被测目标：自有源码编译插桩，且链接期传递给测试 exe；
# 第三方预编译库不受影响（无法也无需插桩），符合"只测自有源码"原则。
set(YOMKRPC_TEST_SANITIZER off CACHE STRING "Sanitizer for tests: off/asan/tsan")
set_property(CACHE YOMKRPC_TEST_SANITIZER PROPERTY STRINGS off asan tsan)
if(NOT YOMKRPC_TEST_SANITIZER STREQUAL "off")
    if(YOMKRPC_TEST_SANITIZER STREQUAL "asan")
        set(_YOMKRPC_SAN_FLAGS -fsanitize=address,undefined -fno-omit-frame-pointer -g -O1)
    elseif(YOMKRPC_TEST_SANITIZER STREQUAL "tsan")
        set(_YOMKRPC_SAN_FLAGS -fsanitize=thread -fno-omit-frame-pointer -g -O1)
    else()
        message(FATAL_ERROR "Unknown YOMKRPC_TEST_SANITIZER=${YOMKRPC_TEST_SANITIZER}, expected off/asan/tsan")
    endif()
    target_compile_options(yomkrpc_under_test PUBLIC ${_YOMKRPC_SAN_FLAGS})
    target_link_options(yomkrpc_under_test PUBLIC ${_YOMKRPC_SAN_FLAGS})
    message(STATUS "[test] YOMKRPC_TEST_SANITIZER=${YOMKRPC_TEST_SANITIZER}, flags=${_YOMKRPC_SAN_FLAGS}")
endif()

# ----------------------------- TSan 运行启动器（ASLR 兼容）-----------------------------
# GCC 11.4 libtsan 与高 ASLR 熵内核(Ubuntu 22.04 vm.mmap_rnd_bits 偏高)不兼容：TSan 运行时在
# main 之前 FATAL("unexpected memory mapping")、进程 EXIT=66(非 race、非 YomkRpc 缺陷)。
# 非 root 标准解法：setarch <arch> -R 仅对该测试进程禁用 ASLR(每进程级、无系统副作用)。
# 以启动器前缀注入各 add_test；off/asan 模式变量为空 → COMMAND 退化为原 exe 路径(行为零变化)。
set(YOMKRPC_TEST_LAUNCHER "")
if(YOMKRPC_TEST_SANITIZER STREQUAL "tsan")
    find_program(SETARCH_EXE NAMES setarch)
    if(SETARCH_EXE)
        set(YOMKRPC_TEST_LAUNCHER ${SETARCH_EXE} ${CMAKE_SYSTEM_PROCESSOR} -R)
        message(STATUS "[test] tsan launcher(ASLR off): ${YOMKRPC_TEST_LAUNCHER}")
    else()
        message(WARNING "[test] setarch not found; tsan tests may FATAL on high-ASLR kernels")
    endif()
endif()

# ----------------------------- Coverage 开关 -----------------------------
# 仅对自有源码统计覆盖率；生成报告时用 gcov 解析并限定 ../src/*.cpp，
# 不可达的第三方失败分支出具书面豁免（见测试报告），覆盖率上限据此标注。
set(YOMKRPC_TEST_COVERAGE OFF CACHE BOOL "Enable gcov coverage instrumentation (own sources only)")
if(YOMKRPC_TEST_COVERAGE)
    target_compile_options(yomkrpc_under_test PUBLIC --coverage -g -O0)
    target_link_options(yomkrpc_under_test PUBLIC --coverage)
    message(STATUS "[test] YOMKRPC_TEST_COVERAGE=ON")
endif()

# ----------------------------- 压力/soak 旋钮（MC7）-----------------------------
# STRESS_ITERS：廉价 op（publish/loan/discard）的量级，可上调至 10 万级做 soak；
# STRESS_CYCLES：node 生命周期 churn 轮数，每轮含 discovery 周期（秒级），故有界、非 10 万级。
# 经 compile definitions 注入 stress exe；改值需重配置+重编 stress 目标（仅 2 个 TU，快）。
set(STRESS_ITERS 5000 CACHE STRING "Stress iterations for cheap ops (publish/loan); raise to 100000 for soak")
set(STRESS_CYCLES 30 CACHE STRING "Stress node create/delete churn cycles (discovery-bound, not 100k-scale)")
message(STATUS "[test] STRESS_ITERS=${STRESS_ITERS}, STRESS_CYCLES=${STRESS_CYCLES}")

# ----------------------------- 测试主题子目录 -----------------------------
enable_testing()
# MC0：基座自检（零 DDS，验证 CHECK/testReport 与被测目标链接）
add_subdirectory(Harness)
# MC1：服务层契约（DDS-free，验证 invoke 白盒入口与 8 端点输入校验/错误码）
add_subdirectory(YomkRpcService)
