# =============================================================================
# MC1：服务层契约测试（DDS-free）
# 目的：白盒经 svc->invoke() 验证 YomkRpcService 8 端点的输入校验与错误码契约，
#       全程不创建任何 DDS participant（解包守卫/节点不存在均在触达 DDS 前早退）。
# =============================================================================
add_executable(TestYomkRpcServiceContract TestYomkRpcServiceContract.cpp)
target_link_libraries(TestYomkRpcServiceContract PRIVATE yomkrpc_under_test)

# 注册到 CTest；LSAN/TSAN 抑制文件经环境变量注入（各自仅在 asan/tsan 模式生效，其它模式无副作用）
add_test(NAME TestYomkRpcServiceContract COMMAND ${YOMKRPC_TEST_LAUNCHER} $<TARGET_FILE:TestYomkRpcServiceContract>)
set_tests_properties(TestYomkRpcServiceContract PROPERTIES
    ENVIRONMENT "LSAN_OPTIONS=suppressions=${CMAKE_SOURCE_DIR}/lsan.supp:print_suppressions=0;TSAN_OPTIONS=suppressions=${CMAKE_SOURCE_DIR}/tsan.supp:print_suppressions=0"
)

# =============================================================================
# MC2：真实 DDS 节点生命周期 + 边界值（创建真实 participant，退出前显式销毁）
# 覆盖 MC1 豁免的 createNode 成功/重名、deleteNode 成功+析构清理，及 domainId/nodeName
# 边界与 setDomainId 重复设置守卫（Part B 直接单测，服务层不可达）。
# =============================================================================
add_executable(TestYomkRpcNodeLifecycle TestYomkRpcNodeLifecycle.cpp)
target_link_libraries(TestYomkRpcNodeLifecycle PRIVATE yomkrpc_under_test)

add_test(NAME TestYomkRpcNodeLifecycle COMMAND ${YOMKRPC_TEST_LAUNCHER} $<TARGET_FILE:TestYomkRpcNodeLifecycle>)
set_tests_properties(TestYomkRpcNodeLifecycle PROPERTIES
    ENVIRONMENT "LSAN_OPTIONS=suppressions=${CMAKE_SOURCE_DIR}/lsan.supp:print_suppressions=0;TSAN_OPTIONS=suppressions=${CMAKE_SOURCE_DIR}/tsan.supp:print_suppressions=0"
)

# =============================================================================
# MC3：主题注册 + 端到端发布订阅（真实 DDS + YomkRpcMsg 真实类型指针）
# 首次功能触达 registerPubTopic/registerSubTopic/getOrCreateTopic/publish/on_data_available
# 与 F1 SubResGuard 所在的 registerSubTopic happy-path；首次引入 YomkRpcMsg 依赖。
# =============================================================================
add_executable(TestYomkRpcTopic TestYomkRpcTopic.cpp)
target_link_libraries(TestYomkRpcTopic PRIVATE yomkrpc_under_test YomkRpcMsg)

add_test(NAME TestYomkRpcTopic COMMAND ${YOMKRPC_TEST_LAUNCHER} $<TARGET_FILE:TestYomkRpcTopic>)
set_tests_properties(TestYomkRpcTopic PROPERTIES
    ENVIRONMENT "LSAN_OPTIONS=suppressions=${CMAKE_SOURCE_DIR}/lsan.supp:print_suppressions=0;TSAN_OPTIONS=suppressions=${CMAKE_SOURCE_DIR}/tsan.supp:print_suppressions=0"
)

# =============================================================================
# MC4：loan 借出/归还（plain 类型免序列化路径）
# 白盒验证 loan/discardLoan 的 plain(MInt32) 借出成功、非 plain(MString) 回退、
# loan→池内填值→publish 免序列化端到端、discard 归还与守卫；loan sample 为 writer 池借出。
# =============================================================================
add_executable(TestYomkRpcLoan TestYomkRpcLoan.cpp)
target_link_libraries(TestYomkRpcLoan PRIVATE yomkrpc_under_test YomkRpcMsg)

add_test(NAME TestYomkRpcLoan COMMAND ${YOMKRPC_TEST_LAUNCHER} $<TARGET_FILE:TestYomkRpcLoan>)
set_tests_properties(TestYomkRpcLoan PROPERTIES
    ENVIRONMENT "LSAN_OPTIONS=suppressions=${CMAKE_SOURCE_DIR}/lsan.supp:print_suppressions=0;TSAN_OPTIONS=suppressions=${CMAKE_SOURCE_DIR}/tsan.supp:print_suppressions=0"
)

# =============================================================================
# MC5：主题/类型分支深化 + 多类型边界 payload 端到端
# 补齐 registerPubTopic 侧类型冲突(FastDDSNode.cpp L215)、getOrCreateTopic 复用从 pub 侧
# 进入(L183)，并系统验证 6 类型(MFloat32/64/MInt64/MBool/MString/MByteArray)边界特殊值
# (NaN/±Inf/整数极值/空超长串/空大 sequence)经 publish 端到端的透传健壮性与数据完整回环。
# =============================================================================
add_executable(TestYomkRpcTypes TestYomkRpcTypes.cpp)
target_link_libraries(TestYomkRpcTypes PRIVATE yomkrpc_under_test YomkRpcMsg)

add_test(NAME TestYomkRpcTypes COMMAND ${YOMKRPC_TEST_LAUNCHER} $<TARGET_FILE:TestYomkRpcTypes>)
set_tests_properties(TestYomkRpcTypes PROPERTIES
    ENVIRONMENT "LSAN_OPTIONS=suppressions=${CMAKE_SOURCE_DIR}/lsan.supp:print_suppressions=0;TSAN_OPTIONS=suppressions=${CMAKE_SOURCE_DIR}/tsan.supp:print_suppressions=0"
)

# =============================================================================
# MC6：并发安全 + TSan 基线（多线程 invoke 锁正确性 / 并发收发 / teardown 竞态）
# 从多线程并发驱动 svc->invoke()，实证双层锁(service mtx_ + node mtx_)正确性、方向2 reader
# 回调路径并发下数据完整性、delete_node 与活跃回调 teardown 竞态安全；off/asan/tsan 三模式验证。
# =============================================================================
add_executable(TestYomkRpcConcurrency TestYomkRpcConcurrency.cpp)
target_link_libraries(TestYomkRpcConcurrency PRIVATE yomkrpc_under_test YomkRpcMsg)

add_test(NAME TestYomkRpcConcurrency COMMAND ${YOMKRPC_TEST_LAUNCHER} $<TARGET_FILE:TestYomkRpcConcurrency>)
set_tests_properties(TestYomkRpcConcurrency PROPERTIES
    ENVIRONMENT "LSAN_OPTIONS=suppressions=${CMAKE_SOURCE_DIR}/lsan.supp:print_suppressions=0;TSAN_OPTIONS=suppressions=${CMAKE_SOURCE_DIR}/tsan.supp:print_suppressions=0"
)

# =============================================================================
# MC7-S：单线程压力/soak（node 生命周期 churn / 持续 publish+loan / 实体增长完整回收）
# 资源采样器（Linux /proc VmRSS/Threads/fd）量化负载前后基线，实证无泄漏/池不耗尽/吞吐稳定/句柄稳定。
# LABELS stress：快反馈套件经 `ctest -LE stress` 排除；压力套件 `ctest -L stress` 选中。
# 旋钮 STRESS_ITERS/STRESS_CYCLES（父 CACHE 选项）经 compile definitions 注入；TIMEOUT 放宽兜底 asan 10 万级。
# =============================================================================
add_executable(TestYomkRpcStressSerial TestYomkRpcStressSerial.cpp)
target_link_libraries(TestYomkRpcStressSerial PRIVATE yomkrpc_under_test YomkRpcMsg)
target_compile_definitions(TestYomkRpcStressSerial PRIVATE
    STRESS_ITERS=${STRESS_ITERS} STRESS_CYCLES=${STRESS_CYCLES})

add_test(NAME TestYomkRpcStressSerial COMMAND ${YOMKRPC_TEST_LAUNCHER} $<TARGET_FILE:TestYomkRpcStressSerial>)
set_tests_properties(TestYomkRpcStressSerial PROPERTIES
    LABELS stress
    TIMEOUT 1800
    ENVIRONMENT "LSAN_OPTIONS=suppressions=${CMAKE_SOURCE_DIR}/lsan.supp:print_suppressions=0;TSAN_OPTIONS=suppressions=${CMAKE_SOURCE_DIR}/tsan.supp:print_suppressions=0"
)

# =============================================================================
# MC7-C：多线程高量压力/soak（并发 publish/loan / 并发 node churn / 并发 pub-sub 活跃回调）
# N=4 线程（nproc=2 适度超订）并发驱动 + 资源采样器，实证并发负载下池不耗尽/teardown 完整回收/
# 方向2 reader 高量无撕裂/句柄稳定；与 serial 独立进程隔离。LABELS stress（同上）。
# =============================================================================
add_executable(TestYomkRpcStressConcurrent TestYomkRpcStressConcurrent.cpp)
target_link_libraries(TestYomkRpcStressConcurrent PRIVATE yomkrpc_under_test YomkRpcMsg)
target_compile_definitions(TestYomkRpcStressConcurrent PRIVATE
    STRESS_ITERS=${STRESS_ITERS} STRESS_CYCLES=${STRESS_CYCLES})

add_test(NAME TestYomkRpcStressConcurrent COMMAND ${YOMKRPC_TEST_LAUNCHER} $<TARGET_FILE:TestYomkRpcStressConcurrent>)
set_tests_properties(TestYomkRpcStressConcurrent PROPERTIES
    LABELS stress
    TIMEOUT 1800
    ENVIRONMENT "LSAN_OPTIONS=suppressions=${CMAKE_SOURCE_DIR}/lsan.supp:print_suppressions=0;TSAN_OPTIONS=suppressions=${CMAKE_SOURCE_DIR}/tsan.supp:print_suppressions=0"
)
