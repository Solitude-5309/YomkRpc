# =============================================================================
# YomkRpcDebugService：调试服务两层测试
# 契约测试（DDS-free，白盒 invoke 验证 6 端点输入校验/错误码/内省）+
# 生命周期测试（真实 DDS：create/重复/删除/重建 + topicPrint 登记契约 + topicInfo 未发现/命中
# + domainId 边界）。链接 YomkRpcMsg：Lifecycle 为 topic_info 命中路径引入最小发布端类型
# （MString），端到端订阅流量仍归 TestFastDDSDebugNode。
# =============================================================================
add_executable(TestYomkRpcDebugServiceContract TestYomkRpcDebugServiceContract.cpp)
target_link_libraries(TestYomkRpcDebugServiceContract PRIVATE yomkrpc_under_test)

add_test(NAME TestYomkRpcDebugServiceContract COMMAND ${YOMKRPC_TEST_LAUNCHER} $<TARGET_FILE:TestYomkRpcDebugServiceContract>)
set_tests_properties(TestYomkRpcDebugServiceContract PROPERTIES
    ENVIRONMENT "LSAN_OPTIONS=suppressions=${CMAKE_SOURCE_DIR}/lsan.supp:print_suppressions=0;TSAN_OPTIONS=suppressions=${CMAKE_SOURCE_DIR}/tsan.supp:print_suppressions=0"
)

add_executable(TestYomkRpcDebugServiceLifecycle TestYomkRpcDebugServiceLifecycle.cpp)
target_link_libraries(TestYomkRpcDebugServiceLifecycle PRIVATE yomkrpc_under_test YomkRpcMsg)

add_test(NAME TestYomkRpcDebugServiceLifecycle COMMAND ${YOMKRPC_TEST_LAUNCHER} $<TARGET_FILE:TestYomkRpcDebugServiceLifecycle>)
set_tests_properties(TestYomkRpcDebugServiceLifecycle PROPERTIES
    ENVIRONMENT "LSAN_OPTIONS=suppressions=${CMAKE_SOURCE_DIR}/lsan.supp:print_suppressions=0;TSAN_OPTIONS=suppressions=${CMAKE_SOURCE_DIR}/tsan.supp:print_suppressions=0"
)
