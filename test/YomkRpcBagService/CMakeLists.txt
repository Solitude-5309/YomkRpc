# =============================================================================
# YomkRpcBagService：bag 服务两层测试
# 契约测试（DDS-free，白盒 invoke 验证 4 端点输入校验/错误码）+
# 生命周期测试（真实 DDS：create → record → Ctrl+C 模拟停止 → delete 全链 + 单节点
# 重复创建拒绝 + 录制定格后重复 record 拒绝）。
# =============================================================================
add_executable(TestYomkRpcBagServiceContract TestYomkRpcBagServiceContract.cpp)
target_link_libraries(TestYomkRpcBagServiceContract PRIVATE yomkrpc_under_test)

add_test(NAME TestYomkRpcBagServiceContract COMMAND ${YOMKRPC_TEST_LAUNCHER} $<TARGET_FILE:TestYomkRpcBagServiceContract>)
set_tests_properties(TestYomkRpcBagServiceContract PROPERTIES
    ENVIRONMENT "LSAN_OPTIONS=suppressions=${CMAKE_SOURCE_DIR}/lsan.supp:print_suppressions=0;TSAN_OPTIONS=suppressions=${CMAKE_SOURCE_DIR}/tsan.supp:print_suppressions=0"
)

add_executable(TestYomkRpcBagServiceLifecycle TestYomkRpcBagServiceLifecycle.cpp)
target_link_libraries(TestYomkRpcBagServiceLifecycle PRIVATE yomkrpc_under_test YomkRpcMsg)

add_test(NAME TestYomkRpcBagServiceLifecycle COMMAND ${YOMKRPC_TEST_LAUNCHER} $<TARGET_FILE:TestYomkRpcBagServiceLifecycle>)
set_tests_properties(TestYomkRpcBagServiceLifecycle PROPERTIES
    ENVIRONMENT "LSAN_OPTIONS=suppressions=${CMAKE_SOURCE_DIR}/lsan.supp:print_suppressions=0;TSAN_OPTIONS=suppressions=${CMAKE_SOURCE_DIR}/tsan.supp:print_suppressions=0"
)
