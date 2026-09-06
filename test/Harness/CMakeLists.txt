# =============================================================================
# MC0：基座自检（零 DDS）
# 目的：证明测试基座端到端可用——yomkrpc_under_test 已从源码编译且可链接、
# TestCheck.h 的 CHECK/testReport 生效、lsan.supp 经 LSAN_OPTIONS 注入。
# 不含任何业务断言（业务契约归 MC1+）。
# =============================================================================
add_executable(TestHarnessSmoke TestHarnessSmoke.cpp)
target_link_libraries(TestHarnessSmoke PRIVATE yomkrpc_under_test)

# 注册到 CTest；LSAN/TSAN 抑制文件经环境变量注入（各自仅在 asan/tsan 模式生效，其它模式无副作用）
# COMMAND 前缀 YOMKRPC_TEST_LAUNCHER：仅 tsan 模式为 setarch -R（ASLR 兼容），off/asan 为空退化原路径。
add_test(NAME TestHarnessSmoke COMMAND ${YOMKRPC_TEST_LAUNCHER} $<TARGET_FILE:TestHarnessSmoke>)
set_tests_properties(TestHarnessSmoke PROPERTIES
    ENVIRONMENT "LSAN_OPTIONS=suppressions=${CMAKE_SOURCE_DIR}/lsan.supp:print_suppressions=0;TSAN_OPTIONS=suppressions=${CMAKE_SOURCE_DIR}/tsan.supp:print_suppressions=0"
)
