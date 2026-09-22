# =============================================================================
# FastDDSDebugNode：类型无关调试订阅节点
# 守卫用例（未入域登记/重复入域/重复登记）+ 端到端用例（FastDDSNode 发布真实 YomkRpcMsg
# 类型制造被观察流量 → FastDDSDebugNode 经发现机制自动解析类型并建订阅 → JSON 结构化输出
# 捕获校验）。链接 YomkRpcMsg 仅为发布端构造真实流量，被测对象本身零 msg 依赖。
# =============================================================================
add_executable(TestFastDDSDebugNode TestFastDDSDebugNode.cpp)
target_link_libraries(TestFastDDSDebugNode PRIVATE yomkrpc_under_test YomkRpcMsg)

# 注册到 CTest；LSAN/TSAN 抑制文件经环境变量注入（各自仅在 asan/tsan 模式生效，其它模式无副作用）
add_test(NAME TestFastDDSDebugNode COMMAND ${YOMKRPC_TEST_LAUNCHER} $<TARGET_FILE:TestFastDDSDebugNode>)
set_tests_properties(TestFastDDSDebugNode PROPERTIES
    ENVIRONMENT "LSAN_OPTIONS=suppressions=${CMAKE_SOURCE_DIR}/lsan.supp:print_suppressions=0;TSAN_OPTIONS=suppressions=${CMAKE_SOURCE_DIR}/tsan.supp:print_suppressions=0"
)
