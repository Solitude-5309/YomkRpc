# =============================================================================
# MC1：服务层契约测试（DDS-free）
# 目的：白盒经 svc->invoke() 验证 YomkRpcService 8 端点的输入校验与错误码契约，
#       全程不创建任何 DDS participant（解包守卫/节点不存在均在触达 DDS 前早退）。
# =============================================================================
add_executable(TestYomkRpcServiceContract TestYomkRpcServiceContract.cpp)
target_link_libraries(TestYomkRpcServiceContract PRIVATE yomkrpc_under_test)

# 注册到 CTest；LSAN 抑制文件经环境变量注入（仅 asan 模式生效，off 模式无副作用）
add_test(NAME TestYomkRpcServiceContract COMMAND TestYomkRpcServiceContract)
set_tests_properties(TestYomkRpcServiceContract PROPERTIES
    ENVIRONMENT "LSAN_OPTIONS=suppressions=${CMAKE_SOURCE_DIR}/lsan.supp:print_suppressions=0"
)
