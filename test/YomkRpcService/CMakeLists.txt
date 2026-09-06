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

# =============================================================================
# MC2：真实 DDS 节点生命周期 + 边界值（创建真实 participant，退出前显式销毁）
# 覆盖 MC1 豁免的 createNode 成功/重名、deleteNode 成功+析构清理，及 domainId/nodeName
# 边界与 setDomainId 重复设置守卫（Part B 直接单测，服务层不可达）。
# =============================================================================
add_executable(TestYomkRpcNodeLifecycle TestYomkRpcNodeLifecycle.cpp)
target_link_libraries(TestYomkRpcNodeLifecycle PRIVATE yomkrpc_under_test)

add_test(NAME TestYomkRpcNodeLifecycle COMMAND TestYomkRpcNodeLifecycle)
set_tests_properties(TestYomkRpcNodeLifecycle PROPERTIES
    ENVIRONMENT "LSAN_OPTIONS=suppressions=${CMAKE_SOURCE_DIR}/lsan.supp:print_suppressions=0"
)

# =============================================================================
# MC3：主题注册 + 端到端发布订阅（真实 DDS + YomkRpcMsg 真实类型指针）
# 首次功能触达 registerPubTopic/registerSubTopic/getOrCreateTopic/publish/on_data_available
# 与 F1 SubResGuard 所在的 registerSubTopic happy-path；首次引入 YomkRpcMsg 依赖。
# =============================================================================
add_executable(TestYomkRpcTopic TestYomkRpcTopic.cpp)
target_link_libraries(TestYomkRpcTopic PRIVATE yomkrpc_under_test YomkRpcMsg)

add_test(NAME TestYomkRpcTopic COMMAND TestYomkRpcTopic)
set_tests_properties(TestYomkRpcTopic PROPERTIES
    ENVIRONMENT "LSAN_OPTIONS=suppressions=${CMAKE_SOURCE_DIR}/lsan.supp:print_suppressions=0"
)
