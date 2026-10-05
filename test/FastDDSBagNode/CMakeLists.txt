# =============================================================================
# FastDDSBagNode：bag 录制节点两层测试
# 启动校验用例（DDS-free 直测节点：无端点主题拒绝 + 混合清单整体报错且不落盘）+
# 录制收尾用例（真实 DDS：MString 发布者制造流量 → record → bagRecordStop 收尾 →
# bag 目录 bag_0.mcap + metadata.json 落盘与 mcap 读回断言）。链接 YomkRpcMsg 仅为
# 发布端构造真实流量，被测对象本身零 msg 依赖（透传原始 CDR 字节）。
# =============================================================================
add_executable(TestFastDDSBagNodeValidation TestFastDDSBagNodeValidation.cpp)
target_link_libraries(TestFastDDSBagNodeValidation PRIVATE yomkrpc_under_test YomkRpcMsg)

add_test(NAME TestFastDDSBagNodeValidation COMMAND ${YOMKRPC_TEST_LAUNCHER} $<TARGET_FILE:TestFastDDSBagNodeValidation>)
set_tests_properties(TestFastDDSBagNodeValidation PROPERTIES
    ENVIRONMENT "LSAN_OPTIONS=suppressions=${CMAKE_SOURCE_DIR}/lsan.supp:print_suppressions=0;TSAN_OPTIONS=suppressions=${CMAKE_SOURCE_DIR}/tsan.supp:print_suppressions=0"
)

add_executable(TestFastDDSBagNodeRecord TestFastDDSBagNodeRecord.cpp)
target_link_libraries(TestFastDDSBagNodeRecord PRIVATE yomkrpc_under_test YomkRpcMsg)

add_test(NAME TestFastDDSBagNodeRecord COMMAND ${YOMKRPC_TEST_LAUNCHER} $<TARGET_FILE:TestFastDDSBagNodeRecord>)
set_tests_properties(TestFastDDSBagNodeRecord PROPERTIES
    ENVIRONMENT "LSAN_OPTIONS=suppressions=${CMAKE_SOURCE_DIR}/lsan.supp:print_suppressions=0;TSAN_OPTIONS=suppressions=${CMAKE_SOURCE_DIR}/tsan.supp:print_suppressions=0"
)

# bag info 用例：手写 metadata.json 临时目录直测静态函数 bagInfoText（输出全行断言 +
# 失败路径契约，DDS-free 无需域号），循 Validation 先例
add_executable(TestFastDDSBagNodeInfo TestFastDDSBagNodeInfo.cpp)
target_link_libraries(TestFastDDSBagNodeInfo PRIVATE yomkrpc_under_test YomkRpcMsg)

add_test(NAME TestFastDDSBagNodeInfo COMMAND ${YOMKRPC_TEST_LAUNCHER} $<TARGET_FILE:TestFastDDSBagNodeInfo>)
set_tests_properties(TestFastDDSBagNodeInfo PROPERTIES
    ENVIRONMENT "LSAN_OPTIONS=suppressions=${CMAKE_SOURCE_DIR}/lsan.supp:print_suppressions=0;TSAN_OPTIONS=suppressions=${CMAKE_SOURCE_DIR}/tsan.supp:print_suppressions=0"
)

# bag reindex 用例：McapWriter 造真实分片临时目录直测静态函数 bagReindex（编号序收集/
# 多分片合并/全字段断言/覆盖重建/无 summary 回退扫描 + 失败路径契约，DDS-free 无需域号），
# 循 Info 先例；MCAP_IMPLEMENTATION 单译元在被测库内，测试仅声明头链接实现
add_executable(TestFastDDSBagNodeReindex TestFastDDSBagNodeReindex.cpp)
target_link_libraries(TestFastDDSBagNodeReindex PRIVATE yomkrpc_under_test YomkRpcMsg)

add_test(NAME TestFastDDSBagNodeReindex COMMAND ${YOMKRPC_TEST_LAUNCHER} $<TARGET_FILE:TestFastDDSBagNodeReindex>)
set_tests_properties(TestFastDDSBagNodeReindex PROPERTIES
    ENVIRONMENT "LSAN_OPTIONS=suppressions=${CMAKE_SOURCE_DIR}/lsan.supp:print_suppressions=0;TSAN_OPTIONS=suppressions=${CMAKE_SOURCE_DIR}/tsan.supp:print_suppressions=0"
)
