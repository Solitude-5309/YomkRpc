cmake_minimum_required(VERSION 3.14)
project(ExampleYomkRpc LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

find_package(YomkServer REQUIRED)
find_package(YomkRpc REQUIRED)

message(STATUS "YomkServer version: ${YomkServer_VERSION}")
message(STATUS "YomkServer include dirs: ${YomkServer_INCLUDE_DIRS}")
message(STATUS "YomkServer lib dir: ${YomkServer_LIB_DIR}")
message(STATUS "YomkServer libraries: ${YomkServer_LIBRARIES}")
message(STATUS "YomkRpc version: ${YomkRpc_VERSION}")
message(STATUS "YomkRpc include dirs: ${YomkRpc_INCLUDE_DIRS}")
message(STATUS "YomkRpc lib dir: ${YomkRpc_LIB_DIR}")
message(STATUS "YomkRpc libraries: ${YomkRpc_LIBRARIES}")

# 导出 target 不含库搜索路径是设计如此，此处由扩展 Config 提供的 lib 目录补齐：
# YomkRpcMsg 无导出包、以裸库名链接，仅能靠 -L 解析（fastdds 已由 Config 的 find_dependency 解析为全路径）
link_directories(${YomkRpc_LIB_DIR})

# 单进程示例：完整发布订阅流程（ExampleYomkRpcTopic）与 loan 借出机制（ExampleYomkRpcTopicLoan）
add_executable(ExampleYomkRpcTopic ExampleYomkRpcTopic.cpp)
target_link_libraries(ExampleYomkRpcTopic PRIVATE YomkRpc::YomkRpc YomkServer::YomkServer YomkRpcMsg)

add_executable(ExampleYomkRpcTopicLoan ExampleYomkRpcTopicLoan.cpp)
target_link_libraries(ExampleYomkRpcTopicLoan PRIVATE YomkRpc::YomkRpc YomkServer::YomkServer YomkRpcMsg)

# 跨进程发布/订阅双示例：ExampleYomkRpcPub（发布端）与 ExampleYomkRpcSub（订阅端）各编译为独立可执行程序
add_executable(ExampleYomkRpcPub ExampleYomkRpcPub.cpp)
target_link_libraries(ExampleYomkRpcPub PRIVATE YomkRpc::YomkRpc YomkServer::YomkServer YomkRpcMsg)

add_executable(ExampleYomkRpcSub ExampleYomkRpcSub.cpp)
target_link_libraries(ExampleYomkRpcSub PRIVATE YomkRpc::YomkRpc YomkServer::YomkServer YomkRpcMsg)

# 示例程序随扩展一并安装到 <安装路径>/bin（运行方式见 README）
# 示例程序可能有多个，此处显式列出全部示例目标名（不能用 ${PROJECT_NAME} 占位）
install(TARGETS
    ExampleYomkRpcTopic
    ExampleYomkRpcTopicLoan
    ExampleYomkRpcPub
    ExampleYomkRpcSub
    RUNTIME DESTINATION bin
)
