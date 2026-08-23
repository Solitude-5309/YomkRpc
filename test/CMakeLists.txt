cmake_minimum_required(VERSION 3.14)
project(TestYomkRpc LANGUAGES CXX)

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
include_directories(${YomkServer_INCLUDE_DIRS})
link_directories(${YomkServer_LIB_DIR})

add_executable(TestRpcTopic TestRpcTopic.cpp)
target_link_libraries(TestRpcTopic PRIVATE YomkRpc::YomkRpc YomkServer::YomkServer YomkRpcMsg)

add_executable(TestRpcTopicLoan TestRpcTopicLoan.cpp)
target_link_libraries(TestRpcTopicLoan PRIVATE YomkRpc::YomkRpc YomkServer::YomkServer YomkRpcMsg)

# 发布/订阅示例程序（用户参考，非断言测试）：另开终端分别运行
add_executable(RpcPubHelloWorld RpcPubHelloWorld.cpp)
target_link_libraries(RpcPubHelloWorld PRIVATE YomkRpc::YomkRpc YomkServer::YomkServer YomkRpcMsg)

add_executable(RpcSubHelloWorld RpcSubHelloWorld.cpp)
target_link_libraries(RpcSubHelloWorld PRIVATE YomkRpc::YomkRpc YomkServer::YomkServer YomkRpcMsg)
