cmake_minimum_required(VERSION 3.14)
project(YomkRpc VERSION 0.0.20 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

find_package(YomkServer REQUIRED)
message(STATUS "YomkServer version: ${YomkServer_VERSION}")  
message(STATUS "YomkServer include dirs: ${YomkServer_INCLUDE_DIRS}")  
message(STATUS "YomkServer lib dir: ${YomkServer_LIB_DIR}")  
message(STATUS "YomkServer libraries: ${YomkServer_LIBRARIES}") 
include_directories(${YomkServer_INCLUDE_DIRS})
link_directories(${YomkServer_LIB_DIR})

# Set third-party directory
set(THIRD_PARTY_DIR "${CMAKE_CURRENT_LIST_DIR}/thirdparty")
set(CMAKE_PREFIX_PATH
  "${THIRD_PARTY_DIR}/Fast-DDS-3.6.1/install"
)
find_package(fastdds 3 REQUIRED)

include_directories(${CMAKE_CURRENT_SOURCE_DIR}/include)

# 编译为动态库
add_library(${PROJECT_NAME} SHARED
    src/YomkRpcService.cpp
    src/FastDDSNode.cpp
)
target_link_libraries(${PROJECT_NAME} PRIVATE YomkServer::YomkServer)
target_link_libraries(${PROJECT_NAME} PUBLIC fastdds)
target_compile_definitions(${PROJECT_NAME} PRIVATE YOMKRPC_VERSION="${PROJECT_VERSION}")

# 安装规则
set(INCLUDE_INSTALL_DIR "include")
set(LIB_INSTALL_DIR "lib")

install(TARGETS ${PROJECT_NAME}
    EXPORT ${PROJECT_NAME}Targets
    LIBRARY DESTINATION ${LIB_INSTALL_DIR}
)
install(DIRECTORY include/ DESTINATION include/${PROJECT_NAME})

# CMake export 导出配置
include(CMakePackageConfigHelpers)

install(EXPORT ${PROJECT_NAME}Targets
    FILE ${PROJECT_NAME}Targets.cmake
    NAMESPACE ${PROJECT_NAME}::
    DESTINATION lib/cmake/${PROJECT_NAME}
)

configure_package_config_file(
    "${CMAKE_CURRENT_SOURCE_DIR}/cmake/ProjectConfig.cmake.in"
    "${CMAKE_CURRENT_BINARY_DIR}/${PROJECT_NAME}Config.cmake"
    INSTALL_DESTINATION lib/cmake/${PROJECT_NAME}
    PATH_VARS INCLUDE_INSTALL_DIR LIB_INSTALL_DIR
)

write_basic_package_version_file(
    "${CMAKE_CURRENT_BINARY_DIR}/${PROJECT_NAME}ConfigVersion.cmake"
    VERSION ${PROJECT_VERSION}
    COMPATIBILITY SameMajorVersion
)

install(FILES
    "${CMAKE_CURRENT_BINARY_DIR}/${PROJECT_NAME}Config.cmake"
    "${CMAKE_CURRENT_BINARY_DIR}/${PROJECT_NAME}ConfigVersion.cmake"
    DESTINATION lib/cmake/${PROJECT_NAME}
)

install(FILES
    "${CMAKE_CURRENT_SOURCE_DIR}/msg/YomkRpcMsg.hpp"
    "${CMAKE_CURRENT_SOURCE_DIR}/msg/YomkRpcMsgPubSubTypes.hpp"
    DESTINATION include/YomkRpcMsg
)

install(DIRECTORY thirdparty/Fast-DDS-3.6.1/install/
    DESTINATION .
)

install(DIRECTORY thirdparty/Fast-DDS-Gen-4.3.0/
    DESTINATION .
)
