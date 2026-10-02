# =============================================================================
# nlohmann json 用户使用示例（独立最小工程，与主库/test 测试树无关）
#
# 构建：
#   cmake -S . -B build
#   cmake --build build
# 运行：
#   ./build/json_example
#
# 说明：nlohmann json 为纯 header-only 库（thirdparty/nlohmann_json/include/nlohmann/），
# 无需任何 IMPLEMENTATION 宏，直接 include <nlohmann/json.hpp> 即用；无第三方硬依赖。
# =============================================================================
cmake_minimum_required(VERSION 3.14)
project(NlohmannJsonExample LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

add_executable(json_example json_example.cpp)
target_include_directories(json_example PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../include)
