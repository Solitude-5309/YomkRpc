# =============================================================================
# mcap 用户使用示例（独立最小工程，与主库/test 测试树无关）
#
# 构建：
#   cmake -S . -B build
#   cmake --build build
# 运行：
#   ./build/mcap_example
#
# 说明：mcap 为 header-only 库（thirdparty/mcap/include/mcap/），实现随
# mcap_example.cpp 编译（单译元宏 MCAP_IMPLEMENTATION）；lz4/zstd 压缩依赖
# 以宏禁用，零第三方硬依赖；启用压缩时删除对应宏即可。
# =============================================================================
cmake_minimum_required(VERSION 3.14)
project(McapExample LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

add_executable(mcap_example mcap_example.cpp)
target_include_directories(mcap_example PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../include)
target_compile_definitions(mcap_example PRIVATE
    MCAP_COMPRESSION_NO_LZ4
    MCAP_COMPRESSION_NO_ZSTD
)
