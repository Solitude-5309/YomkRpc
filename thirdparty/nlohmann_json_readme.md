# nlohmann json 使用说明

## linux下 nlohmann json v3.12.0 C++使用方法

nlohmann json 是 Niels Lohmann 维护的 C++ JSON 库（github.com/nlohmann/json，MIT
license）。本仓以 header-only 的 **single_include 单头形态** vendoring 于
`thirdparty/nlohmann_json/include/nlohmann/`（`json.hpp` 合并版 + `json_fwd.hpp` +
`json_literals.hpp` 共 3 个头，无编译产物）。与 mcap 不同，json 是纯头库，**无需任何
`IMPLEMENTATION` 宏**，直接 `#include <nlohmann/json.hpp>` 即用。示例仅演示基本用法，
json 自身质量由上游提供者测试。

### 编译示例

1. cd thirdparty/nlohmann_json/test
2. cmake -S . -B build
3. cmake --build build

### 运行示例

1. ./build/json_example
2. 预期输出：从字符串解析出一个 JSON 对象后，打印字段访问与类型判断结果、遍历
   数组与对象键值对，再做修改/新增（改 version、数组追加、嵌套对象加字段），最后
   分别打印紧凑序列化 `dump()` 与美化缩进 `dump(2)` 结果

### 使用要点

1. header-only 集成：只需把 include 路径加入编译，`#include <nlohmann/json.hpp>`
   即可使用；无实现宏、无链接库、无第三方硬依赖。C++11 起可用（本仓示例用 C++17
   以启用结构化绑定遍历 `items()`）。
2. 解析：`json::parse(str)` 从字符串/流解析，失败抛 `json::parse_error`；也可用
   `using namespace nlohmann::literals;` 后的 `R"({...})"_json` 字面量直接构造。
3. 访问与类型判断：`j["key"].get<T>()` 取值，`j.contains("key")` 判存在，
   `is_object()` / `is_string()` / `is_number()` 等判类型；对不存在的键取值或类型
   不符会抛 `json::type_error` / `json::out_of_range`。
4. 遍历：数组用下标或 range-for；对象用 `for (auto& [k, v] : j.items())` 遍历键值对。
5. 序列化：`dump()` 输出紧凑串，`dump(2)` 输出 2 空格缩进的美化串（负值表示 tab）。
6. 前向声明：仅需在头文件里声明 `nlohmann::json` 而不引入完整实现时，用更轻量的
   `#include <nlohmann/json_fwd.hpp>`，可显著降低编译开销。
7. 主库集成方式：当前主库与 test 测试树均无 json 消费者，故未挂载到根/test 的
   CMakeLists。将来有 TU 需要使用时，参照 mcap 在根 `CMakeLists.txt` 加
   `target_include_directories(${PROJECT_NAME} PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/thirdparty/nlohmann_json/include)`
   即可（无需编译宏）。
