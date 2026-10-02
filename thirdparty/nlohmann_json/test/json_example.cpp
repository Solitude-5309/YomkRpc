// =============================================================================
// nlohmann json 用户基本使用示例（非测试：nlohmann json 为第三方库，测试责任在提供者）
//
// 演示常用流：
//   解析：json::parse(字符串) / "..."_json 字面量
//   访问：下标取值、类型判断（is_string/contains）、遍历对象与数组
//   修改：改字段、加嵌套对象与数组元素
//   序列化：dump() 紧凑、dump(2) 美化缩进
//
// 编译运行见同目录 CMakeLists.txt 与 thirdparty/nlohmann_json_readme.md。
// =============================================================================
#include <nlohmann/json.hpp>

#include <iostream>
#include <string>

// 引入 json 字面量后缀 _json（"..."_json 直接构造 json 对象）
using nlohmann::json;
using namespace nlohmann::literals;

int main() {
    // ---------------- 解析 ----------------
    // 从字符串字面量解析；解析失败会抛 nlohmann::json::parse_error
    const std::string text = R"({
        "name": "yomkrpc",
        "version": 3,
        "features": ["record", "play"],
        "meta": { "license": "MIT", "header_only": true }
    })";
    json j = json::parse(text);

    // _json 字面量：等价于 json::parse，但写在源码里更直观
    json k = R"({"ok": true, "count": 42})"_json;

    // ---------------- 访问与类型判断 ----------------
    std::cout << "name          = " << j["name"].get<std::string>() << std::endl;
    std::cout << "version       = " << j["version"].get<int>() << std::endl;
    std::cout << "is object     = " << std::boolalpha << j.is_object() << std::endl;
    std::cout << "name is str   = " << j["name"].is_string() << std::endl;
    std::cout << "contains meta = " << j.contains("meta") << std::endl;
    std::cout << "k.count       = " << k["count"].get<int>() << std::endl;

    // 遍历数组
    std::cout << "features      = [";
    for (size_t i = 0; i < j["features"].size(); ++i) {
        std::cout << (i ? ", " : "") << j["features"][i].get<std::string>();
    }
    std::cout << "]" << std::endl;

    // 遍历对象的键值对
    std::cout << "meta keys     :" << std::endl;
    for (const auto& [key, value] : j["meta"].items()) {
        std::cout << "  " << key << " -> " << value.dump() << std::endl;
    }

    // ---------------- 修改与新增 ----------------
    j["version"] = 4;                                  // 改字段
    j["features"].push_back("info");                   // 数组追加
    j["meta"]["stars"] = 100;                          // 嵌套对象新增字段
    j["extra"] = {1, 2, 3};                            // 新增数组字段

    // ---------------- 序列化 ----------------
    std::cout << "compact dump  = " << j.dump() << std::endl;
    std::cout << "pretty dump(2):" << std::endl << j.dump(2) << std::endl;

    return 0;
}
