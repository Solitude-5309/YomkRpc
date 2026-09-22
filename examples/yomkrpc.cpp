/**
 * @file yomkrpc.cpp
 * @brief yomkrpc 命令行工具：观察任意 DDS 主题并在控制台直出消息内容
 *
 * 用法：
 *   yomkrpc topic print [-d N | --domain N] <topic-name>
 *   yomkrpc -h | --help
 *
 * 示例（与 ExampleYomkRpcPub 配合，默认域 0 即开即用）：
 *   yomkrpc topic print hello_world
 *   yomkrpc topic print -d 5 sensor_data
 *
 * 实现：经 YomkRpcDebugService 调试服务（YOMKRPC_DEBUG_* 宏）创建调试节点并登记主题；
 * 远端 DataWriter 经 DDS 发现后自动解析类型建立订阅，消息文本逐条经回调直出 stdout
 * （输出权在调用方，工具侧不落日志）。Ctrl+C 退出前 YOMKRPC_DEBUG_QUIT() 显式清理，
 * 规避 FastDDS 静态析构期段错误（同 ExampleYomkRpcSub 退出前 DEL_NODE 模式）。
 */

#include <YomkRpc/YomkRpcAPI.h>
#include <YomkServer/YomkAPI.h>

#include <atomic>
#include <chrono>
#include <csignal>
#include <cstdint>
#include <cstdlib> // strtoul
#include <iostream>
#include <string>
#include <thread>
#include <vector>

using namespace yomk;

static std::atomic<bool> g_stop{false}; // SIGINT（Ctrl+C）退出标志

static void onSignal(int)
{
    g_stop.store(true);
}

static void printUsage(std::ostream &os)
{
    os << "Usage:\n"
          "  yomkrpc topic print [-d N | --domain N] <topic-name>\n"
          "  yomkrpc -h | --help\n"
          "\n"
          "Options:\n"
          "  -d N, --domain N    DDS 域号（0-232，默认 0，须与发布端同域）\n"
          "  -h, --help          显示帮助\n"
          "\n"
          "Examples:\n"
          "  yomkrpc topic print hello_world\n"
          "  yomkrpc topic print -d 5 sensor_data\n";
}

// 解析域号：十进制、无尾随非数字字符、合法范围 [0,232]
static bool parseDomain(const char *text, uint32_t &domainId)
{
    char *end = nullptr;
    unsigned long value = std::strtoul(text, &end, 10);
    if (end == text || *end != '\0' || value > 232)
    {
        return false;
    }
    domainId = static_cast<uint32_t>(value);
    return true;
}

int main(int argc, char *argv[])
{
    // ---- 参数解析：-h/--help 即刻退出；-d/--domain 可选；位置参数 topic print <topic-name> ----
    uint32_t domainId = 0;
    std::vector<std::string> pos;
    for (int i = 1; i < argc; ++i)
    {
        std::string arg = argv[i];
        if (arg == "-h" || arg == "--help")
        {
            printUsage(std::cout);
            return 0;
        }
        if (arg == "-d" || arg == "--domain")
        {
            if (i + 1 >= argc)
            {
                std::cerr << "yomkrpc: " << arg << " 缺少域号参数\n";
                printUsage(std::cerr);
                return 2;
            }
            if (!parseDomain(argv[++i], domainId))
            {
                std::cerr << "yomkrpc: 非法域号 \"" << argv[i] << "\"（合法范围 [0,232]）\n";
                return 2;
            }
            continue;
        }
        pos.push_back(arg);
    }
    if (pos.size() != 3 || pos[0] != "topic" || pos[1] != "print" || pos[2].empty())
    {
        std::cerr << "yomkrpc: 用法错误\n";
        printUsage(std::cerr);
        return 2;
    }
    const std::string topicName = pos[2];

    YOMK_INIT();
    YOMK_NEW_SERVICE(YomkRpcDebugService);

    // 1. 创建调试节点（单节点模型：重复创建须先删除）
    auto resp = YOMKRPC_DEBUG_NODE(domainId);
    if (resp.m_status != YomkResponse::eOk)
    {
        YOMK_ERROR_TAG("yomkrpc", "create debug node failed: ", resp.m_msg);
        return 1;
    }

    // 2. 登记调试主题：消息文本逐条经回调直出 stdout（输出权在调用方，工具侧不落日志）
    auto onMessage = [](const std::string &text)
    {
        std::cout << text << std::endl; // 逐条直出并刷新
    };
    resp = YOMKRPC_DEBUG_PRINT(topicName, onMessage);
    if (resp.m_status != YomkResponse::eOk)
    {
        YOMK_ERROR_TAG("yomkrpc", "register debug topic failed: ", resp.m_msg);
        YOMKRPC_DEBUG_QUIT(); // 登记失败路径同样须清理已建调试节点
        return 1;
    }

    // 3. 监听 Ctrl+C，主循环等待退出信号
    std::signal(SIGINT, onSignal);
    YOMK_INFO_TAG("yomkrpc", "debug printing topic \"", topicName, "\" on domain ",
                  std::to_string(domainId), ", press Ctrl+C to exit");
    while (!g_stop.load())
    {
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }

    // 4. 退出前显式删除调试节点，确保 DDS 实体在 FastDDS 静态资源销毁前清理
    resp = YOMKRPC_DEBUG_QUIT();
    if (resp.m_status != YomkResponse::eOk)
    {
        YOMK_ERROR_TAG("yomkrpc", "debug quit failed: ", resp.m_msg);
        return 1;
    }
    return 0;
}
