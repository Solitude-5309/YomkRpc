/**
 * @file yomkrpc.cpp
 * @brief yomkrpc 命令行工具：观察任意 DDS 主题（持续打印消息）或列出域内主题与数据类型
 *
 * 用法：
 *   yomkrpc topic print [-d N | --domain N] <topic-name>
 *   yomkrpc topic list [-d N | --domain N]
 *   yomkrpc -h | --help
 *
 * 示例（与 ExampleYomkRpcPub 配合，默认域 0 即开即用）：
 *   yomkrpc topic print hello_world
 *   yomkrpc topic print -d 5 sensor_data
 *   yomkrpc topic list
 *
 * 实现经 YomkRpcDebugService 调试服务（YOMKRPC_DEBUG_* 宏）驱动内部调试节点——
 * topic print：登记主题后远端 DataWriter 经 DDS 发现自动解析类型建立订阅，消息文本
 * 逐条经回调直出 stdout（输出权在调用方，工具侧不落日志），Ctrl+C 退出；
 * topic list：创建节点后短暂等待 EDP 发现重放（异步，约 2s），一次性查询并逐行打印
 * "topicName [typeName]"。两种子命令退出前均 YOMKRPC_DEBUG_QUIT() 显式清理，
 * 规避 FastDDS 静态析构期段错误（同 ExampleYomkRpcSub 退出前 DEL_NODE 模式）。
 *
 * 域 id 两种指定方式（优先级 -d > 环境变量 > 0）：
 *   1. 环境变量 YOMKRPC_DDS_DOMAIN_ID：默认从此读取；启动时无则创建并设默认值 0，
 *      同时幂等写入 ~/.bashrc（仅当其中无该变量时），使新开任意终端可查看并继承；
 *   2. -d N：临时指定，直接使用该值（不读、也绝不写环境变量与 .bashrc）。
 */

#include <YomkRpc/YomkRpcAPI.h>
#include <YomkServer/YomkAPI.h>

#include <atomic>
#include <chrono>
#include <csignal>
#include <cstdint>
#include <cstdlib> // strtoul, getenv, setenv
#include <fstream>
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
          "  yomkrpc topic list [-d N | --domain N]\n"
          "  yomkrpc -h | --help\n"
          "\n"
          "Options:\n"
          "  -d N, --domain N    DDS 域号（0-232），临时指定，不写环境变量；未指定时读\n"
          "                      环境变量 YOMKRPC_DDS_DOMAIN_ID（无则默认 0）\n"
          "  -h, --help          显示帮助\n"
          "\n"
          "Examples:\n"
          "  yomkrpc topic print hello_world\n"
          "  yomkrpc topic print -d 5 sensor_data\n"
          "  yomkrpc topic list\n"
          "  export YOMKRPC_DDS_DOMAIN_ID=5    # 环境变量方式（写入 .bashrc 可持久化）\n"
          "  yomkrpc topic list                # 此后免 -d，等价于 -d 5\n";
}

// 域 id 环境变量：默认路径（启动时无则创建并设默认值 0）；-d 显式指定时不读写它
static constexpr char kDomainEnv[] = "YOMKRPC_DDS_DOMAIN_ID";

// 将默认域 id 持久化到 ~/.bashrc（幂等）：仅当 .bashrc 中不存在该变量时追加一次 export 行，
// 使新开任意终端可直接 echo 查看并自动继承；-d 指定不触发写入（临时语义）。
// 失败（无 HOME/无写权限）仅告警不致命——进程内 setenv 兜底已保证本进程行为正确。
static void persistDefaultDomainEnv()
{
    const char *home = std::getenv("HOME");
    if (home == nullptr)
    {
        return;
    }
    const std::string bashrc = std::string(home) + "/.bashrc";
    std::ifstream in(bashrc);
    if (in)
    {
        std::string line;
        while (std::getline(in, line))
        {
            if (line.find(kDomainEnv) != std::string::npos)
            {
                return; // .bashrc 已含该变量（用户 export 或此前写入），不重复追加
            }
        }
    }
    std::ofstream out(bashrc, std::ios::app);
    if (!out)
    {
        YOMK_ERROR_TAG("yomkrpc", "persist default domain id failed: cannot append ", bashrc);
        return;
    }
    out << "\n# added by yomkrpc: default DDS domain id (priority: -d > env > 0)\n"
        << "export " << kDomainEnv << "=0\n";
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

// topic print 子命令：登记主题持续打印消息，Ctrl+C 退出（原 main 业务体）
static int runPrint(uint32_t domainId, const std::string &topicName)
{
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

// topic list 子命令：一次性列出域内已发现主题与数据类型名（无 Ctrl+C 循环，查完即退）
static int runList(uint32_t domainId)
{
    YOMK_INIT();
    YOMK_NEW_SERVICE(YomkRpcDebugService);

    // 1. 创建调试节点（单节点模型：重复创建须先删除）
    auto resp = YOMKRPC_DEBUG_NODE(domainId);
    if (resp.m_status != YomkResponse::eOk)
    {
        YOMK_ERROR_TAG("yomkrpc", "create debug node failed: ", resp.m_msg);
        return 1;
    }

    // 2. 等待 EDP 发现重放（异步）：短暂等待让发现缓存积累域内既有 writer 主题
    std::this_thread::sleep_for(std::chrono::seconds(2));

    resp = YOMKRPC_DEBUG_LIST();
    if (resp.m_status != YomkResponse::eOk)
    {
        YOMK_ERROR_TAG("yomkrpc", "list topics failed: ", resp.m_msg);
        YOMKRPC_DEBUG_QUIT();
        return 1;
    }
    YomkUnPackPkg(resp.m_data, StringArray, arr);
    if (arr == nullptr)
    {
        YOMK_ERROR_TAG("yomkrpc", "list topics failed: unexpected response payload");
        YOMKRPC_DEBUG_QUIT();
        return 1;
    }
    if (arr->d.empty())
    {
        std::cout << "no topics discovered on domain " << domainId << std::endl;
    }
    else
    {
        for (const auto &line : arr->d)
        {
            std::cout << line << "\n";
        }
        std::cout.flush();
    }

    // 3. 退出前显式删除调试节点，确保 DDS 实体在 FastDDS 静态资源销毁前清理（同 print 不变式）
    resp = YOMKRPC_DEBUG_QUIT();
    if (resp.m_status != YomkResponse::eOk)
    {
        YOMK_ERROR_TAG("yomkrpc", "debug quit failed: ", resp.m_msg);
        return 1;
    }
    return 0;
}

int main(int argc, char *argv[])
{
    // ---- 环境变量：无 YOMKRPC_DDS_DOMAIN_ID 则创建并设默认值 0（进程内生效），并幂等
    // 持久化到 ~/.bashrc 使新开终端可见可继承；用户已 export 或 -d 时不写 ----
    if (std::getenv(kDomainEnv) == nullptr)
    {
        ::setenv(kDomainEnv, "0", 1);
        persistDefaultDomainEnv();
    }

    // ---- 参数解析：-h/--help 即刻退出；-d/--domain 可选（临时指定，不写环境变量）；位置参数 ----
    uint32_t domainId = 0;
    bool hasDomainId = false;
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
            hasDomainId = true;
            continue;
        }
        pos.push_back(arg);
    }

    // ---- 域 id 解析：-d 显式指定优先（临时生效，不写环境变量）；否则读环境变量（默认 0） ----
    if (!hasDomainId)
    {
        const char *envVal = std::getenv(kDomainEnv);
        if (!parseDomain(envVal, domainId))
        {
            std::cerr << "yomkrpc: 环境变量 " << kDomainEnv << " 非法 \"" << envVal
                      << "\"（合法范围 [0,232]）\n";
            return 2;
        }
    }

    // ---- 子命令分派：topic print <topic-name> / topic list ----
    if (pos.size() == 3 && pos[0] == "topic" && pos[1] == "print" && !pos[2].empty())
    {
        return runPrint(domainId, pos[2]);
    }
    if (pos.size() == 2 && pos[0] == "topic" && pos[1] == "list")
    {
        return runList(domainId);
    }
    if (pos.size() == 2 && pos[0] == "topic")
    {
        std::cerr << "yomkrpc: 未知子命令 \"" << pos[1] << "\"\n";
    }
    else
    {
        std::cerr << "yomkrpc: 用法错误\n";
    }
    printUsage(std::cerr);
    return 2;
}
