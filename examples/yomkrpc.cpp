/**
 * @file yomkrpc.cpp
 * @brief yomkrpc 命令行工具：观察任意 DDS 主题（持续打印消息）、列出域内主题或节点、
 *       查询单个主题或单个节点的详情
 *
 * 用法：
 *   yomkrpc topic print [-d N | --domain N] <topic-name>
 *   yomkrpc topic list [-d N | --domain N] [-w N | --wait N] [-t | --types]
 *   yomkrpc topic info [-d N | --domain N] [-w N | --wait N] [-v | --verbose] <topic-name>
 *   yomkrpc topic type [-d N | --domain N] [-w N | --wait N] <topic-name>
 *   yomkrpc topic find [-d N | --domain N] [-w N | --wait N] <type-name>
 *   yomkrpc topic hz [-d N | --domain N] [--window N] <topic-name>
 *   yomkrpc interface show [-d N | --domain N] [-w N | --wait N] <type-name>
 *   yomkrpc interface list [-d N | --domain N] [-w N | --wait N]
 *   yomkrpc topic pub -e | --example [-d N | --domain N] [-w N | --wait N] <topic-name>
 *   yomkrpc topic pub -ef | --example-file [-d N | --domain N] [-w N | --wait N]
 *           [-o <dir> | --output <dir>] <topic-name>
 *   yomkrpc node list [-d N | --domain N] [-w N | --wait N]
 *   yomkrpc node info [-d N | --domain N] [-w N | --wait N] <node-name>
 *   yomkrpc -h | --help
 *
 * 示例（与 ExampleYomkRpcPub 配合，默认域 0 即开即用）：
 *   yomkrpc topic print hello_world
 *   yomkrpc topic print -d 5 sensor_data
 *   yomkrpc topic list
 *   yomkrpc topic list -w 3
 *   yomkrpc topic list -t
 *   yomkrpc topic info hello_world
 *   yomkrpc topic info -v hello_world
 *   yomkrpc topic type hello_world
 *   yomkrpc topic find YomkRpc::MString
 *   yomkrpc topic hz hello_world
 *   yomkrpc interface show YomkRpc::MString
 *   yomkrpc interface list
 *   yomkrpc topic pub -e hello_world
 *   yomkrpc topic pub -ef hello_world
 *   yomkrpc node list
 *   yomkrpc node info my_node
 *
 * 实现经 YomkRpcDebugService 调试服务（YOMKRPC_DEBUG_* 宏）驱动内部调试节点——
 * topic print：登记主题后远端 DataWriter 经 DDS 发现自动解析类型建立订阅，消息文本
 * 逐条经回调直出 stdout（输出权在调用方，工具侧不落日志），Ctrl+C 退出；
 * topic list：创建节点后一次性收敛查询并逐行打印 topicName——自适应收敛：
 * 节点内部每 ~200ms 轮询一次发现缓存快照，连续 waitRounds 次（默认 5，-w 可调）集合不变
 * 即认为发现收敛、立即输出，无需固定等待窗口；-t/--types 类型名模式：每行改输出
 * "主题名 [类型名]"（单空格 + 方括号，对齐 ros2 topic list -t，类型名原样输出）；
 * topic info：创建节点后独立收敛查询单个目标主题的发现详情（节点内部轮询该主题快照，
 * 连续 waitRounds 次不变即返回），输出三行：Type（原始 DDS 类型名，无风格转换）、
 * Publisher count、Subscription count；-v/--verbose 端点详情模式：追加逐端点 Node name
 * （归属参与者名）、Endpoint type（PUBLISHER/SUBSCRIPTION）、GUID（FastDDS 原生格式）与
 * QoS profile（ROS2 风格键值行）详情段（段间空行，count 为 0 的端点类型无清单段）；
 * 未发现主题报错退出。
 * topic type：topic info 的单值快捷方式（复用 /topic_info 非 verbose 端点），输出单行裸类型名
 * （去 "Type: " 前缀，对齐 ros2 topic type，便于脚本 $() 取用）；未发现主题报错退出。
 * topic find：按数据类型名反查域内主题列表（类型名精确匹配，对齐 ros2 topic find），命中
 * 每行输出一个主题名（按主题名排序）；无匹配主题报错退出（可发现类型名拼写错误）。
 * interface show：按数据类型名输出该类型的 IDL 结构描述（类型名精确匹配，FastDDS
 * 无原生类型文本化能力，自设计对齐主流 schema 源语法形态：struct 头 + 四空格缩进字段行
 * + 结尾 };）；未发现类型报错退出（可发现类型名拼写错误）。
 * interface list：列出发现缓存中出现的全部数据类型名（去重字典序排序，每行一个，
 * interface show 的配套导航）；域内无任何类型报错退出（find 族语义）。
 * topic pub -e：按主题名输出发布示例三段行集（Type 类型名 / IDL 结构描述 / example
 * 可复制发布命令——同类型重建 DynamicType 取默认值样本，与发布输入格式对称，改命令里
 * JSON 字段值即可发布）；未发现主题报错退出。
 * topic pub：按主题名发布一条 JSON 载荷消息（仅发一次）：节点层先发现收敛（类型名 +
 * 订阅者数快照稳定）再类型重建 + JSON 解析（失败报错不发布，不建任何发布实体），临时
 * RELIABLE+TRANSIENT_LOCAL writer 匹配收敛后 write 一次；存在 RELIABLE 订阅者时以 ack
 * 确认送达（未全部确认报错退出），全 BEST_EFFORT 或无订阅者退化尽力而为；未发现主题
 * 报错退出。
 * topic pub -ef：按主题名导出消息描述 JSON 文件（查完即退，不发布）：文件内容即默认值
 * 模板 JSON 本身（展开多行缩进，整体就是一份合法发布载荷，可直接 "$(cat 文件)" 填进
 * topic pub 发布命令），文件名 <主题名>_msg_<年-月-日-时-分-秒-毫秒>.json；无 -o 在
 * 当前目录生成，-o 指定目录（相对/绝对均可，目录不存在报错退出，不自动创建）；未发现
 * 主题报错退出。
 * topic hz：订阅主题测量接收频率（复用 topic print 的登记订阅链路，回调只记时间戳不打印
 * 消息），输出对齐 ros2 topic hz：主循环每秒打印一次滚动窗口统计（average rate 为窗口内
 * 相邻消息间隔均值倒数，Hz；min/max 为间隔极值，秒；std dev 为间隔总体标准差，秒；
 * window 为间隔样本数，上限 --window 默认 10000）；无新消息不重复打印，首条消息前静默；
 * Ctrl+C 退出。
 * node list：创建节点后独立收敛查询域内已发现的命名参与者（每 ~200ms 轮询一次参与者
 * 发现缓存快照，连续 waitRounds 次不变即返回），每行一个节点名（participant_name 非空
 * 才列出，空名参与者跳过），按名称排序；调试节点自身不在自身发现回调中，天然不列出。
 * node info：创建节点后独立收敛查询指定节点名参与者的发布/订阅主题清单（归属判定基于
 * RTPS 规范保证的"端点 GUID 前缀 == 所属参与者 GUID 前缀"），输出对齐 ros2 node info
 * 形态：节点名行 + "  Subscribers:" 段 + 每行 "    topic: type" + "  Publishers:" 段同
 * 形态（类型名原样输出；空段仅打段头）；未发现节点名报错退出。
 * 各子命令退出前均 YOMKRPC_DEBUG_QUIT() 显式清理，规避 FastDDS 静态析构期段错误
 * （同 ExampleYomkRpcSub 退出前 DEL_NODE 模式）。
 *
 * 域 id 两种指定方式（优先级 -d > 环境变量 > 0）：
 *   1. 环境变量 YOMKRPC_DDS_DOMAIN_ID：默认从此读取；启动时无则创建并设默认值 0，
 *      同时幂等写入 ~/.bashrc（仅当其中无该变量时），使新开任意终端可查看并继承；
 *   2. -d N：临时指定，直接使用该值（不读、也绝不写环境变量与 .bashrc）。
 */

#include <YomkRpc/YomkRpcAPI.h>
#include <YomkServer/YomkAPI.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cmath>
#include <csignal>
#include <cstdint>
#include <cstdlib> // strtoul, getenv, setenv
#include <ctime>
#include <deque>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <mutex>
#include <numeric>
#include <sstream>
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
          "  yomkrpc topic list [-d N | --domain N] [-w N | --wait N] [-t | --types]\n"
          "  yomkrpc topic info [-d N | --domain N] [-w N | --wait N] [-v | --verbose] <topic-name>\n"
          "  yomkrpc topic type [-d N | --domain N] [-w N | --wait N] <topic-name>\n"
          "  yomkrpc topic find [-d N | --domain N] [-w N | --wait N] <type-name>\n"
          "  yomkrpc topic hz [-d N | --domain N] [--window N] <topic-name>\n"
          "  yomkrpc interface show [-d N | --domain N] [-w N | --wait N] <type-name>\n"
          "  yomkrpc interface list [-d N | --domain N] [-w N | --wait N]\n"
          "  yomkrpc topic pub -e | --example [-d N | --domain N] [-w N | --wait N] <topic-name>\n"
          "  yomkrpc topic pub -ef | --example-file [-d N | --domain N] [-w N | --wait N]\n"
          "          [-o <dir> | --output <dir>] <topic-name>\n"
          "  yomkrpc topic pub [-d N | --domain N] [-w N | --wait N] <topic-name> <json>\n"
          "  yomkrpc node list [-d N | --domain N] [-w N | --wait N]\n"
          "  yomkrpc node info [-d N | --domain N] [-w N | --wait N] <node-name>\n"
          "  yomkrpc -h | --help\n"
          "\n"
          "Options:\n"
          "  -d N, --domain N    DDS 域号（0-232），临时指定，不写环境变量；未指定时读\n"
          "                      环境变量 YOMKRPC_DDS_DOMAIN_ID（无则默认 0）\n"
          "  -w N, --wait N      收敛判定次数：连续 N 次 200ms 快照不变即输出\n"
          "                      （默认 5，topic list、topic info、topic find、interface show、interface list、\n"
          "                      topic pub -e、topic pub -ef、topic pub、node list 与 node info 生效）\n"
          "  -v, --verbose       端点详情模式（仅 topic info 生效）：追加逐端点 Node name、\n"
          "                      Endpoint type、GUID 与 QoS profile 详情段\n"
          "  -t, --types         类型名模式（仅 topic list 生效）：每行输出 \"主题名 [类型名]\"\n"
          "  -o, --output <dir>  输出目录（仅 topic pub -ef 生效）：消息描述文件生成位置，相对/\n"
          "                      绝对路径均可，缺省当前目录；目录不存在报错退出，不自动创建\n"
          "  --window N          频率统计窗口大小（相邻消息间隔样本数上限，默认 10000，仅 topic hz 生效）\n"
          "  -h, --help          显示帮助\n"
          "\n"
          "Examples:\n"
          "  yomkrpc topic print hello_world\n"
          "  yomkrpc topic print -d 5 sensor_data\n"
          "  yomkrpc topic list\n"
          "  yomkrpc topic list -w 3\n"
          "  yomkrpc topic list -t\n"
          "  yomkrpc topic info hello_world\n"
          "  yomkrpc topic info -v hello_world\n"
          "  yomkrpc topic type hello_world\n"
          "  yomkrpc topic find YomkRpc::MString\n"
          "  yomkrpc topic hz hello_world\n"
          "  yomkrpc interface show YomkRpc::MString\n"
          "  yomkrpc interface list\n"
          "  yomkrpc topic pub -e hello_world\n"
          "  yomkrpc topic pub -ef hello_world\n"
          "  yomkrpc topic pub -ef -o /tmp hello_world\n"
          "  yomkrpc topic pub hello_world '{\"data\":\"hi\"}'\n"
          "  yomkrpc node list\n"
          "  yomkrpc node info my_node\n"
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
    resp = YOMKRPC_DEBUG_TOPIC_PRINT(topicName, onMessage);
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

// topic hz 子命令：订阅主题测量消息接收频率（复用 topic print 的登记订阅链路，回调只记
// 时间戳不打印消息），输出对齐 ros2 topic hz：主循环每秒打印一次滚动窗口统计——
// "average rate"（窗口内相邻消息间隔均值的倒数，Hz）、min/max（间隔极值，秒）、
// std dev（间隔的总体标准差，除以 n）、window（间隔样本数，上限 --window）；无新消息
// 不重复打印（对齐 ros2 哨兵语义），首条消息前静默等待；Ctrl+C 退出
static int runHz(uint32_t domainId, const std::string &topicName, size_t windowSize)
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

    // 2. 登记调试主题：回调在 DDS 监听线程内逐条投递，仅锁内记录相邻消息间隔（消息内容
    // 不使用），统计由主循环消费——回调极轻不阻塞交付
    std::mutex hzMtx;
    std::deque<double> hzTimes; // 窗口内相邻消息间隔（秒），size 上限 windowSize
    std::chrono::steady_clock::time_point hzLast{};
    bool hzHasLast = false;
    auto onMessage = [&](const std::string &)
    {
        const auto now = std::chrono::steady_clock::now();
        std::lock_guard<std::mutex> lock(hzMtx);
        if (hzHasLast)
        {
            hzTimes.push_back(std::chrono::duration<double>(now - hzLast).count());
            while (hzTimes.size() > windowSize)
            {
                hzTimes.pop_front(); // 滚动窗口（O(1) 收缩，对齐 ros2 pop(0) 语义）
            }
        }
        hzLast = now;
        hzHasLast = true;
    };
    resp = YOMKRPC_DEBUG_TOPIC_PRINT(topicName, onMessage);
    if (resp.m_status != YomkResponse::eOk)
    {
        YOMK_ERROR_TAG("yomkrpc", "register debug topic failed: ", resp.m_msg);
        YOMKRPC_DEBUG_QUIT(); // 登记失败路径同样须清理已建调试节点
        return 1;
    }

    // 3. 主循环每秒打印一次统计（对齐 ros2 1s 打印线程节奏）：无新消息（末消息时间戳未变）
    // 跳过；窗口空（尚无完整间隔，仅收到 0/1 条消息）跳过——首条消息前静默
    std::signal(SIGINT, onSignal);
    YOMK_INFO_TAG("yomkrpc", "measuring hz of topic \"", topicName, "\" on domain ",
                  std::to_string(domainId), ", press Ctrl+C to exit");
    auto lastPrinted = hzLast;
    std::cout << std::fixed;
    while (!g_stop.load())
    {
        std::this_thread::sleep_for(std::chrono::seconds(1));
        std::deque<double> times;
        {
            std::lock_guard<std::mutex> lock(hzMtx);
            if (!hzHasLast || hzLast == lastPrinted)
            {
                continue; // 尚未收到任何消息或无新消息：不重复打印
            }
            lastPrinted = hzLast;
            times = hzTimes;
        }
        if (times.empty())
        {
            continue; // 仅 1 条消息（无完整间隔）：无统计意义
        }
        const double mean = std::accumulate(times.begin(), times.end(), 0.0) / times.size();
        const double rate = mean > 0.0 ? 1.0 / mean : 0.0;
        double sumSq = 0.0;
        for (double t : times)
        {
            const double d = t - mean;
            sumSq += d * d;
        }
        const double stdDev = std::sqrt(sumSq / times.size()); // 总体标准差（除以 n）
        std::cout << "average rate: " << std::setprecision(3) << rate << "\n"
                  << "        min: " << *std::min_element(times.begin(), times.end())
                  << "s max: " << *std::max_element(times.begin(), times.end())
                  << "s std dev: " << std::setprecision(5) << stdDev
                  << "s window: " << times.size() << std::endl;
    }

    // 4. 退出前显式删除调试节点，确保 DDS 实体在 FastDDS 静态资源销毁前清理（同 print 不变式）
    resp = YOMKRPC_DEBUG_QUIT();
    if (resp.m_status != YomkResponse::eOk)
    {
        YOMK_ERROR_TAG("yomkrpc", "debug quit failed: ", resp.m_msg);
        return 1;
    }
    return 0;
}

// topic list 子命令：自适应收敛查询域内已发现主题与数据类型名（每 ~200ms 轮询一次发现缓存
// 快照，连续 waitRounds 次集合不变即收敛立即输出；types 模式（-t）每行输出 "主题名 [类型名]"；
// 无 Ctrl+C 循环，查完即退）
static int runList(uint32_t domainId, uint32_t waitRounds, bool types = false)
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

    // 2. 收敛查询：节点内部轮询发现缓存快照（~200ms 间隔），连续 waitRounds 次集合不变即返回，
    //    发现重放完成即立即输出，无需固定等待窗口；types 模式（-t）每行 "主题名 [类型名]"
    if (types)
    {
        resp = YOMKRPC_DEBUG_TOPIC_LIST_T(waitRounds, 200);
    }
    else
    {
        resp = YOMKRPC_DEBUG_TOPIC_LIST(waitRounds, 200);
    }
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

// topic info 子命令：独立收敛查询单个主题的发现详情（节点内部轮询该主题快照——存在标志 +
// 类型名 + 端点计数，连续 waitRounds 次不变即返回），输出三行：Type / Publisher count /
// Subscription count；verbose 模式（-v）追加逐端点详情段（Node name/Endpoint type/GUID/
// QoS profile，段间空行，count 为 0 的端点类型无清单段）；未发现主题报错退出（无 Ctrl+C
// 循环，查完即退）
static int runInfo(uint32_t domainId, const std::string &topicName, uint32_t waitRounds,
                   bool verbose = false)
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

    // 2. 独立收敛查询单主题详情（与 list 查询完全分开的路径）：命中返回 StringArray，
    //    行内容由服务层拼装（非 verbose 三行 / verbose 逐端点详情段），原样直出；
    //    未发现主题（含收敛窗口内始终不可见）返回 eNo
    if (verbose)
    {
        resp = YOMKRPC_DEBUG_TOPIC_INFO_V(topicName, waitRounds, 200);
    }
    else
    {
        resp = YOMKRPC_DEBUG_TOPIC_INFO(topicName, waitRounds, 200);
    }
    if (resp.m_status != YomkResponse::eOk)
    {
        YOMK_ERROR_TAG("yomkrpc", "topic info failed: ", resp.m_msg);
        YOMKRPC_DEBUG_QUIT();
        return 1;
    }
    YomkUnPackPkg(resp.m_data, StringArray, arr);
    if (arr == nullptr)
    {
        YOMK_ERROR_TAG("yomkrpc", "topic info failed: unexpected response payload");
        YOMKRPC_DEBUG_QUIT();
        return 1;
    }
    for (const auto &line : arr->d)
    {
        std::cout << line << "\n";
    }
    std::cout.flush();

    // 3. 退出前显式删除调试节点，确保 DDS 实体在 FastDDS 静态资源销毁前清理（同 print/list 不变式）
    resp = YOMKRPC_DEBUG_QUIT();
    if (resp.m_status != YomkResponse::eOk)
    {
        YOMK_ERROR_TAG("yomkrpc", "debug quit failed: ", resp.m_msg);
        return 1;
    }
    return 0;
}

// topic type 子命令：查询单主题数据类型（topic info 的单值快捷方式，复用 /topic_info 非 verbose
// 端点）：取首行 "Type: xxx" 去前缀裸出类型名（对齐 ros2 topic type，便于脚本 $() 取用）；
// 未发现主题报错退出（无 Ctrl+C 循环，查完即退）
static int runTopicType(uint32_t domainId, const std::string &topicName, uint32_t waitRounds)
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

    // 2. 独立收敛查询单主题详情（非 verbose 三行），仅消费首行类型名
    resp = YOMKRPC_DEBUG_TOPIC_INFO(topicName, waitRounds, 200);
    if (resp.m_status != YomkResponse::eOk)
    {
        YOMK_ERROR_TAG("yomkrpc", "topic type failed: ", resp.m_msg);
        YOMKRPC_DEBUG_QUIT();
        return 1;
    }
    YomkUnPackPkg(resp.m_data, StringArray, arr);
    if (arr == nullptr || arr->d.empty())
    {
        YOMK_ERROR_TAG("yomkrpc", "topic type failed: unexpected response payload");
        YOMKRPC_DEBUG_QUIT();
        return 1;
    }
    const std::string kTypePrefix = "Type: ";
    const std::string &first = arr->d.front();
    if (first.rfind(kTypePrefix, 0) == 0)
    {
        std::cout << first.substr(kTypePrefix.size()) << "\n"; // 裸类型名
    }
    else
    {
        std::cout << first << "\n"; // 防御：前缀不匹配时原样输出
    }
    std::cout.flush();

    // 3. 退出前显式删除调试节点，确保 DDS 实体在 FastDDS 静态资源销毁前清理（同 print/list 不变式）
    resp = YOMKRPC_DEBUG_QUIT();
    if (resp.m_status != YomkResponse::eOk)
    {
        YOMK_ERROR_TAG("yomkrpc", "debug quit failed: ", resp.m_msg);
        return 1;
    }
    return 0;
}

// topic find 子命令：按数据类型名反查域内主题列表（类型名精确匹配，对齐 ros2 topic find）：
// 命中每行输出一个主题名（按主题名排序）；无匹配主题报错退出（可发现类型名拼写错误）。
// 无 Ctrl+C 循环，查完即退
static int runTopicFind(uint32_t domainId, const std::string &typeName, uint32_t waitRounds)
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

    // 2. 独立收敛按类型名反查主题列表
    resp = YOMKRPC_DEBUG_TOPIC_FIND(typeName, waitRounds, 200);
    if (resp.m_status != YomkResponse::eOk)
    {
        YOMK_ERROR_TAG("yomkrpc", "topic find failed: ", resp.m_msg);
        YOMKRPC_DEBUG_QUIT();
        return 1;
    }
    YomkUnPackPkg(resp.m_data, StringArray, arr);
    if (arr == nullptr || arr->d.empty())
    {
        YOMK_ERROR_TAG("yomkrpc", "topic find failed: unexpected response payload");
        YOMKRPC_DEBUG_QUIT();
        return 1;
    }
    for (const auto &line : arr->d)
    {
        std::cout << line << "\n";
    }
    std::cout.flush();

    // 3. 退出前显式删除调试节点，确保 DDS 实体在 FastDDS 静态资源销毁前清理（同 print/list 不变式）
    resp = YOMKRPC_DEBUG_QUIT();
    if (resp.m_status != YomkResponse::eOk)
    {
        YOMK_ERROR_TAG("yomkrpc", "debug quit failed: ", resp.m_msg);
        return 1;
    }
    return 0;
}

// interface show 子命令：按数据类型名输出该类型的 IDL 结构描述（类型名精确匹配，FastDDS
// 无原生类型文本化能力，自设计对齐主流 schema 源语法形态）：命中逐行输出（struct 头 +
// 四空格缩进字段行 + 结尾 };）；未发现类型报错退出（可发现类型名拼写错误）。
// 无 Ctrl+C 循环，查完即退
static int runInterfaceShow(uint32_t domainId, const std::string &typeName, uint32_t waitRounds)
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

    // 2. 独立收敛按类型名查询 IDL 结构行集
    resp = YOMKRPC_DEBUG_INTERFACE_SHOW(typeName, waitRounds, 200);
    if (resp.m_status != YomkResponse::eOk)
    {
        YOMK_ERROR_TAG("yomkrpc", "interface show failed: ", resp.m_msg);
        YOMKRPC_DEBUG_QUIT();
        return 1;
    }
    YomkUnPackPkg(resp.m_data, StringArray, arr);
    if (arr == nullptr || arr->d.empty())
    {
        YOMK_ERROR_TAG("yomkrpc", "interface show failed: unexpected response payload");
        YOMKRPC_DEBUG_QUIT();
        return 1;
    }
    for (const auto &line : arr->d)
    {
        std::cout << line << "\n";
    }
    std::cout.flush();

    // 3. 退出前显式删除调试节点，确保 DDS 实体在 FastDDS 静态资源销毁前清理（同 print/list 不变式）
    resp = YOMKRPC_DEBUG_QUIT();
    if (resp.m_status != YomkResponse::eOk)
    {
        YOMK_ERROR_TAG("yomkrpc", "debug quit failed: ", resp.m_msg);
        return 1;
    }
    return 0;
}

// interface list 子命令：列出发现缓存中出现的全部数据类型名（去重字典序排序，每行一个，
// interface show 的配套导航；同类型多主题仅出一行）；域内无任何类型报错退出（find 族语义）。
// 无 Ctrl+C 循环，查完即退
static int runInterfaceList(uint32_t domainId, uint32_t waitRounds)
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

    // 2. 独立收敛收集发现缓存中的全部类型名
    resp = YOMKRPC_DEBUG_INTERFACE_LIST(waitRounds, 200);
    if (resp.m_status != YomkResponse::eOk)
    {
        YOMK_ERROR_TAG("yomkrpc", "interface list failed: ", resp.m_msg);
        YOMKRPC_DEBUG_QUIT();
        return 1;
    }
    YomkUnPackPkg(resp.m_data, StringArray, arr);
    if (arr == nullptr)
    {
        YOMK_ERROR_TAG("yomkrpc", "interface list failed: unexpected response payload");
        YOMKRPC_DEBUG_QUIT();
        return 1;
    }
    for (const auto &line : arr->d)
    {
        std::cout << line << "\n";
    }
    std::cout.flush();

    // 3. 退出前显式删除调试节点，确保 DDS 实体在 FastDDS 静态资源销毁前清理（同 print/list 不变式）
    resp = YOMKRPC_DEBUG_QUIT();
    if (resp.m_status != YomkResponse::eOk)
    {
        YOMK_ERROR_TAG("yomkrpc", "debug quit failed: ", resp.m_msg);
        return 1;
    }
    return 0;
}

// 将紧凑 JSON 展开为多行缩进形式（字符串感知单遍扫描；仅用于美化服务层返回的紧凑
// JSON——json_serialize 输出无空白已实证，空对象/空数组保持单行）：{ / [ 后换行并加深缩进
// （2 空格/层），, 后换行，: 后补空格，} / ] 前换行并收浅缩进；\" 转义对整体拷贝，
// 不误判字符串边界；indentBase 为起始缩进层级（嵌进外层 JSON 时传外层深度，缺省 0）
static std::string prettyJson(const std::string &json, int indentBase = 0)
{
    std::string out;
    out.reserve(json.size() * 2);
    int depth = indentBase;
    bool inStr = false;
    for (size_t i = 0; i < json.size(); ++i)
    {
        const char c = json[i];
        if (inStr)
        {
            out += c;
            if (c == '\\' && i + 1 < json.size())
            {
                out += json[++i]; // 转义对整体拷贝（含 \" ，不误判字符串边界）
            }
            else if (c == '"')
            {
                inStr = false;
            }
            continue;
        }
        switch (c)
        {
        case '"':
            inStr = true;
            out += c;
            break;
        case '{':
        case '[':
            out += c;
            // 空对象/空数组保持单行（紧凑输入下紧邻即闭括号）
            if (i + 1 < json.size() &&
                    ((c == '{' && json[i + 1] == '}') || (c == '[' && json[i + 1] == ']')))
            {
                out += json[++i];
                break;
            }
            ++depth;
            out += "\n";
            out.append(static_cast<size_t>(depth) * 2, ' ');
            break;
        case '}':
        case ']':
            --depth;
            out += "\n";
            out.append(static_cast<size_t>(depth) * 2, ' ');
            out += c;
            break;
        case ',':
            out += ",\n";
            out.append(static_cast<size_t>(depth) * 2, ' ');
            break;
        case ':':
            out += ": ";
            break;
        default:
            out += c;
            break;
        }
    }
    return out;
}

// topic pub -ef 子命令：按主题名导出消息描述 JSON 文件（查完即退，不发布）：服务层返回
// 两要素（d[0]=类型名仅用于 stdout 提示、d[1]=紧凑 msg JSON），本函数负责文件名拼装与
// 落盘：文件名 <主题名>_msg_<年-月-日-时-分-秒-毫秒>.json；无 -o 时当前目录，有 -o 时
// 指定目录（相对/绝对均可，目录不存在报错退出，不自动创建）；文件内容即 msg 默认值
// 模板 JSON 本身（展开多行缩进），整体就是一份合法发布载荷，可直接 "$(cat 文件)" 填进
// topic pub 发布命令
static int runTopicPubMsgFile(uint32_t domainId, const std::string &topicName,
        const std::string &outDir, uint32_t waitRounds)
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

    // 2. 独立收敛查询消息描述两要素（StringArray 恰 2 行：类型名 + 紧凑 msg JSON）
    resp = YOMKRPC_DEBUG_TOPIC_MSG(topicName, waitRounds, 200);
    if (resp.m_status != YomkResponse::eOk)
    {
        YOMK_ERROR_TAG("yomkrpc", "topic msg failed: ", resp.m_msg);
        YOMKRPC_DEBUG_QUIT();
        return 1;
    }
    YomkUnPackPkg(resp.m_data, StringArray, arr);
    if (arr == nullptr || arr->d.size() != 2)
    {
        YOMK_ERROR_TAG("yomkrpc", "topic msg failed: unexpected response payload");
        YOMKRPC_DEBUG_QUIT();
        return 1;
    }
    const std::string &typeName = arr->d[0];
    const std::string &compactJson = arr->d[1];

    // 3. 输出目录检查：指定目录不存在报错退出（不自动创建；相对/绝对路径均可）
    std::string dir = outDir;
    if (!dir.empty() && dir.back() != '/')
    {
        dir += "/";
    }
    if (!dir.empty() &&
            (!std::filesystem::exists(dir) || !std::filesystem::is_directory(dir)))
    {
        YOMK_ERROR_TAG("yomkrpc", "output directory not found: ", dir);
        YOMKRPC_DEBUG_QUIT();
        return 1;
    }

    // 4. 时间戳文件名：<主题名>_msg_<年-月-日-时-分-秒-毫秒>.json（strftime 无毫秒，取
    //    epoch 毫秒低 3 位手拼补零）
    const auto now = std::chrono::system_clock::now();
    const std::time_t tt = std::chrono::system_clock::to_time_t(now);
    const int msPart = static_cast<int>(
            std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch())
                    .count() % 1000);
    std::ostringstream ts;
    ts << std::put_time(std::localtime(&tt), "%Y-%m-%d-%H-%M-%S-")
       << std::setw(3) << std::setfill('0') << msPart;
    const std::string filePath = dir + topicName + "_msg_" + ts.str() + ".json";

    // 5. 写文件：文件内容即 msg 默认值模板 JSON 本身（经 prettyJson 展开多行缩进，顶层基准
    //    缩进），整体就是一份合法发布载荷，可直接 "$(cat 文件)" 填进 topic pub 发布命令
    std::ofstream out(filePath);
    if (!out)
    {
        YOMK_ERROR_TAG("yomkrpc", "cannot open file for write: ", filePath);
        YOMKRPC_DEBUG_QUIT();
        return 1;
    }
    out << prettyJson(compactJson) << "\n";
    out.close();
    std::cout << "message description written: " << filePath
              << " (topic: " << topicName << ", type: " << typeName << ")\n";
    std::cout.flush();

    // 6. 退出前显式删除调试节点，确保 DDS 实体在 FastDDS 静态资源销毁前清理（同其余子命令不变式）
    resp = YOMKRPC_DEBUG_QUIT();
    if (resp.m_status != YomkResponse::eOk)
    {
        YOMK_ERROR_TAG("yomkrpc", "debug quit failed: ", resp.m_msg);
        return 1;
    }
    return 0;
}

// topic pub -e 子命令：按主题名输出发布示例三段行集（Type / IDL / example 可复制发布命令），供
// 用户按 JSON 模板填好字段值后发布（查完即退，不发布）；调试节点内部依次收敛：类型名 →
// IDL 行集 → JSON 示例，独立收敛参数同其余查询（无 Ctrl+C 循环）
static int runTopicPubExample(uint32_t domainId, const std::string &topicName, uint32_t waitRounds)
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

    // 2. 独立收敛查询发布示例三段行集
    resp = YOMKRPC_DEBUG_TOPIC_EXAMPLE(topicName, waitRounds, 200);
    if (resp.m_status != YomkResponse::eOk)
    {
        YOMK_ERROR_TAG("yomkrpc", "topic example failed: ", resp.m_msg);
        YOMKRPC_DEBUG_QUIT();
        return 1;
    }
    YomkUnPackPkg(resp.m_data, StringArray, arr);
    if (arr == nullptr)
    {
        YOMK_ERROR_TAG("yomkrpc", "topic example failed: unexpected response payload");
        YOMKRPC_DEBUG_QUIT();
        return 1;
    }
    for (const auto &line : arr->d)
    {
        // 段标题（IDL: / example:）前空一行，三段展示不拥挤（空行属展示层修饰，
        // 不入数据行集）
        if (line == "IDL:" || line == "example:")
        {
            std::cout << "\n";
        }
        std::cout << line << "\n";
    }
    std::cout.flush();

    // 3. 退出前显式删除调试节点，确保 DDS 实体在 FastDDS 静态资源销毁前清理（同 print/list 不变式）
    resp = YOMKRPC_DEBUG_QUIT();
    if (resp.m_status != YomkResponse::eOk)
    {
        YOMK_ERROR_TAG("yomkrpc", "debug quit failed: ", resp.m_msg);
        return 1;
    }
    return 0;
}

// topic pub 子命令：按主题名发布一条 JSON 载荷消息（仅发一次，查完即退）；调试节点内部
// 依次：发现收敛（主题+订阅者数稳定）→ 类型重建 + JSON 解析 → 临时 RELIABLE writer 匹配
// 收敛 → write 一次 → 存在 RELIABLE 订阅者时 ack 确认送达（超时报错），全 BEST_EFFORT 或
// 无订阅者退化尽力而为（无 Ctrl+C 循环）
static int runTopicPub(uint32_t domainId, const std::string &topicName, const std::string &json,
        uint32_t waitRounds)
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

    // 2. 收敛后发布一次（ack 自适应确认）；未发现/载荷不合法/未确认均报错退出
    resp = YOMKRPC_DEBUG_TOPIC_PUB(topicName, json, waitRounds, 200);
    if (resp.m_status != YomkResponse::eOk)
    {
        YOMK_ERROR_TAG("yomkrpc", "topic pub failed: ", resp.m_msg);
        YOMKRPC_DEBUG_QUIT();
        return 1;
    }
    std::cout << "delivered to topic " << topicName << std::endl;

    // 3. 退出前显式删除调试节点，确保 DDS 实体在 FastDDS 静态资源销毁前清理（同 print/list 不变式）
    resp = YOMKRPC_DEBUG_QUIT();
    if (resp.m_status != YomkResponse::eOk)
    {
        YOMK_ERROR_TAG("yomkrpc", "debug quit failed: ", resp.m_msg);
        return 1;
    }
    return 0;
}

// node list 子命令：独立收敛查询域内已发现的命名参与者（节点内部轮询参与者发现缓存快照，
// 连续 waitRounds 次不变即返回），每行一个节点名（participant_name 非空才列出，空名参与者
// 跳过），按名称排序；调试节点自身不在自身发现回调中，天然不列出（无 Ctrl+C 循环，查完即退）
static int runNodeList(uint32_t domainId, uint32_t waitRounds)
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

    // 2. 独立收敛查询域内命名参与者（与 topic 查询完全分开的路径）：命中返回 StringArray，
    //    每行一个节点名；域内暂无命名参与者返回空列表（合法，输出提示行）
    resp = YOMKRPC_DEBUG_NODE_LIST(waitRounds, 200);
    if (resp.m_status != YomkResponse::eOk)
    {
        YOMK_ERROR_TAG("yomkrpc", "node list failed: ", resp.m_msg);
        YOMKRPC_DEBUG_QUIT();
        return 1;
    }
    YomkUnPackPkg(resp.m_data, StringArray, arr);
    if (arr == nullptr)
    {
        YOMK_ERROR_TAG("yomkrpc", "node list failed: unexpected response payload");
        YOMKRPC_DEBUG_QUIT();
        return 1;
    }
    if (arr->d.empty())
    {
        std::cout << "no nodes discovered on domain " << domainId << std::endl;
    }
    else
    {
        for (const auto &line : arr->d)
        {
            std::cout << line << "\n";
        }
        std::cout.flush();
    }

    // 3. 退出前显式删除调试节点，确保 DDS 实体在 FastDDS 静态资源销毁前清理（同 print/list/info 不变式）
    resp = YOMKRPC_DEBUG_QUIT();
    if (resp.m_status != YomkResponse::eOk)
    {
        YOMK_ERROR_TAG("yomkrpc", "debug quit failed: ", resp.m_msg);
        return 1;
    }
    return 0;
}

// node info 子命令：独立收敛查询指定节点名（participant_name）参与者的发布/订阅主题清单
// （节点内部轮询归属快照，连续 waitRounds 次不变即返回；归属判定基于 RTPS 规范保证的
// "端点 GUID 前缀 == 所属参与者 GUID 前缀"），输出对齐 ros2 node info 形态：节点名行 +
// "  Subscribers:" 段 + 每行 "    topic: type" + "  Publishers:" 段同形态（类型名原样输出；
// 空段仅打段头）；未发现节点名报错退出（无 Ctrl+C 循环，查完即退）
static int runNodeInfo(uint32_t domainId, const std::string &nodeName, uint32_t waitRounds)
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

    // 2. 独立收敛查询单节点清单（与 list 查询完全分开的路径）：命中返回 StringArray 多行，
    //    行文案由服务层拼装（节点名 + Subscribers/Publishers 段），原样直出；
    //    未发现节点名（含收敛窗口内始终不可见）返回 eNo
    resp = YOMKRPC_DEBUG_NODE_INFO(nodeName, waitRounds, 200);
    if (resp.m_status != YomkResponse::eOk)
    {
        YOMK_ERROR_TAG("yomkrpc", "node info failed: ", resp.m_msg);
        YOMKRPC_DEBUG_QUIT();
        return 1;
    }
    YomkUnPackPkg(resp.m_data, StringArray, arr);
    if (arr == nullptr)
    {
        YOMK_ERROR_TAG("yomkrpc", "node info failed: unexpected response payload");
        YOMKRPC_DEBUG_QUIT();
        return 1;
    }
    for (const auto &line : arr->d)
    {
        std::cout << line << "\n";
    }
    std::cout.flush();

    // 3. 退出前显式删除调试节点，确保 DDS 实体在 FastDDS 静态资源销毁前清理（同其余子命令不变式）
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

    // ---- 参数解析：-h/--help 即刻退出；-d/--domain 与 -w/--wait 可选；位置参数 ----
    uint32_t domainId = 0;
    bool hasDomainId = false;
    uint32_t waitRounds = 5; // 收敛判定次数（-w 覆盖；topic list、topic info、topic find、interface show、interface list、node list 与 node info 生效）
    bool verbose = false;    // 端点详情模式（-v/--verbose；仅 topic info 生效）
    bool types = false;      // 类型名模式（-t/--types；仅 topic list 生效）
    size_t windowSize = 10000; // 频率统计窗口大小（--window；仅 topic hz 生效，对齐 ros2 默认）
    std::string msgOutDir;   // 输出目录（-o/--output；仅 topic pub -ef 生效，缺省当前目录）
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
        if (arg == "-w" || arg == "--wait")
        {
            if (i + 1 >= argc)
            {
                std::cerr << "yomkrpc: " << arg << " 缺少收敛次数参数\n";
                printUsage(std::cerr);
                return 2;
            }
            char *end = nullptr;
            unsigned long value = std::strtoul(argv[++i], &end, 10);
            if (end == argv[i] || *end != '\0' || value == 0 || value > UINT32_MAX)
            {
                std::cerr << "yomkrpc: 非法收敛次数 \"" << argv[i] << "\"（须为 >=1 的整数）\n";
                return 2;
            }
            waitRounds = static_cast<uint32_t>(value);
            continue;
        }
        if (arg == "-o" || arg == "--output")
        {
            if (i + 1 >= argc)
            {
                std::cerr << "yomkrpc: " << arg << " 缺少输出目录参数\n";
                printUsage(std::cerr);
                return 2;
            }
            msgOutDir = argv[++i];
            continue;
        }
        if (arg == "-v" || arg == "--verbose")
        {
            verbose = true;
            continue;
        }
        if (arg == "-t" || arg == "--types")
        {
            types = true;
            continue;
        }
        if (arg == "--window")
        {
            if (i + 1 >= argc)
            {
                std::cerr << "yomkrpc: " << arg << " 缺少窗口大小参数\n";
                printUsage(std::cerr);
                return 2;
            }
            char *end = nullptr;
            unsigned long value = std::strtoul(argv[++i], &end, 10);
            if (end == argv[i] || *end != '\0' || value == 0)
            {
                std::cerr << "yomkrpc: 非法窗口大小 \"" << argv[i] << "\"（须为 >=1 的整数）\n";
                return 2;
            }
            windowSize = static_cast<size_t>(value);
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

    // ---- 子命令分派：topic print <topic-name> / topic list / topic info <topic-name> /
    //      node list / node info <node-name> ----
    if (pos.size() == 3 && pos[0] == "topic" && pos[1] == "print" && !pos[2].empty())
    {
        return runPrint(domainId, pos[2]);
    }
    if (pos.size() == 2 && pos[0] == "topic" && pos[1] == "list")
    {
        return runList(domainId, waitRounds, types);
    }
    if (pos.size() == 3 && pos[0] == "topic" && pos[1] == "type" && !pos[2].empty())
    {
        return runTopicType(domainId, pos[2], waitRounds);
    }
    if (pos.size() == 3 && pos[0] == "topic" && pos[1] == "find" && !pos[2].empty())
    {
        return runTopicFind(domainId, pos[2], waitRounds);
    }
    if (pos.size() == 3 && pos[0] == "interface" && pos[1] == "show" && !pos[2].empty())
    {
        return runInterfaceShow(domainId, pos[2], waitRounds);
    }
    if (pos.size() == 2 && pos[0] == "interface" && pos[1] == "list")
    {
        return runInterfaceList(domainId, waitRounds);
    }
    if (pos.size() == 4 && pos[0] == "topic" && pos[1] == "pub" &&
            (pos[2] == "-e" || pos[2] == "--example") && !pos[3].empty())
    {
        return runTopicPubExample(domainId, pos[3], waitRounds);
    }
    if (pos.size() == 4 && pos[0] == "topic" && pos[1] == "pub" &&
            (pos[2] == "-ef" || pos[2] == "--example-file") && !pos[3].empty())
    {
        return runTopicPubMsgFile(domainId, pos[3], msgOutDir, waitRounds);
    }
    if (pos.size() == 4 && pos[0] == "topic" && pos[1] == "pub" &&
            !pos[2].empty() && !pos[3].empty())
    {
        return runTopicPub(domainId, pos[2], pos[3], waitRounds);
    }
    if (pos.size() >= 2 && pos[0] == "topic" && pos[1] == "pub")
    {
        std::cerr << "yomkrpc: topic pub 支持 -e | --example <主题名>（示例模式）、-ef | --example-file <主题名>（导出消息描述文件）或 <主题名> <json>（发布模式）\n";
    }
    if (pos.size() == 3 && pos[0] == "topic" && pos[1] == "hz" && !pos[2].empty())
    {
        return runHz(domainId, pos[2], windowSize);
    }
    if (pos.size() == 3 && pos[0] == "topic" && pos[1] == "info" && !pos[2].empty())
    {
        return runInfo(domainId, pos[2], waitRounds, verbose);
    }
    if (pos.size() == 2 && pos[0] == "node" && pos[1] == "list")
    {
        return runNodeList(domainId, waitRounds);
    }
    if (pos.size() == 3 && pos[0] == "node" && pos[1] == "info" && !pos[2].empty())
    {
        return runNodeInfo(domainId, pos[2], waitRounds);
    }
    if (pos.size() == 2 && (pos[0] == "topic" || pos[0] == "node"))
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
