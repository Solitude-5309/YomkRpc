#pragma once
// YomkRpcDebugService 对外头：定义 YOMKRPC_DEBUG_* 宏（见 YomkRpcAPI.h）所打包的请求结构
// （DDSDebugNode/DDSDebugTopic）、调试输出回调类型 DDSDebugOutputFunc，以及服务类
// YomkRpcDebugService 的声明。调试能力由内部的 FastDDSDebugNode 提供（类型无关：经 DDS 发现
// 机制取回远端 TypeObject 动态建订阅，消息以 JSON 文本投递给用户回调，零消息类型依赖）。
#include <YomkServer/YomkAPI.h>

#include <atomic>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
class FastDDSDebugNode;
using namespace yomk;

class YomkRpcDebugService : public YomkService
{
public:
    YomkRpcDebugService(YomkServer* server);
    virtual ~YomkRpcDebugService();
    virtual int init() override;

private:
    YomkResponse getVersion(YomkPkgPtr pkg);
    YomkResponse createNode(YomkPkgPtr pkg);
    YomkResponse topicPrint(YomkPkgPtr pkg);
    YomkResponse listTopics(YomkPkgPtr pkg);
    YomkResponse topicInfo(YomkPkgPtr pkg);
    YomkResponse topicFind(YomkPkgPtr pkg);
    YomkResponse interfaceShow(YomkPkgPtr pkg);
    YomkResponse interfaceList(YomkPkgPtr pkg);
    YomkResponse topicExample(YomkPkgPtr pkg);
    YomkResponse topicMsg(YomkPkgPtr pkg);
    YomkResponse topicPub(YomkPkgPtr pkg);
    YomkResponse listNodes(YomkPkgPtr pkg);
    YomkResponse nodeInfo(YomkPkgPtr pkg);
    YomkResponse deleteNode(YomkPkgPtr pkg);

private:
    // 单 debug 节点：一个进程至多一个实例，重复 create 须先 delete；服务析构时自动销毁节点
    // 并按节点内部顺序清理全部 DDS 实体（推荐进程退出前经 /delete_node 显式清理）。
    std::unique_ptr<FastDDSDebugNode> node_;
    std::mutex mtx_;  // 串行化 node_ 的增删查改与节点入口调用
};

// topic pub 持续发布停止标志：发布循环（FastDDSDebugNode::topicPub 内部，repeatIntervalMs>0
// 时）只读，SIGINT 处理函数（CLI onSignal）经 debugPubStop 写——atomic 无锁 store 保证
// async-signal-safe（不可经 YOMK_REQUEST 停止：请求链路含锁与内存分配，handler 内禁用）。
// 调用方在发起持续发布前应先 debugPubReset 复位，避免上次会话残留置位导致循环秒退。
namespace yomk
{
inline std::atomic<bool> g_debugPubStop{false};
inline void debugPubStop()
{
    g_debugPubStop.store(true);
}
inline void debugPubReset()
{
    g_debugPubStop.store(false);
}
}  // namespace yomk

// 调试输出回调：每条消息投递一次格式化 JSON 文本，由 /topic_print 调用方自定义（服务层不打印）。
// 回调在 DDS 数据接收线程被调用，须线程安全且快速返回。
using DDSDebugOutputFunc = std::function<void(const std::string&)>;

// create_node 请求负载。
struct DDSDebugNode
{
    uint32_t domainId;  // DDS 域号，合法范围 [0,232]
};

// topic_print 请求负载。
struct DDSDebugTopic
{
    std::string topicName;      // 待调试主题
    DDSDebugOutputFunc output;  // 用户自定义输出回调；空回调拒绝
};

// list_topics 请求负载：自适应收敛参数（0 值由节点层钳制为默认 5 次/200ms）
struct DDSDebugList
{
    uint32_t stableRounds;  // 连续不变快照次数阈值；1 即单次快照免等待
    uint32_t intervalMs;    // 快照轮询间隔毫秒
    bool types = false;     // 类型名模式：true 时每行输出 "主题名 [类型名]"
};

// topic_info 请求负载：单主题详情查询（独立收敛参数，语义同 list_topics，0 值由节点层钳制为默认）
struct DDSDebugInfo
{
    std::string topicName;  // 待查询主题
    uint32_t stableRounds;  // 连续不变快照次数阈值；1 即单次快照免等待
    uint32_t intervalMs;    // 快照轮询间隔毫秒
    bool verbose = false;   // 端点详情模式：true 时逐端点输出 Node name/GUID/QoS profile
};

// list_nodes 请求负载：域内命名参与者列表查询（独立收敛参数，语义同 topic_info，0 值由节点层钳制为默认）
struct DDSNodeList
{
    uint32_t stableRounds;  // 连续不变快照次数阈值；1 即单次快照免等待
    uint32_t intervalMs;    // 快照轮询间隔毫秒
};

// node_info 请求负载：指定节点名（participant_name）的发布/订阅主题清单查询（独立收敛参数，
// 语义同 list_nodes，0 值由节点层钳制为默认）
struct DDSNodeInfo
{
    std::string nodeName;   // 待查询节点名（participant_name 精确匹配）
    uint32_t stableRounds;  // 连续不变快照次数阈值；1 即单次快照免等待
    uint32_t intervalMs;    // 快照轮询间隔毫秒
};

// topic_find 请求负载：按数据类型名反查主题列表（独立收敛参数，语义同 list_topics，
// 0 值由节点层钳制为默认）
struct DDSDebugFind
{
    std::string typeName;   // 待查询数据类型名（精确匹配）
    uint32_t stableRounds;  // 连续不变快照次数阈值；1 即单次快照免等待
    uint32_t intervalMs;    // 快照轮询间隔毫秒
};

// interface_show 请求负载：按数据类型名输出 IDL 结构描述（独立收敛参数，语义同 topic_find，
// 0 值由节点层钳制为默认）
struct DDSDebugInterfaceShow
{
    std::string typeName;   // 待查询数据类型名（精确匹配）
    uint32_t stableRounds;  // 连续不变快照次数阈值；1 即单次快照免等待
    uint32_t intervalMs;    // 快照轮询间隔毫秒
};

// interface_list 请求负载：列出发现缓存中出现的全部数据类型名（去重排序，interface show
// 的配套导航；独立收敛参数，0 值由节点层钳制为默认）
struct DDSDebugInterfaceList
{
    uint32_t stableRounds;  // 连续不变快照次数阈值；1 即单次快照免等待
    uint32_t intervalMs;    // 快照轮询间隔毫秒
};

// topic_example 请求负载：按主题名查询发布示例三段行集（Type/IDL/example 命令；独立收敛
// 参数，0 值由节点层钳制为默认）
struct DDSDebugTopicExample
{
    std::string topicName;  // 待查询主题名（精确匹配）
    uint32_t stableRounds;  // 连续不变快照次数阈值；1 即单次快照免等待
    uint32_t intervalMs;    // 快照轮询间隔毫秒
};

// topic_msg 请求负载：按主题名查询消息描述两要素（供导出消息描述文件；StringArray 恰
// 2 行：d[0]=类型名、d[1]=紧凑 msg JSON；独立收敛参数，0 值由节点层钳制为默认）
struct DDSDebugTopicMsg
{
    std::string topicName;  // 待查询主题名（精确匹配）
    uint32_t stableRounds;  // 连续不变快照次数阈值；1 即单次快照免等待
    uint32_t intervalMs;    // 快照轮询间隔毫秒
};

// topic_pub 请求负载：按主题名发布 JSON 载荷消息（首轮完整强校验：节点层先发现收敛+
// 匹配收敛再发布，存在 RELIABLE 订阅者时以 wait_for_acknowledgments 确认送达，全
// BEST_EFFORT 或无订阅者退化尽力而为；收敛参数 0 值由节点层钳制为默认）。requiredSubscribers
// >0 为期望建匹配订阅端数门槛（CLI -w N）：matched 计数达到该值才继续发布，否则一直等待
// （节点层打印等待进度日志）直至达标或停止标志置位中断（不设超时）；=0 自动收敛。repeatIntervalMs
// >0 为持续发布模式：首轮校验成功后保留发布链按该周期重复 write（尽力而为：write 失败仅
// 计数不返回，不再等 ack），直到停止标志 g_debugPubStop 置位（CLI Ctrl+C 经 debugPubStop）
// 才退出并清理，发布条数统计经出参 total/failed 带回响应
struct DDSDebugTopicPub
{
    std::string topicName;  // 目标主题名（须已在域内被发现）
    std::string json;       // 发布载荷（JSON 格式，与 topic_example 输出模板同构）
    uint32_t stableRounds;  // 连续不变快照次数阈值；1 即单次快照免等待
    uint32_t intervalMs;    // 快照轮询间隔毫秒
    uint32_t repeatIntervalMs = 0;  // 持续发布间隔毫秒；0=仅发一次（缺省兼容旧 4 字段聚合初始化）
    uint32_t requiredSubscribers = 0;  // 期望建匹配订阅端数门槛；0=自动收敛（缺省兼容旧 5 字段聚合初始化）
};

// clang-format off
// YomkMsg 是 YomkServer 第三方宏，cppcheck 未 --library 配置识别（unknownMacro 属工具配置需求，非自有源码缺陷）
// cppcheck-suppress unknownMacro
YomkMsg(DDSDebugNode, DDSDebugNode, msg)
YomkMsg(DDSDebugTopic, DDSDebugTopic, msg)
YomkMsg(DDSDebugList, DDSDebugList, msg)
YomkMsg(DDSDebugInfo, DDSDebugInfo, msg)
YomkMsg(DDSNodeList, DDSNodeList, msg)
YomkMsg(DDSNodeInfo, DDSNodeInfo, msg)
YomkMsg(DDSDebugFind, DDSDebugFind, msg)
YomkMsg(DDSDebugInterfaceShow, DDSDebugInterfaceShow, msg)
YomkMsg(DDSDebugInterfaceList, DDSDebugInterfaceList, msg)
YomkMsg(DDSDebugTopicExample, DDSDebugTopicExample, msg)
YomkMsg(DDSDebugTopicMsg, DDSDebugTopicMsg, msg)
YomkMsg(DDSDebugTopicPub, DDSDebugTopicPub, msg)
