#pragma once
// YomkRpcDebugService 对外头：定义 YOMKRPC_DEBUG_* 宏（见 YomkRpcAPI.h）所打包的请求结构
// （DDSDebugNode/DDSDebugTopic）、调试输出回调类型 DDSDebugOutputFunc，以及服务类
// YomkRpcDebugService 的声明。调试能力由内部的 FastDDSDebugNode 提供（类型无关：经 DDS 发现
// 机制取回远端 TypeObject 动态建订阅，消息以 JSON 文本投递给用户回调，零消息类型依赖）。
#include <YomkServer/YomkAPI.h>

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
    YomkResponse listNodes(YomkPkgPtr pkg);
    YomkResponse nodeInfo(YomkPkgPtr pkg);
    YomkResponse deleteNode(YomkPkgPtr pkg);

private:
    // 单 debug 节点：一个进程至多一个实例，重复 create 须先 delete；服务析构时自动销毁节点
    // 并按节点内部顺序清理全部 DDS 实体（推荐进程退出前经 /delete_node 显式清理）。
    std::unique_ptr<FastDDSDebugNode> node_;
    std::mutex mtx_;  // 串行化 node_ 的增删查改与节点入口调用
};

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
};

// topic_info 请求负载：单主题详情查询（独立收敛参数，语义同 list_topics，0 值由节点层钳制为默认）
struct DDSDebugInfo
{
    std::string topicName;  // 待查询主题
    uint32_t stableRounds;  // 连续不变快照次数阈值；1 即单次快照免等待
    uint32_t intervalMs;    // 快照轮询间隔毫秒
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

// clang-format off
// YomkMsg 是 YomkServer 第三方宏，cppcheck 未 --library 配置识别（unknownMacro 属工具配置需求，非自有源码缺陷）
// cppcheck-suppress unknownMacro
YomkMsg(DDSDebugNode, DDSDebugNode, msg)
YomkMsg(DDSDebugTopic, DDSDebugTopic, msg)
YomkMsg(DDSDebugList, DDSDebugList, msg)
YomkMsg(DDSDebugInfo, DDSDebugInfo, msg)
YomkMsg(DDSNodeList, DDSNodeList, msg)
YomkMsg(DDSNodeInfo, DDSNodeInfo, msg)
