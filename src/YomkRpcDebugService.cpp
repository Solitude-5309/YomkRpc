// YomkRpcDebugService 实现：将 /YomkRpcDebugService/* 端点的请求分发到对应 handler，委托给内部的
// FastDDSDebugNode（类型无关调试节点：发现 → 动态类型 → 自动订阅 → JSON 结构化输出）。
//
// 输出契约：消息内容零打印进服务层——/topic_print 携带用户回调，经 setOutputSink 原样转交节点，
// JSON 文本逐条投递给回调；仅服务自身状态错误走 YOMK_ERROR_TAG。
//
// 线程安全：node_ 的增删查改与节点入口调用统一以 mtx_ 串行化（createNode 内的 setDomainId 即
// create_participant，沿用 YomkRpcService::createNode 的既有结论：必须在锁内串行，见其注释）。
#include "YomkRpcDebugService.h"

#include "FastDDSDebugNode.h"

#include <utility>
#include <vector>

namespace
{
constexpr uint32_t kMaxDomainId = 232;  // DDS 域号上限
}  // namespace

YomkRpcDebugService::YomkRpcDebugService(YomkServer* server) : YomkService(server)
{
    name("/YomkRpcDebugService");
}

YomkRpcDebugService::~YomkRpcDebugService() = default;

int YomkRpcDebugService::init()
{
    YomkInstallFunc("/version", YomkRpcDebugService::getVersion);
    YomkInstallFunc("/create_node", YomkRpcDebugService::createNode);
    YomkInstallFunc("/topic_print", YomkRpcDebugService::topicPrint);
    YomkInstallFunc("/list_topics", YomkRpcDebugService::listTopics);
    YomkInstallFunc("/topic_info", YomkRpcDebugService::topicInfo);
    YomkInstallFunc("/topic_find", YomkRpcDebugService::topicFind);
    YomkInstallFunc("/list_nodes", YomkRpcDebugService::listNodes);
    YomkInstallFunc("/node_info", YomkRpcDebugService::nodeInfo);
    YomkInstallFunc("/delete_node", YomkRpcDebugService::deleteNode);
    return 0;
}

YomkResponse YomkRpcDebugService::getVersion(YomkPkgPtr pkg)
{
    std::string version = "YomkRpc v" YOMKRPC_VERSION_STRING;
    return YomkResponse(YomkResponse::eOk, "ok", YomkMkPtr(String, version));
}

YomkResponse YomkRpcDebugService::createNode(YomkPkgPtr pkg)
{
    YomkUnPackPkgResponse(pkg, DDSDebugNode, p);

    if (p->msg.domainId > kMaxDomainId)
    {
        YOMK_ERROR_TAG(
            "YomkRpcDebugService::createNode", "domainId [", p->msg.domainId, "] out of valid range [0,232]");
        return YomkResponse(
            YomkResponse::eNo, "domainId [" + std::to_string(p->msg.domainId) + "] out of valid range [0,232]");
    }

    std::lock_guard<std::mutex> lock(mtx_);
    if (node_ != nullptr)
    {
        YOMK_ERROR_TAG("YomkRpcDebugService::createNode", "debug node already exists, delete it first");
        return YomkResponse(YomkResponse::eNo, "debug node already exists, delete it first");
    }

    auto node = std::make_unique<FastDDSDebugNode>();
    if (!node->setDomainId(p->msg.domainId))
    {
        YOMK_ERROR_TAG("YomkRpcDebugService::createNode", "create debug node failed: setDomainId error");
        return YomkResponse(YomkResponse::eNo, "create debug node failed: setDomainId error");
    }
    node_ = std::move(node);
    return YomkResponse(YomkResponse::eOk, "ok");
}

YomkResponse YomkRpcDebugService::topicPrint(YomkPkgPtr pkg)
{
    YomkUnPackPkgResponse(pkg, DDSDebugTopic, p);

    std::lock_guard<std::mutex> lock(mtx_);
    if (node_ == nullptr)
    {
        YOMK_ERROR_TAG("YomkRpcDebugService::topicPrint", "debug node not created");
        return YomkResponse(YomkResponse::eNo, "debug node not created");
    }
    if (p->msg.output == nullptr)
    {
        YOMK_ERROR_TAG("YomkRpcDebugService::topicPrint", "output callback is empty");
        return YomkResponse(YomkResponse::eNo, "output callback is empty");
    }
    // 先 setOutputSink 后 subscribeTopic：sink 须在工作线程建立订阅前就位（FastDDSDebugNode 约定）。
    node_->setOutputSink(p->msg.output);
    if (!node_->subscribeTopic(p->msg.topicName))
    {
        YOMK_ERROR_TAG("YomkRpcDebugService::topicPrint", "subscribeTopic [", p->msg.topicName, "] failed");
        return YomkResponse(YomkResponse::eNo, "subscribeTopic [" + p->msg.topicName + "] failed");
    }
    return YomkResponse(YomkResponse::eOk, "ok");
}

YomkResponse YomkRpcDebugService::listTopics(YomkPkgPtr pkg)
{
    YomkUnPackPkgResponse(pkg, DDSDebugList, p);

    std::lock_guard<std::mutex> lock(mtx_);
    if (node_ == nullptr)
    {
        YOMK_ERROR_TAG("YomkRpcDebugService::listTopics", "debug node not created");
        return YomkResponse(YomkResponse::eNo, "debug node not created");
    }
    // node_ 非空即已入域，listTopics 必返回 true；持锁收敛等待（最长约 stableRounds*intervalMs）：
    // 锁保护 node_ 生命周期不被并发 deleteNode 破坏（同 createNode 锁内 setDomainId 的既有约定），
    // 等待期间本服务其他端点请求被串行化（单用户 CLI 场景无实际影响）；0 值参数由节点层钳制默认
    std::vector<std::pair<std::string, std::string>> pairs;
    node_->listTopics(pairs, p->msg.stableRounds, p->msg.intervalMs);
    // 非 types 模式仅主题名；types 模式每行 "主题名 [类型名]"（单空格 + 方括号，对齐
    // ros2 topic list -t 形态；类型名原样输出，pair 已按主题名排序）
    std::vector<std::string> lines;
    lines.reserve(pairs.size());
    for (const auto& entry : pairs)
    {
        lines.emplace_back(p->msg.types ? entry.first + " [" + entry.second + "]" : entry.first);
    }
    return YomkResponse(YomkResponse::eOk, "ok", YomkMkPtr(StringArray, lines));
}

YomkResponse YomkRpcDebugService::topicFind(YomkPkgPtr pkg)
{
    YomkUnPackPkgResponse(pkg, DDSDebugFind, p);

    std::lock_guard<std::mutex> lock(mtx_);
    if (node_ == nullptr)
    {
        YOMK_ERROR_TAG("YomkRpcDebugService::topicFind", "debug node not created");
        return YomkResponse(YomkResponse::eNo, "debug node not created");
    }
    // 持锁取发现缓存全集快照（listTopics 已按主题名排序），按类型名精确匹配过滤；
    // 无匹配按查询失败处理（info 族语义），便于调用方发现类型名拼写错误
    std::vector<std::pair<std::string, std::string>> pairs;
    node_->listTopics(pairs, p->msg.stableRounds, p->msg.intervalMs);
    std::vector<std::string> lines;
    for (const auto& entry : pairs)
    {
        if (entry.second == p->msg.typeName)
        {
            lines.emplace_back(entry.first);
        }
    }
    if (lines.empty())
    {
        YOMK_ERROR_TAG("YomkRpcDebugService::topicFind", "type [", p->msg.typeName, "] not found");
        return YomkResponse(YomkResponse::eNo, "type [" + p->msg.typeName + "] not found");
    }
    return YomkResponse(YomkResponse::eOk, "ok", YomkMkPtr(StringArray, lines));
}

YomkResponse YomkRpcDebugService::topicInfo(YomkPkgPtr pkg)
{
    YomkUnPackPkgResponse(pkg, DDSDebugInfo, p);

    std::lock_guard<std::mutex> lock(mtx_);
    if (node_ == nullptr)
    {
        YOMK_ERROR_TAG("YomkRpcDebugService::topicInfo", "debug node not created");
        return YomkResponse(YomkResponse::eNo, "debug node not created");
    }
    // node_ 非空即已入域；持锁对单个目标主题做独立收敛查询（最长约 stableRounds*intervalMs）：
    // 锁保护 node_ 生命周期不被并发 deleteNode 破坏（同 listTopics 既有约定）；0 值参数由节点层钳制；
    // verbose 模式同一次查询顺带取端点详情（避免二次收敛轮询）
    std::string typeName;
    size_t publisherCount = 0;
    size_t subscriptionCount = 0;
    std::vector<FastDDSDebugNode::EndpointDetail> pubDetails;
    std::vector<FastDDSDebugNode::EndpointDetail> subDetails;
    if (!node_->topicInfo(p->msg.topicName, typeName, publisherCount, subscriptionCount,
                p->msg.stableRounds, p->msg.intervalMs,
                p->msg.verbose ? &pubDetails : nullptr,
                p->msg.verbose ? &subDetails : nullptr))
    {
        YOMK_ERROR_TAG("YomkRpcDebugService::topicInfo", "topic [", p->msg.topicName, "] not found");
        return YomkResponse(YomkResponse::eNo, "topic [" + p->msg.topicName + "] not found");
    }
    // 非 verbose 三行：类型名原样输出（不做任何风格转换）+ 两端点计数；verbose 追加逐端点
    // 详情段（空行规则：Type 后一段、每端点块前一段；count 为 0 的端点类型无清单段）
    std::vector<std::string> lines;
    if (!p->msg.verbose)
    {
        lines.reserve(3);
        lines.emplace_back("Type: " + typeName);
        lines.emplace_back("Publisher count: " + std::to_string(publisherCount));
        lines.emplace_back("Subscription count: " + std::to_string(subscriptionCount));
        return YomkResponse(YomkResponse::eOk, "ok", YomkMkPtr(StringArray, lines));
    }
    lines.emplace_back("Type: " + typeName);
    lines.emplace_back("");
    lines.emplace_back("Publisher count: " + std::to_string(publisherCount));
    for (const auto& detail : pubDetails)
    {
        lines.emplace_back("");
        lines.emplace_back("Node name: " + detail.nodeName);
        lines.emplace_back("Endpoint type: PUBLISHER");
        lines.emplace_back("GUID: " + detail.guid);
        lines.emplace_back("QoS profile:");
        lines.insert(lines.end(), detail.qosLines.begin(), detail.qosLines.end());
    }
    lines.emplace_back("");
    lines.emplace_back("Subscription count: " + std::to_string(subscriptionCount));
    for (const auto& detail : subDetails)
    {
        lines.emplace_back("");
        lines.emplace_back("Node name: " + detail.nodeName);
        lines.emplace_back("Endpoint type: SUBSCRIPTION");
        lines.emplace_back("GUID: " + detail.guid);
        lines.emplace_back("QoS profile:");
        lines.insert(lines.end(), detail.qosLines.begin(), detail.qosLines.end());
    }
    return YomkResponse(YomkResponse::eOk, "ok", YomkMkPtr(StringArray, lines));
}

YomkResponse YomkRpcDebugService::listNodes(YomkPkgPtr pkg)
{
    YomkUnPackPkgResponse(pkg, DDSNodeList, p);

    std::lock_guard<std::mutex> lock(mtx_);
    if (node_ == nullptr)
    {
        YOMK_ERROR_TAG("YomkRpcDebugService::listNodes", "debug node not created");
        return YomkResponse(YomkResponse::eNo, "debug node not created");
    }
    // node_ 非空即已入域，listNodes 必返回 true；持锁收敛等待（最长约 stableRounds*intervalMs）：
    // 锁保护 node_ 生命周期不被并发 deleteNode 破坏（同 listTopics 既有约定）；0 值参数由节点层
    // 钳制为默认。无 "not found" 分支——空列表合法（域内暂无命名参与者），语义同 list_topics 空域
    std::vector<std::string> names;
    node_->nodeList(names, p->msg.stableRounds, p->msg.intervalMs);
    return YomkResponse(YomkResponse::eOk, "ok", YomkMkPtr(StringArray, names));
}

YomkResponse YomkRpcDebugService::nodeInfo(YomkPkgPtr pkg)
{
    YomkUnPackPkgResponse(pkg, DDSNodeInfo, p);

    std::lock_guard<std::mutex> lock(mtx_);
    if (node_ == nullptr)
    {
        YOMK_ERROR_TAG("YomkRpcDebugService::nodeInfo", "debug node not created");
        return YomkResponse(YomkResponse::eNo, "debug node not created");
    }
    // node_ 非空即已入域；持锁对单个目标节点名做独立收敛查询（最长约 stableRounds*intervalMs）：
    // 锁保护 node_ 生命周期不被并发 deleteNode 破坏（同 listNodes 既有约定）；0 值参数由节点层钳制
    std::vector<std::pair<std::string, std::string>> publishers;
    std::vector<std::pair<std::string, std::string>> subscribers;
    if (!node_->nodeInfo(p->msg.nodeName, publishers, subscribers, p->msg.stableRounds, p->msg.intervalMs))
    {
        YOMK_ERROR_TAG("YomkRpcDebugService::nodeInfo", "node [", p->msg.nodeName, "] not found");
        return YomkResponse(YomkResponse::eNo, "node [" + p->msg.nodeName + "] not found");
    }
    // 输出行（对齐 ros2 node info 形态）：节点名行 + Subscribers 段 + Publishers 段，每条主题
    // 一行 "    <topic>: <type>"（类型名原样输出，无任何风格转换）；空段仅打段头
    std::vector<std::string> lines;
    lines.reserve(3 + publishers.size() + subscribers.size());
    lines.emplace_back(p->msg.nodeName);
    lines.emplace_back("  Subscribers:");
    for (const auto& entry : subscribers)
    {
        lines.emplace_back("    " + entry.first + ": " + entry.second);
    }
    lines.emplace_back("  Publishers:");
    for (const auto& entry : publishers)
    {
        lines.emplace_back("    " + entry.first + ": " + entry.second);
    }
    return YomkResponse(YomkResponse::eOk, "ok", YomkMkPtr(StringArray, lines));
}

YomkResponse YomkRpcDebugService::deleteNode(YomkPkgPtr pkg)
{
    (void)pkg;  // 单 debug 节点模型无参数载荷

    std::lock_guard<std::mutex> lock(mtx_);
    if (node_ == nullptr)
    {
        YOMK_ERROR_TAG("YomkRpcDebugService::deleteNode", "debug node not created");
        return YomkResponse(YomkResponse::eNo, "debug node not created");
    }
    node_.reset();  // 节点析构即按其内部顺序（停工作线程 → reader → topic → subscriber → participant）清理
    return YomkResponse(YomkResponse::eOk, "ok");
}
