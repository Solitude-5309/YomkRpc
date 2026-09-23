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
    (void)pkg;  // 列举端点无参数载荷（同 delete_node）

    std::lock_guard<std::mutex> lock(mtx_);
    if (node_ == nullptr)
    {
        YOMK_ERROR_TAG("YomkRpcDebugService::listTopics", "debug node not created");
        return YomkResponse(YomkResponse::eNo, "debug node not created");
    }
    // node_ 非空即已入域，listTopics 必返回 true；空列表（发现重放未完成/无 writer）为正常状态
    std::vector<std::pair<std::string, std::string>> pairs;
    node_->listTopics(pairs);
    std::vector<std::string> lines;
    lines.reserve(pairs.size());
    for (const auto& entry : pairs)
    {
        lines.emplace_back(entry.first + " [" + entry.second + "]");
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
