// YomkRpcBagService 实现：将 /YomkRpcBagService/* 端点的请求分发到对应 handler，委托给内部的
// FastDDSBagNode（类型无关录制节点：发现校验 → 透传订阅 → mcap 直写 → Ctrl+C 收尾）。
//
// 输出契约：/bag_record 成功时返回 StringArray（首行 bag 目录名，其后每主题一行
// "topic: N 条 / M 字节"统计）；仅服务自身状态错误与录制校验失败走 eNo（m_msg 携带原因）。
//
// 线程安全：node_ 的增删查改与节点入口调用统一以 mtx_ 串行化（createNode 内的 setDomainId 即
// create_participant，沿用 YomkRpcService::createNode 的既有结论：必须在锁内串行）；
// /bag_record 长驻阻塞期间持锁——锁保护 node_ 生命周期不被并发 deleteNode 破坏（同
// YomkRpcDebugService::listTopics 持锁收敛等待的既有约定），停止经 yomk::bagRecordStop
// 由 SIGINT 处理函数异步置位，不依赖本服务其他端点。
#include "YomkRpcBagService.h"

#include "FastDDSBagNode.h"

#include <utility>

namespace
{
constexpr uint32_t kMaxDomainId = 232;  // DDS 域号上限
}  // namespace

YomkRpcBagService::YomkRpcBagService(YomkServer* server) : YomkService(server)
{
    name("/YomkRpcBagService");
}

YomkRpcBagService::~YomkRpcBagService() = default;

int YomkRpcBagService::init()
{
    YomkInstallFunc("/version", YomkRpcBagService::getVersion);
    YomkInstallFunc("/create_node", YomkRpcBagService::createNode);
    YomkInstallFunc("/bag_record", YomkRpcBagService::bagRecord);
    YomkInstallFunc("/delete_node", YomkRpcBagService::deleteNode);
    return 0;
}

YomkResponse YomkRpcBagService::getVersion(YomkPkgPtr pkg)
{
    (void)pkg;  // 无参数载荷
    std::string version = "YomkRpc v" YOMKRPC_VERSION_STRING;
    return YomkResponse(YomkResponse::eOk, "ok", YomkMkPtr(String, version));
}

YomkResponse YomkRpcBagService::createNode(YomkPkgPtr pkg)
{
    YomkUnPackPkgResponse(pkg, DDSBagNode, p);

    if (p->msg.domainId > kMaxDomainId)
    {
        YOMK_ERROR_TAG(
            "YomkRpcBagService::createNode", "domainId [", p->msg.domainId, "] out of valid range [0,232]");
        return YomkResponse(
            YomkResponse::eNo, "domainId [" + std::to_string(p->msg.domainId) + "] out of valid range [0,232]");
    }

    std::lock_guard<std::mutex> lock(mtx_);
    if (node_ != nullptr)
    {
        YOMK_ERROR_TAG("YomkRpcBagService::createNode", "bag node already exists, delete it first");
        return YomkResponse(YomkResponse::eNo, "bag node already exists, delete it first");
    }

    auto node = std::make_unique<FastDDSBagNode>();
    if (!node->setDomainId(p->msg.domainId))
    {
        YOMK_ERROR_TAG("YomkRpcBagService::createNode", "create bag node failed: setDomainId error");
        return YomkResponse(YomkResponse::eNo, "create bag node failed: setDomainId error");
    }
    node_ = std::move(node);
    return YomkResponse(YomkResponse::eOk, "ok");
}

YomkResponse YomkRpcBagService::bagRecord(YomkPkgPtr pkg)
{
    YomkUnPackPkgResponse(pkg, DDSBagRecord, p);

    // topics 校验锁外前置（对齐 createNode 的 domainId 锁外校验先例：纯输入校验不触 DDS，
    // 也未建节点时可先于节点检查报出明确的输入问题）
    if (p->msg.topics.empty())
    {
        YOMK_ERROR_TAG("YomkRpcBagService::bagRecord", "no topics given");
        return YomkResponse(YomkResponse::eNo, "no topics given");
    }
    for (const auto& topic : p->msg.topics)
    {
        if (topic.empty())
        {
            YOMK_ERROR_TAG("YomkRpcBagService::bagRecord", "empty topic name in topic list");
            return YomkResponse(YomkResponse::eNo, "empty topic name in topic list");
        }
    }

    std::lock_guard<std::mutex> lock(mtx_);
    if (node_ == nullptr)
    {
        YOMK_ERROR_TAG("YomkRpcBagService::bagRecord", "bag node not created");
        return YomkResponse(YomkResponse::eNo, "bag node not created");
    }

    // 兜底复位残留停止标志（上次会话 Ctrl+C 残留置位会导致录制秒退，对齐 debugPubReset 约定）；
    // 只复位 stop 不动 paused——暂停标志是调用方启动意图（CLI --start-paused / 库用户
    // bagRecordPause 置位后发起录制），handler 清除会使暂停态静默失效
    yomk::g_bagRecordStop.store(false);

    // 长驻阻塞：录制至 SIGINT（yomk::g_bagRecordStop 置位）触发节点层收尾后返回；
    // 启动校验失败（无端点主题）快速返回 eNo，m_msg 逐主题列出输入有误项
    std::string error;
    std::vector<FastDDSBagNode::BagTopicStat> stats;
    if (!node_->record(p->msg.topics, stats, &error, 0, 0, p->msg.outputDir, p->msg.maxBagSize,
                       p->msg.maxBagDurationSec, p->msg.maxCacheSize))
    {
        if (error.empty())
        {
            error = "bag record failed";  // 兜底，避免空 m_msg
        }
        YOMK_ERROR_TAG("YomkRpcBagService::bagRecord", "record failed: ", error);
        return YomkResponse(YomkResponse::eNo, error);
    }

    // 成功回执：首行 bag 目录名，其后每主题一行 "topic: N 条 / M 字节"（对齐 list_topics
    // 的 StringArray 返回形态）
    std::vector<std::string> lines;
    lines.reserve(stats.size() + 1);
    lines.emplace_back(node_->bagDir());
    for (const auto& stat : stats)
    {
        lines.emplace_back(stat.topic + ": " + std::to_string(stat.count) + " 条 / " +
                           std::to_string(stat.bytes) + " 字节");
    }
    return YomkResponse(YomkResponse::eOk, "ok", YomkMkPtr(StringArray, lines));
}

YomkResponse YomkRpcBagService::deleteNode(YomkPkgPtr pkg)
{
    (void)pkg;  // 单 bag 节点模型无参数载荷

    std::lock_guard<std::mutex> lock(mtx_);
    if (node_ == nullptr)
    {
        YOMK_ERROR_TAG("YomkRpcBagService::deleteNode", "bag node not created");
        return YomkResponse(YomkResponse::eNo, "bag node not created");
    }
    node_.reset();  // 节点析构即按其内部顺序（reader → data → topic → subscriber → participant）清理
    return YomkResponse(YomkResponse::eOk, "ok");
}
