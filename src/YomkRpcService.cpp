// YomkRpcService 实现：将 /YomkRpcService/* 端点的请求分发到对应 handler，再委托给 FastDDSNode 执行 DDS 操作。
//
// 内存所有权协议（贯穿全部 handler）：
//   - type（DDSTopic/DDSSubRequest.type）：caller new，服务端全路径接管——无论成功或失败均由本服务 delete，caller 不再释放；
//   - data（DDSPublish.data）：借用（非所有权），publish 同步写入后 caller 自行管理其生命周期；
//   - sample（DDSLoan/DDSLoanResult.sample）：loan 从 writer 池借出，所有权经 write（发布）或 discard（归还）交回中间件。
//
// 线程安全：nodes_ 的增删查改统一以 mtx_ 串行化（见各 handler 的 lock_guard）；create_node 的并发约束详见其内部注释。
#include "YomkRpcService.h"

#include "FastDDSNode.h"

namespace
{
constexpr uint32_t kMaxDomainId = 232;  // DDS 域号上限
// loan 响应构造期间的异常安全守卫：构造抛异常时栈展开归还池；成功后由 caller 解除（见 loan()）。
struct LoanGuard
{
    FastDDSNode* node = nullptr;
    const std::string* topicName = nullptr;
    void* sample = nullptr;
    ~LoanGuard()
    {
        if (node != nullptr && sample != nullptr && topicName != nullptr)
        {
            node->discardLoan(*topicName, sample);
        }
    }
};
}  // namespace

YomkRpcService::YomkRpcService(YomkServer* server) : YomkService(server)
{
    name("/YomkRpcService");
}

YomkRpcService::~YomkRpcService() = default;

int YomkRpcService::init()
{
    YomkInstallFunc("/version", YomkRpcService::getVersion);
    YomkInstallFunc("/create_node", YomkRpcService::createNode);
    YomkInstallFunc("/delete_node", YomkRpcService::deleteNode);
    YomkInstallFunc("/register_pub_topic", YomkRpcService::registerPubTopic);
    YomkInstallFunc("/register_sub_topic", YomkRpcService::registerSubTopic);
    YomkInstallFunc("/publish", YomkRpcService::publish);
    YomkInstallFunc("/loan", YomkRpcService::loan);
    YomkInstallFunc("/discard_loan", YomkRpcService::discardLoan);
    return 0;
}

YomkResponse YomkRpcService::getVersion(YomkPkgPtr pkg)
{
    std::string version = "YomkRpc v" YOMKRPC_VERSION_STRING;
    return YomkResponse(YomkResponse::eOk, "ok", YomkMkPtr(String, version));
}

YomkResponse YomkRpcService::createNode(YomkPkgPtr pkg)
{
    YomkUnPackPkgResponse(pkg, DDSNode, p);

    if (p->msg.nodeName.empty())
    {
        YOMK_ERROR_TAG("YomkRpcService::createNode", "node name is empty");
        return YomkResponse(YomkResponse::eNo, "node name is empty");
    }
    if (p->msg.domainId > kMaxDomainId)
    {
        YOMK_ERROR_TAG("YomkRpcService::createNode", "domainId [", p->msg.domainId, "] out of valid range [0,232]");
        return YomkResponse(
            YomkResponse::eNo, "domainId [" + std::to_string(p->msg.domainId) + "] out of valid range [0,232]");
    }

    // create_participant（经 setDomainId）必须在全局 mtx_ 内串行，不可“锁外构造”优化：
    // FastDDS 3.6.1 的 DomainParticipantFactory 单例 lazy-init(get_shared_instance/load_profiles) 与
    // SystemInfo::get_username()(→ 非线程安全 getpwuid，写 libc 静态缓冲) 在并发 create_participant 下
    // data race。tsan 实证：移出锁后 A1 3 race / stress 2 race（create-vs-create），锁内串行则 0 race
    // （git stash 基线对照 6×0）。并发安全优先于该罕见路径（create_node 多在启动期）的微优化。
    std::lock_guard<std::mutex> lock(mtx_);
    if (nodes_.find(p->msg.nodeName) != nodes_.end())
    {
        YOMK_ERROR_TAG("YomkRpcService::createNode", "node [", p->msg.nodeName, "] already exists");
        return YomkResponse(YomkResponse::eNo, "node [" + p->msg.nodeName + "] already exists");
    }

    auto node = std::make_unique<FastDDSNode>();
    if (!node->setDomainId(p->msg.domainId))
    {
        YOMK_ERROR_TAG("YomkRpcService::createNode", "create node [", p->msg.nodeName, "] failed: setDomainId error");
        return YomkResponse(YomkResponse::eNo, "create node [" + p->msg.nodeName + "] failed: setDomainId error");
    }
    nodes_[p->msg.nodeName] = std::move(node);
    return YomkResponse(YomkResponse::eOk, "ok");
}

YomkResponse YomkRpcService::deleteNode(YomkPkgPtr pkg)
{
    YomkUnPackPkgResponse(pkg, String, p);

    std::lock_guard<std::mutex> lock(mtx_);
    auto it = nodes_.find(p->d);
    if (it == nodes_.end())
    {
        YOMK_ERROR_TAG("YomkRpcService::deleteNode", "node [", p->d, "] not exists");
        return YomkResponse(YomkResponse::eNo, "node [" + p->d + "] not exists");
    }
    nodes_.erase(it);
    return YomkResponse(YomkResponse::eOk, "ok");
}

YomkResponse YomkRpcService::registerPubTopic(YomkPkgPtr pkg)
{
    YomkUnPackPkgResponse(pkg, DDSTopic, p);

    std::lock_guard<std::mutex> lock(mtx_);
    auto it = nodes_.find(p->msg.nodeName);
    if (it == nodes_.end())
    {
        YOMK_ERROR_TAG("YomkRpcService::registerPubTopic", "node [", p->msg.nodeName, "] not exists");
        // type 所有权无条件接管：节点不存在未委托到节点层，须在此释放，避免泄漏。
        delete static_cast<eprosima::fastdds::dds::TopicDataType*>(p->msg.type);
        return YomkResponse(YomkResponse::eNo, "node [" + p->msg.nodeName + "] not exists");
    }
    if (!it->second->registerPubTopic(p->msg.topicName, p->msg.type))
    {
        YOMK_ERROR_TAG(
            "YomkRpcService::registerPubTopic",
            "registerPubTopic [",
            p->msg.topicName,
            "] failed on node [",
            p->msg.nodeName,
            "]");
        return YomkResponse(
            YomkResponse::eNo, "registerPubTopic [" + p->msg.topicName + "] failed on node [" + p->msg.nodeName + "]");
    }
    return YomkResponse(YomkResponse::eOk, "ok");
}

YomkResponse YomkRpcService::registerSubTopic(YomkPkgPtr pkg)
{
    YomkUnPackPkgResponse(pkg, DDSSubRequest, p);

    std::lock_guard<std::mutex> lock(mtx_);
    auto it = nodes_.find(p->msg.nodeName);
    if (it == nodes_.end())
    {
        YOMK_ERROR_TAG("YomkRpcService::registerSubTopic", "node [", p->msg.nodeName, "] not exists");
        // type 所有权无条件接管：节点不存在未委托到节点层，须在此释放，避免泄漏。
        delete static_cast<eprosima::fastdds::dds::TopicDataType*>(p->msg.type);
        return YomkResponse(YomkResponse::eNo, "node [" + p->msg.nodeName + "] not exists");
    }
    if (!it->second->registerSubTopic(p->msg.topicName, p->msg.type, p->msg.callback))
    {
        YOMK_ERROR_TAG(
            "YomkRpcService::registerSubTopic",
            "registerSubTopic [",
            p->msg.topicName,
            "] failed on node [",
            p->msg.nodeName,
            "]");
        return YomkResponse(
            YomkResponse::eNo, "registerSubTopic [" + p->msg.topicName + "] failed on node [" + p->msg.nodeName + "]");
    }
    return YomkResponse(YomkResponse::eOk, "ok");
}

YomkResponse YomkRpcService::publish(YomkPkgPtr pkg)
{
    YomkUnPackPkgResponse(pkg, DDSPublish, p);

    std::lock_guard<std::mutex> lock(mtx_);
    auto it = nodes_.find(p->msg.nodeName);
    if (it == nodes_.end())
    {
        YOMK_ERROR_TAG("YomkRpcService::publish", "node [", p->msg.nodeName, "] not exists");
        return YomkResponse(YomkResponse::eNo, "node [" + p->msg.nodeName + "] not exists");
    }
    if (!it->second->publish(p->msg.topicName, p->msg.data))
    {
        YOMK_ERROR_TAG(
            "YomkRpcService::publish", "publish [", p->msg.topicName, "] failed on node [", p->msg.nodeName, "]");
        return YomkResponse(
            YomkResponse::eNo, "publish [" + p->msg.topicName + "] failed on node [" + p->msg.nodeName + "]");
    }
    return YomkResponse(YomkResponse::eOk, "ok");
}

YomkResponse YomkRpcService::loan(YomkPkgPtr pkg)
{
    YomkUnPackPkgResponse(pkg, DDSLoan, p);

    std::lock_guard<std::mutex> lock(mtx_);
    auto it = nodes_.find(p->msg.nodeName);
    if (it == nodes_.end())
    {
        YOMK_ERROR_TAG("YomkRpcService::loan", "node [", p->msg.nodeName, "] not exists");
        return YomkResponse(YomkResponse::eNo, "node [" + p->msg.nodeName + "] not exists");
    }
    void* sample = nullptr;
    if (!it->second->loan(p->msg.topicName, sample) || sample == nullptr)
    {
        YOMK_ERROR_TAG(
            "YomkRpcService::loan",
            "loan [",
            p->msg.topicName,
            "] failed on node [",
            p->msg.nodeName,
            "] (非 plain 类型或池耗尽，回退普通发布)");
        return YomkResponse(
            YomkResponse::eNo, "loan [" + p->msg.topicName + "] failed on node [" + p->msg.nodeName + "]");
    }
    // 异常安全：响应包构造若抛 bad_alloc，守卫在栈展开时归还池，避免借出样本滞留致池耗尽。
    LoanGuard loanGuard{it->second.get(), &p->msg.topicName, sample};
    YomkResponse resp(YomkResponse::eOk, "ok", YomkMkPtr(DDSLoanResult, DDSLoanResult{sample}));
    loanGuard.sample = nullptr;  // 响应已持有 sample，解除守卫
    return resp;
}

YomkResponse YomkRpcService::discardLoan(YomkPkgPtr pkg)
{
    YomkUnPackPkgResponse(pkg, DDSLoan, p);

    std::lock_guard<std::mutex> lock(mtx_);
    auto it = nodes_.find(p->msg.nodeName);
    if (it == nodes_.end())
    {
        YOMK_ERROR_TAG("YomkRpcService::discardLoan", "node [", p->msg.nodeName, "] not exists");
        return YomkResponse(YomkResponse::eNo, "node [" + p->msg.nodeName + "] not exists");
    }
    if (!it->second->discardLoan(p->msg.topicName, p->msg.sample))
    {
        YOMK_ERROR_TAG(
            "YomkRpcService::discardLoan",
            "discardLoan [",
            p->msg.topicName,
            "] failed on node [",
            p->msg.nodeName,
            "]");
        return YomkResponse(
            YomkResponse::eNo, "discardLoan [" + p->msg.topicName + "] failed on node [" + p->msg.nodeName + "]");
    }
    return YomkResponse(YomkResponse::eOk, "ok");
}
