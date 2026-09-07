#include "YomkRpcService.h"
#include "FastDDSNode.h"

namespace
{
    constexpr uint32_t kMaxDomainId = 232;
    struct LoanGuard
    {
        FastDDSNode *node = nullptr;
        const std::string *topicName = nullptr;
        void *sample = nullptr;
        ~LoanGuard()
        {
            if (node != nullptr && sample != nullptr && topicName != nullptr)
            {
                node->discardLoan(*topicName, sample);
            }
        }
    };
} // namespace

YomkRpcService::YomkRpcService(YomkServer *server)
    : YomkService(server)
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
        return YomkResponse(YomkResponse::eNo,
                            "domainId [" + std::to_string(p->msg.domainId) + "] out of valid range [0,232]");
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
        return YomkResponse(YomkResponse::eNo,
                            "create node [" + p->msg.nodeName + "] failed: setDomainId error");
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
        // register_pub_topic 无条件接管 type；节点不存在时未委托到 FastDDSNode，
        // 须在此释放 caller 的 type（与节点层守卫释放对称），避免泄漏。delete nullptr 安全。
        delete static_cast<eprosima::fastdds::dds::TopicDataType *>(p->msg.type);
        return YomkResponse(YomkResponse::eNo, "node [" + p->msg.nodeName + "] not exists");
    }
    if (!it->second->registerPubTopic(p->msg.topicName, p->msg.type))
    {
        YOMK_ERROR_TAG("YomkRpcService::registerPubTopic", "registerPubTopic [", p->msg.topicName, "] failed on node [", p->msg.nodeName, "]");
        return YomkResponse(YomkResponse::eNo,
                            "registerPubTopic [" + p->msg.topicName + "] failed on node [" + p->msg.nodeName + "]");
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
        // register_sub_topic 无条件接管 type；节点不存在时未委托到 FastDDSNode，
        // 须在此释放 caller 的 type，避免泄漏。delete nullptr 安全。
        delete static_cast<eprosima::fastdds::dds::TopicDataType *>(p->msg.type);
        return YomkResponse(YomkResponse::eNo, "node [" + p->msg.nodeName + "] not exists");
    }
    if (!it->second->registerSubTopic(p->msg.topicName, p->msg.type, p->msg.callback))
    {
        YOMK_ERROR_TAG("YomkRpcService::registerSubTopic", "registerSubTopic [", p->msg.topicName, "] failed on node [", p->msg.nodeName, "]");
        return YomkResponse(YomkResponse::eNo,
                            "registerSubTopic [" + p->msg.topicName + "] failed on node [" + p->msg.nodeName + "]");
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
        YOMK_ERROR_TAG("YomkRpcService::publish", "publish [", p->msg.topicName, "] failed on node [", p->msg.nodeName, "]");
        return YomkResponse(YomkResponse::eNo,
                            "publish [" + p->msg.topicName + "] failed on node [" + p->msg.nodeName + "]");
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
    void *sample = nullptr;
    if (!it->second->loan(p->msg.topicName, sample) || sample == nullptr)
    {
        YOMK_ERROR_TAG("YomkRpcService::loan", "loan [", p->msg.topicName, "] failed on node [", p->msg.nodeName, "] (非 plain 类型或池耗尽，回退普通发布)");
        return YomkResponse(YomkResponse::eNo,
                            "loan [" + p->msg.topicName + "] failed on node [" + p->msg.nodeName + "]");
    }
    // 异常安全：sample 已从 writer 池借出，响应包构造若抛 bad_alloc 须归还池（否则借出样本
    // 滞留、反复 OOM 致池耗尽）。守卫成功构造响应后解除（所有权路径转由 caller 经 write/discard 管理）。
    LoanGuard loanGuard{it->second.get(), &p->msg.topicName, sample};
    YomkResponse resp(YomkResponse::eOk, "ok", YomkMkPtr(DDSLoanResult, DDSLoanResult{sample}));
    loanGuard.sample = nullptr; // 响应已持有 sample，解除守卫
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
        YOMK_ERROR_TAG("YomkRpcService::discardLoan", "discardLoan [", p->msg.topicName, "] failed on node [", p->msg.nodeName, "]");
        return YomkResponse(YomkResponse::eNo,
                            "discardLoan [" + p->msg.topicName + "] failed on node [" + p->msg.nodeName + "]");
    }
    return YomkResponse(YomkResponse::eOk, "ok");
}
