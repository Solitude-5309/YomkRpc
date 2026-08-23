#include "YomkRpcService.h"
#include "FastDDSNode.h"

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
    std::string version = "YomkRpc v" YOMKRPC_VERSION " (WIP)";
    return YomkResponse(YomkResponse::eOk, "ok", YomkMkPtr(String, version));
}

YomkResponse YomkRpcService::createNode(YomkPkgPtr pkg)
{
    YomkUnPackPkgResponse(pkg, DDSNode, p);

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
    return YomkResponse(YomkResponse::eOk, "ok", YomkMkPtr(DDSLoanResult, DDSLoanResult{sample}));
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
