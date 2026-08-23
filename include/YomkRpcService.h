#pragma once
#include <YomkServer/YomkAPI.h>

#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <string>
class FastDDSNode;
using namespace yomk;

class YomkRpcService : public YomkService
{
public:
    YomkRpcService(YomkServer *server);
    virtual ~YomkRpcService();
    virtual int init() override;

private:
    YomkResponse getVersion(YomkPkgPtr pkg);
    YomkResponse createNode(YomkPkgPtr pkg);
    YomkResponse deleteNode(YomkPkgPtr pkg);
    YomkResponse registerPubTopic(YomkPkgPtr pkg);
    YomkResponse registerSubTopic(YomkPkgPtr pkg);
    YomkResponse publish(YomkPkgPtr pkg);
    YomkResponse loan(YomkPkgPtr pkg);
    YomkResponse discardLoan(YomkPkgPtr pkg);

private:
    std::map<std::string, std::unique_ptr<FastDDSNode>> nodes_;
    std::mutex mtx_;
};

using DDSCallbackFunc = std::function<void(const void *)>;

struct DDSNode
{
    uint32_t domainId;
    std::string nodeName;
};

struct DDSTopic
{
    std::string nodeName;
    std::string topicName;
    void *type;
};

struct DDSSubRequest
{
    std::string nodeName;
    std::string topicName;
    void *type;
    DDSCallbackFunc callback;
};

struct DDSPublish
{
    std::string nodeName;
    std::string topicName;
    void *data;
};

struct DDSLoan
{
    std::string nodeName;
    std::string topicName;
    void *sample; // loan 请求忽略；discard 请求携带待归还的借出指针
};

struct DDSLoanResult
{
    void *sample;
};

// clang-format off
YomkMsg(DDSNode, DDSNode, msg)
YomkMsg(DDSTopic, DDSTopic, msg)
YomkMsg(DDSSubRequest, DDSSubRequest, msg)
YomkMsg(DDSPublish, DDSPublish, msg)
YomkMsg(DDSLoan, DDSLoan, msg)
YomkMsg(DDSLoanResult, DDSLoanResult, msg)