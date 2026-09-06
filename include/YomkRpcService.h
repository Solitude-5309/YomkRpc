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
    void *type; // TopicDataType*；所有权移交服务端：无论成功或失败，调用后 caller 不再持有/释放
};

struct DDSSubRequest
{
    std::string nodeName;
    std::string topicName;
    void *type; // TopicDataType*；所有权移交服务端：无论成功或失败，调用后 caller 不再持有/释放
    DDSCallbackFunc callback;
};

struct DDSPublish
{
    std::string nodeName;
    std::string topicName;
    void *data; // 借用（非所有权）：publish 同步写入，caller 保留并在调用后自行管理 data 生命周期
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