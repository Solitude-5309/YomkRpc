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

// 订阅回调：交付的 const void* 指向反序列化后的消息对象，始终按消息类型对齐，可安全 static_cast 后解引用
// （所有架构，含严格对齐 ARM）；指针仅回调期间有效，须同步消费、勿跨调用持有。
// 注：订阅端以对齐反序列化交付（方向2修复），不再使用零拷贝 reader loan。
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
    // /loan 借出的待发样本指针（FastDDS loan_sample 透传）。严格对齐架构告诫：该指针指向 CDR payload
    // body(base+4)，对 plain 且 alignof>4 的类型(MFloat64/MInt64)仅 4 字节对齐，直接类型化写入在严格
    // 对齐架构(ARM)上触发 SIGBUS(x86-64 良性)；跨严格对齐平台须按对齐安全方式写入。
    void *sample;
};

// clang-format off
YomkMsg(DDSNode, DDSNode, msg)
YomkMsg(DDSTopic, DDSTopic, msg)
YomkMsg(DDSSubRequest, DDSSubRequest, msg)
YomkMsg(DDSPublish, DDSPublish, msg)
YomkMsg(DDSLoan, DDSLoan, msg)
YomkMsg(DDSLoanResult, DDSLoanResult, msg)