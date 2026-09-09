#pragma once
// YomkRpcService 对外头：定义 YOMKRPC_* 宏所打包的请求/响应结构（DDSNode/DDSTopic/...）、
// 订阅回调类型 DDSCallbackFunc，以及服务类 YomkRpcService 的声明。用户通常直接使用 YomkRpcAPI.h 中的宏。
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

// 订阅回调：收到消息时被调用，参数为交付的消息实例指针（const void*），static_cast 为自己的消息类型后读取。
// 指针仅回调期间有效，须同步消费、勿跨调用持有（return_loan 后失效）。
// 不同类型的安全消费方式见 README。
using DDSCallbackFunc = std::function<void(const void *)>;

// create_node 请求负载。
struct DDSNode
{
    uint32_t domainId;    // DDS 域号，合法范围 [0,232]
    std::string nodeName; // 节点唯一名，不可为空
};

// register_pub_topic 请求负载。
struct DDSTopic
{
    std::string nodeName;
    std::string topicName;
    void *type; // TopicDataType*；所有权移交服务端：无论成功或失败，调用后 caller 不再持有/释放
};

// register_sub_topic 请求负载。
struct DDSSubRequest
{
    std::string nodeName;
    std::string topicName;
    void *type;               // TopicDataType*；所有权移交服务端：无论成功或失败，调用后 caller 不再持有/释放
    DDSCallbackFunc callback; // 收到消息时回调
};

// publish 请求负载。
struct DDSPublish
{
    std::string nodeName;
    std::string topicName;
    void *data; // 借用（非所有权）：publish 同步写入，caller 保留并在调用后自行管理 data 生命周期
};

// loan / discard_loan 请求负载。
struct DDSLoan
{
    std::string nodeName;
    std::string topicName;
    void *sample; // loan 请求忽略；discard 请求携带待归还的借出指针
};

// loan 成功响应负载。
struct DDSLoanResult
{
    // /loan 借出的待发样本指针；填值后经 YOMKRPC_PUB_MSG 发布，write 后指针失效。
    void *sample;
};

// clang-format off
// YomkMsg 是 YomkServer 第三方宏，cppcheck 未 --library 配置识别（unknownMacro 属工具配置需求，非自有源码缺陷）
// cppcheck-suppress unknownMacro
YomkMsg(DDSNode, DDSNode, msg)
YomkMsg(DDSTopic, DDSTopic, msg)
YomkMsg(DDSSubRequest, DDSSubRequest, msg)
YomkMsg(DDSPublish, DDSPublish, msg)
YomkMsg(DDSLoan, DDSLoan, msg)
YomkMsg(DDSLoanResult, DDSLoanResult, msg)