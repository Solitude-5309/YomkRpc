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

// 订阅回调：交付的 const void* 指针，由通用 take() 机制交付；交付方式随消息“可借出性”自动二选一——
//
//   1) 非 plain 类型 / 未启用 data-sharing：FastDDS 内部反序列化进对齐实例（create_data 分配，按 alignof 对齐）→
//      交付指针恒对齐，可直接 static_cast 后解引用（所有架构，含严格对齐 ARM）。string / sequence / 无界类型属此列。
//   2) plain 且 data-sharing 生效：零拷贝借出，交付指针指向接收缓冲 CDR body = base+4（4=RTPS 头部长度，仅 4 字节对齐）→
//      须按消息 alignof 分两种情形消费：
//        ✓ alignof≤4 的 plain 类型 —— 可安全直接 static_cast 后解引用（base+4 天然对齐，全架构安全）：
//          bool / byte(octet) / char / int8 / uint8 / int16 / uint16 / int32 / uint32 / float，
//          及其定长数组（如 float pts[N]（点云）、octet pixels[N]（图像））—— 这是零拷贝大数组负载的推荐表示。
//        ✗ alignof>4 的 plain 标量 —— double / int64 / uint64（如 MFloat64/MInt64）：借出指针仅 4 字节对齐，
//          须 memcpy 到对齐局部再读（与发布侧 YOMKRPC_LOAN 同一契约）；x86-64 良性，严格对齐 ARM 直接解引用会 SIGBUS。
//
//   类型选型建议：大数据 / 点云 / 图像负载优先选用 alignof≤4 的 plain 类型（float / byte / int32），从源头规避对齐边界，
//   勿用定长 double / int64 标量作为借出负载。
//
// 生命周期：交付指针仅回调期间有效，须同步消费、勿跨调用持有（return_loan 后失效）。
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
// YomkMsg 是 YomkServer 第三方宏，cppcheck 未 --library 配置识别（unknownMacro 属工具配置需求，非自有源码缺陷）
// cppcheck-suppress unknownMacro
YomkMsg(DDSNode, DDSNode, msg)
YomkMsg(DDSTopic, DDSTopic, msg)
YomkMsg(DDSSubRequest, DDSSubRequest, msg)
YomkMsg(DDSPublish, DDSPublish, msg)
YomkMsg(DDSLoan, DDSLoan, msg)
YomkMsg(DDSLoanResult, DDSLoanResult, msg)