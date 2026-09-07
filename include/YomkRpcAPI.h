#pragma once
#include <YomkRpc/YomkRpcService.h>

// YomkRpc 对外 API 宏：每个宏封装一次对 /YomkRpcService/* 端点的 YOMK_REQUEST 调用。
// 除 YOMKRPC_VERSION（无返回值）外，其余宏均返回 YomkResponse，调用后须判 m_status==YomkResponse::eOk。

// 创建 DDS 节点（每节点对应一个独立的 DDS 参与者）。
// domainId：DDS 域号，合法范围 [0,232]，仅同域节点可互通；nodeName：节点唯一名。
#define YOMKRPC_NODE(domainId, nodeName) \
    YOMK_REQUEST("/YomkRpcService/create_node", YomkMkPtr(DDSNode, DDSNode{domainId, nodeName}))

// 查询扩展版本：宏内部自动解包并打印（成功走 YOMK_INFO_TAG、失败走 YOMK_ERROR_TAG），无返回值。
#define YOMKRPC_VERSION()                                                          \
    do                                                                             \
    {                                                                              \
        auto __resp = YOMK_REQUEST("/YomkRpcService/version", nullptr);            \
        if (__resp.m_status == YomkResponse::eOk)                                  \
        {                                                                          \
            YomkUnPackPkg(__resp.m_data, String, __ver);                           \
            if (__ver)                                                             \
            {                                                                      \
                YOMK_INFO_TAG("YomkRpcService", __ver->d);                         \
            }                                                                      \
        }                                                                          \
        else                                                                       \
        {                                                                          \
            YOMK_ERROR_TAG("YomkRpcService", "getVersion failed: ", __resp.m_msg); \
        }                                                                          \
    } while (0)

// 在指定节点上注册发布主题。
// type：消息类型实例（new XxxPubSubType()），所有权移交服务端——调用后 caller 不再持有/释放（无论成功或失败）。
#define YOMKRPC_PUB_TOPIC(nodeName, topicName, type) \
    YOMK_REQUEST("/YomkRpcService/register_pub_topic", YomkMkPtr(DDSTopic, DDSTopic{nodeName, topicName, type}))

// 在指定节点上注册订阅主题。
// type：同 YOMKRPC_PUB_TOPIC，所有权移交服务端；callback：std::function<void(const void*)>，每次收到消息时被调用。
#define YOMKRPC_SUB_TOPIC(nodeName, topicName, type, callback) \
    YOMK_REQUEST("/YomkRpcService/register_sub_topic", YomkMkPtr(DDSSubRequest, DDSSubRequest{nodeName, topicName, type, callback}))

// 向指定节点的已注册发布主题发送一条消息。
// data：消息实例指针（&msg），借用（非所有权）——publish 同步写入，caller 保留并在调用后自行管理其生命周期。
#define YOMKRPC_PUB_MSG(nodeName, topicName, data) \
    YOMK_REQUEST("/YomkRpcService/publish", YomkMkPtr(DDSPublish, DDSPublish{nodeName, topicName, data}))

// 借出发布缓冲（仅 plain 类型）：outPtr 为输出参数，成功时指向 writer 池内待发样本，直接在池内填值后
// 经 YOMKRPC_PUB_MSG 发布免序列化；类型不支持 loan（非 plain）或池耗尽时 outPtr 保持 nullptr，回退普通发布路径。
// 每次 write 后指针即被中间件收回，须重新借出。
#define YOMKRPC_LOAN(nodeName, topicName, outPtr)                                              \
    do                                                                                         \
    {                                                                                          \
        (outPtr) = nullptr;                                                                    \
        auto __resp = YOMK_REQUEST("/YomkRpcService/loan",                                     \
                                   YomkMkPtr(DDSLoan, DDSLoan{nodeName, topicName, nullptr})); \
        if (__resp.m_status == YomkResponse::eOk && __resp.m_data)                             \
        {                                                                                      \
            YomkUnPackPkg(__resp.m_data, DDSLoanResult, __loan);                               \
            if (__loan)                                                                        \
            {                                                                                  \
                (outPtr) = __loan->msg.sample;                                                 \
            }                                                                                  \
        }                                                                                      \
    } while (0)

// 归还未发布的借出样本（sample 为 YOMKRPC_LOAN 借出的指针），避免 writer 池泄漏。返回 YomkResponse。
#define YOMKRPC_DISCARD_LOAN(nodeName, topicName, sample) \
    YOMK_REQUEST("/YomkRpcService/discard_loan", YomkMkPtr(DDSLoan, DDSLoan{nodeName, topicName, sample}))

// 删除节点并销毁其全部 DDS 实体；nodeName 不存在时返回错误。返回 YomkResponse。
#define YOMKRPC_DEL_NODE(nodeName) \
    YOMK_REQUEST("/YomkRpcService/delete_node", YomkMkPtr(String, nodeName))
