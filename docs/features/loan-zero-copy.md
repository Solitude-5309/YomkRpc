# 功能文档：Loan 借出零拷贝发布

> **本文档覆盖范围**：Loan（借出）零拷贝发布机制——`YOMKRPC_LOAN` / `YOMKRPC_DISCARD_LOAN` 两个宏的用法、plain 类型判定、借出→填值→发布→归还完整流程、池语义与失败回退、指针生命周期，以及订阅端自动受益的零拷贝接收。

## 功能定位

普通发布（`YOMKRPC_PUB_MSG` 传用户自有实例）需经中间件序列化拷贝；Loan 机制允许发布端向 writer 的样本池借出一块发送缓冲，在池内直接填值后发布，免序列化。plain 类型（如 MInt32）适用；loan 失败（非 plain 或池耗尽）时回退普通发布路径。订阅端完全透明、自动受益，适合高频 / 大负载的发布场景。

## 能力清单

| 能力 | 宏 API | URL 端点 | 说明 |
|---|---|---|---|
| 借出发送缓冲 | `YOMKRPC_LOAN(nodeName, topicName, outPtr)` | `/YomkRpcService/loan` | outPtr 为输出参数，成功指向 `DDSLoanResult{sample}` 池内样本、失败置 nullptr；仅 plain 类型支持 |
| 归还未发布的借出缓冲 | `YOMKRPC_DISCARD_LOAN(nodeName, topicName, sample)` | `/YomkRpcService/discard_loan` | 归还未发布样本避免池泄漏 |
| 订阅端零拷贝接收 | —（自动，无需调用） | — | `FastDDSNode` 内部自动使用 reader loan 接收，回调接口与指针语义不变 |

返回值约定同核心通信：除 `YOMKRPC_VERSION()` 外其余宏均返回 `YomkResponse`，调用后须判 `m_status == YomkResponse::eOk`；`YOMKRPC_LOAN` 的借出结果另经 outPtr 判空获取。

## 使用方式概述

摘自 README 5.2：plain 类型（如 MInt32）可借出 writer 池内样本直接填值发布，免序列化；loan 失败（非 plain 或池耗尽）时回退普通发布路径。

```cpp
#include <YomkServer/YomkAPI.h>
#include <YomkRpc/YomkRpcAPI.h>
#include <YomkRpcMsg/YomkRpcMsg.hpp>
#include <YomkRpcMsg/YomkRpcMsgPubSubTypes.hpp>
#include <chrono>
#include <thread>

using namespace yomk;

int main(int argc, char *argv[])
{
    // 1. 初始化框架并启动 RPC 服务
    YOMK_INIT();
    YOMK_NEW_SERVICE(YomkRpcService);

    // 2. 创建节点与发布主题（loan 仅支持 plain 类型：纯基础类型成员 + FINAL 可扩展性；
    //    domainId 缺省取环境变量 YOMKRPC_DDS_DOMAIN_ID）
    YOMKRPC_NODE("node0");
    YOMKRPC_PUB_TOPIC("node0", "int_topic", new YomkRpc::MInt32PubSubType());

    // 3. 等待 DDS discovery 完成后借出发布
    std::this_thread::sleep_for(std::chrono::seconds(1));
    for (int i = 0; i < 5; ++i)
    {
        void *sample = nullptr;
        YOMKRPC_LOAN("node0", "int_topic", sample);
        if (sample != nullptr)
        {
            // 直接在池内填值后发布，免序列化；write 后中间件收回指针，不可再访问
            static_cast<YomkRpc::MInt32 *>(sample)->data(i);
            YOMKRPC_PUB_MSG("node0", "int_topic", sample);
        }
        else
        {
            // loan 失败：回退普通发布路径
            YomkRpc::MInt32 msg;
            msg.data(i);
            YOMKRPC_PUB_MSG("node0", "int_topic", &msg);
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    // 4. 借出后不发布时须归还缓冲（否则池泄漏）
    void *discardSample = nullptr;
    YOMKRPC_LOAN("node0", "int_topic", discardSample);
    if (discardSample != nullptr)
    {
        YOMKRPC_DISCARD_LOAN("node0", "int_topic", discardSample);
    }

    // 5. 退出前删除节点（显式销毁 DDS 实体）
    YOMKRPC_DEL_NODE("node0");
    return 0;
}
```

标准流程拆解（借出→判空→填值→发布→归还）：

1. **借出**：`YOMKRPC_LOAN("node0", "int_topic", sample)` 向该主题 writer 的样本池借出一块发送缓冲。
2. **判空**：`sample != nullptr` 才是借出成功；失败（非 plain 或池耗尽）走 else 分支回退普通发布路径。
3. **填值**：`static_cast<YomkRpc::MInt32 *>(sample)->data(i)` 直接在池内填值，免序列化。
4. **发布**：`YOMKRPC_PUB_MSG("node0", "int_topic", sample)` 发布（与普通发布同一宏）；write 后中间件收回指针，不可再访问，下一条消息须重新借出。
5. **归还**：借出后决定不发布时，用 `YOMKRPC_DISCARD_LOAN("node0", "int_topic", sample)` 归还，避免池泄漏。

## 参数与约束

- **outPtr 输出参数**：成功指向 `DDSLoanResult{sample}` 池内样本、失败置 nullptr；调用后必须判空再使用。
- **plain 类型判定**：仅 plain 类型可借出——纯基础类型成员 + FINAL 可扩展性（如 MInt32、MColorRGBA）；含 string/sequence 的类型 loan 失败返回 nullptr。
- **池语义**：样本来自 writer 池；loan 失败（非 plain 或池耗尽）时自动回退普通发布路径，业务代码以判空分支承载两条路径。
- **发布路径复用**：借出样本仍经 `YOMKRPC_PUB_MSG` 发布，data 借用、同步写入语义不变。
- **指针生命周期**：发布端 loaned 指针 write/discard 后不可再访问；订阅端指针仅回调期间有效。
- **DISCARD_LOAN 归还对象**：仅针对「借出后未发布」的样本；已发布的样本 write 后已被中间件收回，无需归还。
- **订阅端完全透明**：`FastDDSNode` 内部自动使用 reader loan 接收（回调接口与指针语义不变，仅回调期间有效），零序列化拷贝，配合 data-sharing 跨进程零拷贝读取；零拷贝借出路径下回调指针的对齐消费契约见 [core-communication.md](./core-communication.md) 的「订阅回调与数据消费」。

## 错误与边界行为

- **loan 失败的两种情形**：非 plain 类型（含 string/sequence 的类型）/ 池耗尽——outPtr 置 nullptr，须以判空分支回退普通发布，不可假定借出必然成功。
- **未归还即池泄漏**：借出后不发布须 `YOMKRPC_DISCARD_LOAN` 归还。
- **write 后访问指针非法**：发布后指针被中间件收回，不可再访问。
- **宏返回值**：两宏均返回 `YomkResponse`，须判 `m_status == YomkResponse::eOk`；失败可读 `m_msg` 获取错误信息。

## 与其他功能的关系

- 借出样本仍经 `YOMKRPC_PUB_MSG` 发布；节点创建、主题注册与普通发布的借用语义见 [core-communication.md](./core-communication.md)。
- 订阅端零拷贝接收的回调对齐消费（alignof≤4 直接解引用 / alignof>4 需 memcpy）见 [core-communication.md](./core-communication.md)。

## 教程链接

- [Loan 零拷贝教程](../manual/loan-tutorial.md)：完整 loan 示例走读、何时用/不用 loan、失败回退。
