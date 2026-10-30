# 使用手册：Loan 零拷贝发布教程

> **本文档覆盖范围**：用借出（loan）样本方式发布 plain 类型消息——`YOMKRPC_LOAN` 借出、判空、池内填值、`YOMKRPC_PUB_MSG` 发布、未用样本 `YOMKRPC_DISCARD_LOAN` 归还的完整流程（README 5.2 示例全文走读），plain 类型判定规则、失败回退路径与何时用/不用 loan（README 5.3 机制说明）。订阅端的零拷贝读取约束见 [pub-sub-tutorial.md](./pub-sub-tutorial.md)。

## 目标

完成本文档后，你将能够：

1. 对 plain 类型消息（如 MInt32）用 loan 方式发布：借出 writer 池内样本直接填值，免序列化；
2. 正确处理 loan 失败：判空并回退普通发布路径；
3. 归还借出后未发布的样本，避免池泄漏。

## 前提与准备

- 已按 [setup-and-build.md](./setup-and-build.md) 完成安装，并跑通 [quick-start.md](./quick-start.md) 的普通发布订阅；
- 理解普通发布路径：`YOMKRPC_PUB_MSG(nodeName, topicName, data)` 传入消息实例指针（借用，同步写入，caller 保留所有权）；
- 了解 plain 类型判定规则（见下方「plain 类型判定与失败回退」）。

## 分步操作

### 第 1 步：认识 loan 发布流程

loan 发布的四步循环：

1. `YOMKRPC_LOAN(nodeName, topicName, outPtr)` 借出——`outPtr` 为输出参数，成功指向池内样本、失败置 `nullptr`；
2. **判空**：`nullptr` 则回退普通发布路径；
3. 借出成功：直接在池内填值后 `YOMKRPC_PUB_MSG` 发布，免序列化；**write 后中间件收回指针，不可再访问**；
4. 借出后不发布时，用 `YOMKRPC_DISCARD_LOAN(nodeName, topicName, sample)` 归还，避免池泄漏。

### 第 2 步：完整示例程序

以下为完整可编译程序（借出发布 5 条 MInt32，失败自动回退普通发布，最后演示归还未用样本）：

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

### 第 3 步：代码走读要点

| 片段 | 说明 |
|---|---|
| `void *sample = nullptr; YOMKRPC_LOAN("node0", "int_topic", sample);` | 借出 writer 池内样本；`sample` 是输出参数，宏内部先置 `nullptr` 再尝试借出 |
| `if (sample != nullptr)` | **必须判空**：非 plain 类型或池耗尽时保持 `nullptr` |
| `static_cast<YomkRpc::MInt32 *>(sample)->data(i);` | 直接在池内样本上填值，免序列化 |
| `YOMKRPC_PUB_MSG("node0", "int_topic", sample);` | 用与普通发布相同的宏发布借出样本；write 后指针即被中间件收回，**不可再访问、须重新借出** |
| `else` 分支 | loan 失败回退普通发布路径：栈上实例填值后按借用指针发布 |
| `YOMKRPC_DISCARD_LOAN(...)` | 归还未发布的借出缓冲，避免池泄漏；仅对借出后**未发布**的样本调用 |

### plain 类型判定与失败回退

- **适用条件**：仅 plain 类型可借出（纯基础类型成员 + FINAL 可扩展性，如 MInt32、MColorRGBA）；
- **失败回退**：含 string/sequence 的类型 loan 失败返回 nullptr，自动回退普通发布（示例程序因此无论借出成败都能发出消息）。

### 何时用 / 不用 loan

结合 loan 机制说明（README 5.3）：

- **订阅端完全透明**：`FastDDSNode` 内部自动使用 reader loan 接收（回调接口与指针语义不变，仅回调期间有效），零序列化拷贝，配合 data-sharing 跨进程零拷贝读取——订阅端无需为此写任何额外代码；
- **发布端显式 API**：`YOMKRPC_LOAN` 借出 writer 池内样本，直接在池内填值后 `YOMKRPC_PUB_MSG` 发布免序列化；每次 write 后指针即被中间件收回，须重新借出——适合高频、消息构造后即发即弃的发布路径；
- **不建议使用 loan 的场景**：消息类型含 string/sequence（非 plain，loan 必失败，直接走普通发布即可）；或需要构造消息后暂存、多次修改再发布的流程——loaned 指针 write/discard 后不可再访问，暂存需求更适合普通发布路径。

## 预期输出

示例程序无打印语句，程序正常退出（返回码 0）即代表 5 条消息均已发出（loan 成功走借出路径、失败走回退路径，两者对发送结果无差别）。

想观察消息确实发出，可在另一终端用命令行工具观察同域同主题（默认域 0）：

```bash
yomkrpc topic print int_topic
```

应逐条打印 5 条 JSON 消息文本（MInt32 的 `data` 字段取值 0 至 4，输出格式对齐 `yomkrpc topic print` 的 JSON 直出形态）。

## 常见错误与排障

| 现象 | 原因与处理 |
|---|---|
| `YOMKRPC_LOAN` 返回 `nullptr` | 两种正常原因：1）主题类型非 plain（含 string/sequence 成员，如 MString），loan 不支持——走普通发布回退路径即可；2）池耗尽——稍后重试或走回退路径。示例程序的判空分支即为此设计 |
| 借出并发布后再访问 `sample` 崩溃或数据错乱 | write 后指针即被中间件收回，不可再访问；每次发布须重新借出 |
| 未发布的借出样本既不发布也不归还 | 造成 writer 池泄漏：借出后决定不发布时，调用 `YOMKRPC_DISCARD_LOAN` 归还 |
| 把非 plain 类型硬改成 loan 发布 | 无需如此：含 string/sequence 的类型 loan 失败返回 nullptr，自动回退普通发布；保持判空分支即可 |
| `int_topic` 上 `yomkrpc topic print` 收不到 | 域号或 discovery 时序问题：确认观察端与发布端同域（`YOMKRPC_DDS_DOMAIN_ID`），观察端先启动再运行发布端 |

## 延伸阅读

- [../features/loan-zero-copy.md](../features/loan-zero-copy.md)——Loan 借出零拷贝发布的能力边界、池语义与参数约束
- [pub-sub-tutorial.md](./pub-sub-tutorial.md)——订阅回调的数据消费与对齐约束（零拷贝读取侧的正确姿势）
- [返回手册入口](./README.md)
