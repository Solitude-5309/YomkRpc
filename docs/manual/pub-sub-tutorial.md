# 使用手册：发布订阅进阶教程

> **本文档覆盖范围**：在跑通第一个发布订阅（见 [quick-start.md](./quick-start.md)）之后，讲透三件事——订阅回调与数据消费的正确姿势（含转型、生命周期与对齐约束）、消息类型从哪来（msg/ 目录 IDL 定义与 fastddsgen 生成代码）、一个进程内多节点多主题的注册方式与资源清理。QoS 配置见 [qos-tutorial.md](./qos-tutorial.md)。

## 目标

完成本文档后，你将能够：

1. 正确编写订阅回调：转型读取消息字段、遵守 `data` 指针生命周期、按消息类型选对消费方式（含 ARM 严格对齐平台的安全写法）；
2. 说清楚 `MString` 等消息类型的来源（msg/ 目录 IDL + fastddsgen 生成代码 + YomkRpcMsg 类型库），并在代码中正确包含头文件；
3. 在一个进程里创建多个节点、注册多个主题并正确清理。

## 前提与准备

- 已按 [setup-and-build.md](./setup-and-build.md) 完成安装，并按 [quick-start.md](./quick-start.md) 跑通第一个程序；
- 工程链接 `YomkRpc::YomkRpc YomkServer::YomkServer YomkRpcMsg`（CMake 写法见 quick-start 第 1 步）；
- 知道消息类型实例用法：`new XxxPubSubType()` 传给注册宏后所有权移交服务端，caller 不再持有/释放。

## 分步操作

### 第 1 步：订阅回调与数据消费

`YOMKRPC_SUB_TOPIC` 注册的回调签名为 `std::function<void(const void *data)>`，每次收到消息时被调用：

- **转型读取**：`data` 是交付的消息实例指针，`static_cast<const 你的消息类型 *>(data)` 后即可读取字段（如 `msg->data()`）；
- **生命周期**：`data` 仅在回调执行期间有效，须在回调内同步消费（读取或拷贝到自有存储），**不要**跨回调持有该指针（回调返回后即失效）；
- **对齐与类型选型**（关系到严格对齐平台如 ARM 的稳定性）：交付方式由消息「可借出性」自动决定——
  - 非 plain 类型 / 未启用 data-sharing：FastDDS 反序列化进对齐实例，指针恒对齐，可直接解引用（全架构安全）；`string` / `sequence` / 无界类型属此列；
  - plain 且 data-sharing 生效：走零拷贝借出，指针指向接收缓冲 CDR body（`base+4`，源于 CDR representation header 固定 4 字节，仅 4 字节对齐），须按消息 `alignof` 分两种情形消费：
    - **推荐**：`alignof<=4` 的 plain 类型（`float` / `byte` / `int32` 及其定长数组，如点云 `float pts[N]`、图像 `octet pixels[N]`）——借出指针天然对齐，可直接解引用，从零拷贝获益且无对齐风险。大数据 / 点云 / 图像负载应优先选用这类类型；
    - **注意**：`alignof>4` 的 plain 标量（`double` / `int64` / `uint64`，如 `MFloat64` / `MInt64`）——借出指针仅 4 字节对齐，回调中应 `memcpy` 到对齐局部变量再读取；直接解引用在 x86-64 良性，但在严格对齐 ARM 上会触发 SIGBUS。勿用定长 `double` / `int64` 标量作为借出负载；
  - 底层交付路径（loan 零拷贝 vs 对齐反序列化）与对齐契约的实现细节见 `src/FastDDSNode.cpp`。

回调消费写法参考：

```cpp
#include <cstring>

auto onMessage = [&](const void *data)
{
    // alignof<=4 的 plain 类型（如 MInt32、float/byte 定长数组）：直接转型解引用
    int32_t v = static_cast<const YomkRpc::MInt32 *>(data)->data();

    // alignof>4 的 plain 标量（如 MFloat64）：从裸指针 memcpy 到对齐局部再读，规避严格对齐平台 SIGBUS
    // double d;
    // std::memcpy(&d, data, sizeof(d)); // MFloat64 的 double 成员位于偏移 0
};
```

`data` 由内部 `create_data()` 自动创建——你只需转型读取，无需负责其创建与释放。

### 第 2 步：消息类型从哪来

示例中用到的 `YomkRpc::MString`、`YomkRpc::MInt32` 等类型来自仓库的 `msg/` 目录：

- `msg/YomkRpcMsg.idl` 是 IDL 消息定义（如 `MString`：`module YomkRpc` 内 `struct MString { string data; }`）；
- 类型库提供 43 个 ROS2 风格消息类型：布尔/字节/字符/字符串（`MBool`、`MByte`、`MChar`、`MString`），有符号整型 `MInt8/16/32/64`、无符号整型 `MUInt8/16/32/64`、浮点 `MFloat32/64`，复合类型 `MColorRGBA`、`MDuration`、`MHeader`、`MTime`；每个数值类型均配套 `Array` 与 `MultiArray` 变体（如 `MInt32Array`/`MInt32MultiArray`、`MByteArray`/`MByteMultiArray`）；
- 目录内其余 `.hpp/.cxx` 等文件为 fastddsgen 生成代码，编译为独立类型库 **YomkRpcMsg**（含 SWIG Python 绑定，这也是安装依赖里需要 swig 与 python3-dev 的原因）。

代码中包含两个头文件即可使用全部消息类型与其 PubSubType：

```cpp
#include <YomkRpcMsg/YomkRpcMsg.hpp>              // 消息类型（MString、MInt32 等）
#include <YomkRpcMsg/YomkRpcMsgPubSubTypes.hpp>   // 对应 PubSubType（MStringPubSubType 等）
```

注册主题时成对使用：

```cpp
YOMKRPC_PUB_TOPIC("node0", "my_topic", new YomkRpc::MStringPubSubType());
YOMKRPC_SUB_TOPIC("node0", "my_topic", new YomkRpc::MStringPubSubType(), onMessage);
```

两端主题类型必须一致（同一 PubSubType 生成的类型）才能匹配收发。

### 第 3 步：多节点多主题

`YOMKRPC_NODE` 每调用一次即创建一个独立 DDS 参与者（每节点一个独立 DDS 参与者；节点名唯一、domainId 合法范围 [0,232]），一个进程可创建多个节点，每个节点可注册多个发布/订阅主题。完整示例（两个节点、两个主题）：

```cpp
#include <YomkServer/YomkAPI.h>
#include <YomkRpc/YomkRpcAPI.h>
#include <YomkRpcMsg/YomkRpcMsg.hpp>
#include <YomkRpcMsg/YomkRpcMsgPubSubTypes.hpp>
#include <atomic>
#include <chrono>
#include <string>
#include <thread>

using namespace yomk;

int main(int argc, char *argv[])
{
    // 1. 初始化框架并启动 RPC 服务
    YOMK_INIT();
    YOMK_NEW_SERVICE(YomkRpcService);

    // 2. 创建两个节点（每节点一个独立 DDS 参与者）
    YOMKRPC_NODE("node_pub");
    YOMKRPC_NODE("node_sub");

    // 3. 在同一节点上注册多个主题；发布/订阅也可分属不同节点
    YOMKRPC_PUB_TOPIC("node_pub", "topic_a", new YomkRpc::MStringPubSubType());
    YOMKRPC_PUB_TOPIC("node_pub", "topic_b", new YomkRpc::MInt32PubSubType());

    std::atomic<int> recvA{0};
    std::atomic<int> recvB{0};
    auto onA = [&](const void *data)
    {
        auto *msg = static_cast<const YomkRpc::MString *>(data);
        recvA++;
        YOMK_INFO_TAG("MultiTopicExample", "[RECV A] ", msg->data());
    };
    auto onB = [&](const void *data)
    {
        auto *msg = static_cast<const YomkRpc::MInt32 *>(data);
        recvB++;
        YOMK_INFO_TAG("MultiTopicExample", "[RECV B] ", msg->data());
    };
    YOMKRPC_SUB_TOPIC("node_sub", "topic_a", new YomkRpc::MStringPubSubType(), onA);
    YOMKRPC_SUB_TOPIC("node_sub", "topic_b", new YomkRpc::MInt32PubSubType(), onB);

    // 4. 等待 DDS discovery 完成后，两个主题各自循环发布
    std::this_thread::sleep_for(std::chrono::seconds(1));
    for (int i = 0; i < 5; ++i)
    {
        YomkRpc::MString msgA;
        msgA.data("hello " + std::to_string(i));
        YOMKRPC_PUB_MSG("node_pub", "topic_a", &msgA);

        YomkRpc::MInt32 msgB;
        msgB.data(i);
        YOMKRPC_PUB_MSG("node_pub", "topic_b", &msgB);

        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    // 5. 等待接收完成
    std::this_thread::sleep_for(std::chrono::seconds(1));
    YOMK_INFO_TAG("MultiTopicExample", "topic_a received=", recvA.load(), "/5");
    YOMK_INFO_TAG("MultiTopicExample", "topic_b received=", recvB.load(), "/5");

    // 6. 退出前逐个删除节点
    YOMKRPC_DEL_NODE("node_pub");
    YOMKRPC_DEL_NODE("node_sub");
    return 0;
}
```

### 第 4 步：发布节奏与资源清理

- **发布节奏**：`YOMKRPC_PUB_MSG` 为同步写入（data 为借用指针，caller 保留所有权），两次发布之间的间隔由你控制（示例用 `sleep_for(milliseconds(100))`）；持续发布的参考形态见安装包内 `ExampleYomkRpcPub`（每 1s 发布一次，Ctrl+C 退出）；
- **资源清理**：每个 `YOMKRPC_NODE` 对应一次 `YOMKRPC_DEL_NODE`，销毁节点及其全部 DDS 实体；删除不存在的节点返回错误。退出前不删节点可能造成程序退出阶段的资源清理问题，务必在 `return` 前逐个删除。

## 预期输出

运行第 3 步的多节点多主题示例，观察两类关键输出（时间戳前缀以实际环境为准）：

- `[RECV A] hello 0` 至 `hello 4`（topic_a 的 MString 消息）；
- `[RECV B] 0` 至 `4`（topic_b 的 MInt32 消息，输出其整数字段）；
- 结尾两行统计：`topic_a received=5/5` 与 `topic_b received=5/5`（首条消息可能因 discovery 时序丢失，偶尔为 4/5）。

## 常见错误与排障

| 现象 | 原因与处理 |
|---|---|
| 回调里保存了 `data` 指针、之后读取崩溃或数据异常 | `data` 仅回调执行期间有效：回调内同步消费（读取或拷贝到自有存储），不要跨回调持有 |
| ARM 设备上读取 `double`/`int64` 字段触发 SIGBUS | `alignof>4` 的 plain 标量在零拷贝交付下仅 4 字节对齐：回调中 `memcpy` 到对齐局部变量再读取（见第 1 步）；大数据负载优先选 `alignof<=4` 的 plain 类型 |
| 两端注册了同名主题但收不到 | type 不一致：两端必须用同一种消息类型（同一 PubSubType）；并确认两端域号一致（`YOMKRPC_DDS_DOMAIN_ID`，合法范围 [0,232]） |
| 宏调用返回非 `eOk` | 读 `m_msg` 获取原因；常见如节点名重复、删除不存在的节点——每节点名唯一，`YOMKRPC_NODE` 前确认同名节点已删除 |
| 多主题只收到其中一个 | 同样是 discovery 时序：注册全部主题后再统一 `sleep 1s`，不要边注册边立即发布 |
| 收到的消息内容错乱 | 检查是否在回调外复用了发布侧的栈变量指针，或把 `const void *data` 强转成了错误的消息类型 |

## 延伸阅读

- [../features/core-communication.md](../features/core-communication.md)——节点/主题/发布订阅/回调的能力边界与参数约束
- [qos-tutorial.md](./qos-tutorial.md)——控制可靠性、历史深度、持久化等传输语义
- [返回手册入口](./README.md)
