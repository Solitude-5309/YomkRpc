# 功能文档：核心通信（节点与发布订阅）

> **本文档覆盖范围**：`YomkRpcService` 服务域的核心通信能力——框架初始化前提、节点生命周期（创建与删除）、主题注册（发布/订阅）、消息发布、订阅回调与数据消费、版本查询。Loan 借出零拷贝发布见 [loan-zero-copy.md](./loan-zero-copy.md)；端点 QoS 配置见 [qos-profiles.md](./qos-profiles.md)。

## 功能定位

YomkRpc 是基于 YomkServer 框架的 RPC 分布式通信扩展，集成 Fast DDS 提供分布式发布订阅能力。核心通信是 YomkRpc 的基础能力域：用户程序经 `YOMKRPC_*` 宏 API（定义于 `YomkRpcAPI.h`）创建节点、注册主题、发布与订阅消息，宏内部打包请求交由 `YomkRpcService` 服务端落为对应的 Fast DDS 实体——每节点一个独立 DDS 参与者，注册主题创建对应端点，发布为同步写入。

## 能力清单

| 能力 | 宏 API | URL 端点 | 说明 |
|---|---|---|---|
| 版本查询 | `YOMKRPC_VERSION()` | `/YomkRpcService/version` | 宏内部自动解包并打印版本，无返回值 |
| 创建节点 | `YOMKRPC_NODE(nodeName[, domainId[, udpDiscoverServerLocator[, tcpDiscoverServerLocator]]])` | `/YomkRpcService/create_node` | 每节点一个独立 DDS 参与者 |
| 删除节点 | `YOMKRPC_DEL_NODE(nodeName)` | `/YomkRpcService/delete_node` | 销毁节点及其全部 DDS 实体，不存在的节点返回错误 |
| 注册发布主题 | `YOMKRPC_PUB_TOPIC(nodeName, topicName, type[, qos])` | `/YomkRpcService/register_pub_topic` | type 为 `new XxxPubSubType()`，所有权移交服务端 |
| 注册订阅主题 | `YOMKRPC_SUB_TOPIC(nodeName, topicName, type, callback[, qos])` | `/YomkRpcService/register_sub_topic` | callback 收到消息时回调，接收缓冲 data 由内部自动创建 |
| 发布数据 | `YOMKRPC_PUB_MSG(nodeName, topicName, data)` | `/YomkRpcService/publish` | data 为数据实例指针（借用，同步写入，caller 保留所有权） |
| 加载 XML profiles | `YOMKRPC_LOAD_XML([xmlPath])` | `/YomkRpcService/load_xml` | participant 级配置入口，详见 [xml-profiles.md](./xml-profiles.md) |
| Loan 借出零拷贝发布 | `YOMKRPC_LOAN(nodeName, topicName, outPtr)` / `YOMKRPC_DISCARD_LOAN(nodeName, topicName, sample)` | `/YomkRpcService/loan`、`/YomkRpcService/discard_loan` | 详见 [loan-zero-copy.md](./loan-zero-copy.md) |

返回值约定：除 `YOMKRPC_VERSION()` 外，其余宏均返回 `YomkResponse`，调用后须判 `m_status == YomkResponse::eOk`；失败时可读 `m_msg` 获取错误信息。

## 使用方式概述

### 前提：初始化框架并启动 RPC 服务

调用任何 `YOMKRPC_*` 宏之前，必须先执行以下两步（初始化 YomkServer 框架并启动 YomkRpc RPC 服务）：

```cpp
YOMK_INIT();
YOMK_NEW_SERVICE(YomkRpcService);
```

### 最小闭环示例（普通发布订阅）

摘自 README 5.1：同一节点内注册发布与订阅主题，发布 5 条 MString 消息并等待接收。

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
    YOMKRPC_VERSION(); // 查看版本（宏内部自动解包并打印）

    // 2. 创建节点（每节点一个独立 DDS 参与者；domainId 缺省取环境变量 YOMKRPC_DDS_DOMAIN_ID）
    YOMKRPC_NODE("node0");

    // 3. 注册发布主题（type 所有权移交 FastDDSNode）
    YOMKRPC_PUB_TOPIC("node0", "my_topic", new YomkRpc::MStringPubSubType());

    // 4. 注册订阅主题（data 由内部 create_data() 创建，回调中转型使用）
    std::atomic<int> received{0};
    auto onMessage = [&](const void *data)
    {
        auto *msg = static_cast<const YomkRpc::MString *>(data);
        received++;
        YOMK_INFO_TAG("PubSubExample", "[RECV] ", msg->data());
    };
    YOMKRPC_SUB_TOPIC("node0", "my_topic", new YomkRpc::MStringPubSubType(), onMessage);

    // 5. 等待 DDS discovery 完成后发布数据
    std::this_thread::sleep_for(std::chrono::seconds(1));
    for (int i = 0; i < 5; ++i)
    {
        YomkRpc::MString msg;
        msg.data("hello " + std::to_string(i));
        YOMKRPC_PUB_MSG("node0", "my_topic", &msg);
        YOMK_INFO_TAG("PubSubExample", "[SEND] ", msg.data());
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    // 6. 等待接收完成（首条消息可能因 discovery 时序丢失）
    std::this_thread::sleep_for(std::chrono::seconds(1));
    YOMK_INFO_TAG("PubSubExample", "received=", received.load(), "/5");

    // 7. 退出前删除节点（显式销毁 DDS 实体）
    YOMKRPC_DEL_NODE("node0");
    return 0;
}
```

### 订阅回调与数据消费

`YOMKRPC_SUB_TOPIC` 注册的回调签名为 `std::function<void(const void *data)>`，每次收到消息时被调用：

- **转型读取**：`data` 是交付的消息实例指针，`static_cast<const 你的消息类型 *>(data)` 后即可读取字段（如 `msg->data()`）。
- **生命周期**：`data` 仅在回调执行期间有效，须在回调内同步消费（读取或拷贝到自有存储），**不要**跨回调持有该指针（回调返回后即失效）。
- **对齐与类型选型**（关系到严格对齐平台如 ARM 的稳定性）：交付方式由消息「可借出性」自动决定——
  - 非 plain 类型 / 未启用 data-sharing：FastDDS 反序列化进对齐实例，指针恒对齐，可直接解引用（全架构安全）；`string` / `sequence` / 无界类型属此列。
  - plain 且 data-sharing 生效：走零拷贝借出，指针指向接收缓冲 CDR body（`base+4`，源于 CDR representation header 固定 4 字节，仅 4 字节对齐），须按消息 `alignof` 分两种情形消费：
    - **推荐**：`alignof≤4` 的 plain 类型（`float` / `byte` / `int32` 及其定长数组，如点云 `float pts[N]`、图像 `octet pixels[N]`）——借出指针天然对齐，可直接解引用，从零拷贝获益且无对齐风险。大数据 / 点云 / 图像负载应优先选用这类类型。
    - **注意**：`alignof>4` 的 plain 标量（`double` / `int64` / `uint64`，如 `MFloat64` / `MInt64`）——借出指针仅 4 字节对齐，回调中应 `memcpy` 到对齐局部变量再读取；直接解引用在 x86-64 良性，但在严格对齐 ARM 上会触发 SIGBUS。勿用定长 `double` / `int64` 标量作为借出负载。

回调体片段（摘自 README 5.4，配合上面完整程序的 `onMessage` 理解）：

```cpp
#include <cstring>

auto onMessage = [&](const void *data)
{
    // alignof≤4 的 plain 类型（如 MInt32、float/byte 定长数组）：直接转型解引用
    int32_t v = static_cast<const YomkRpc::MInt32 *>(data)->data();

    // alignof>4 的 plain 标量（如 MFloat64）：从裸指针 memcpy 到对齐局部再读，规避严格对齐平台 SIGBUS
    // double d;
    // std::memcpy(&d, data, sizeof(d)); // MFloat64 的 double 成员位于偏移 0
};
```

## 参数与约束

### 节点创建（YOMKRPC_NODE）

四种形态（形态 3/4 命名沿 README 5.7）：

| 形态 | 调用示例 | 说明 |
|---|---|---|
| 1 | `YOMKRPC_NODE("node0")` | 仅节点名，domainId 与两 serverLocator 全缺省 |
| 2 | `YOMKRPC_NODE("node", 0)` | 显式 domainId |
| 3 | `YOMKRPC_NODE("ds_node", 0, "127.0.0.1:11811")` | 仅 UDP 通道直配 |
| 4 | `YOMKRPC_NODE("ds_dual_node", 0, "127.0.0.1:11811", "127.0.0.1:42100")` | UDP+TCP 双通道直配；仅 TCP 直配时 UDP 传空串占位 |

- **nodeName 必填**，不可为空。
- **domainId** 合法范围 [0,232]，越界返回错误；可缺省——缺省时经 DDSNode 类内缺省初始化器读环境变量 `YOMKRPC_DDS_DOMAIN_ID`（未设置/为空/非法/超范围回落 0）。
- **每节点一个独立 DDS 参与者**。
- **udpDiscoverServerLocator / tcpDiscoverServerLocator 可缺省尾参**：缺省时经类内缺省初始化器读环境变量自动直配——总开关 `YOMKRPC_USE_DISCOVER_SERVER` 为 true/1（大小写不敏感）时分别取 `YOMKRPC_UDP_DISCOVER_SERVERS`/`YOMKRPC_TCP_DISCOVER_SERVERS`，开关关闭时两串变量被忽略恒空（=不直配）。显式直配聚合串格式为分号分隔 `ip[:port]` 多 server（port 缺省 11811、合法范围 [1,65535]，非法串创建失败回显）；直配后 participant 以 CLIENT 发现协议运行（禁用多播 PDP，发现流量单播直送 server）；TCP 通道 port 为 server 的 TCP 监听口，非空时 participant 自动补注入 TCPv4 传输（与内置 UDPv4/SHM 共存，数据通道不受影响）。直配语义、跨域互通与环境变量门控详见 [discovery-server.md](./discovery-server.md)。

### 主题注册（YOMKRPC_PUB_TOPIC / YOMKRPC_SUB_TOPIC）

- **type** 为 `new XxxPubSubType()`，所有权移交服务端（调用后 caller 不再持有/释放）。
- **callback**（仅订阅）收到消息时回调，接收缓冲 data 由内部自动创建。
- **qos 可缺省尾参**：端点 QoS 描述 `DDSQosProfile`（8 项 history/depth/reliability/durability/liveliness/deadline/lifespan/liveliness lease 可独立设置，未设置项为哨兵保持底座值）；缺省（全哨兵）=Fast DDS 端点默认底座（现状行为）；订阅端推荐 `DDSQosProfile::ros2DefaultSubscriber()`（BEST_EFFORT requested 对任意 offered 均可匹配）。完整语义与底座清单见 [qos-profiles.md](./qos-profiles.md)。

### 发布（YOMKRPC_PUB_MSG）

- **data** 为数据实例指针（借用，同步写入，caller 保留所有权）；同步写入意味着调用返回时数据已交由 writer 处理，实例后续可复用。

### 版本查询（YOMKRPC_VERSION()）

- 无参数；宏内部自动解包并打印版本，无返回值（无需判 `eOk`）。

### 代码位置与链接要求

- 宏封装位于 `include/YomkRpcAPI.h`；DDS 节点实现位于 `src/FastDDSNode.h` / `src/FastDDSNode.cpp`（发布订阅与 QoS/Loan 底层交付路径）；请求包与服务声明位于 `include/YomkRpcService.h`。
- 示例均为完整可复制编译的程序，链接 `YomkRpc::YomkRpc YomkServer::YomkServer YomkRpcMsg`。

## 错误与边界行为

- **返回值**：除 `YOMKRPC_VERSION()` 外的宏调用后须判 `m_status == YomkResponse::eOk`，失败读 `m_msg`。
- **节点创建失败**：domainId 非法（超出 [0,232]）或 nodeName 为空，返回错误；serverLocator 非法串创建失败回显（不静默回落多播发现）。
- **节点删除**：`YOMKRPC_DEL_NODE` 对不存在的节点返回错误。
- **discovery 时序**：注册订阅后远端发布者需要时间被发现——示例在发布前等待 1 秒（`std::this_thread::sleep_for(std::chrono::seconds(1))`），首条消息可能因 discovery 时序丢失；对收发条数做精确预期的程序须预留等待。
- **type 所有权**：移交服务端后 caller 不再持有/释放。
- **回调 data 生命周期**：仅回调执行期间有效，不得跨回调持有。

## 与其他功能的关系

- **XML profiles**：`YOMKRPC_LOAD_XML` 是 participant 级发现/传输/QoS 配置入口，三级提取链与「必须先于建节点」时序约束见 [xml-profiles.md](./xml-profiles.md)。
- **发现服务器直配**：`YOMKRPC_NODE` 的两 serverLocator 尾参（及同名环境变量自动直配）承担发现服务器直配，UDP/TCP 格式、CLIENT 协议、跨域互通见 [discovery-server.md](./discovery-server.md)。
- **QoS**：主题注册宏的 qos 尾参（`DDSQosProfile` 8 项与 ROS2 预置底座）见 [qos-profiles.md](./qos-profiles.md)。
- **Loan 借出零拷贝发布**：`YOMKRPC_LOAN`/`YOMKRPC_DISCARD_LOAN` 在本篇的发布链路上提供 plain 类型免序列化路径，见 [loan-zero-copy.md](./loan-zero-copy.md)。

## 教程链接

- [快速开始](../manual/quick-start.md)：编译运行第一个发布订阅 + 双进程 hello world
- [发布订阅进阶教程](../manual/pub-sub-tutorial.md)：回调数据消费细节、消息类型定义、多节点多主题、发布频率与资源清理
