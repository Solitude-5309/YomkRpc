# YomkRpc 扩展

基于 [YomkServer](https://github.com/Solitude-5309/YomkServer) 框架的 RPC 分布式通信扩展。

## 1 功能

基于 YomkServer 框架的 RPC 扩展，集成 FastDDS 提供分布式发布订阅能力。

| 宏 API（用户调用） | URL 端点 | 功能 | 说明 |
|--------------------|----------|------|------|
| `YOMKRPC_VERSION()` | `/YomkRpcService/version` | 版本查询 | 宏内部自动解包并打印版本，无返回值 |
| `YOMKRPC_NODE(domainId, nodeName)` | `/YomkRpcService/create_node` | 创建节点 | 打包 `DDSNode{domainId, nodeName}`，每节点一个独立 DDS 参与者；domainId 合法范围 [0,232]、nodeName 不可为空，否则返回错误 |
| `YOMKRPC_DEL_NODE(nodeName)` | `/YomkRpcService/delete_node` | 删除节点 | 销毁节点及其全部 DDS 实体，不存在的节点返回错误 |
| `YOMKRPC_PUB_TOPIC(nodeName, topicName, type)` | `/YomkRpcService/register_pub_topic` | 注册发布主题 | 打包 `DDSTopic`；type 为 `new XxxPubSubType()`，所有权移交服务端（调用后 caller 不再持有/释放） |
| `YOMKRPC_SUB_TOPIC(nodeName, topicName, type, callback)` | `/YomkRpcService/register_sub_topic` | 注册订阅主题 | 打包 `DDSSubRequest`；type 同上移交所有权，callback 收到消息时回调，接收缓冲 data 由内部自动创建 |
| `YOMKRPC_PUB_MSG(nodeName, topicName, data)` | `/YomkRpcService/publish` | 发布数据 | 打包 `DDSPublish`；data 为数据实例指针（借用，同步写入，caller 保留所有权） |
| `YOMKRPC_LOAN(nodeName, topicName, outPtr)` | `/YomkRpcService/loan` | 借出发送缓冲 | outPtr 为输出参数，成功指向 `DDSLoanResult{sample}` 池内样本、失败置 nullptr；仅 plain 类型支持 |
| `YOMKRPC_DISCARD_LOAN(nodeName, topicName, sample)` | `/YomkRpcService/discard_loan` | 归还未发布的借出缓冲 | 打包 `DDSLoan{nodeName, topicName, sample}`，归还未发布样本避免池泄漏 |
| `YOMKRPC_DEBUG_NODE(domainId)` | `/YomkRpcDebugService/create_node` | 创建调试节点 | 打包 `DDSDebugNode{domainId}`；单节点模型（一个进程至多一个，重复创建须先删除）；domainId 合法范围 [0,232]，仅同域节点的主题可被调试 |
| `YOMKRPC_DEBUG_TOPIC_PRINT(topicName, output)` | `/YomkRpcDebugService/topic_print` | 登记调试主题 | 打包 `DDSDebugTopic{topicName, output}`；发现匹配的远端 DataWriter 后自动解析类型建立订阅（类型无关，无需 IDL 生成代码），消息文本逐条投递 output（用户自定义回调，服务层不打印）；登记为发现驱动的延迟行为，主题尚未被发布者上线时仍返回成功（登记约 3s 后仍无发布者则打一次性等待提示）；须先创建调试节点，失败时 `m_msg` 携具体拒绝原因（`debug node not created` / `subscriber not created` / `empty topic name` / `already registered`） |
| `YOMKRPC_DEBUG_TOPIC_LIST(stableRounds, intervalMs)` | `/YomkRpcDebugService/list_topics` | 列出域内主题 | 打包 `DDSDebugList{stableRounds, intervalMs}`；自适应收敛查询：内部每 ~200ms 轮询一次发现缓存快照，连续 stableRounds 次集合不变即返回（0 值钳制默认 5 次/200ms，最长阻塞约 stableRounds\*intervalMs）；远端 DataWriter 与 DataReader 均记录（仅有订阅者而无发布者的主题同样列出）；返回 StringArray 包，每行一个 topicName（仅主题名，不含类型）按主题名排序；须先创建调试节点，列表可为空（域内无 writer/reader） |
| `YOMKRPC_DEBUG_TOPIC_INFO(topicName, stableRounds, intervalMs)` | `/YomkRpcDebugService/topic_info` | 查询主题详情 | 打包 `DDSDebugInfo{topicName, stableRounds, intervalMs}`；独立收敛查询单个主题的发现详情（不依赖 list_topics），连续 stableRounds 次快照不变即返回（0 值钳制默认 5 次/200ms）；命中返回 StringArray 三行：`Type: 原始 DDS 类型名`（不做任何风格转换）、`Publisher count: N`、`Subscription count: N`；未发现主题返回错误；须先创建调试节点 |
| `YOMKRPC_DEBUG_NODE_LIST(stableRounds, intervalMs)` | `/YomkRpcDebugService/list_nodes` | 列出域内节点 | 打包 `DDSNodeList{stableRounds, intervalMs}`；独立收敛查询域内已发现的命名参与者（不依赖 list_topics/topic_info），连续 stableRounds 次快照不变即返回（0 值钳制默认 5 次/200ms）；返回 StringArray，每行一个节点名（participant_name 非空且非 "/" 才列出，空名与 ROS2 参与者默认占位名 "/" 跳过，不输出 GUID 串）按名称排序；调试节点自身不在自身发现回调中，天然不列出；须先创建调试节点，列表可为空（域内无有效命名参与者） |
| `YOMKRPC_DEBUG_QUIT()` | `/YomkRpcDebugService/delete_node` | 退出调试 | 无参宏（载荷 nullptr）；删除调试节点并销毁其全部 DDS 实体，未创建时返回错误 |
| `YOMKRPC_BAG_NODE(domainId)` | `/YomkRpcBagService/create_node` | 创建 bag 节点 | 打包 `DDSBagNode{domainId}`；单节点模型（一个进程至多一个，重复创建须先删除）；domainId 合法范围 [0,232]，仅同域端点可被发现与录制 |
| `YOMKRPC_BAG_RECORD(topics)` | `/YomkRpcBagService/bag_record` | 录制主题列表 | 打包 `DDSBagRecord{topics}`；长驻阻塞至 Ctrl+C（SIGINT 经 `yomk::bagRecordStop` 无锁置位）收尾返回。清单项支持通配模式（恰好一个 `*`：前缀 `hello_*` / 后缀 `*_hello` / 中间 `pre*suf`，单独 `*` 匹配全部；≥2 个 `*` 返回错误），模式项启动校验时按发现缓存展开为实际主题集合（去重升序、与精确项合并），模式未命中或精确主题无任何端点整体返回错误（不建 bag 目录、不产生任何文件）；通过校验后透传订阅（QoS 跟随远端 offered）直写原始 CDR 字节到 mcap（bag 目录缺省当前路径下 `bag_<YYYY-MM-DD_HH-MM-SS_mmm>`，含 bag_0.mcap 与 metadata.json）；须先创建 bag 节点；成功返回 StringArray 包：首行 bag 目录名，其后每主题一行 "topic: N 条 / M 字节" 统计 |
| `YOMKRPC_BAG_DEL_NODE()` | `/YomkRpcBagService/delete_node` | 删除 bag 节点 | 删除 bag 节点并销毁其全部 DDS 实体，未创建时返回错误 |

> 除 `YOMKRPC_VERSION()` 外，其余宏均返回 `YomkResponse`，调用后须判 `m_status == YomkResponse::eOk`；失败时可读 `m_msg` 获取错误信息。

## 2 前置条件

- C++17 编译器
- CMake >= 3.14
- YomkServer 已安装（通过 `build_ubuntu.sh` 安装后会自动配置环境变量 `YOMK_PREFIX_PATH` 指向安装路径）
- swig 与 python3-dev（msg 类型库 SWIG Python 绑定编译依赖）

## 3 编译

```bash
source build_ubuntu.sh
```

> **交互式编译说明**：
>
> - **路径询问**：启动后依次询问 YomkServer 安装路径（前置路径）与扩展安装路径，默认均取 `$YOMK_PREFIX_PATH`，可直接回车确认或修改
> - **依赖检测**：脚本启动时自动检测 gcc / swig / python3-dev / build-essential / cmake 等编译依赖，缺失时提示一键 `sudo apt install` 补齐
> - **三步编译安装**：主库 YomkRpc → msg 类型库 YomkRpcMsg（含 SWIG Python 绑定）→ 示例程序
> - **动态库注册**：安装完成后将 `${安装路径}/lib` 幂等注册到 `/etc/ld.so.conf.d/yomk.conf` 并执行 `sudo ldconfig` 刷新缓存，新开任意终端即可找到扩展 so（无需手动设置 `LD_LIBRARY_PATH`）
> - **安装布局**：扩展库与 YomkServer 安装到一起，头文件路径由 `YomkServer::YomkServer` 的 INTERFACE include 统一提供

安装后可直接运行的程序（位于 `<安装路径>/bin`）：

| 程序 | 用途 |
|---|---|
| `ExampleYomkRpcTopic` | 发布订阅完整流程演示 |
| `ExampleYomkRpcTopicLoan` | loan 借出机制演示 |
| `ExampleYomkRpcPub` | 跨进程发布端示例（每 1s 发布 hello world，Ctrl+C 退出） |
| `ExampleYomkRpcSub` | 跨进程订阅端示例（订阅 hello_world，Ctrl+C 退出） |
| `yomkrpc` | 命令行工具：观察任意 DDS 主题（topic print）、列出域内主题（topic list）、查询单个主题详情（topic info）、列出域内节点（node list）、查询单个节点详情（node info）、录制主题为 mcap bag（bag record），详见 YomkRpc 调试章节 |

## 4 工程结构

```
YomkRpc/
├── include/
│   ├── YomkRpcService.h        # RPC 服务头文件（消息包定义 + 类声明）
│   ├── YomkRpcDebugService.h   # 调试服务头文件（消息包定义 + 类声明）
│   ├── YomkRpcBagService.h     # bag 录制服务头文件（消息包定义 + 类声明 + bagRecordStop/Reset）
│   └── YomkRpcAPI.h            # API 宏封装（简化调用）
├── src/
│   ├── YomkRpcService.cpp      # RPC 服务实现
│   ├── YomkRpcDebugService.cpp # 调试服务实现
│   ├── FastDDSNode.h           # DDS 节点头文件（发布订阅）
│   ├── FastDDSNode.cpp         # DDS 节点实现
│   ├── FastDDSDebugNode.h      # 类型无关调试订阅节点头文件
│   ├── FastDDSDebugNode.cpp    # 调试订阅节点实现（发现→动态类型→订阅→JSON 输出）
│   ├── FastDDSBagNode.h        # 类型无关 bag 录制节点头文件
│   └── FastDDSBagNode.cpp      # bag 录制节点实现（发现校验→通配展开→透传订阅→mcap 直写→Ctrl+C 收尾）
├── msg/
│   ├── YomkRpcMsg.idl      # IDL 消息定义（如 MString）
│   └── ...                 # fastddsgen 生成代码（独立类型库，含 SWIG Python 绑定）
├── examples/
│   ├── CMakeLists.txt              # 示例程序构建（随主库安装到 bin/）
│   ├── ExampleYomkRpcTopic.cpp     # 发布订阅完整流程演示
│   ├── ExampleYomkRpcTopicLoan.cpp # loan 借出机制演示
│   ├── ExampleYomkRpcPub.cpp       # 发布端示例程序（每 1s 发布 hello world，Ctrl+C 退出）
│   ├── ExampleYomkRpcSub.cpp       # 订阅端示例程序（订阅 hello_world，Ctrl+C 退出）
│   └── yomkrpc.cpp                 # yomkrpc 命令行工具（调试观察 + bag record 录制）
├── cmake/
│   └── ProjectConfig.cmake.in  # CMake 导出配置模板
├── test/
│   ├── CMakeLists.txt            # 测试树构建配置（独立 CMake 工程，只测自有源码）
│   ├── run_tests.sh              # 一键全量测试运行器（含现场残留清理）
│   ├── TestCheck.h               # 极简断言头（CHECK / testReport）
│   ├── Harness/                  # 基座自检（TestHarnessSmoke）
│   ├── YomkRpcService/           # RPC 服务层测试（8 个，含 2 个 stress）
│   ├── FastDDSDebugNode/         # 调试节点层测试（守卫用例 + 发现→订阅→JSON 输出端到端）
│   ├── YomkRpcDebugService/      # 调试服务层测试（契约 DDS-free + 真实 DDS 生命周期）
│   ├── YomkRpcBagService/        # bag 服务层测试（契约 DDS-free + 真实 DDS 录制生命周期）
│   └── FastDDSBagNode/           # bag 节点层测试（启动校验/通配展开 + 录制收尾端到端）
├── CMakeLists.txt            # CMake 构建配置
├── build_ubuntu.sh           # 一键编译脚本（交互式）
└── README.md
```

## 5 使用示例

示例统一使用 `YOMKRPC_*` 宏 API（定义于 `YomkRpcAPI.h`），均为完整可复制编译的程序（链接 `YomkRpc::YomkRpc YomkServer::YomkServer YomkRpcMsg`）。调试与域内状态观察类工具与示例见「YomkRpc 调试」章节。

### 5.1 普通发布订阅示例

同一节点内注册发布与订阅主题，发布 5 条 MString 消息并等待接收：

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

    // 2. 创建节点（每节点一个独立 DDS 参与者）
    YOMKRPC_NODE(0, "node0");

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

### 5.2 借出（Loan）发布示例

plain 类型（如 MInt32）可借出 writer 池内样本直接填值发布，免序列化；
loan 失败（非 plain 或池耗尽）时回退普通发布路径：

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

    // 2. 创建节点与发布主题（loan 仅支持 plain 类型：纯基础类型成员 + FINAL 可扩展性）
    YOMKRPC_NODE(0, "node0");
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

### 5.3 Loan（借出）机制说明

- **订阅端完全透明**：`FastDDSNode` 内部自动使用 reader loan 接收（回调接口与指针语义不变，仅回调期间有效），零序列化拷贝，配合 data-sharing 跨进程零拷贝读取
- **发布端显式 API**：`YOMKRPC_LOAN` 借出 writer 池内样本，直接在池内填值后 `YOMKRPC_PUB_MSG` 发布免序列化；每次 write 后指针即被中间件收回，须重新借出
- **适用条件**：仅 plain 类型可借出（纯基础类型成员 + FINAL 可扩展性，如 MInt32、MColorRGBA）；含 string/sequence 的类型 loan 失败返回 nullptr，自动回退普通发布
- **指针生命周期**：发布端 loaned 指针 write/discard 后不可再访问；订阅端指针仅回调期间有效

### 5.4 订阅回调与数据消费

`YOMKRPC_SUB_TOPIC` 注册的回调签名为 `std::function<void(const void *data)>`，每次收到消息时被调用：

- **转型读取**：`data` 是交付的消息实例指针，`static_cast<const 你的消息类型 *>(data)` 后即可读取字段（如 `msg->data()`）。
- **生命周期**：`data` 仅在回调执行期间有效，须在回调内同步消费（读取或拷贝到自有存储），**不要**跨回调持有该指针（回调返回后即失效）。
- **对齐与类型选型**（关系到严格对齐平台如 ARM 的稳定性）：交付方式由消息「可借出性」自动决定——
  - 非 plain 类型 / 未启用 data-sharing：FastDDS 反序列化进对齐实例，指针恒对齐，可直接解引用（全架构安全）；`string` / `sequence` / 无界类型属此列。
  - plain 且 data-sharing 生效：走零拷贝借出，指针指向接收缓冲 CDR body（`base+4`，源于 CDR representation header 固定 4 字节，仅 4 字节对齐），须按消息 `alignof` 分两种情形消费：
    - **推荐**：`alignof≤4` 的 plain 类型（`float` / `byte` / `int32` 及其定长数组，如点云 `float pts[N]`、图像 `octet pixels[N]`）——借出指针天然对齐，可直接解引用，从零拷贝获益且无对齐风险。大数据 / 点云 / 图像负载应优先选用这类类型。
    - **注意**：`alignof>4` 的 plain 标量（`double` / `int64` / `uint64`，如 `MFloat64` / `MInt64`）——借出指针仅 4 字节对齐，回调中应 `memcpy` 到对齐局部变量再读取；直接解引用在 x86-64 良性，但在严格对齐 ARM 上会触发 SIGBUS。勿用定长 `double` / `int64` 标量作为借出负载。
  - 底层交付路径（loan 零拷贝 vs 对齐反序列化）与对齐契约的实现细节见 `src/FastDDSNode.cpp`。

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

### 5.5 双进程示例程序（hello world）

`examples/` 下提供两个独立进程的参考程序（纯宏 API，编译后可直接运行）：

- `ExampleYomkRpcPub`：创建 `pub_node`，注册 `hello_world` 主题（MString），每隔 1s 发布一次，收到 Ctrl+C 退出信号后停止发布并干净退出
- `ExampleYomkRpcSub`：创建 `sub_node`，订阅 `hello_world`，收到每条消息打印 `[RECV]` 内容，Ctrl+C 退出并打印累计接收条数

另开两个终端分别运行即可观察跨进程发布/订阅（安装脚本已将扩展 lib 注册进系统动态库缓存，无需手动设置 LD_LIBRARY_PATH）：

```bash
# 终端 1（先启动订阅端）
ExampleYomkRpcSub
# 终端 2（再启动发布端）
ExampleYomkRpcPub
```

## 6 YomkRpc 调试

`yomkrpc` 命令行工具经 `YomkRpcDebugService` 调试服务观察任意 DDS 域（类型无关，无需 IDL 生成代码）；库用户可在代码中用等价的 `YOMKRPC_DEBUG_*` 宏（定义于 `YomkRpcAPI.h`）实现相同能力，各命令小节附等价宏调用；`bag record` 例外——经 `YomkRpcBagService` bag 服务实现，等价宏为 `YOMKRPC_BAG_*`（见 6.7）。

### 6.1 yomkrpc 命令清单

| 命令 | 含义 |
|---|---|
| `yomkrpc topic print <主题名>` | 订阅指定主题，消息 JSON 文本逐条直出控制台 |
| `yomkrpc topic list` | 一次性列出域内全部已发现主题（每行一个主题名，按主题名排序） |
| `yomkrpc topic list [-t]` | 列出域内全部已发现主题（按名称排序；-t 每行附类型名：主题名 [类型名]） |
| `yomkrpc topic info <主题名>` | 查询单个主题详情（类型名 / 发布者数 / 订阅者数三行；-v 逐端点 Node name/GUID/QoS 详情段） |
| `yomkrpc topic type <主题名>` | 查询单主题数据类型名（单行裸输出，对齐 ros2 topic type，便于脚本取用） |
| `yomkrpc topic find <类型名>` | 按数据类型名反查域内主题列表（每行一个主题名，精确匹配，对齐 ros2 topic find） |
| `yomkrpc topic hz <主题名>` | 订阅主题测量接收频率（每秒一行滚动窗口统计，对齐 ros2 topic hz） |
| `yomkrpc interface show <类型名>` | 按类型名输出该类型的 IDL 结构描述（struct 头 + 字段行 + 结尾） |
| `yomkrpc interface list` | 列出已发现的全部消息类型名（去重字典序排序，interface show 配套导航） |
| `yomkrpc topic pub -e <主题名>` | 按主题名输出发布示例三段行集（类型名 / IDL / JSON 发布载荷模板） |
| `yomkrpc topic pub -ef <主题名> [-o 目录]` | 按主题名导出消息描述 JSON 文件（内容即默认值模板 JSON 本身，多行缩进可直接 `$(cat)` 填进发布命令；仅发描述不发布） |
| `yomkrpc topic pub <主题名> <JSON数据>` | 按主题名发布 JSON 载荷消息（缺省 1Hz 持续发送，Ctrl+C 停止并输出统计，首轮收敛 + ack 确认；`-w N` 期望建匹配订阅端数，计数达到 N 才开始发布否则一直等待（主题未发现时先等待主题出现）；`-r N` 按 N Hz 持续发布；`-t N` 条数上限，发满即停排空 200ms 后退出；`-w`/`-r`/`-t` 可任意顺序叠加） |
| `yomkrpc topic pub <主题名> -f <文件>` | 按主题名发布 JSON 载荷文件（文件内容整体作为载荷，与 topic pub -ef 导出文件对接；缺省 1Hz 持续发送，`-w N` 等待订阅者匹配、`-r N` 持续发布、`-t N` 条数上限，语义同直发） |
| `yomkrpc node list` | 列出域内全部已发现的命名参与者（每行一个节点名，按名称排序） |
| `yomkrpc node info <节点名>` | 查询指定节点的发布/订阅主题清单（节点名行 + Subscribers/Publishers 两段，形态对齐 ros2 node info） |
| `yomkrpc bag record [-o <目录>\|--output <目录>] [-b <字节>\|--max-bag-size <字节>] [-d <秒>\|--max-bag-duration <秒>] [--start-paused] <主题名\|模式> [<主题名\|模式> ...]` | 录制主题列表为 mcap bag（启动校验后透传订阅直写原始 CDR 字节，长驻阻塞至 Ctrl+C 收尾；清单项支持通配——恰好一个 `*` 的前缀 `hello_*` / 后缀 `*_hello` / 中间 `pre*suf` 模式，单独 `*` 匹配全部，按发现缓存展开为实际主题集合去重升序，未命中报错；bag 目录名经 `-o` 指定，缺省按时间戳命名，目录已存在报错退出；`-b <字节>` 单分片最大字节数——写满即滚动 `bag_N.mcap` 分片（0=不分片缺省，下限 1024 字节过小报错，实际分片粒度不小于 chunk 落盘边界）；`-d <秒>`/`--max-bag-duration <秒>` 单分片最大时长秒——时长达限即滚动 `bag_N.mcap` 分片（0=不分片缺省，无下限校验；与 `-b` 同用时先到先分，暂停期不计入分片时长）；`--start-paused` 暂停态启动——订阅与发现照常、收到的消息丢弃不写入；交互键（tty 下）：SPACE 开始录制、p 暂停、r 继续，Ctrl+C 停止；校验失败不建目录不产生文件，详见 6.7） |

公共参数（各命令通用）：

- **环境变量 `YOMKRPC_DDS_DOMAIN_ID`**：指定 DDS 域号（合法范围 [0,232]，默认 0）。yomkrpc 启动时无该变量则自动创建并默认 0，同时**幂等写入 `~/.bashrc`**（仅当其中无该变量时，带 `# added by yomkrpc` 注释便于识别）——新开任意终端可直接 `echo $YOMKRPC_DDS_DOMAIN_ID` 查看并自动继承；已打开的终端须 `source ~/.bashrc` 或重开才生效。修改默认域 id 可直接编辑 .bashrc 中该行，或临时 `export YOMKRPC_DDS_DOMAIN_ID=5`
- **环境变量 `YOMKRPC_DDS_DISCOVER_ROUNDS`**：收敛判定次数——查询类命令内部每 ~200ms 轮询一次发现缓存快照，连续 N 次集合不变即认为发现收敛、立即输出（默认 5；`topic list`、`topic info`、`topic type`、`topic find`、`interface show`、`interface list`、`topic pub -e`、`topic pub -ef`、`topic pub`、`node list` 与 `node info` 生效，`topic print`/`topic hz` 为持续订阅/测量型不用）。该环境变量为收敛次数唯一配置入口：yomkrpc 启动时无该变量则自动创建并默认 5，同时**幂等写入 `~/.bashrc`**（仅当其中无该变量时，带 `# added by yomkrpc` 注释）——新开任意终端可直接 `echo $YOMKRPC_DDS_DISCOVER_ROUNDS` 查看并自动继承；已打开的终端须 `source ~/.bashrc` 或重开才生效。修改收敛次数可直接编辑 .bashrc 中该行，或临时 `export YOMKRPC_DDS_DISCOVER_ROUNDS=7`（值须为 >=1 的整数，非法报错退出）

### 6.2 调试主题观察（yomkrpc topic print）

`yomkrpc` 命令行工具订阅任意 DDS 主题（类型无关，无需 IDL 生成代码），经 FastDDS 发现机制自动解析远端类型建立订阅，消息 JSON 文本逐条直出控制台：

```bash
yomkrpc topic print hello_world        # 默认域 0
```

与发布端配合观察（另开两个终端）：

```bash
# 终端 1（先启动观察端）
yomkrpc topic print hello_world
# 终端 2（再启动发布端）
ExampleYomkRpcPub
```

输出示例（每条消息一行状态头 + 一行 JSON 体）：

```
[17:18:34.868398] topic=hello_world type=YomkRpc::MString seq=1
{"data":"hello world 0"}
```

登记后三类提示与失败原因（均为节点层直接输出，不经回调）：

- **主题尚未发现**：登记满约 3s 发现宽限窗后域内仍无该主题的 DataWriter（主题名拼错 / 发布者未上线 / 仅有订阅者）→ 由工作线程一次性打印 `[FastDDSDebugNode] topic [<主题名>] not discovered yet, waiting for publisher...`（仅提示，不阻塞不改返回码；订阅建立本就是发现驱动的延迟行为，发布者上线后自动建订）；
- **主题已发现但类型不可重建**（典型为 ROS2 rmw_fastrtps 端点：编译期类型映射，不提供 XTypes TypeObject）→ 工作线程打印 `warning: TypeObject not ready, keep pending. topic=... type=... -- the remote endpoint does not publish XTypes TypeInformation/TypeObject ...`，**首次立即打印、之后按 5s 节流**重复（既不静默也不因 100ms 重试轮询而刷屏）；同类告警覆盖 DynamicType 构建失败 / `create_topic` 失败 / `create_datareader` 失败 / 建订异常四类；
- **登记被拒**：`register debug topic failed: subscribeTopic [<主题名>] failed: <具体原因>`（重复登记为 `topic [<主题名>] already registered`）。

工具经 `YomkRpcDebugService` 调试服务实现，等价的用户代码（`YOMKRPC_DEBUG_*` 宏定义于 `YomkRpcAPI.h`，链接 `YomkRpc::YomkRpc YomkServer::YomkServer`）：

```cpp
#include <YomkServer/YomkAPI.h>
#include <YomkRpc/YomkRpcAPI.h>

#include <iostream>
#include <string>

using namespace yomk;

int main(int argc, char *argv[])
{
    YOMK_INIT();
    YOMK_NEW_SERVICE(YomkRpcDebugService);

    // 1. 创建调试节点（单节点模型：一个进程至多一个，重复创建须先删除）
    auto resp = YOMKRPC_DEBUG_NODE(0);
    if (resp.m_status != YomkResponse::eOk)
    {
        YOMK_ERROR_TAG("DebugExample", "create debug node failed: ", resp.m_msg);
        return 1;
    }

    // 2. 登记调试主题：发现匹配远端 DataWriter 后自动建订阅，
    //    消息文本逐条投递 output 回调（输出权在调用方，服务层不打印）
    resp = YOMKRPC_DEBUG_TOPIC_PRINT("hello_world", [](const std::string &text)
        { std::cout << text << std::endl; });
    if (resp.m_status != YomkResponse::eOk)
    {
        YOMK_ERROR_TAG("DebugExample", "register debug topic failed: ", resp.m_msg);
        YOMKRPC_DEBUG_QUIT();
        return 1;
    }

    // 3. 业务逻辑后退出前显式清理（规避 FastDDS 静态析构期段错误）
    resp = YOMKRPC_DEBUG_QUIT();
    if (resp.m_status != YomkResponse::eOk)
    {
        YOMK_ERROR_TAG("DebugExample", "debug quit failed: ", resp.m_msg);
        return 1;
    }
    return 0;
}
```

### 6.3 列出域内主题（yomkrpc topic list）

`yomkrpc topic list` 一次性列出当前域内全部已发现主题，每行一个 topicName 按主题名排序；加 `-t`（`--types`）进入类型名模式，每行输出 `主题名 [类型名]`（单空格 + 方括号，对齐 `ros2 topic list -t` 形态，类型名原样输出）：

```bash
yomkrpc topic list          # 默认域 0
yomkrpc topic list -t       # 类型名模式：每行输出 主题名 [类型名]
```

与发布端配合观察（建议先启动发布端——工具创建调试节点入域后内部自适应收敛查询：每 ~200ms 轮询一次发现缓存快照，连续 5 次集合不变即认为发现收敛、立即输出，无需固定等待窗口）：

```bash
# 终端 1（先启动发布端）
ExampleYomkRpcPub
# 终端 2
yomkrpc topic list
```

输出示例（仅主题列表，发现过程静默不刷屏）：

```
hello_world
```

`-t` 类型名模式输出示例（实测）：

```
hello_world [YomkRpc::MString]
```

域内无任何已发布主题时输出 `no topics discovered on domain N`（types 模式同）。`-t` 模式对应 4 参宏 `YOMKRPC_DEBUG_TOPIC_LIST_T(收敛次数, 间隔ms)`，输出内容与 CLI 一致。等价宏调用序列（流程与 print 示例同构，链接库相同，宏定义见上表）：

```cpp
YOMK_INIT();
YOMK_NEW_SERVICE(YomkRpcDebugService);
auto resp = YOMKRPC_DEBUG_NODE(0);          // 1. 创建调试节点
resp = YOMKRPC_DEBUG_TOPIC_LIST(5, 200);          // 2. 收敛查询：连续 5 次 200ms 快照不变即返回 StringArray 包
YomkUnPackPkg(resp.m_data, StringArray, arr);
if (arr != nullptr)
{
    for (const auto &line : arr->d) { std::cout << line << "\n"; }
}
resp = YOMKRPC_DEBUG_QUIT();                // 3. 退出前显式清理
```

### 6.4 查询主题详情（yomkrpc topic info）

`yomkrpc topic info <主题名>` 查询单个主题的发现详情，命中输出三行：消息类型名（原始 DDS 类型名，不做任何风格转换）、发布者数量、订阅者数量；加 `-v`（`--verbose`）进入端点详情模式，在计数之外逐端点列出 Node name（归属参与者名）、Endpoint type（PUBLISHER/SUBSCRIPTION）、GUID（FastDDS 原生格式）与 QoS profile（ROS2 风格键值行）：

```bash
yomkrpc topic info hello_world        # 默认域 0
yomkrpc topic info -v hello_world     # 端点详情模式：逐端点列出 Node name/GUID/QoS profile
```

与发布端配合观察（先启动发布端——工具创建调试节点入域后对该主题独立收敛查询：节点内部每 ~200ms 轮询一次该主题详情快照，连续 5 次不变即返回）：

```bash
# 终端 1（先启动发布端）
ExampleYomkRpcPub
# 终端 2
yomkrpc topic info hello_world
```

输出示例（三行详情，类型名与计数直出）：

```
Type: YomkRpc::MString
Publisher count: 1
Subscription count: 0
```

`-v` 端点详情模式输出示例（实测；count 为 0 的端点类型无清单段，多端点时逐端点块以空行分隔）：

```
Type: YomkRpc::MString

Publisher count: 1

Node name: pub_node
Endpoint type: PUBLISHER
GUID: 01.0f.b3.36.8d.c3.21.fa.00.00.00.00|0.0.1.3
QoS profile:
  Reliability: RELIABLE
  Durability: TRANSIENT_LOCAL
  Deadline: Infinite
  Latency Budget: 0 s
  Lifespan: Infinite
  Liveliness: AUTOMATIC
  Liveliness lease duration: Infinite
  Ownership: SHARED
  Ownership Strength: 0
  Destination Order: BY_RECEPTION_TIMESTAMP
  Partition: []

Subscription count: 0
```

QoS 键值行说明：核心组（Reliability / Durability / Deadline / Latency Budget / Lifespan /
Liveliness / Liveliness lease duration / Ownership / Ownership Strength——仅发布端 /
Destination Order / Partition）恒输出；扩展组（History / Resource Limits / Publish Mode /
Transport Priority 等）仅当发现数据携带时追加。GUID 为 FastDDS 原生格式直出（"12 字节前缀 |
4 字节实体 ID"点分十六进制，实体段不补零）。

目标主题在收敛窗口内始终未被发现时报错退出（`topic [<主题名>] not found`，verbose 模式同）。`-v` 模式对应 4 参宏 `YOMKRPC_DEBUG_TOPIC_INFO_V(主题名, 收敛次数, 间隔ms)`，输出内容与 CLI 一致。等价宏调用序列（流程与 list 示例同构，链接库相同，宏定义见上表）：

```cpp
YOMK_INIT();
YOMK_NEW_SERVICE(YomkRpcDebugService);
auto resp = YOMKRPC_DEBUG_NODE(0);                 // 1. 创建调试节点
resp = YOMKRPC_DEBUG_TOPIC_INFO("hello_world", 5, 200);  // 2. 独立收敛查询：连续 5 次 200ms 快照不变即返回 StringArray 三行
YomkUnPackPkg(resp.m_data, StringArray, arr);
if (arr != nullptr)
{
    for (const auto &line : arr->d) { std::cout << line << "\n"; }
}
resp = YOMKRPC_DEBUG_QUIT();                       // 3. 退出前显式清理
```

#### topic type：单主题数据类型快捷查询

`yomkrpc topic type <主题名>` 是 topic info 的单值快捷方式（复用同一 `/topic_info` 端点与收敛语义），输出单行裸类型名（去 `Type: ` 前缀，对齐 `ros2 topic type`，便于脚本 `$(...)` 取用）：

```bash
yomkrpc topic type hello_world        # 输出：YomkRpc::MString
```

未发现主题时报错退出（`topic [<主题名>] not found`，与 topic info 同语义）。等价宏调用同上（`YOMKRPC_DEBUG_TOPIC_INFO` 取首行去前缀）。

#### topic find：按数据类型反查主题

`yomkrpc topic find <类型名>` 按数据类型名精确匹配反查域内主题（独立 `/topic_find` 端点，收敛语义同 topic list），命中时每行输出一个主题名（按主题名排序，对齐 `ros2 topic find`）：

```bash
yomkrpc topic find YomkRpc::MString       # 输出：hello_world
```

域内无该类型主题时报错退出（`type [<类型名>] not found`，info 族语义，可发现类型名拼写错误）。等价宏调用：`YOMKRPC_DEBUG_TOPIC_FIND(typeName, stableRounds, intervalMs)`。

#### topic hz：主题接收频率测量

`yomkrpc topic hz <主题名>` 订阅主题测量接收频率（复用 topic print 的登记订阅链路，回调只记时间戳不打印消息），主循环每秒打印一次滚动窗口统计，输出形态对齐 `ros2 topic hz`：

```text
average rate: 1.000
        min: 0.998s max: 1.004s std dev: 0.00209s window: 10
```

各输出项含义（对齐 ros2 topic hz）：`average rate` 为窗口内相邻消息间隔均值的倒数（Hz）；`min`/`max` 为间隔极值（秒）；`std dev` 为间隔的总体标准差（秒，除以 n）；`window` 为间隔样本数（上限 `--window`，默认 10000，对齐 ros2 默认窗口）。无新消息不重复打印；首条消息前不打印统计（登记后的等待提示与类型不可重建告警同 `topic print`，见 6.2）；Ctrl+C 退出。

#### interface show：按类型名输出 IDL 结构

`yomkrpc interface show <类型名>` 按数据类型名输出该类型的 IDL 结构描述（类型名精确匹配，同 `topic find` 的匹配语义）。FastDDS 仅有机器侧 TypeObject 描述与 DynamicType 遍历 API、无原生类型文本化输出，本指令复用发现链路 TypeInformation → TypeObject → DynamicType 做纯类型内省（不建订阅），输出自设计对齐主流序列化库的 schema 源语法形态（Protobuf DebugString、grpcurl describe、ros2 interface show 同族）——可直接粘回 .idl 文件复用：

```bash
$ yomkrpc interface show YomkRpc::MString
struct YomkRpc::MString {
    string data;
};
```

字段类型名为 ROS2/IDL4 风格映射（bool/int32/uint32/float32/string 等；有界 `string<N>`；`sequence<T>`/`sequence<T, N>`；数组 `T[N]`；嵌套 struct/enum/alias 显示成员子类型名不递归展开）。仅支持顶层为 struct 的类型；收敛判定次数经环境变量 `YOMKRPC_DDS_DISCOVER_ROUNDS` 配置（同其他查询生效）。失败报错退出，错误文本按卡点分层归类（可直接判定问题在哪一层，不再统一报 not found）：

| 错误文本 | 卡点 |
|---|---|
| `type [<类型名>] not found` | 类型名未出现在发现缓存（拼写错误或端点不在本域） |
| `type object for [<类型名>] not available` | 对端未提供 XTypes TypeObject（**ROS2 rmw_fastrtps 端点即此情形**）或 TypeLookup 尚未完成 |
| `type [<类型名>] is not a struct, interface display unsupported` | 顶层为 enum/alias 等非 struct 类型 |
| `interface introspection failed for [<类型名>]` | DynamicType 成员枚举失败 |

#### interface list：列出已发现的全部类型名

`yomkrpc interface list` 列出发现缓存中出现的全部数据类型名（去重、字典序排序，每行一个；同类型多主题仅出一行），是 `interface show` 的配套导航——先 list 拿到类型名全集，再 show 查看具体结构：

```bash
$ yomkrpc interface list
YomkRpc::MString
```

域内无任何类型报错退出（find 族语义）；收敛判定次数经环境变量 `YOMKRPC_DDS_DISCOVER_ROUNDS` 配置（同其他查询生效）。

#### topic pub -e：查看主题的发布示例（Type / IDL / 可复制命令）

`yomkrpc topic pub -e <主题名>`（等价 `--example`）按主题名输出发布示例三段行集：类型名、IDL 结构描述（同 `interface show`）、example 可复制发布命令——同类型重建 DynamicType 取默认值样本生成，与发布输入格式对称；命令里 JSON 已整体包好单引号（裸传会被 shell 剥掉内层双引号导致解析失败），改字段值即可直接发布：

```bash
$ yomkrpc topic pub -e hello_world
Type: YomkRpc::MString

IDL:
struct YomkRpc::MString {
    string data;
};

example:
yomkrpc topic pub hello_world '{"data":""}'
```

失败报错退出，错误文本按节点层三步骤归类（步骤 1 发现 → 步骤 2 类型重建 → 步骤 3 示例生成），不再统一报 `topic [...] not found`；收敛判定次数经环境变量 `YOMKRPC_DDS_DISCOVER_ROUNDS` 配置（同其他查询生效）。

| 错误文本 | 卡点 |
|---|---|
| `topic [<主题名>] not found` | 步骤 1：主题在收敛窗口内始终未被发现（主题名拼错 / 未加 `rt/` 前缀 / 对端未发布 / 域号不一致） |
| `type [<类型名>] not found` | 步骤 2：类型名未入发现缓存（`interface show` 同款语义） |
| `type object for [<类型名>] not available` | 步骤 2：对端未提供 XTypes TypeObject（**ROS2 rmw_fastrtps 端点即此情形**）或 TypeLookup 尚未完成 |
| `type [<类型名>] is not a struct, interface display unsupported` | 步骤 2：顶层非 struct，IDL 段无法拼装 |
| `type rebuild failed for [<类型名>]` | 步骤 3：DynamicType 构建失败 |
| `example generation failed for [<类型名>]: create_data` / `: json_serialize` | 步骤 3：默认值样本创建或 JSON 序列化失败 |

#### topic pub：向主题发布消息（JSON 载荷，缺省 1Hz 持续发送）

`yomkrpc topic pub <主题名> <json>` 向指定主题发布消息（与 `topic pub -e` 示例模式共存），载荷可直接给 JSON 参数，或经 `-f/--file` 从文件整体读取（`yomkrpc topic pub <主题名> -f <文件>`）；缺省按 1 Hz 持续发送直到 Ctrl+C（首轮发布成功后每秒重发，Ctrl+C 停止并输出统计）；追加 `-r/--rate N`（单位 Hz）调整持续频率（见下文持续发布模式）；追加 `-w/--wait N` 指定期望匹配的订阅端数量（计数达到 N 才开始发布，否则一直等待并打印进度，见下文等待订阅者匹配）；追加 `-t/--times N` 指定条数上限（发满 N 条自动停止，见下文条数上限模式）。发布链路：

1. **发现收敛**：轮询主题类型名 + 订阅者数快照，连续 N 轮（默认 5，环境变量 `YOMKRPC_DDS_DISCOVER_ROUNDS` 可调）不变即收敛——保证不早发（避免订阅者还没被发现就漏收）；未发现主题报错退出（`-w N` 时改为先等待主题出现，订阅端上线即宣告主题与类型，见下文等待订阅者匹配）；
2. **类型重建 + 载荷解析**：由发现的类型重建 DynamicType，`json_deserialize` 解析载荷（与 `topic pub -e` 输出的 example 命令中的 JSON 格式对称）；解析失败报错退出，不建任何发布实体；
3. **匹配收敛 + 首轮发布**：临时 RELIABLE + TRANSIENT_LOCAL writer（请求 ≤ 提供，最大兼容既有订阅者 QoS）匹配收敛后 **write 首轮消息**；`-w N` 时匹配收敛前先阻塞等待 matched 计数达到 N（见下文等待订阅者匹配）；匹配收敛后执行 **matched 校验**：以 writer 实际匹配的订阅端 GUID 集与发现缓存中该主题 RELIABLE 订阅者逐一比对，缓存里有订阅者却不在匹配集（订阅者在匹配建立前已挂起/异常退出，EDP 不可达）则不发布、报错退出；
4. **ack 自适应确认送达（首轮）**：存在 RELIABLE 订阅者时以 `wait_for_acknowledgments` 协议级确认——每个 RELIABLE 订阅者确认样本已入 reader history 才返回成功（未全部确认报错退出，如订阅者已挂起）；全 BEST_EFFORT 或无订阅者退化尽力而为（保底窗后返回成功，BEST_EFFORT 无协议保证）；首轮成功后进入持续发布循环（缺省 1 Hz，`-r N` 可调整），Ctrl+C 停止并输出统计（`-t N` 时发满即停，排空 200ms 后退出）。

```bash
$ yomkrpc topic pub -e hello_world          # 先拿发布模板
Type: YomkRpc::MString

IDL:
struct YomkRpc::MString {
    string data;
};

example:
yomkrpc topic pub hello_world '{"data":""}'

$ yomkrpc topic pub hello_world '{"data":"hello yomkrpc"}'
publishing to topic "hello_world" at 1.000000 Hz, press Ctrl+C to stop
publishing #2 to topic hello_world
^Cpublished total=5 failed=0 to topic hello_world   # 首轮经所有 RELIABLE 订阅者 ack 确认

$ yomkrpc topic pub -ef hello_world          # 也可导出消息描述文件（内容即载荷模板）
message description written: hello_world_msg_2026-09-28-12-14-42-626.json (topic: hello_world, type: YomkRpc::MString)
# 编辑文件改字段值后按文件发布（同一发布链路，多行缩进 JSON 直接可发）
$ yomkrpc topic pub hello_world -f hello_world_msg_2026-09-28-12-14-42-626.json
publishing to topic "hello_world" at 1.000000 Hz, press Ctrl+C to stop
^Cpublished total=3 failed=0 to topic hello_world

# 持续发布：首轮校验发布后按 10 Hz 周期重发，Ctrl+C 停止并输出统计
$ yomkrpc topic pub -r 10 hello_world '{"data":"hello yomkrpc"}'
publishing to topic "hello_world" at 10.000000 Hz, press Ctrl+C to stop
publishing #2 to topic hello_world
publishing #3 to topic hello_world
^Cpublished total=17 failed=0 to topic hello_world

# 等待订阅者匹配：期望建匹配 2 个订阅端，计数达标才开始发布（否则持续打印等待进度，Ctrl+C 中断）
$ yomkrpc topic pub -w 2 hello_world '{"data":"hello yomkrpc"}'
waiting for subscribers: 1/2 matched on topic hello_world
waiting for subscribers: 2/2 matched on topic hello_world
publishing to topic "hello_world" at 1.000000 Hz, press Ctrl+C to stop
^Cpublished total=4 failed=0 to topic hello_world

# 先发布后订阅：订阅端尚未上线（主题未被发现）时先等待主题出现，上线后自动进入匹配等待
$ yomkrpc topic pub -w 1 hello_world '{"data":"hello yomkrpc"}'
waiting for topic hello_world to be discovered
waiting for subscribers: 0/1 matched on topic hello_world
publishing to topic "hello_world" at 1.000000 Hz, press Ctrl+C to stop
^Cpublished total=2 failed=0 to topic hello_world

# -w 与 -r 叠加（顺序任意）：先等 2 个订阅端匹配，再按 10 Hz 持续发布
$ yomkrpc topic pub -r 10 -w 2 hello_world '{"data":"hello yomkrpc"}'

# 条数上限：发满 5 条自动停止（排空 200ms 后退出，无需 Ctrl+C）
$ yomkrpc topic pub -t 5 hello_world '{"data":"hello yomkrpc"}'
publishing to topic "hello_world" at 1.000000 Hz, limit 5 messages, press Ctrl+C to stop
publishing #2 to topic hello_world
publishing #5 to topic hello_world
published total=5 failed=0 to topic hello_world

# 条数上限与频率、等待叠加（顺序任意）：先等 1 个订阅端匹配，再按 5 Hz 发满 5 条退出
$ yomkrpc topic pub -t 5 -r 5 -w 1 hello_world '{"data":"hello yomkrpc"}'
```

非法 JSON 报错退出（`invalid json for type [...]`）；未确认送达报错退出（`not all subscribers acknowledged`）；订阅者在场却无一与发布端匹配报错退出（`subscribers exist but not all matched (suspended or offline)`）；`-f` 文件不存在报错退出（`cannot open json file`），文件内容为空报错退出（`json file is empty`）；`-r/--rate` 频率非法（非数值或 ≤0）报错退出（`非法频率 "..."`），`-r` 后缺参数同用法错误退出；`-w/--wait` 等待被 Ctrl+C 中断报错退出（`waiting for subscribers interrupted`），订阅者数量非法（非数值、0 或溢出）报错退出（`非法订阅者数量 "..."`），`-w` 后缺参数同用法错误退出；`-t/--times` 条数非法（非数值、0 或溢出）报错退出（`非法发布条数 "..."`），`-t` 后缺参数同用法错误退出；收敛判定次数经环境变量 `YOMKRPC_DDS_DISCOVER_ROUNDS` 配置（同其他查询生效）。

ack 确认的覆盖语义（端到端实测校准）：

- 订阅者先于发布在场且被发现（常规路径）：RELIABLE 订阅者运行中 → ack 协议级确认送达；RELIABLE 订阅者中途挂起（如 SIGSTOP）→ ack 超时报错退出（`not all subscribers acknowledged`），不假成功；
- 订阅者在发现缓存有记录但与发布端 QoS 不兼容（如请求 PERSISTENT）→ matched 校验报错退出（`subscribers exist but not all matched (suspended or offline)`）；
- 订阅者异常死亡（kill -9、断网等无 graceful goodbye）→ 发现缓存残留记录，matched 校验报错退出（lease 过期前的短暂区间内可能误报一次，重试即恢复）；正常退出（graceful dispose）由发现缓存离线清理同步移除，发布照常成功；
- 订阅者在发布端启动前已离线/挂起（发布端从未发现过该订阅者）→ 发布端无从期待，照常返回成功——这是 DDS 发现机制的客观边界，任何发布端机制均不可达。

BEST_EFFORT 订阅者不参与 ack 与 matched 校验，始终尽力而为。

#### topic pub 等待订阅者匹配（-w/--wait N）

两种发布形态（JSON 直发 / `-f` 文件）均支持 `-w N | --wait N`（N 为 >=1 整数），用于关闭"自动收敛可能漏掉尚未上线订阅者"的缺口——显式指定期望匹配的订阅端数量，以 N 作为收敛结束：

- **主题未发现时先等待主题出现**：`-w N` 下若域内尚无该主题任何端点（典型为订阅端尚未上线的"先发布后订阅"场景），无法重建类型，先无限轮询等待主题被发现（订阅端上线即经 SPDP/EDP 宣告主题与类型），每 ~200ms 轮询、首轮打印 `waiting for topic <主题名> to be discovered`、约每 5 秒重复打印防静默；主题出现后自动进入类型重建与匹配等待；
- **计数达标才发布**：临时 writer 建立后轮询 `PublicationMatchedStatus.current_count`（writer 实际匹配的订阅端计数，含 BEST_EFFORT 订阅端，与 `ros2 topic pub -w` 口径一致），达到 N 才继续发布；未达到则每 ~200ms 轮询一次并打印等待进度日志（计数变化时立即打印，长期无变化约每 5 秒重复打印）：`waiting for subscribers: 1/2 matched on topic hello_world`；
- **一直等待不设超时**：主题等待与匹配等待两阶段均持续等待（N 大于实际订阅端总数、或主题迟迟未出现将无限等待），Ctrl+C 中断等待并报错退出（exit=1，`waiting for subscribers interrupted`），不发布任何数据；
- **达标后仍走完整安全网**：达到 N 后进入既有稳定收敛确认、matched GUID 校验与 ack 自适应确认——N 是"至少 N 个"的门槛，已发现但未匹配的 RELIABLE 订阅者（挂起/异常退出）仍会如实报错；
- **与 `-r`/`-t` 任意叠加**：`-w` 与 `-r`/`-t` 独立识别、顺序任意（`-w 2 -r 10` 等价 `-r 10 -w 2`）——先等待 N 个订阅端匹配，再首轮完整校验发布，随后进入持续发布循环（等待期 Ctrl+C 同样可中断）；
- 省略 `-w`（或 N=0）行为与旧版一致：发现稳定即发（自动收敛），随后进入持续发布循环。

#### topic pub 持续发布模式（缺省 1 Hz；-r/--rate N 调整频率）

发布缺省即 1 Hz 持续发送（JSON 直发 / `-f` 文件两种形态一致），`-r N | --rate N`（单位 Hz，支持小数如 `-r 0.5`）调整持续频率：

- **首轮完整强校验**：发现收敛 → 类型重建 → 匹配收敛 → matched 校验 → write → ack 确认，失败即报错退出——发布开始前确认订阅者在场且可达；
- **周期重发尽力而为**：首轮成功后保留发布链，按 N Hz 周期重复 write（RELIABLE 的 NACK 重传由协议自主完成，不再同步等 ack）；每轮输出 `publishing #N to topic <主题名>` 进度；write 失败仅计入 failed 不中断（订阅者中途离场不停止发布，重新上线后可继续收到后续数据）；
- **绝对节拍网格**：每轮唤醒时刻按 `首发时刻 + k×间隔` 的绝对网格调度（sleep_until），单轮过睡/唤醒延迟由下一轮自动变短补偿，平均频率精确贴合 N Hz；单轮调度毛刺超过周期时网格重锚到未来最近节拍点（不追发，与 rcl_timer overrun 处理一致）；
- **Ctrl+C 优雅停止**：SIGINT 置停止标志（async-signal-safe 无锁原子），发布循环退出并清理发布链，输出统计 `published total=N failed=M to topic <主题名>`（total 含首轮），退出码 0；
- 频率换算：发送间隔 = round(1000/rate) ms（下限 1ms）；省略 `-r` 则缺省按 1 Hz 持续发送（间隔 1000ms）。

#### topic pub 条数上限模式（-t N | --times N 发满即停）

两种发布形态（JSON 直发 / `-f` 文件）均支持 `-t N | --times N`（N 为 >=1 整数，含首轮）：

- **发满即停**：持续发布循环发满 N 条（含首轮）自动停止，销毁发布链前保底排空 200ms 等待异步传输落地——进程即将退出、writer 即将销毁，末条消息失去一切后续重传机会（RELIABLE 首轮另有 ack 确认，BEST_EFFORT 全靠排空窗），随后输出统计 `published total=N failed=M to topic <主题名>` 并退出（exit=0），无需 Ctrl+C；
- **与 `-w`/`-r` 正交叠加**：三个参数独立识别、顺序任意（`-t 5 -r 5 -w 1` 等价 `-w 1 -r 5 -t 5`）——先按 `-w` 等订阅端匹配，再按 `-r` 频率发满 `-t` 条退出；缺省：`-w` 自动收敛、`-r` 1 Hz、`-t` 不限（持续到 Ctrl+C）；
- **发送期间 Ctrl+C**：立即停止（不排空），输出已发统计，exit=0；
- **短名 `-t` 复用判定**：`-t` 下一参数为整数时视为条数（topic pub 语境），否则为 `topic list` 的类型名模式开关（`--times` 长名恒为条数）；
- `-t 0`、负数或非数值报错退出（`非法发布条数 "..."`）。

#### topic pub -ef：导出消息描述文件（与发布输入对称）

`yomkrpc topic pub -ef <主题名>`（等价 `--example-file`）按主题名导出消息描述 JSON 文件（查完即退，不发布）：文件内容即默认值模板的 JSON 本身（展开多行缩进），**整体就是一份合法发布载荷，可直接填进 `topic pub` 发布命令**：

```bash
$ yomkrpc topic pub -ef hello_world
message description written: hello_world_msg_2026-09-28-12-14-42-626.json (topic: hello_world, type: YomkRpc::MString)
```

文件名 `<主题名>_msg_<时间戳>.json`，时间戳格式为 年-月-日-时-分-秒-毫秒（同上例 `2026-09-28-12-14-42-626`）。文件内容：

```json
{
  "data": ""
}
```

- **`-o <目录>` / `--output <目录>`**：指定生成目录（相对/绝对路径均可），缺省当前目录；目录不存在报错退出，不自动创建（`bag record` 的 `-o` 语义不同——创建 bag 目录名，见 6.7）
- 文件整体可直接填进发布命令（与 `topic pub -e` 输出的 example 命令同构对称）：`yomkrpc topic pub hello_world "$(cat hello_world_msg_2026-09-28-12-14-42-626.json)"`——`"$(cat)"` 命令替换结果不再经历引号删除，内层双引号与换行原样保留，多行缩进 JSON 解析无碍；主题名/类型名不在文件内，从文件名与 stdout 提示行追溯
- 失败报错退出，错误文本按节点层两步骤归类（步骤 1 `topic [<主题名>] not found` 主题未发现；步骤 2 类型重建与 JSON 模板生成侧原因，同 `topic pub -e` 的步骤 2/3：`type object for [...] not available`（ROS2 端点即此情形）、`type rebuild failed for [...]`、`example generation failed for [...]: create_data|json_serialize` 等）；收敛判定次数经环境变量 `YOMKRPC_DDS_DISCOVER_ROUNDS` 配置（同其他查询生效）

### 6.5 列出域内节点（yomkrpc node list）

`yomkrpc node list` 独立收敛查询域内已发现的命名参与者（与主题查询完全分开的路径），每行输出一个节点名（participant_name 非空且非 "/" 才列出，空名参与者跳过；"/" 是 ROS2 参与者的默认占位名——rmw_fastrtps 把参与者名统一置为根 enclave "/"，其真实节点名走 `ros2 node list` 所依赖的另一通道，`node list` 不输出这类行），按名称排序；调试节点自身不在自身发现回调中，天然不列出。节点名称在创建节点时经 `YOMKRPC_NODE(domainId, nodeName)` 下沉到 DDS 参与者（随 SPDP 发现传播给同域对端）：

```bash
yomkrpc node list          # 默认域 0
```

与发布端配合观察（先启动发布端——`ExampleYomkRpcPub` 创建节点 `pub_node`，工具创建调试节点入域后对参与者发现缓存独立收敛查询）：

```bash
# 终端 1（先启动发布端）
ExampleYomkRpcPub
# 终端 2
yomkrpc node list
```

输出示例（每行一个节点名，域内仅发布端一个命名参与者时恰一行）：

```
pub_node
```

域内暂无命名参与者时输出提示行（`no nodes discovered on domain <N>`）。等价宏调用序列（流程与 list/info 示例同构，链接库相同，宏定义见上表）：

```cpp
YOMK_INIT();
YOMK_NEW_SERVICE(YomkRpcDebugService);
auto resp = YOMKRPC_DEBUG_NODE(0);            // 1. 创建调试节点
resp = YOMKRPC_DEBUG_NODE_LIST(5, 200);       // 2. 独立收敛查询：连续 5 次 200ms 快照不变即返回 StringArray
YomkUnPackPkg(resp.m_data, StringArray, arr);
if (arr != nullptr)
{
    for (const auto &line : arr->d) { std::cout << line << "\n"; }
}
resp = YOMKRPC_DEBUG_QUIT();                  // 3. 退出前显式清理
```

### 6.6 查询节点详情（yomkrpc node info）

`yomkrpc node info <节点名>` 独立收敛查询指定节点名参与者的发布/订阅主题清单（与 list 查询完全分开的路径；归属判定基于 RTPS 规范保证的"端点 GUID 前缀 == 所属参与者 GUID 前缀"），输出形态对齐 `ros2 node info`：节点名行 + `Subscribers:` 段 + 每条订阅一行 `    <主题名>: <类型名>` + `Publishers:` 段同形态（类型名为原始 DDS 类型名，无任何风格转换；空段仅打段头）：

```bash
yomkrpc node info pub_node          # 默认域 0
```

与发布端配合观察（先启动发布端并等待其稳定入域——工具每次运行创建全新调试节点，发现经 PDP/EDP 传播约需 1-3 秒，随后对该节点名独立收敛查询）：

```bash
# 终端 1（先启动发布端，等 2-3 秒）
ExampleYomkRpcPub
# 终端 2
yomkrpc node info pub_node
```

输出示例（发布端仅注册发布主题：Subscribers 段空、Publishers 段一行）：

```
pub_node
  Subscribers:
  Publishers:
    hello_world: YomkRpc::MString
```

订阅端（`ExampleYomkRpcSub`，节点 `sub_node`）则相反——Subscribers 段列出 `hello_world`。节点名在收敛窗口内始终未被发现时报错退出（`node [<节点名>] not found`）。同名多参与者的端点合并列出。等价宏调用序列（流程与 list/info 示例同构，链接库相同，宏定义见上表）：

```cpp
YOMK_INIT();
YOMK_NEW_SERVICE(YomkRpcDebugService);
auto resp = YOMKRPC_DEBUG_NODE(0);                      // 1. 创建调试节点
resp = YOMKRPC_DEBUG_NODE_INFO("pub_node", 5, 200);     // 2. 独立收敛查询：连续 5 次 200ms 快照不变即返回 StringArray 多行
YomkUnPackPkg(resp.m_data, StringArray, arr);
if (arr != nullptr)
{
    for (const auto &line : arr->d) { std::cout << line << "\n"; }
}
resp = YOMKRPC_DEBUG_QUIT();                            // 3. 退出前显式清理
```

### 6.7 录制主题到 bag（yomkrpc bag record）

`yomkrpc bag record [-o <目录>|--output <目录>] [-b <字节>|--max-bag-size <字节>] [-d <秒>|--max-bag-duration <秒>] [--start-paused] <主题名|模式> [<主题名|模式> ...]` 将主题列表录制为 mcap bag（类型无关，无需 IDL 生成代码——发现匹配远端 DataWriter 自动解析类型，透传订阅将原始 CDR 字节直写 mcap；命令形态对齐 `ros2 bag record`：显式主题清单 + 可选 `-o` 指定 bag 目录名 + 可选 `-b` 单分片最大字节数 + 可选 `-d`/`--max-bag-duration` 单分片最大时长秒 + 可选 `--start-paused` 暂停态启动 + 交互键 SPACE/p/r + Ctrl+C 停止）。域号经环境变量 `YOMKRPC_DDS_DOMAIN_ID` 选择（同其他命令）。经 `YomkRpcBagService` bag 服务实现（等价宏 `YOMKRPC_BAG_*` 定义于 `YomkRpcAPI.h`，见本节末尾）：

```bash
yomkrpc bag record hello_world           # 录制单主题（默认域 0）
yomkrpc bag record rt/chatter rt/tf      # 多主题清单
yomkrpc bag record 'hello_*' '*_world'   # 通配模式（清单项恰好含一个 *，引号防 shell glob 展开）
yomkrpc bag record -o my_session hello_world   # 指定 bag 目录名（相对/绝对路径均可）
yomkrpc bag record -b 1048576 hello_world      # 单分片最大 1MB（写满滚动 bag_N.mcap 分片，0=不分片缺省）
yomkrpc bag record -d 60 hello_world          # 单分片最大 60s（-d/--max-bag-duration；时长达限滚动 bag_N.mcap 分片，0=不分片缺省）
yomkrpc bag record --start-paused hello_world  # 暂停态启动（订阅照常、消息丢弃不写入，按 SPACE 开始录制）
```

与发布端配合观察（另开两个终端）：

```bash
# 终端 1（先启动发布端）
ExampleYomkRpcPub
# 终端 2
yomkrpc bag record hello_world
```

输出示例（CLI 启动行显示清单项数与交互键提示——指定 `-o` 时追加 `output dir=<目录>` 回显，指定 `-b` 非零值时追加 `max bag size=<字节>` 回显，指定 `--max-bag-duration` 非零值时追加 `max bag duration=<秒>s` 回显；节点层逐主题输出实际录制清单——通配展开结果以此为准；交互键回显：p 暂停→`recording paused`、r 继续→`recording resumed`、SPACE 开始→`recording started`；Ctrl+C 触发收尾后输出统计——首行 bag 目录名，其后每主题一行统计，随后进程退出）：

```
[19:37:30.123456] recording topics from 1 list item(s) on domain 0, press p to pause, r to resume, Ctrl+C to stop
recording topic=hello_world type=YomkRpc::MString
recording paused
recording resumed
^C
bag_2026-10-02_19-37-33_726
hello_world: 12 条 / 288 字节
```

录制流程（调用长驻阻塞，收尾完成后返回）：

1. **启动校验**：清单快速校验（空清单/空名/重复/多通配/`-o` 目录已存在即报错）先于发现轮询瞬间返回；随后轮询发现缓存快照（每 200ms 一轮，连续 15 轮不变或全部清单项确认有端点即提前收敛；总窗不足 6s 自动提升轮数，防 SPDP 公告期误判；校验参数固定，不经 `YOMKRPC_DDS_DISCOVER_ROUNDS` 调整）；**模式项逐轮展开**——按发现缓存全表匹配合并进录制清单（去重升序）。收敛后逐项判定，任一精确主题既无发布者也无订阅者、或任一模式未命中任何主题即整体报错退出（逐项列出；**校验失败发生在建目录之前，不产生任何文件**）。仅有订阅者的主题同样通过（类型名取自订阅端点公告，先建订阅，发布者上线匹配后自动开始录流）；
2. **建目录与 writer**：bag 目录名经 `-o` 指定（相对/绝对路径均可，父目录自动多级创建）或缺省当前路径下 `bag_<YYYY-MM-DD_HH-MM-SS_mmm>`（毫秒精度防同秒重名），内建 `bag_0.mcap`；`-b` 非零（下限 1024 字节，过小在启动校验报错）或 `-d`/`--max-bag-duration` 非零时启用分片录制——每条消息写盘前检查本分片已落盘字节与已录时长（时长为距本分片首条消息接收时刻的间隔，暂停期不计入），任一达上限即先滚动下一分片 `bag_N.mcap` 再写本条（写前检查对齐 ros2：size 与时长双条件先到先分、超限消息整体落新分片，不产生空片；close→open 切换后各主题 Channel 由 mcap 首见 channelId 自动补写，channelId 跨分片一致；受存储层 chunk 缓冲影响，`-b` 实际分片粒度不小于 chunk 落盘边界——默认 786432 字节，小于该值的 `-b` 实际每 chunk 一片，时长分片不受 chunk 影响；IO 由 chunk 批量化，分片检查零额外开销）；
3. **透传订阅**：每主题一个 reader（QoS 的 Reliability/Durability 跟随远端 writer offered 值，requested ≤ offered 恒成立）与一个 mcap Channel（`messageEncoding="cdr"`、`schemaId=0`），消息以 CDR 全量字节（含 encapsulation header）逐条直写，sequence 从 0 单调递增；每主题建订成功即输出一行 `recording topic=<名> type=<类型>`（stdout）——实际录制清单以此为准；
4. **等待停止与交互键**：SIGINT 经无锁原子置位停止标志（录制循环 100ms 轮询），进程内自收尾，无外部命令依赖；CLI 键盘监听线程（始终启动，tty 下生效）经 termios 非规范模式 + `poll()` 100ms 轮询 stdin 实现运行态/暂停态状态机——SPACE 暂停态→运行态（输出 `recording started`）、p 运行态→暂停态（输出 `recording paused`）、r 暂停态→运行态（输出 `recording resumed`），按键仅在状态实际变化时生效（防重复按键冗余输出），Ctrl+C 后线程最多 100ms 内还原终端干净退出，stdin 非 tty（管道/重定向）时跳过键盘处理（Ctrl+C 退出不受影响）；暂停态期间暂停标志置位——订阅与发现照常，回调照常 take 排空（防恢复后旧数据涌入）但丢弃不写不计数；暂停期不计入 starting_time/duration（首条写入消息方置位起始时间，sequence 恢复后仍从 0 起）；
5. **收尾**：删全部 reader（杜绝并发回调）→ `writer.close()` 补写 mcap summary 三层索引 → 写 `metadata.json` → 输出统计退出（exit 0）。

#### 通配模式（清单项恰好含一个 `*`）

| 模式 | 匹配语义 |
|---|---|
| `hello_*` | 前缀匹配（以 `hello_` 开头的全部已发现主题） |
| `*_hello` | 后缀匹配 |
| `pre*suf` | 中间夹逼（前后缀之间夹任意片段，`*` 不与首尾共享字符，对齐 fnmatch） |
| `*` | 匹配全部已发现主题 |

- 清单项含 0 个 `*` 为精确主题名；**恰好 1 个 `*` 为通配模式；≥2 个 `*` 输入有误报错退出**（fail-fast）
- 模式在启动校验时按发现缓存展开为实际主题集合，与精确项**去重合并**（`hello_*` 与精确 `hello_world` 同清单只订阅一次），展开清单升序——统计与 metadata 输出确定
- 模式未命中任何主题报错退出（`模式 [...] 未匹配到任何主题`）
- 展开于启动校验时一次定型：录制期间新上线的匹配主题不自动加入
- **shell 下模式须加引号**：bash 会对未加引号的 `*` 做当前目录文件名 glob 预展开（清单项被替换为文件名），程序内无法防御，务必 `'hello_*'` 传参

#### 落盘产物（bag 目录两文件）

- **`bag_0.mcap`**：标准 mcap 容器，逐主题 Channel（encoding=cdr、无 schema——schemaless 对齐透传语义），消息 record 保存原始 CDR 字节；收尾 close 补写 summary 三层索引，可用任意 mcap 标准读库读回；指定 `-b`/`-d` 时滚动产生同格式分片 `bag_1.mcap`、`bag_2.mcap`、...（各分片独立完整可读回，消息按写入顺序连续切分——不重复不空洞，同主题 channelId 跨分片一致）
- **`metadata.json`**：顶层平铺 bag 元信息（`version` 为 yomkrpc 自有格式版本，1 起步）：

```json
{
    "version": 1,
    "storage_identifier": "mcap",
    "relative_file_paths": ["bag_0.mcap"],
    "starting_time": {
        "nanoseconds_since_epoch": 1791953853726412000,
        "nanoseconds_since_epoch_format": "2026-10-02_19-37-33-726-412-000"
    },
    "duration": {
        "nanoseconds": 4012345678,
        "nanoseconds_format": "00-00-04_012-345-678"
    },
    "message_count": 12,
    "topics_with_message_count": [
        {
            "topic_metadata": {
                "name": "hello_world",
                "type": "YomkRpc::MString"
            },
            "message_count": 12
        }
    ]
}
```

  起始时间与时长各附 `_format` 可读伴生键（格式 `YYYY-MM-DD_HH-MM-SS_毫秒-微秒-纳秒`，本地时区；时长为 `HH-MM-SS_毫秒-微秒-纳秒`）；一条消息都未录到时 starting_time 与 duration 均为 0。`topics_with_message_count` 按展开后录制清单排列（纯精确清单为输入顺序，含模式为去重升序）。`relative_file_paths` 列出全部分片文件名（默认单分片即 `bag_0.mcap`；分片时按序列全 `bag_0.mcap`...`bag_N.mcap`），起始时间/时长/条数统计保持全局累计（跨分片）。

失败报错退出（录制阶段失败 exit=2），错误文本按卡点归类（校验类逐项列出）：

| 错误文本 | 卡点 |
|---|---|
| `no topics given` | 未给主题清单 |
| `topic [x] 中 '*' 出现多次，仅支持单个通配` | 清单项含 ≥2 个 `*` |
| `empty topic name in topic list` | 清单含空名主题 |
| `duplicate topic [x] in topic list` | 清单内精确主题名重复 |
| `bag directory [x] already exists, remove it or choose another name` | `-o` 指定的目录已存在（fail-fast 于发现校验前，不落盘） |
| `max bag size [x] too small, minimum split file size is 1024 bytes (0 to disable splitting)` | `-b` 分片上限过小（非 0 且低于 1024 字节；fail-fast 于发现校验前，不落盘） |
| `主题 [x] 既无发布者也无订阅者，请检查主题名输入（域 N，已等待 M ms）` | 校验收敛后该精确主题仍无任何端点（主题名拼错 / 对端未上线 / 域号不一致） |
| `模式 [x] 未匹配到任何主题，请检查通配输入（域 N，已等待 M ms）` | 校验收敛后该模式命中 0 主题（模式拼错或域内确无匹配主题） |

工具经 `YomkRpcBagService` bag 服务实现，等价的用户代码（`YOMKRPC_BAG_*` 宏定义于 `YomkRpcAPI.h`，链接 `YomkRpc::YomkRpc YomkServer::YomkServer`；SIGINT 处理经 `yomk::bagRecordStop` 无锁置位——async-signal-safe，声明于 `YomkRpcBagService.h`；暂停控制同头文件 `yomk::bagRecordPause()`/`bagRecordResume()`，即 CLI 交互键 p/r（与 `--start-paused` 的 SPACE 启动）的库侧等价物）：

```cpp
#include <YomkServer/YomkAPI.h>
#include <YomkRpc/YomkRpcAPI.h>
#include <YomkRpc/YomkRpcBagService.h> // bagRecordStop/bagRecordReset（停止标志读写端）

#include <csignal>
#include <iostream>
#include <vector>

using namespace yomk;

void onSignal(int) { bagRecordStop(); }  // 无锁原子置位，录制循环读它收尾

int main()
{
    YOMK_INIT();
    YOMK_NEW_SERVICE(YomkRpcBagService);

    // 1. 复位残留停止标志（防上次会话置位导致秒退）后创建 bag 节点（单节点模型）
    bagRecordReset();
    auto resp = YOMKRPC_BAG_NODE(0);
    if (resp.m_status != YomkResponse::eOk)
    {
        return 1;
    }

    // 2. Ctrl+C → bagRecordStop 置位；可选暂停态启动：录制前 bagRecordPause() 置位
    //    （订阅照常、消息丢弃不写入，等价 CLI --start-paused），bagRecordResume() 恢复写入
    std::signal(SIGINT, onSignal);

    // 3. 长驻阻塞：启动校验、订阅与录制、收尾落盘都在服务端完成，返回即录制结束
    std::vector<std::string> topics{"hello_world", "hello_*"};
    resp = YOMKRPC_BAG_RECORD(topics, "my_bag", 0, 0);  // 第二参 bag 目录名（空串=缺省时间戳名），
    //                                                   第三参单分片最大字节数（0=不分片，>0 写满滚动 bag_N.mcap），
    //                                                   第四参单分片最大时长秒（0=不分片，>0 时长达限滚动；与字节上限同用时先到先分）
    if (resp.m_status != YomkResponse::eOk)
    {
        // m_msg 为逐项原因（校验类多行）；失败路径同样须删除已建 bag 节点
        YOMKRPC_BAG_DEL_NODE();
        return 2;
    }

    // 4. 成功回执 StringArray：首行 bag 目录名，其后每主题一行 "topic: N 条 / M 字节"
    YomkUnPackPkg(resp.m_data, StringArray, lines);
    if (lines != nullptr)
    {
        for (const auto &line : lines->d) { std::cout << line << "\n"; }
    }

    // 5. 退出前显式删除 bag 节点（确保 DDS 实体在 FastDDS 静态资源销毁前清理）
    YOMKRPC_BAG_DEL_NODE();
    return 0;
}
```

## 7 测试

### 7.1 编译测试

测试树（`test/`）为独立 CMake 工程，只测 YomkRpc 自有源码（`src/`、`include/`），第三方（FastDDS / YomkServer / YomkRpcMsg）仅链接不插桩：

```bash
cmake -S test -B test/build -DCMAKE_PREFIX_PATH="${YOMK_PREFIX_PATH:-/opt/yomk}"
cmake --build test/build -j
```

构建产物为 16 个测试可执行（位于 `test/build/Harness/`、`test/build/YomkRpcService/`、`test/build/FastDDSDebugNode/`、`test/build/YomkRpcDebugService/`、`test/build/YomkRpcBagService/`、`test/build/FastDDSBagNode/`）：

| 模块 | 测试目标 |
|---|---|
| Harness (1) | TestHarnessSmoke（零 DDS 基座自检） |
| YomkRpcService (8) | TestYomkRpcServiceContract、TestYomkRpcNodeLifecycle、TestYomkRpcTopic、TestYomkRpcLoan、TestYomkRpcTypes、TestYomkRpcConcurrency、TestYomkRpcStressSerial、TestYomkRpcStressConcurrent |
| FastDDSDebugNode (1) | TestFastDDSDebugNode（节点层守卫 + 发现→动态类型→订阅→JSON 输出端到端） |
| YomkRpcDebugService (2) | TestYomkRpcDebugServiceContract（DDS-free 契约）、TestYomkRpcDebugServiceLifecycle（真实 DDS 生命周期） |
| YomkRpcBagService (2) | TestYomkRpcBagServiceContract（DDS-free 契约）、TestYomkRpcBagServiceLifecycle（真实 DDS 录制生命周期） |
| FastDDSBagNode (2) | TestFastDDSBagNodeValidation（启动校验与输入校验分支，含通配展开判定）、TestFastDDSBagNodeRecord（发布→录制→Ctrl+C 收尾→落盘断言端到端，含通配模式录制） |

可选构建开关（CMake cache 变量）：

- `-DYOMKRPC_TEST_SANITIZER=off/asan/tsan`：对自有源码插桩 sanitizer（默认 `off`；tsan 模式经 `setarch -R` 启动以兼容高 ASLR 内核）
- `-DSTRESS_ITERS=5000 -DSTRESS_CYCLES=30`：压测规模（编译期旋钮，闭环规模 100000/50）

### 7.2 一键运行全量测试

```bash
./test/run_tests.sh                # 全量运行（含 2 个 stress，编译期规模 5000/30）
./test/run_tests.sh --skip-stress  # 快速冒烟（跳过 2 个 stress）
./test/run_tests.sh --full         # 闭环规模：自动重配+重编 stress（100000/50）后全量运行
./test/run_tests.sh --bin DIR      # 指定测试可执行根目录（默认 test/build，自动定位子目录）
./test/run_tests.sh --timeout N    # 单测试超时秒数（默认 300；stress 默认 1800）
```

运行器行为：

- **失败即停**：任一测试退出码非 0（含超时被杀、正常退出但 /dev/shm 残留未自清理）立即终止，输出 `[FAIL]` 用例摘要与完整日志路径
- **日志落盘**：`test/test_logs/<时间戳>/` 下每个测试一份日志 + `summary.log` 汇总
- **现场清理**：每个测试在独立临时目录运行（隔离 CWD）；运行前清理 `/dev/shm` 中 FastDDS 上次遗留的共享内存（崩溃/超时时 SHM 段不会自清），每个测试结束后与全部结束后复查残留，发现即清理并判定失败
- **超时保护**：每个测试由 `timeout` 包裹，防卡死

### 7.3 单独运行与压测规模

每个测试为独立可执行（纯 `main()` + `CHECK` 断言，返回 0 = 全部通过，非 0 = 存在失败用例），可直接单独运行：

```bash
./test/build/YomkRpcService/TestYomkRpcTopic
```

2 个 stress 测试（Serial / Concurrent）的规模是**编译期**旋钮，由 CMake 变量 `STRESS_ITERS` / `STRESS_CYCLES` 注入（缺省 5000/30，闭环规模 100000/50）。需调整时重新配置并重编 stress 目标：

```bash
cmake -S test -B test/build -DSTRESS_ITERS=100000 -DSTRESS_CYCLES=50
cmake --build test/build --target TestYomkRpcStressSerial TestYomkRpcStressConcurrent -j
```

`./test/run_tests.sh --full` 即自动执行上述重配+重编后再全量运行。

## 8 开发状态

- ✅ 基础框架搭建完成
- ✅ CMake 构建系统集成
- ✅ FastDDS 集成（FastDDSNode 发布订阅）
- ✅ YomkRpcService DDS 接口（节点管理/主题注册/发布）
- ✅ Loan 借出机制（订阅端透明自动切换，发布端 loan/discard 接口）
- ✅ 跨进程通信验证（ExampleYomkRpcPub/ExampleYomkRpcSub 双进程示例）
- ✅ 类型无关调试（FastDDSDebugNode + YomkRpcDebugService + yomkrpc 命令行工具）
- 🚧 更多数据类型支持

## 9 License

MIT License - 详见 [LICENSE.txt](../../LICENSE.txt)

## 10 链接

- [YomkServer 官方仓库](https://github.com/Solitude-5309/YomkServer)
