# YomkRpc 扩展

基于 [YomkServer](https://github.com/Solitude-5309/YomkServer) 框架的 RPC 分布式通信扩展。

## 功能

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
| `YOMKRPC_DEBUG_PRINT(topicName, output)` | `/YomkRpcDebugService/topic_print` | 登记调试主题 | 打包 `DDSDebugTopic{topicName, output}`；发现匹配的远端 DataWriter 后自动解析类型建立订阅（类型无关，无需 IDL 生成代码），消息文本逐条投递 output（用户自定义回调，服务层不打印）；须先创建调试节点，重复登记同一主题返回错误 |
| `YOMKRPC_DEBUG_LIST()` | `/YomkRpcDebugService/list_topics` | 列出域内主题 | 无参宏（载荷 nullptr）；返回 StringArray 包，每行 "topicName [typeName]" 按主题名排序；须先创建调试节点，入域后列表可为空（发现重放异步，建议等待约 2s 再查询） |
| `YOMKRPC_DEBUG_QUIT()` | `/YomkRpcDebugService/delete_node` | 退出调试 | 无参宏（载荷 nullptr）；删除调试节点并销毁其全部 DDS 实体，未创建时返回错误 |

> 除 `YOMKRPC_VERSION()` 外，其余宏均返回 `YomkResponse`，调用后须判 `m_status == YomkResponse::eOk`；失败时可读 `m_msg` 获取错误信息。

## 前置条件

- C++17 编译器
- CMake >= 3.14
- YomkServer 已安装（通过 `build_ubuntu.sh` 安装后会自动配置环境变量 `YOMK_PREFIX_PATH` 指向安装路径）
- swig 与 python3-dev（msg 类型库 SWIG Python 绑定编译依赖）

## 编译

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
| `ExampleYomkRpcPub` | 跨进程发布端示例（每 1s 发布 hello world，持续 60 秒） |
| `ExampleYomkRpcSub` | 跨进程订阅端示例（订阅 hello_world，Ctrl+C 退出） |
| `yomkrpc` | 命令行工具，观察任意 DDS 主题（`yomkrpc topic print [-d N] <主题名>`，详见使用示例） |

## 工程结构

```
YomkRpc/
├── include/
│   ├── YomkRpcService.h        # RPC 服务头文件（消息包定义 + 类声明）
│   ├── YomkRpcDebugService.h   # 调试服务头文件（消息包定义 + 类声明）
│   └── YomkRpcAPI.h            # API 宏封装（简化调用）
├── src/
│   ├── YomkRpcService.cpp      # RPC 服务实现
│   ├── YomkRpcDebugService.cpp # 调试服务实现
│   ├── FastDDSNode.h           # DDS 节点头文件（发布订阅）
│   ├── FastDDSNode.cpp         # DDS 节点实现
│   ├── FastDDSDebugNode.h      # 类型无关调试订阅节点头文件
│   └── FastDDSDebugNode.cpp    # 调试订阅节点实现（发现→动态类型→订阅→JSON 输出）
├── msg/
│   ├── YomkRpcMsg.idl      # IDL 消息定义（如 MString）
│   └── ...                 # fastddsgen 生成代码（独立类型库，含 SWIG Python 绑定）
├── examples/
│   ├── CMakeLists.txt              # 示例程序构建（随主库安装到 bin/）
│   ├── ExampleYomkRpcTopic.cpp     # 发布订阅完整流程演示
│   ├── ExampleYomkRpcTopicLoan.cpp # loan 借出机制演示
│   ├── ExampleYomkRpcPub.cpp       # 发布端示例程序（每 1s 发布 hello world，持续 60 秒）
│   ├── ExampleYomkRpcSub.cpp       # 订阅端示例程序（订阅 hello_world，Ctrl+C 退出）
│   └── yomkrpc.cpp                 # yomkrpc 命令行工具（topic print 观察任意主题）
├── cmake/
│   └── ProjectConfig.cmake.in  # CMake 导出配置模板
├── test/
│   ├── CMakeLists.txt            # 测试树构建配置（独立 CMake 工程，只测自有源码）
│   ├── run_tests.sh              # 一键全量测试运行器（含现场残留清理）
│   ├── TestCheck.h               # 极简断言头（CHECK / testReport）
│   ├── Harness/                  # 基座自检（TestHarnessSmoke）
│   ├── YomkRpcService/           # RPC 服务层测试（8 个，含 2 个 stress）
│   ├── FastDDSDebugNode/         # 调试节点层测试（守卫用例 + 发现→订阅→JSON 输出端到端）
│   └── YomkRpcDebugService/      # 调试服务层测试（契约 DDS-free + 真实 DDS 生命周期）
├── CMakeLists.txt            # CMake 构建配置
├── build_ubuntu.sh           # 一键编译脚本（交互式）
└── README.md
```

## 使用示例

示例统一使用 `YOMKRPC_*` 宏 API（定义于 `YomkRpcAPI.h`），均为完整可复制编译的程序（链接 `YomkRpc::YomkRpc YomkServer::YomkServer YomkRpcMsg`）。

### 普通发布订阅示例

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

### 借出（Loan）发布示例

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

### Loan（借出）机制说明

- **订阅端完全透明**：`FastDDSNode` 内部自动使用 reader loan 接收（回调接口与指针语义不变，仅回调期间有效），零序列化拷贝，配合 data-sharing 跨进程零拷贝读取
- **发布端显式 API**：`YOMKRPC_LOAN` 借出 writer 池内样本，直接在池内填值后 `YOMKRPC_PUB_MSG` 发布免序列化；每次 write 后指针即被中间件收回，须重新借出
- **适用条件**：仅 plain 类型可借出（纯基础类型成员 + FINAL 可扩展性，如 MInt32、MColorRGBA）；含 string/sequence 的类型 loan 失败返回 nullptr，自动回退普通发布
- **指针生命周期**：发布端 loaned 指针 write/discard 后不可再访问；订阅端指针仅回调期间有效

### 订阅回调与数据消费

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

### 双进程示例程序（hello world）

`examples/` 下提供两个独立进程的参考程序（纯宏 API，编译后可直接运行）：

- `ExampleYomkRpcPub`：创建 `pub_node`，注册 `hello_world` 主题（MString），每隔 1s 发布一次，持续 60 秒后自行干净退出
- `ExampleYomkRpcSub`：创建 `sub_node`，订阅 `hello_world`，收到每条消息打印 `[RECV]` 内容，Ctrl+C 退出并打印累计接收条数

另开两个终端分别运行即可观察跨进程发布/订阅（安装脚本已将扩展 lib 注册进系统动态库缓存，无需手动设置 LD_LIBRARY_PATH）：

```bash
# 终端 1（先启动订阅端）
ExampleYomkRpcSub
# 终端 2（再启动发布端）
ExampleYomkRpcPub
```

### 调试主题观察（yomkrpc topic print）

`yomkrpc` 命令行工具订阅任意 DDS 主题（类型无关，无需 IDL 生成代码），经 FastDDS 发现机制自动解析远端类型建立订阅，消息 JSON 文本逐条直出控制台：

```bash
yomkrpc topic print hello_world        # 默认域 0
yomkrpc topic print -d 5 sensor_data   # 指定 DDS 域号
```

域 id 两种指定方式（优先级 `-d` > 环境变量 > 0）：`-d N` 为临时指定，直接使用该值（不读、也不写环境变量）；环境变量 `YOMKRPC_DDS_DOMAIN_ID` 为默认路径，yomkrpc 启动时无该变量则自动创建并默认 0，同时**幂等写入 `~/.bashrc`**（仅当其中无该变量时，带 `# added by yomkrpc` 注释便于识别）——新开任意终端可直接 `echo $YOMKRPC_DDS_DOMAIN_ID` 查看并自动继承；已打开的终端须 `source ~/.bashrc` 或重开才生效。修改默认域 id 可直接编辑 .bashrc 中该行。

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
    resp = YOMKRPC_DEBUG_PRINT("hello_world", [](const std::string &text)
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

### 列出域内主题（yomkrpc topic list）

`yomkrpc topic list` 一次性列出当前域内全部已发现主题与数据类型名，每行 "topicName [typeName]" 按主题名排序：

```bash
yomkrpc topic list          # 默认域 0
yomkrpc topic list -d 5     # 指定 DDS 域号
```

域 id 指定方式与 `topic print` 相同：`-d N` 临时指定（显式覆盖，不写环境变量）；或设置环境变量 `YOMKRPC_DDS_DOMAIN_ID`（启动时无则自动创建默认 0，可持久化到 .bashrc）后免 `-d` 运行。

与发布端配合观察（建议先启动发布端——调试节点入域后工具等待约 2s 收集域内发现信息，再一次性查询输出）：

```bash
# 终端 1（先启动发布端）
ExampleYomkRpcPub
# 终端 2
yomkrpc topic list
```

输出示例（仅主题列表，发现过程静默不刷屏）：

```
hello_world [YomkRpc::MString]
```

域内无任何已发布主题时输出 `no topics discovered on domain N`。等价宏调用序列（流程与 print 示例同构，链接库相同，宏定义见上表）：

```cpp
YOMK_INIT();
YOMK_NEW_SERVICE(YomkRpcDebugService);
auto resp = YOMKRPC_DEBUG_NODE(0);          // 1. 创建调试节点
/* 等待约 2s 收集发现信息（EDP 发现重放是异步的） */
resp = YOMKRPC_DEBUG_LIST();                // 2. 一次性查询：返回 StringArray 包
YomkUnPackPkg(resp.m_data, StringArray, arr);
if (arr != nullptr)
{
    for (const auto &line : arr->d) { std::cout << line << "\n"; }
}
resp = YOMKRPC_DEBUG_QUIT();                // 3. 退出前显式清理
```

## 测试

### 1. 编译测试

测试树（`test/`）为独立 CMake 工程，只测 YomkRpc 自有源码（`src/`、`include/`），第三方（FastDDS / YomkServer / YomkRpcMsg）仅链接不插桩：

```bash
cmake -S test -B test/build -DCMAKE_PREFIX_PATH="${YOMK_PREFIX_PATH:-/opt/yomk}"
cmake --build test/build -j
```

构建产物为 12 个测试可执行（位于 `test/build/Harness/`、`test/build/YomkRpcService/`、`test/build/FastDDSDebugNode/`、`test/build/YomkRpcDebugService/`）：

| 模块 | 测试目标 |
|---|---|
| Harness (1) | TestHarnessSmoke（零 DDS 基座自检） |
| YomkRpcService (8) | TestYomkRpcServiceContract、TestYomkRpcNodeLifecycle、TestYomkRpcTopic、TestYomkRpcLoan、TestYomkRpcTypes、TestYomkRpcConcurrency、TestYomkRpcStressSerial、TestYomkRpcStressConcurrent |
| FastDDSDebugNode (1) | TestFastDDSDebugNode（节点层守卫 + 发现→动态类型→订阅→JSON 输出端到端） |
| YomkRpcDebugService (2) | TestYomkRpcDebugServiceContract（DDS-free 契约）、TestYomkRpcDebugServiceLifecycle（真实 DDS 生命周期） |

可选构建开关（CMake cache 变量）：

- `-DYOMKRPC_TEST_SANITIZER=off/asan/tsan`：对自有源码插桩 sanitizer（默认 `off`；tsan 模式经 `setarch -R` 启动以兼容高 ASLR 内核）
- `-DSTRESS_ITERS=5000 -DSTRESS_CYCLES=30`：压测规模（编译期旋钮，闭环规模 100000/50）

### 2. 一键运行全量测试

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

### 3. 单独运行与压测规模

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

## 开发状态

- ✅ 基础框架搭建完成
- ✅ CMake 构建系统集成
- ✅ FastDDS 集成（FastDDSNode 发布订阅）
- ✅ YomkRpcService DDS 接口（节点管理/主题注册/发布）
- ✅ Loan 借出机制（订阅端透明自动切换，发布端 loan/discard 接口）
- ✅ 跨进程通信验证（ExampleYomkRpcPub/ExampleYomkRpcSub 双进程示例）
- ✅ 类型无关调试（FastDDSDebugNode + YomkRpcDebugService + yomkrpc 命令行工具）
- 🚧 更多数据类型支持

## License

MIT License - 详见 [LICENSE.txt](../../LICENSE.txt)

## 链接

- [YomkServer 官方仓库](https://github.com/Solitude-5309/YomkServer)
