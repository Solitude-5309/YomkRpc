# YomkRpc 扩展

基于 [YomkServer](https://github.com/Solitude-5309/YomkServer) 框架的 RPC 分布式通信扩展。

## 功能

基于 YomkServer 框架的 RPC 扩展，集成 FastDDS 提供分布式发布订阅能力。

| URL | 功能 | 说明 |
|-----|------|------|
| `/YomkRpcService/version` | 版本查询 | 返回 YomkRpc 扩展版本信息 |
| `/YomkRpcService/create_node` | 创建节点 | 传入 `DDSNode{domainId, nodeName}`，每节点一个独立 DDS 参与者 |
| `/YomkRpcService/delete_node` | 删除节点 | 销毁节点及其全部 DDS 实体，不存在的节点返回错误 |
| `/YomkRpcService/register_pub_topic` | 注册发布主题 | 传入 `DDSTopic{nodeName, topicName, type}`，type 为 PubSubType 实例指针 |
| `/YomkRpcService/register_sub_topic` | 注册订阅主题 | 传入 `DDSSubRequest{nodeName, topicName, type, callback}`，data 由内部自动创建 |
| `/YomkRpcService/publish` | 发布数据 | 传入 `DDSPublish{nodeName, topicName, data}`，data 为数据实例指针 |
| `/YomkRpcService/loan` | 借出发送缓冲 | 传入 `DDSLoan{nodeName, topicName}`，成功返回 `DDSLoanResult{sample}` 池内指针，仅 plain 类型支持 |
| `/YomkRpcService/discard_loan` | 归还未发布的借出缓冲 | 传入 `DDSLoan{nodeName, topicName, sample}` |

## 前置条件

- C++17 编译器
- CMake >= 3.14
- YomkServer 已安装
- swig 与 python3-dev（msg 类型库 SWIG Python 绑定编译依赖）

## 编译

```bash
# 默认安装到 YomkRpc/install/
source build.sh -DCMAKE_PREFIX_PATH=~/YomkServer/install

# 自定义安装目录
source build.sh -DCMAKE_PREFIX_PATH=~/YomkServer/install -DCMAKE_INSTALL_PREFIX=~/YomkServer/install
```

脚本启动时先执行环境检查：自动检测 gcc / swig / python3-dev / build-essential / cmake 等编译依赖，缺失时提示一键 `sudo apt install` 补齐。随后按三步流程执行：先编译安装主库（YomkRpc），再编译安装 msg 类型库（YomkRpcMsg，固定安装至 ~/YomkServer/install），最后编译并依次运行 `TestRpcTopic`（服务接口端到端测试）与 `TestRpcTopicLoan`（loan 借出机制专项测试），并设置好 LD_LIBRARY_PATH/PATH 环境变量。示例程序 `RpcPubHelloWorld`/`RpcSubHelloWorld` 随测试工程一起编译，不参与自动测试。

## 工程结构

```
YomkRpc/
├── include/
│   ├── YomkRpcService.h    # 服务头文件（消息包定义 + 类声明）
│   └── YomkRpcAPI.h        # API 宏封装（简化调用）
├── src/
│   ├── YomkRpcService.cpp  # 服务实现
│   ├── FastDDSNode.h       # DDS 节点头文件
│   └── FastDDSNode.cpp     # DDS 节点实现
├── msg/
│   ├── YomkRpcMsg.idl      # IDL 消息定义（如 MString）
│   └── ...                 # fastddsgen 生成代码（独立类型库，含 SWIG Python 绑定）
├── test/
│   ├── CMakeLists.txt        # 测试程序构建
│   ├── TestRpcTopic.cpp      # 服务接口端到端测试
│   ├── TestRpcTopicLoan.cpp  # loan 借出机制专项测试
│   ├── RpcPubHelloWorld.cpp  # 发布端示例程序（每 1s 发布 hello world，持续 60 秒）
│   └── RpcSubHelloWorld.cpp  # 订阅端示例程序（订阅 hello world，Ctrl+C 退出）
├── cmake/
│   └── ProjectConfig.cmake.in  # CMake 导出配置模板
├── CMakeLists.txt            # CMake 构建配置
├── build.sh                  # 一键编译脚本
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

### 双进程示例程序（hello world）

`test/` 下提供两个独立进程的参考程序（纯宏 API，编译后可直接运行）：

- `RpcPubHelloWorld`：创建 `pub_node`，注册 `hello_world` 主题（MString），每隔 1s 发布一次，持续 60 秒后自行干净退出
- `RpcSubHelloWorld`：创建 `sub_node`，订阅 `hello_world`，收到每条消息打印 `[RECV]` 内容，Ctrl+C 退出并打印累计接收条数

另开两个终端分别运行即可观察跨进程发布/订阅（运行前需 export LD_LIBRARY_PATH 含 YomkRpc/install/lib 与 ~/YomkServer/install/lib）：

```bash
# 终端 1（先启动订阅端）
RpcSubHelloWorld
# 终端 2（再启动发布端）
RpcPubHelloWorld
```

## 开发状态

- ✅ 基础框架搭建完成
- ✅ CMake 构建系统集成
- ✅ FastDDS 集成（FastDDSNode 发布订阅）
- ✅ YomkRpcService DDS 接口（节点管理/主题注册/发布）
- ✅ Loan 借出机制（订阅端透明自动切换，发布端 loan/discard 接口）
- ✅ 跨进程通信验证（RpcPubHelloWorld/RpcSubHelloWorld 双进程示例）
- 🚧 更多数据类型支持

## License

MIT License - 详见 [LICENSE.txt](../../LICENSE.txt)

## 链接

- [YomkServer 官方仓库](https://github.com/Solitude-5309/YomkServer)
- [YomkExtensions 扩展集合](https://github.com/Solitude-5309/YomkExtensions)
