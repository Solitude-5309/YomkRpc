# 使用手册：快速开始（第一个发布订阅）

> **本文档覆盖范围**：用一个最小闭环程序跑通 YomkRpc 普通发布订阅——同进程内发布并收到消息（README 5.1 完整代码逐段讲解 + 工程编译方法），以及用两个安装好的示例程序观察跨进程发布/订阅（README 5.5 双进程 hello world）。回调消费细节、消息类型定制与多节点多主题见 [pub-sub-tutorial.md](./pub-sub-tutorial.md)。

## 目标

完成本文档后，你将：

1. 编译并运行你的第一个 YomkRpc 发布订阅程序（同进程内发布 5 条 MString 消息并在订阅回调中收到）；
2. 用两个终端分别运行 `ExampleYomkRpcPub` / `ExampleYomkRpcSub`，观察跨进程发布/订阅；
3. 掌握两条关键注意事项：discovery 时序（发布前等待）与 type 所有权移交。

## 前提与准备

- 已按 [setup-and-build.md](./setup-and-build.md) 完成 YomkRpc 编译安装（能直接运行 `ExampleYomkRpcTopic`）；
- 一个可用的 C++17 + CMake（>= 3.14）工程环境；
- YomkRpc 示例统一使用 `YOMKRPC_*` 宏 API（定义于 `YomkRpcAPI.h`），均为完整可复制编译的程序，链接 `YomkRpc::YomkRpc YomkServer::YomkServer YomkRpcMsg`。

## 分步操作

### 第 1 步：准备工程与 CMake

新建工程目录，放入下文的 `first_pubsub.cpp`，并编写 `CMakeLists.txt`：

```cmake
cmake_minimum_required(VERSION 3.14)
project(first_pubsub LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

find_package(YomkServer REQUIRED)
find_package(YomkRpc REQUIRED)

# YomkRpcMsg 无导出包、以裸库名链接，需由扩展 Config 提供的 lib 目录补齐搜索路径
link_directories(${YomkRpc_LIB_DIR})

add_executable(first_pubsub first_pubsub.cpp)
target_link_libraries(first_pubsub PRIVATE YomkRpc::YomkRpc YomkServer::YomkServer YomkRpcMsg)
```

要点：链接 `YomkRpc::YomkRpc YomkServer::YomkServer YomkRpcMsg` 三个库目标；`YomkRpcMsg` 以裸库名链接，须保留 `link_directories(${YomkRpc_LIB_DIR})` 这一行（该写法与仓库 `examples/CMakeLists.txt` 一致）。

### 第 2 步：编写第一个发布订阅程序

`first_pubsub.cpp` 完整内容（同一节点内注册发布与订阅主题，发布 5 条 MString 消息并等待接收）：

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

逐段讲解：

| 片段 | 作用 |
|---|---|
| `YOMK_INIT()` + `YOMK_NEW_SERVICE(YomkRpcService)` | 初始化 YomkServer 框架并启动 RPC 服务（`YomkServer/YomkAPI.h` 提供）；每个使用 YomkRpc 宏 API 的程序开头都需要 |
| `YOMKRPC_VERSION()` | 版本查询宏，内部自动解包并打印，无返回值 |
| `YOMKRPC_NODE("node0")` | 创建节点，每节点一个独立 DDS 参与者；domainId 缺省取环境变量 `YOMKRPC_DDS_DOMAIN_ID`（未设置/为空/非法/超范围回落 0，合法范围 [0,232]） |
| `YOMKRPC_PUB_TOPIC(...)` / `YOMKRPC_SUB_TOPIC(...)` | 在节点上注册发布/订阅主题；type 实参为 `new XxxPubSubType()`，**所有权移交服务端，调用后 caller 不再持有/释放** |
| `YOMKRPC_SUB_TOPIC` 的 `onMessage` | 回调签名 `std::function<void(const void *data)>`，收到消息时被调用；`data` 是交付的消息实例指针，回调中转型使用（详见 [pub-sub-tutorial.md](./pub-sub-tutorial.md)） |
| `sleep_for(seconds(1))` 后再发布 | **discovery 时序**：DDS 参与者互相发现需要时间，发布前等待 discovery 完成；首条消息可能因 discovery 时序丢失 |
| `YOMKRPC_PUB_MSG("node0", "my_topic", &msg)` | 发布数据；data 为数据实例指针（借用，同步写入，caller 保留所有权） |
| `YOMKRPC_DEL_NODE("node0")` | 退出前删除节点，显式销毁 DDS 实体 |

除 `YOMKRPC_VERSION()` 外，其余宏均返回 `YomkResponse`，调用后须判 `m_status == YomkResponse::eOk`，失败时可读 `m_msg` 获取错误信息（上例为最小篇幅省略了判断，正式代码建议参照 `examples/ExampleYomkRpcTopic.cpp` 逐宏判错）。

### 第 3 步：编译并运行

```bash
cmake -B build -DCMAKE_PREFIX_PATH="$YOMK_PREFIX_PATH"
cmake --build build
./build/first_pubsub
```

### 第 4 步：双进程演示（跨进程发布/订阅）

安装包自带两个独立进程的参考程序（纯宏 API，编译后可直接运行）：

- `ExampleYomkRpcPub`：创建 `pub_node`，注册 `hello_world` 主题（MString），每隔 1s 发布一次，收到 Ctrl+C 退出信号后停止发布并干净退出；
- `ExampleYomkRpcSub`：创建 `sub_node`，订阅 `hello_world`，收到每条消息打印 `[RECV]` 内容，Ctrl+C 退出并打印累计接收条数。

另开两个终端分别运行即可观察跨进程发布/订阅（安装脚本已将扩展 lib 注册进系统动态库缓存，无需手动设置 `LD_LIBRARY_PATH`）：

```bash
# 终端 1（先启动订阅端）
ExampleYomkRpcSub
```

```bash
# 终端 2（再启动发布端）
ExampleYomkRpcPub
```

Ctrl+C 先停发布端，再停订阅端，订阅端会打印累计接收条数。

## 预期输出

### 同进程程序（first_pubsub）

运行后依次看到（时间戳等日志前缀以实际环境为准）：

- 版本号打印（`YOMKRPC_VERSION` 输出）；
- `[SEND] hello 0` 至 `[SEND] hello 4` 共 5 条发送日志；
- 交错的 `[RECV] hello 0` 至 `[RECV] hello 4` 接收日志；
- 结尾 `received=5/5`。

首条消息可能因 discovery 时序丢失，偶尔出现 `received=4/5`——这是示例特意保留的正常现象（见下方排障）。

### 双进程演示

- 终端 2（发布端）每秒打印一条 `[SEND] hello world N`（N 递增）；
- 终端 1（订阅端）同步打印 `[RECV] hello world N`；
- 发布端 Ctrl+C 后打印 `publish done: total=... failed=...` 退出；订阅端 Ctrl+C 后打印 `received total=N messages` 退出。

## 常见错误与排障

| 现象 | 原因与处理 |
|---|---|
| 订阅端一条消息都收不到 | 先查两端**域号**是否一致：`YOMKRPC_NODE` 的 domainId 缺省读环境变量 `YOMKRPC_DDS_DOMAIN_ID`（合法范围 [0,232]，默认 0），两个进程域号不同即互不可见，`echo $YOMKRPC_DDS_DOMAIN_ID` 对齐；再查 **discovery 是否完成**：注册订阅后立即发布，首条消息可能因 discovery 时序丢失，发布前 `sleep 1s`（本文示例已内置） |
| 双进程演示订阅端有输出、自己编译的程序收不到 | 检查两端主题名是否逐字节一致（本文示例为 `my_topic` / `hello_world`），以及 type 是否为同一种消息类型 |
| 编译报找不到 `YomkRpc/YomkRpcAPI.h` 等头文件 | CMake 未找到包：确认 `-DCMAKE_PREFIX_PATH="$YOMK_PREFIX_PATH"`，且 `find_package(YomkServer REQUIRED)` 与 `find_package(YomkRpc REQUIRED)` 均在 target 之前 |
| 链接报 `cannot find -lYomkRpcMsg` | `YomkRpcMsg` 无导出包、以裸库名链接：保留 `link_directories(${YomkRpc_LIB_DIR})` 一行（见第 1 步 CMake 片段） |
| 运行报 `error while loading shared libraries` | 动态库缓存未注册：按 [setup-and-build.md](./setup-and-build.md) 排障一节处理（检查 `/etc/ld.so.conf.d/yomk.conf` 并 `sudo ldconfig`） |
| 程序退出前崩溃或资源告警 | 未删除节点即退出：退出前调用 `YOMKRPC_DEL_NODE("node0")` 显式销毁 DDS 实体 |

## 延伸阅读

- [pub-sub-tutorial.md](./pub-sub-tutorial.md)——订阅回调与数据消费详解、消息类型定义（msg/ IDL）、多节点多主题
- [setup-and-build.md](./setup-and-build.md)——回看安装布局与示例程序清单
- [返回手册入口](./README.md)
