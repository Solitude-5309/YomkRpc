# 使用手册：发现服务器与跨域互通教程

> **本文档覆盖范围**：在无多播、跨网段或跨域（domain id 不同）环境中，用发现服务器（Discovery Server）完成 YomkRpc 节点间的发现与互通——启动发现服务器（`examples/LaunchDiscoverServer.sh`）、UDP/TCP 直配（宏尾参显式直配与环境变量门控两种方式）、直配与 XML profiles 打底的组合使用，并用 examples 自带的跨域示例（域 100/200）验证端到端互通。功能原理与参数约束详见[功能文档](../features/discovery-server.md)。

## 目标

完成本文档后，你将：

1. 用 `LaunchDiscoverServer.sh` 启动一个 UDP 或 TCP 发现服务器；
2. 跑通跨域互通示例：发布端（域 100）与订阅端（域 200）经同一发现服务器持续收发数据；
3. 掌握三种直配方式：宏尾参显式直配、环境变量自动直配（进程级，无需改代码）、XML 打底加直配覆盖的组合。

## 前提与准备

- 已按 [setup-and-build.md](./setup-and-build.md) 完成 YomkRpc 编译安装：`source build_ubuntu.sh` 三步编译安装会一并构建示例程序，安装后 `<安装路径>/bin` 下有 `ExampleDiscoverServerPub`、`ExampleDiscoverServerSub` 与 `LaunchDiscoverServer.sh`；
- 如需按 README 5.8 的路径（`./examples/build/`）运行，可在仓库根目录单独构建 examples：

```bash
cmake -S examples -B examples/build -DCMAKE_PREFIX_PATH="$YOMK_PREFIX_PATH"
cmake --build examples/build
```

- 两个背景概念（详见 [../features/discovery-server.md](../features/discovery-server.md)）：
  - **多播发现的局限**：缺省简单发现依赖多播，无多播环境/跨网段不可用，且域 id 不同的节点互相隔离（域隔离语义仅在简单发现（多播）路径生效）；
  - **发现服务器模式**：直配后 participant 以 CLIENT 发现协议运行（禁用多播 PDP，发现流量单播直送 server）；DS 模式下 client 的域 id 与 server 无关——server 不做域校验（Fast DDS 源码 PDPClientListener/PDPServerListener："In Discovery Server we don't impose domain ids to be the same"），因此跨域互通可行（本仓连通性测试实证：A/B 两节点异域 201/202 直配同一 server 后正常收发数据）。

## 分步操作

### 第 1 步：启动发现服务器

任选一台主机（跨域演示用本机即可）运行启动脚本：

```bash
./examples/LaunchDiscoverServer.sh --ip 127.0.0.1 --port 11811
```

参数说明（全部可省略；前台运行，Ctrl+C 停止；server id 固定 `-i 0`）：

| 参数 | 含义 | 缺省 |
|---|---|---|
| `--type` | 监听协议 udp/tcp | udp |
| `--ip` | 监听地址 | 127.0.0.1 |
| `--port` | 监听端口 | 随 `--type`：udp→11811，tcp→42100 |

启动后打印横幅，其中包含可直接复制到客户端终端的环境变量提示（UDP 场景）：

```text
============================================================
 Fast DDS Discovery Server
   监听协议: UDP（-l 监听地址 / -p 监听端口）
   监听地址: 127.0.0.1:11811
   client 等价直配串: 127.0.0.1:11811
   client 环境变量自动直配（另开终端执行）：
     export YOMKRPC_USE_DISCOVER_SERVER=true
     export YOMKRPC_UDP_DISCOVER_SERVERS=127.0.0.1:11811
   （另开两个终端运行 ExampleDiscoverServerPub/Sub，
     域 id 100/200 跨域互通，详见 README 5.8）
============================================================
```

脚本内部按序探测 YomkServer/Fast DDS 安装产物中的 `fast-discovery-server` 工具并前台拉起；等价的手工命令为（`-i` server id、`-l` 监听地址、`-p` 监听端口）：

```bash
fastdds discovery -i 0 -l 127.0.0.1 -p 11811
```

### 第 2 步：UDP 直配跨域示例走读（域 100 与域 200 经 server 互通）

examples 提供一对发现服务器示例：`ExampleDiscoverServerPub`（域 100 发布端）与 `ExampleDiscoverServerSub`（域 200 订阅端）。两个示例的节点创建**不携带直配尾参**——直配串经环境变量自动填入（启动前 export，见第 4 步），两者经同一发现服务器以 CLIENT 模式互通（异域互通的原理见「前提与准备」）。

`YOMKRPC_NODE` 完整签名（四参形态，后三参均可缺省）：

```cpp
YOMKRPC_NODE(nodeName[, domainId[, udpDiscoverServerLocator[, tcpDiscoverServerLocator]]])
```

发布端关键代码（节选自 `examples/ExampleDiscoverServerPub.cpp`，完整程序见仓库源文件）：

```cpp
YOMK_INIT();
YOMK_NEW_SERVICE(YomkRpcService);

// 1. 创建节点：域 id 显式 100（DS 模式下可与对端异域）；省略双通道尾参 →
//    直配串自动取环境变量（总开关开启时生效，participant 以 CLIENT 发现协议运行）
auto resp = YOMKRPC_NODE("ds_pub_node", 100);
if (resp.m_status != YomkResponse::eOk)
{
    YOMK_ERROR_TAG("ExampleDiscoverServerPub", "create node failed: ", resp.m_msg);
    return 1;
}

// 2. 注册发布主题：type 所有权移交服务端，new 后无需自行 delete
resp = YOMKRPC_PUB_TOPIC("ds_pub_node", "ds_hello_world", new YomkRpc::MStringPubSubType());
```

发布主循环（每 1s 发布一次直到 Ctrl+C；`YOMKRPC_PUB_MSG` 失败计数）：

```cpp
while (!g_stop.load())
{
    YomkRpc::MString msg;
    msg.data("hello world " + std::to_string(total));
    resp = YOMKRPC_PUB_MSG("ds_pub_node", "ds_hello_world", &msg);
    // ... 成功打印 [SEND]，失败打印 [SEND] publish failed ...
    total++;
    std::this_thread::sleep_for(std::chrono::seconds(1));
}
```

订阅端关键代码（节选自 `examples/ExampleDiscoverServerSub.cpp`，仅域 id 与主题注册方式不同）：

```cpp
// 1. 创建节点：域 id 显式 200（DS 模式下可与对端异域）；省略双通道尾参 → 同上
auto resp = YOMKRPC_NODE("ds_sub_node", 200);

// 2. 注册订阅主题：type 所有权移交服务端；收到消息时回调 onMessage
std::atomic<int> received{0};
auto onMessage = [&](const void* data)
{
    auto* msg = static_cast<const YomkRpc::MString*>(data);
    received++;
    YOMK_INFO_TAG("ExampleDiscoverServerSub", "[RECV] ", msg->data());
};
resp = YOMKRPC_SUB_TOPIC("ds_sub_node", "ds_hello_world", new YomkRpc::MStringPubSubType(), onMessage);
```

两端主题均为 `ds_hello_world`（MString 类型）；三个终端的运行命令见「预期输出」一节。

### 第 3 步：TCP 直配（显式尾参写法）

不依赖环境变量、在代码里写死直配串时用宏尾参（UDP/TCP 通道格式一致：`"ip[:port]"` 分号分隔多 server，UDP port 缺省 11811，非法串创建失败回显）：

```cpp
// client 侧：三节点宏尾参直配
YOMKRPC_NODE("ds_node", 0, "127.0.0.1:11811");            // 形态 3：仅 UDP 通道
YOMKRPC_DEBUG_NODE(0, "127.0.0.1:11811");
YOMKRPC_BAG_NODE(0, "127.0.0.1:11811");
// 形态 4：UDP+TCP 双通道（TCP port 为 server 的 TCP 监听口；仅 TCP 直配时 UDP 传空串占位）
YOMKRPC_NODE("ds_dual_node", 0, "127.0.0.1:11811", "127.0.0.1:42100");
YOMKRPC_NODE("ds_tcp_only_node", 0, "", "10.0.0.2:42100");
```

TCP 通道机制（README 5.7 原文口径）：

- 内置传输默认集不含 TCPv4——任一 TCP 直配项存在时 participant **自动补注入 TCPv4 传输描述符**（与内置 UDPv4/SHM "alongside" 共存，UDP/SHM 数据通道不受影响；XML 提取已注入 TCPv4 描述符时不重复注入）；
- 两通道各自可分号分隔多 server，任一通道 server 在场即完成发现。

TCP 场景实操（与第 2 步同一对示例）：

```bash
# 终端 1：换 TCP 启动
./examples/LaunchDiscoverServer.sh --type tcp --port 42100
```

```bash
# 终端 2/3：export 换 TCP 通道串（总开关不变）
export YOMKRPC_USE_DISCOVER_SERVER=true
export YOMKRPC_TCP_DISCOVER_SERVERS="127.0.0.1:42100"
./examples/build/ExampleDiscoverServerSub   # 终端 2
./examples/build/ExampleDiscoverServerPub   # 终端 3
```

### 第 4 步：环境变量门控方式（进程级直配，无需改代码）

三环境变量控制节点创建时发现服务器直配缺省值（与显式尾参的关系见下表）：

```bash
export YOMKRPC_USE_DISCOVER_SERVER=true
export YOMKRPC_UDP_DISCOVER_SERVERS="192.168.1.10:11811;192.168.1.11:11811"  # TCP 场景换 YOMKRPC_TCP_DISCOVER_SERVERS
```

规则（README 5.7 原文口径）：

- 总开关 `YOMKRPC_USE_DISCOVER_SERVER`：`true`/`1` 视为开启（大小写不敏感）；未设置/空/`false`/`0`/其余任意值关闭；
- 两通道 server 聚合串 `YOMKRPC_UDP_DISCOVER_SERVERS`/`YOMKRPC_TCP_DISCOVER_SERVERS` 格式同宏尾参直配串：`ip[:port]` 分号分隔多 server；
- 开关开启时，`YOMKRPC_NODE` 缺省（省略）的 serverLocator 尾参自动取对应环境变量值；`YOMKRPC_DEBUG_NODE`/`YOMKRPC_BAG_NODE` 同款支持全缺省（无参）形态——三字段（含 domainId）一并经缺省初始化器读 env（domainId 读 `YOMKRPC_DDS_DOMAIN_ID`）；
- 开关关闭时两串变量被忽略恒空（=不直配，行为同未设置）；
- 非法串不静默回落——节点创建失败并回显，避免误配退化成多播发现而"看似正常实则不通"。

与显式参数的关系：

| 场景 | 行为 |
|---|---|
| 尾参显式传值 + 开关开启 | 显式尾参覆盖对应通道 env 缺省（**按通道独立**——仅显式传 UDP 尾参时，TCP 通道缺省仍读 env；需关闭通道直配时显式传空串或保持开关关闭） |
| 尾参缺省 + 开关开启 | 直配串自动取对应通道环境变量 |
| 尾参缺省 + 开关关闭 | 两串变量被忽略恒空（=不直配，行为同未设置） |

另有一个 ROS2 生态通用的等价环境变量形态：`ROS_DISCOVERY_SERVER=127.0.0.1:11811`（id 即 `-i` 指定的 server 位序）。全部环境变量的作用域与默认值见 [env-vars-reference.md](./env-vars-reference.md)。

### 第 5 步：直配与 XML 打底组合验证

直配可与 `YOMKRPC_LOAD_XML` 组合：**XML 打底，直配 server 项覆盖 XML `discoveryServersList`**。操作步骤：

1. 先准备一份 profiles XML 并在代码最前面加载（XML 的发现配置作为打底，编写与加载方法见 [xml-profiles-tutorial.md](./xml-profiles-tutorial.md)；注意 `YOMKRPC_LOAD_XML` 必须在任何节点创建之前调用）；
2. 再创建节点并带直配（显式尾参或 env 门控均可），例如：

```cpp
YOMKRPC_LOAD_XML("/path/to/profiles.xml");   // XML 打底
YOMKRPC_NODE("combo_node", 0, "127.0.0.1:11811");   // 直配 server 项覆盖 XML discoveryServersList
```

3. 验证点：发现流量走直配 server（CLIENT 单播），XML 中其余打底配置继续生效——按「预期输出」一节跑通发布/订阅即证明发现链路建立。

## 预期输出

三个终端按序运行（README 5.8）：

```bash
# 终端 1：启动发现服务器（--type udp/tcp 可选缺省 udp；--ip/--port 可选，端口缺省随类型 11811/42100；前台运行 Ctrl+C 停止）
./examples/LaunchDiscoverServer.sh --ip 127.0.0.1 --port 11811
```

```bash
# 终端 2：订阅端（域 200，env 直配；export 两行可直接复制终端 1 启动横幅提示）
export YOMKRPC_USE_DISCOVER_SERVER=true
export YOMKRPC_UDP_DISCOVER_SERVERS="127.0.0.1:11811"
./examples/build/ExampleDiscoverServerSub
```

```bash
# 终端 3：发布端（域 100，env 直配，同组环境变量）
export YOMKRPC_USE_DISCOVER_SERVER=true
export YOMKRPC_UDP_DISCOVER_SERVERS="127.0.0.1:11811"
./examples/build/ExampleDiscoverServerPub
```

正常输出（时间戳等日志前缀以实际环境为准）：

- 终端 1：第 1 步的启动横幅，进程常驻；
- 终端 2（订阅端）：启动打印 `subscribing ds_hello_world, press Ctrl+C to exit`，互通建立后随发布端逐条打印 `[RECV] hello world N`；
- 终端 3（发布端）：每秒打印一条 `[SEND] hello world N`（N 递增；发布失败时打印 `[SEND] publish failed: ...`）；
- Ctrl+C 停止：发布端打印 `publish done: total=N failed=M` 后退出（有失败时退出码 1）；订阅端打印 `received total=N messages` 后退出。

TCP 场景输出差异：终端 1 横幅中「监听协议」为 `TCP（-t 监听地址 / -q 监听端口）`、环境变量提示为 `export YOMKRPC_TCP_DISCOVER_SERVERS=127.0.0.1:42100`；终端 2/3 的 export 相应换成 `YOMKRPC_TCP_DISCOVER_SERVERS="127.0.0.1:42100"`（开关不变）。数据收发输出与 UDP 场景一致。

注意：未配置环境变量时两示例回落多播发现，域 100/200 跨域不通属预期（缺省域隔离语义）——这不是故障；配置正确后同机/跨机跨域即可互通。

## 常见错误与排障

| 现象 | 原因与处理 |
|---|---|
| 节点创建失败并回显（如 `create node failed: ...`） | 直配聚合串非法时不静默回落——节点创建失败并回显（避免误配退化成多播发现而"看似正常实则不通"）：检查串格式为 `ip[:port]` 分号分隔多 server，port 合法范围 [1,65535]（UDP 缺省 11811） |
| 订阅端一直无 `[RECV]`，跨域互通失败 | 先确认 server 未启动/已退出：回终端 1 看横幅与进程是否存活；再核对客户端直配串与 server 监听地址端口逐字节一致（可直接复制横幅的 client 等价直配串与环境变量提示）；最后确认两端 export 了同一组环境变量（含总开关） |
| 未 export 环境变量时域 100/200 不通 | 属预期：示例回落多播发现，缺省域隔离语义生效——按「预期输出」一节先 export 再运行 |
| 启动脚本报 `错误: 未找到 fast-discovery-server 工具（YomkServer/Fast DDS 安装产物）` | 按脚本提示设置 `YOMK_PREFIX_PATH` 指向安装路径，或将工具所在 bin 目录加入 PATH |
| 启动脚本报端口错误 | `--port` 须为 [1,65535] 内整数（脚本回显 `错误: --port 须为 [1,65535] 内整数，当前为 '...'`）；端口被其他进程占用时换一个 `--port` 重启，或结束占用进程 |
| 启动脚本报 `错误: --type 仅支持 udp 或 tcp，当前为 '...'` | `--type` 只接受 udp 或 tcp，修正后重跑 |
| 显式尾参与环境变量混用后某通道仍在直配 | 显式尾参覆盖对应通道 env 缺省是按通道独立的：仅显式传 UDP 尾参时，TCP 通道缺省仍读 env——需关闭该通道直配时显式传空串或保持总开关关闭 |
| TCP 直配后 UDP/SHM 数据通道异常 | 属误解：TCP 直配项存在时仅自动补注入 TCPv4 传输描述符，与内置 UDPv4/SHM "alongside" 共存，数据通道不受影响 |

## 延伸阅读

- [../features/discovery-server.md](../features/discovery-server.md)——发现服务器功能定位、直配串格式、CLIENT 协议与域号语义的完整参数约束
- [./env-vars-reference.md](./env-vars-reference.md)——`YOMKRPC_USE_DISCOVER_SERVER` 等 5 个环境变量的作用域、默认值与优先级总参考
- [./xml-profiles-tutorial.md](./xml-profiles-tutorial.md)——XML profiles 编写与加载教程（第 5 步 XML 打底的配套操作）
