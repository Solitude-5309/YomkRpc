# 功能文档：发现服务器（UDP/TCP 直配）

> **本文档覆盖范围**：发现服务器直配——多播发现受限（无多播环境）与跨网段场景下，把参与者的发现流量从多播改为单播直送发现服务器（本方为 CLIENT 角色）。覆盖三个节点创建宏的 UDP/TCP 定位串尾参、显式直配聚合串格式、环境变量门控直配、CLIENT 发现协议语义、TCPv4 传输注入、跨域互通模型与发现服务器启动脚本。XML profiles 打底配置见 [xml-profiles.md](./xml-profiles.md)；节点本身的创建/删除见 [core-communication.md](./core-communication.md)。

## 功能定位

Fast DDS 默认走**简单发现**（多播 PDP）：同网段参与者经多播互相发现，域隔离语义仅在多播路径生效。两类场景多播发现不可用或不够用：

- **无多播环境**：部分容器/云网络禁用多播，参与者互相不可见；
- **跨网段/跨域互通**：不同网段（乃至不同域号）的参与者需要互相发现并收发数据。

发现服务器解决这两类问题：用集中式发现服务器（server 角色）中转发现信息，参与方以 **CLIENT 发现协议**直配——禁用多播 PDP，发现流量单播直送 server。YomkRpc 在三个节点创建宏上提供 UDP/TCP 双通道直配尾参，并配套进程级环境变量门控（不改代码、容器/CI 批量注入友好）。

## 能力清单

| 能力 | 入口 |
|---|---|
| 三个节点创建宏的 UDP/TCP 发现服务器定位串尾参 | `YOMKRPC_NODE` / `YOMKRPC_DEBUG_NODE` / `YOMKRPC_BAG_NODE` |
| 显式直配聚合串（分号分隔 `ip[:port]` 多 server） | 宏尾参 `udpDiscoverServerLocator` / `tcpDiscoverServerLocator` |
| 环境变量门控直配（进程级，无需改代码） | `YOMKRPC_USE_DISCOVER_SERVER` + `YOMKRPC_UDP_DISCOVER_SERVERS` / `YOMKRPC_TCP_DISCOVER_SERVERS` |
| CLIENT 发现协议（禁用多播 PDP，发现流量单播直送 server） | 直配后自动生效 |
| TCP 通道自动补注入 TCPv4 传输 | TCP 直配串非空时自动 |
| 与 XML profiles 组合（XML 打底，直配 server 项覆盖） | 配合 `YOMKRPC_LOAD_XML` |
| 跨域互通（域号与 server 无关） | CLIENT 直配语义 |
| 发现服务器启动脚本 | `examples/LaunchDiscoverServer.sh` |

## 使用方式概述

### server 侧：启动发现服务器

任一台主机用 Fast DDS 自带工具启动（`-i` server id、`-l` 监听地址、`-p` 监听端口）：

```bash
fastdds discovery -i 0 -l 127.0.0.1 -p 11811
```

### client 侧方式一：宏尾参直配

三个节点创建宏的定位串尾参直配（UDP/TCP 通道格式一致）：

```cpp
// client 侧：三节点宏尾参直配（UDP/TCP 通道格式一致："ip[:port]" 分号分隔多 server，
// UDP port 缺省 11811，非法串创建失败回显）
YOMKRPC_NODE("ds_node", 0, "127.0.0.1:11811");            // 形态 3：仅 UDP 通道
YOMKRPC_DEBUG_NODE(0, "127.0.0.1:11811");
YOMKRPC_BAG_NODE(0, "127.0.0.1:11811");
// 形态 4：UDP+TCP 双通道（TCP port 为 server 的 TCP 监听口；仅 TCP 直配时 UDP 传空串占位）
YOMKRPC_NODE("ds_dual_node", 0, "127.0.0.1:11811", "127.0.0.1:42100");
YOMKRPC_NODE("ds_tcp_only_node", 0, "", "10.0.0.2:42100");
```

### client 侧方式二：环境变量自动直配

```bash
export YOMKRPC_USE_DISCOVER_SERVER=true
export YOMKRPC_UDP_DISCOVER_SERVERS="192.168.1.10:11811;192.168.1.11:11811"  # TCP 场景换 YOMKRPC_TCP_DISCOVER_SERVERS
```

开关开启时，`YOMKRPC_NODE` 缺省（省略）的 serverLocator 尾参自动取对应环境变量值；`YOMKRPC_DEBUG_NODE`/`YOMKRPC_BAG_NODE` 同款支持全缺省（无参）形态——三字段（含 domainId）一并经缺省初始化器读 env（domainId 读 `YOMKRPC_DDS_DOMAIN_ID`）。

另有 ROS2 生态通用等价形态：`ROS_DISCOVERY_SERVER=127.0.0.1:11811`（id 即 `-i` 指定的 server 位序）。

### 跨域互通示例（examples）

`ExampleDiscoverServerPub`（域 100 发布端）与 `ExampleDiscoverServerSub`（域 200 订阅端）节点创建不携带直配尾参——经环境变量自动直配，两者经同一发现服务器以 CLIENT 模式直配互通。三个终端按序运行：

```bash
# 终端 1：启动发现服务器（--type udp/tcp 可选缺省 udp；--ip/--port 可选，端口缺省随类型 11811/42100；前台运行 Ctrl+C 停止）
./examples/LaunchDiscoverServer.sh --ip 127.0.0.1 --port 11811

# 终端 2：订阅端（域 200，env 直配；export 两行可直接复制终端 1 启动横幅提示）
export YOMKRPC_USE_DISCOVER_SERVER=true
export YOMKRPC_UDP_DISCOVER_SERVERS="127.0.0.1:11811"
./examples/build/ExampleDiscoverServerSub

# 终端 3：发布端（域 100，env 直配，同组环境变量）
export YOMKRPC_USE_DISCOVER_SERVER=true
export YOMKRPC_UDP_DISCOVER_SERVERS="127.0.0.1:11811"
./examples/build/ExampleDiscoverServerPub
```

TCP 场景：终端 1 换 `--type tcp --port 42100` 启动，终端 2/3 换 export `YOMKRPC_TCP_DISCOVER_SERVERS="127.0.0.1:42100"`（开关不变）。`LaunchDiscoverServer.sh` 启动横幅打印当次传输类型对应的 export 提示（总开关 + 对应通道 server 串），可直接复制到终端 2/3。运行后发布端每秒打印 `[SEND] hello world N`，订阅端同步打印 `[RECV] hello world N`，Ctrl+C 优雅退出。

## 参数与约束

### 直配宏尾参

| 宏 | 直配尾参位置 | 说明 |
|---|---|---|
| `YOMKRPC_NODE(nodeName[, domainId[, udpDiscoverServerLocator[, tcpDiscoverServerLocator]]])` | 第 3/4 尾参 | 缺省尾参经类内缺省初始化器按开关门控读环境变量 |
| `YOMKRPC_DEBUG_NODE([domainId[, udpDiscoverServerLocator[, tcpDiscoverServerLocator]]])` | 第 2/3 尾参 | 语义同 `YOMKRPC_NODE` 形态 3/4（CLIENT 协议强制，非法串创建失败回显） |
| `YOMKRPC_BAG_NODE([domainId[, udpDiscoverServerLocator[, tcpDiscoverServerLocator]]])` | 第 2/3 尾参 | 同上 |

### 直配聚合串格式

| 项 | 约束 |
|---|---|
| 格式 | 分号分隔 `ip[:port]` 多 server |
| UDP port 缺省 | 11811 |
| port 合法范围 | [1,65535] |
| 非法串 | 创建失败回显（不静默回落） |

### 环境变量门控

| 环境变量 | 作用 | 缺省/关闭行为 |
|---|---|---|
| `YOMKRPC_USE_DISCOVER_SERVER` | 直配总开关 | 默认 `false`；`true`/`1` 视为开启（大小写不敏感）；未设置/空/`false`/`0`/其余任意值关闭 |
| `YOMKRPC_UDP_DISCOVER_SERVERS` | UDP 通道 server 聚合串（格式同宏尾参直配串） | 默认空；仅开关开启时读取，开关关闭时被忽略恒空（=不直配，行为同未设置） |
| `YOMKRPC_TCP_DISCOVER_SERVERS` | TCP 通道 server 聚合串（格式同上） | 同上 |

语义要点：

- 显式尾参覆盖对应通道 env 缺省（**按通道独立**——仅显式传 UDP 尾参时，TCP 通道缺省仍读 env，需关闭通道直配时显式传空串或保持开关关闭）；
- 非法串不静默回落——节点创建失败并回显，避免误配退化成多播发现而"看似正常实则不通"；
- 空串 = 不直配（该通道回落多播发现）；
- `yomkrpc` CLI 同款支持三环境变量（启动时无则创建默认值并幂等写入 `~/.bashrc`，仅可见性、行为同未设置），详见[环境变量与公共配置参考](../manual/env-vars-reference.md)。

### TCP 通道

| 项 | 说明 |
|---|---|
| port 含义 | server 的 TCP 监听口（非数据端口） |
| 传输注入 | 内置传输默认集不含 TCPv4——任一 TCP 直配项存在时 participant 自动补注入 TCPv4 传输描述符 |
| 与内置传输关系 | 与内置 UDPv4/SHM "alongside" 共存，UDP/SHM 数据通道不受影响 |
| 不重复注入 | XML 提取已注入 TCPv4 描述符时不重复注入 |
| 多 server | 两通道各自可分号分隔多 server，任一通道 server 在场即完成发现 |
| 双通道 | 两通道可同时使用；仅 TCP 直配时 UDP 传空串占位 |

### LaunchDiscoverServer.sh 参数

| 参数 | 说明 |
|---|---|
| `--type` | udp/tcp 可选，缺省 udp |
| `--ip` | 可选 |
| `--port` | 可选，端口缺省随类型 11811/42100 |

前台运行 Ctrl+C 停止。

## 错误与边界行为

- **直配串非法**：节点创建失败并回显，不静默回落多播发现；
- **开关关闭/串为空**：两串环境变量被忽略恒空，等于不直配，回落多播发现；
- **DS 模式下域号与 server 无关**：server 不做域校验（Fast DDS 源码 PDPClientListener/PDPServerListener："In Discovery Server we don't impose domain ids to be the same"；跨域丢弃仅存在于简单发现 PDPSimple 的基类校验）。本仓连通性测试实证：A/B 两节点异域（201/202）直配同一 server 后正常收发数据。**域隔离语义仅在简单发现（多播）路径生效**；
- **示例回落行为**：未配置环境变量时 `ExampleDiscoverServerPub`/`ExampleDiscoverServerSub` 回落多播发现，域 100/200 跨域不通属预期（缺省域隔离语义）。

## 与其他功能的关系

- **XML profiles**：直配可单独使用或配合 `YOMKRPC_LOAD_XML`——XML 打底，直配 server 项覆盖 XML `discoveryServersList`；叠加细节以 [xml-profiles.md](./xml-profiles.md) 为主责方；
- **调试/Bag 节点**：`YOMKRPC_DEBUG_NODE`、`YOMKRPC_BAG_NODE` 的直配尾参与环境变量语义与 `YOMKRPC_NODE` 完全一致，分别见 [debug-service.md](./debug-service.md) 与 [bag-service.md](./bag-service.md)；
- **域号**：client 域 id 经 `YOMKRPC_DDS_DOMAIN_ID` 或宏显式 domainId 指定，语义见 [core-communication.md](./core-communication.md)；
- **CLI 公共参数**：`yomkrpc` 各子命令创建调试/bag 节点时同款直配，环境变量族详见[环境变量与公共配置参考](../manual/env-vars-reference.md)。

## 教程链接

- 分步实操（启动 server → UDP/TCP 直配跨域 → 直配与 XML 打底组合）：[发现服务器与跨域互通教程](../manual/discovery-server-tutorial.md)
- 环境变量全集（门控开关、域号、收敛次数、`.bashrc` 幂等写入行为）：[环境变量与公共配置参考](../manual/env-vars-reference.md)
