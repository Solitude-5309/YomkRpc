# 使用手册：yomkrpc 观察手册（print/list/info/hz/node）

> **本文档覆盖范围**：用 `yomkrpc` 命令行工具观察任意 DDS 域内的主题与节点——订阅直出消息（topic print）、测量频率（topic hz）、列出主题（topic list）、查询主题详情（topic info / topic type / topic find）、列出节点（node list）与查询节点发布/订阅清单（node info），以及观察类命令的 QoS 调参与收敛查询语义（README 6.1-6.6）。发布消息与类型内省（topic pub / interface show / interface list）见 [cli-publish-handbook.md](./cli-publish-handbook.md)。

## 目标

完成本文档后，你将：

1. 用 `yomkrpc topic print` 订阅任意主题并逐条直出消息 JSON（无需 IDL 生成代码，类型无关）；
2. 用 `yomkrpc topic hz` 测量主题接收频率，并懂得对接高频流时调整 QoS 防丢帧偏频；
3. 用 `topic list` / `topic info` / `topic type` / `topic find` 探明域内主题的类型名与收发端点数；
4. 用 `node list` / `node info` 探明域内命名参与者及其发布/订阅主题清单；
5. 理解收敛查询语义与 `YOMKRPC_DDS_DOMAIN_ID` / `YOMKRPC_DDS_DISCOVER_ROUNDS` 两个公共参数，能独立排查「看不到对方」类问题。

## 前提与准备

- 已按 [setup-and-build.md](./setup-and-build.md) 完成编译安装：`yomkrpc` 命令行工具位于 `<安装路径>/bin`，可直接运行；
- 观察是**类型无关**的：工具经 `YomkRpcDebugService` 调试服务接入 Fast DDS 发现机制，自动解析远端类型建立订阅或查询，**无需 IDL 生成代码**，可观察任意 DDS 域（含 ROS2 端点，见排障一节的类型重建限制）；
- 一个用于对照的发布端：本文全程以安装自带的 `ExampleYomkRpcPub`（节点 `pub_node`，以 1 Hz 发布 `hello_world` 主题）为被观察对象；
- **域号**由环境变量 `YOMKRPC_DDS_DOMAIN_ID` 指定（合法范围 [0,232]，默认 0）：yomkrpc 启动时无该变量则自动创建并默认 0，同时幂等写入 `~/.bashrc`（仅当其中无该变量时，带 `# added by yomkrpc` 注释便于识别）——新开任意终端可直接 `echo $YOMKRPC_DDS_DOMAIN_ID` 查看并自动继承；已打开的终端须 `source ~/.bashrc` 或重开才生效。临时换域用 `export YOMKRPC_DDS_DOMAIN_ID=5`，或用 CLI 显式选项 `-D/--domain <N>` 单次指定（优先于环境变量，详见 [env-vars-reference.md](./env-vars-reference.md)），与被观察端保持一致是「互相可见」的第一前提；
- 无多播/跨网段环境需接发现服务器时，配合三个直配环境变量 `YOMKRPC_USE_DISCOVER_SERVER` / `YOMKRPC_UDP_DISCOVER_SERVERS` / `YOMKRPC_TCP_DISCOVER_SERVERS` 使用，详见 [env-vars-reference.md](./env-vars-reference.md)；
- 验证域内可见性（可选）：

```bash
which yomkrpc                     # 确认工具在 PATH 中
echo $YOMKRPC_DDS_DOMAIN_ID       # 确认域号
```

## 分步操作

### 收敛查询语义与公共参数（先读，决定「命令何时输出」）

查询类命令（`topic list`、`topic info`、`topic type`、`topic find`、`interface show`、`interface list`、`topic pub -e`、`topic pub -ef`、`topic pub`、`node list` 与 `node info`）内部每 **~200ms** 轮询一次发现缓存快照，连续 **N 次**集合不变即认为发现收敛、立即输出——**无需固定等待窗口**，被观察端先上线则几乎瞬时出结果。

- 收敛次数 N 经环境变量 `YOMKRPC_DDS_DISCOVER_ROUNDS` 配置（默认 5，该变量为收敛次数唯一配置入口）：启动时无该变量则自动创建并默认 5，幂等写入 `~/.bashrc`（带 `# added by yomkrpc` 注释），已打开的终端须 `source ~/.bashrc` 或重开才生效；临时调整 `export YOMKRPC_DDS_DISCOVER_ROUNDS=7`（值须为 >=1 的整数，非法报错退出）；
- `topic print` / `topic hz` 为**持续订阅/测量型**，不使用收敛语义：一旦建订持续运行到 Ctrl+C；
- 域内端点少且稳定时默认 5 轮（约 1 秒）即输出；若发现频繁变动想多等一会儿再取快照，调大该值即可。

### topic print：订阅主题直出 JSON

- **用途**：订阅指定主题，消息 JSON 文本逐条直出控制台（每条消息一行状态头 + 一行 JSON 体）。
- **完整命令行**：

```bash
yomkrpc topic print [--qos-ros | --qos-KEY VAL ... | --qos-file FILE] <主题名>
```

- **关键选项**：reader QoS 经 `--qos-ros`/`--qos-*`/`--qos-file` 可调（缺省 `BEST_EFFORT+VOLATILE` depth 1），完整参数族见下文「观察命令的 QoS 参数」。
- **示例输出**：

```
[17:18:34.868398] topic=hello_world type=YomkRpc::MString seq=1
{"data":"hello world 0"}
```

- **登记后三类提示**（均为节点层直接输出，不经回调）：
  1. **主题尚未发现**：登记满约 3s 发现宽限窗后域内仍无该主题的 DataWriter（主题名拼错 / 发布者未上线 / 仅有订阅者）→ 一次性打印 `[FastDDSDebugNode] topic [<主题名>] not discovered yet, waiting for publisher...`（仅提示，不阻塞不改返回码；发布者上线后自动建订）；
  2. **主题已发现但类型不可重建**（典型为 ROS2 rmw_fastrtps 端点：编译期类型映射，不提供 XTypes TypeObject）→ 打印 `warning: TypeObject not ready, keep pending. topic=... type=... -- the remote endpoint does not publish XTypes TypeInformation/TypeObject ...`，首次立即打印、之后按 5s 节流重复；
  3. **登记被拒**：`register debug topic failed: subscribeTopic [<主题名>] failed: <具体原因>`（重复登记为 `topic [<主题名>] already registered`）。
- **等价宏名**：`YOMKRPC_DEBUG_NODE(0)` + `YOMKRPC_DEBUG_TOPIC_PRINT("hello_world", 回调)`（定义于 `YomkRpcAPI.h`，链接 `YomkRpc::YomkRpc YomkServer::YomkServer`，完整用户代码见 README 6.2 或 [../features/debug-service.md](../features/debug-service.md)）。

与发布端配合观察（另开两个终端）：

```bash
# 终端 1（先启动观察端）
yomkrpc topic print hello_world
# 终端 2（再启动发布端）
ExampleYomkRpcPub
```

Ctrl+C 退出。

### topic hz：测量主题接收频率

- **用途**：订阅主题测量接收频率（复用 topic print 的登记订阅链路，回调只记时间戳不打印消息），主循环每秒打印一次滚动窗口统计，输出形态对齐 `ros2 topic hz`。
- **完整命令行**：

```bash
yomkrpc topic hz [--window N] [--qos-ros | --qos-KEY VAL ... | --qos-file FILE] <主题名>
```

- **关键选项**：`--window` 为窗口样本数上限（相邻消息间隔样本数），**默认 10000**；reader QoS 经 `--qos-ros`/`--qos-*`/`--qos-file` 可调（缺省 `BEST_EFFORT+VOLATILE` depth 1，**对接高频流建议 `--qos-ros` 防 depth 1 丢帧偏频**）。
- **示例输出**：

```
average rate: 1.000
        min: 0.998s max: 1.004s std dev: 0.00209s window: 10
```

各输出项含义（对齐 ros2 topic hz）：`average rate` 为窗口内相邻消息间隔均值的倒数（Hz）；`min`/`max` 为间隔极值（秒）；`std dev` 为间隔的总体标准差（秒，除以 n）；`window` 为间隔样本数。无新消息不重复打印；首条消息前不打印统计；登记后的等待提示与类型不可重建告警同 `topic print`（见上一小节）；Ctrl+C 退出。
- **等价宏名**：无独立宏（经 `/topic_print` 端点实现，复用 topic print 登记链路，回调侧改记时间戳）。

### topic list：列出域内主题

- **用途**：一次性列出当前域内全部已发现主题，每行一个 topicName 按主题名排序。
- **完整命令行**：

```bash
yomkrpc topic list          # 默认域 0
yomkrpc topic list -t       # 类型名模式：每行输出 主题名 [类型名]
```

- **关键选项**：`-t`（`--types`）进入类型名模式，每行输出 `主题名 [类型名]`（单空格 + 方括号，对齐 `ros2 topic list -t` 形态，类型名原样输出）。
- **示例输出**：

```
hello_world
```

`-t` 类型名模式输出示例：

```
hello_world [YomkRpc::MString]
```

域内无任何已发布主题时输出 `no topics discovered on domain N`（types 模式同）。
- **等价宏名**：`YOMKRPC_DEBUG_TOPIC_LIST(5, 200)`；`-t` 模式对应 `YOMKRPC_DEBUG_TOPIC_LIST_T(收敛次数, 间隔ms)`（输出内容与 CLI 一致）。

建议先启动发布端再执行 `topic list`——工具创建调试节点入域后内部自适应收敛查询，无需固定等待窗口。

### topic info：查询主题详情

- **用途**：查询单个主题的发现详情，命中输出三行：消息类型名（原始 DDS 类型名，不做任何风格转换）、发布者数量、订阅者数量。
- **完整命令行**：

```bash
yomkrpc topic info hello_world        # 默认域 0
yomkrpc topic info -v hello_world     # 端点详情模式：逐端点列出 Node name/GUID/QoS profile
```

- **关键选项**：`-v`（`--verbose`）进入端点详情模式，在计数之外逐端点列出 Node name（归属参与者名）、Endpoint type（PUBLISHER/SUBSCRIPTION）、GUID（FastDDS 原生格式）与 QoS profile（ROS2 风格键值行）。
- **示例输出**（三行详情，类型名与计数直出）：

```
Type: YomkRpc::MString
Publisher count: 1
Subscription count: 0
```

`-v` 端点详情模式输出示例（count 为 0 的端点类型无清单段，多端点时逐端点块以空行分隔）：

```
Type: YomkRpc::MString

Publisher count: 1

Node name: pub_node
Endpoint type: PUBLISHER
GUID: 01.0f.b3.36.8d.c3.21.fa.00.00.00.00|0.0.1.3
QoS profile:
  History: not advertised
  Depth: not advertised
  Reliability: RELIABLE
  Durability: TRANSIENT_LOCAL
  Deadline: Infinite
  Lifespan: Infinite
  Liveliness: AUTOMATIC
  Liveliness lease duration: Infinite
  Latency Budget: 0 s
  Ownership: SHARED
  Ownership Strength: 0
  Destination Order: BY_RECEPTION_TIMESTAMP
  Partition: []

Subscription count: 0
```

QoS 键值行分两组：**ROS2 对齐组**恒输出前 8 行（History/Depth/Reliability/Durability/Deadline/Lifespan/Liveliness/Liveliness lease duration，发现数据未携带时输出 `not advertised` 兜底行——writer 端点在 RTPS 发现协议中不携带 history 字段，属协议层 optional 字段缺席，非「无 QoS」）；**DDS 扩展组**紧随其后（Latency Budget/Ownership/Ownership Strength/Time Based Filter/Destination Order/Partition 等，optional 项仅当发现数据携带时追加）。输出的全部是对端随发现数据（SPDP/SEDP）声明的 QoS，即「对端宣称值」而非本地配置。GUID 为 FastDDS 原生格式直出（12 字节前缀 | 4 字节实体 ID 点分十六进制）。

目标主题在收敛窗口内始终未被发现时报错退出（`topic [<主题名>] not found`，verbose 模式同）。
- **等价宏名**：`YOMKRPC_DEBUG_TOPIC_INFO("hello_world", 5, 200)`；`-v` 模式对应 `YOMKRPC_DEBUG_TOPIC_INFO_V(主题名, 收敛次数, 间隔ms)`。

### topic type：单主题类型名快捷查询

- **用途**：topic info 的单值快捷方式，输出单行裸类型名（去 `Type: ` 前缀，对齐 `ros2 topic type`，便于脚本 `$(...)` 取用）。
- **完整命令行**：

```bash
yomkrpc topic type hello_world        # 输出：YomkRpc::MString
```

- **关键选项**：无；未发现主题时报错退出（`topic [<主题名>] not found`，与 topic info 同语义）。
- **等价宏名**：`YOMKRPC_DEBUG_TOPIC_INFO`（取首行去前缀，复用同一 `/topic_info` 端点与收敛语义）。

### topic find：按类型名反查主题

- **用途**：按数据类型名精确匹配反查域内主题（独立 `/topic_find` 端点，收敛语义同 topic list），命中时每行输出一个主题名（按主题名排序，对齐 `ros2 topic find`）。
- **完整命令行**：

```bash
yomkrpc topic find YomkRpc::MString       # 输出：hello_world
```

- **关键选项**：无；类型名精确匹配（可发现类型名拼写错误），域内无该类型主题时报错退出（`type [<类型名>] not found`）。
- **等价宏名**：`YOMKRPC_DEBUG_TOPIC_FIND(typeName, stableRounds, intervalMs)`。

### node list：列出域内节点

- **用途**：独立收敛查询域内已发现的**命名参与者**（与主题查询完全分开的路径），每行一个节点名按名称排序。
- **完整命令行**：

```bash
yomkrpc node list          # 默认域 0
```

- **关键选项**：无。列出规则（了解后可解释「为什么有的进程没出现」）：
  - 仅列 `participant_name` 非空且非 `/` 的参与者——`/` 是 ROS2 参与者的默认占位名（rmw_fastrtps 把参与者名统一置为根 enclave `/`，其真实节点名走 `ros2 node list` 所依赖的另一通道）；
  - 调试节点自身不在自身发现回调中，天然不列出；同域其他进程的调试节点与 bag 节点分别以唯一名 `yomkrpc_debug_<uid>` / `yomkrpc_bag_<uid>` 列出（uid 为 12 位随机 hex）；
  - 节点名称由库用户在创建节点时经 `YOMKRPC_NODE(nodeName[, domainId[, serverLocator]])` 下沉到 DDS 参与者（随 SPDP 发现传播给同域对端）。
- **示例输出**（域内仅发布端一个命名参与者时恰一行）：

```
pub_node
```

域内暂无命名参与者时输出提示行 `no nodes discovered on domain <N>`。
- **等价宏名**：`YOMKRPC_DEBUG_NODE_LIST(5, 200)`。

### node info：查询节点发布/订阅清单

- **用途**：独立收敛查询指定节点名参与者的发布/订阅主题清单（归属判定基于 RTPS 规范保证的「端点 GUID 前缀 == 所属参与者 GUID 前缀」），输出形态对齐 `ros2 node info`：节点名行 + `Subscribers:` 段 + 每条订阅一行 `    <主题名>: <类型名>` + `Publishers:` 段同形态（类型名为原始 DDS 类型名，无任何风格转换；空段仅打段头）。
- **完整命令行**：

```bash
yomkrpc node info pub_node          # 默认域 0
```

- **关键选项**：无；同名多参与者的端点合并列出；节点名在收敛窗口内始终未被发现时报错退出（`node [<节点名>] not found`）。
- **示例输出**（发布端仅注册发布主题：Subscribers 段空、Publishers 段一行）：

```
pub_node
  Subscribers:
  Publishers:
    hello_world: YomkRpc::MString
```

- **等价宏名**：`YOMKRPC_DEBUG_NODE_INFO("pub_node", 5, 200)`。

### 观察命令的 QoS 参数（仅 topic print / topic hz 生效）

命令行形态 `[--qos-ros | --qos-KEY VAL ... | --qos-file FILE]`，三层叠加：

- **`--qos-ros`**：整体切 ROS2 底座 QoS——print/hz 端 KEEP_LAST(10)+BEST_EFFORT+VOLATILE（时间项 ∞）；与 `--qos-*`/`--qos-file` 叠加时作底座被逐项覆盖；
- **8 项逐项覆盖**（值大小写不敏感，ros2 小写与 DDS 大写均收）：
  - `--qos-reliability REL`：reliable | best_effort
  - `--qos-durability DUR`：transient_local | volatile
  - `--qos-history HIST`：keep_last | keep_all
  - `--qos-depth N`：history depth（须 >=1 整数；keep_all 时忽略；未设 `--qos-history` 时底座即 keep_last，直接生效）
  - `--qos-deadline SEC`：deadline 周期秒（非负小数，0=∞；下同）
  - `--qos-lifespan SEC`：lifespan 秒（writer 侧策略，print/hz 订阅端忽略）
  - `--qos-liveliness KIND`：automatic | manual_by_topic
  - `--qos-liveliness-lease-duration SEC`：liveliness 租约秒
- **`--qos-file FILE`**：从 JSON 文件加载逐项覆盖（扁平对象，键为 reliability/durability/history/depth/deadline/lifespan/liveliness/liveliness_lease_duration，值形态同对应 `--qos-*` 选项，如 `{"reliability":"reliable","depth":10,"deadline":0.5}`）
- **优先级**：命令行逐项 > 文件逐项 > `--qos-ros` 底座 > 默认（print/hz 缺省 BEST_EFFORT+VOLATILE depth 1）。

典型用法：

```bash
yomkrpc topic print --qos-ros hello_world                    # ROS2 底座订阅（KEEP_LAST(10) 防 ROS2 高频流丢样本）
yomkrpc topic hz --qos-ros high_rate_topic                   # 高频流测频：加大深度防 depth 1 丢帧偏频
yomkrpc topic print --qos-reliability best_effort hello_world
```

## 预期输出

完整观察会话（终端 1 先启动发布端并等 2-3 秒让发现传播稳定，终端 2 逐条执行；时间戳等前缀以实际环境为准）：

```bash
# 终端 1（先启动发布端）
ExampleYomkRpcPub
```

```bash
# 终端 2：主题与类型观察
yomkrpc topic list -t
# hello_world [YomkRpc::MString]

yomkrpc topic info hello_world
# Type: YomkRpc::MString
# Publisher count: 1
# Subscription count: 0

yomkrpc topic type hello_world
# YomkRpc::MString

yomkrpc topic find YomkRpc::MString
# hello_world
```

```bash
# 终端 2：节点观察
yomkrpc node list
# pub_node

yomkrpc node info pub_node
# pub_node
#   Subscribers:
#   Publishers:
#     hello_world: YomkRpc::MString
```

```bash
# 终端 2：数据流观察（持续型，Ctrl+C 退出）
yomkrpc topic print hello_world
# [17:18:34.868398] topic=hello_world type=YomkRpc::MString seq=1
# {"data":"hello world 0"}
# （每秒新增一条）

yomkrpc topic hz hello_world
# average rate: 1.000
#         min: 0.998s max: 1.004s std dev: 0.00209s window: 10
# （每秒刷新一行）
```

再开一个终端运行订阅端 `ExampleYomkRpcSub` 后重跑 `topic info hello_world`，可见 `Subscription count: 1`；`yomkrpc node list` 增加 `sub_node` 一行。

## 常见错误与排障

| 现象 | 原因与处理 |
|---|---|
| 所有查询命令都看不到对方（列表为空 / not found） | 域号不一致：被观察端与 yomkrpc 的 `YOMKRPC_DDS_DOMAIN_ID` 必须一致（合法范围 [0,232]，默认 0），两端分别 `echo $YOMKRPC_DDS_DOMAIN_ID` 对齐；已打开的终端改完环境变量须 `source ~/.bashrc` 或重开才生效 |
| `topic list` 输出为空（`no topics discovered on domain N`） | 域内确无任何已发布主题，或发布端尚未上线/发现尚未收敛：先启动发布端、稍等后重试；工具收敛查询无需固定等待，但发布端自身入域发现经 PDP/EDP 传播约需 1-3 秒；仍为空确认发布端域号一致、确有 DataWriter 在发 |
| `topic info`/`topic type`/`topic find` 报 `topic [<主题名>] not found` / `type [<类型名>] not found` | 主题名或类型名拼写错误，或目标端点不在当前域（先 `yomkrpc topic list -t` 核对实际名称）；收敛窗口内始终未发现即报错，重试或调大 `YOMKRPC_DDS_DISCOVER_ROUNDS` |
| `topic print` 只打印 not discovered yet，收不到数据 | `[FastDDSDebugNode] topic [<主题名>] not discovered yet, waiting for publisher...` 表示域内尚无该主题的 DataWriter：发布者未上线 / 主题名拼错 / 仅有订阅者；发布者上线后自动建订，无需重启观察端 |
| `topic print` 反复刷 TypeObject warning，收不到数据 | 对端未提供 XTypes TypeObject（**ROS2 rmw_fastrtps 端点即此情形**：编译期类型映射，不提供 TypeInformation/TypeObject）或 TypeLookup 尚未完成：提示按 5s 节流重复属预期，此类端点当前无法动态重建类型观察 |
| `topic print`/`topic hz` 收到了但条数/频率明显偏低 | depth 1 丢帧：观察端缺省 `BEST_EFFORT+VOLATILE` depth 1，对接高频流用 `--qos-ros`（切 KEEP_LAST(10) 底座）或 `--qos-depth N` 加大深度，必要时 `--qos-reliability best_effort` 显式声明 |
| `node list` 里看不到某个进程 | 该进程参与者名为空或为 `/`（ROS2 占位名不列出）；或该进程未创建命名节点；调试节点自身不列出（同域其他 yomkrpc 调试节点以 `yomkrpc_debug_<uid>`、bag 节点以 `yomkrpc_bag_<uid>` 列出） |
| `node info` 报 `node [<节点名>] not found` | 节点名在收敛窗口内始终未被发现：先 `yomkrpc node list` 核对实际节点名；发布端刚启动时等 2-3 秒再查 |
| 收敛次数想调整 / 改了不生效 | `YOMKRPC_DDS_DISCOVER_ROUNDS`（默认 5，值须为 >=1 的整数，非法报错退出）；改 `~/.bashrc` 中该行或临时 `export YOMKRPC_DDS_DISCOVER_ROUNDS=7`，已打开的终端须 `source ~/.bashrc` |
| 无多播/跨网段环境互相发现不了 | 接发现服务器：`export YOMKRPC_USE_DISCOVER_SERVER=true` 并填 `YOMKRPC_UDP_DISCOVER_SERVERS` 或 `YOMKRPC_TCP_DISCOVER_SERVERS`（格式 `ip[:port]` 分号分隔多 server），详见 [env-vars-reference.md](./env-vars-reference.md) |

## 延伸阅读

- [../features/debug-service.md](../features/debug-service.md)——调试服务能力全貌：调试节点模型、`YOMKRPC_DEBUG_*` 宏全集与 CLI 双入口对照
- [env-vars-reference.md](./env-vars-reference.md)——`YOMKRPC_DDS_DOMAIN_ID`、`YOMKRPC_DDS_DISCOVER_ROUNDS` 与发现服务器三环境变量的作用域、默认值与优先级
- [cli-publish-handbook.md](./cli-publish-handbook.md)——不写代码发布消息（topic pub）与类型内省（interface show/list），与本文档的观察命令构成调试闭环
- [返回手册入口](./README.md)
