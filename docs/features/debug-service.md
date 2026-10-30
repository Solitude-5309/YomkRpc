# 功能文档：调试服务（域内观察与消息发布）

> **本文档覆盖范围**：`YomkRpcDebugService` 调试服务与 `yomkrpc` 命令行观察/发布能力的完整说明——调试节点模型、类型无关的域内观察原理（主题观察/列表/详情、节点列表/详情、类型反查、类型内省）、发布辅助与 JSON 载荷发布、收敛查询语义与错误边界。各命令的逐条实操与输出示例见 [CLI 观察手册](../manual/cli-observe-handbook.md) 与 [CLI 发布与内省手册](../manual/cli-publish-handbook.md)。

## 功能定位

调试服务回答三个问题：**域内有什么**（主题/节点/类型）、**它长什么样**（端点详情/IDL 结构/发布示例）、**能否不发代码就发条消息**（JSON 载荷发布）。全程类型无关——经 FastDDS 发现机制自动解析远端类型，无需 IDL 生成代码。

两个入口，能力对齐：

- **宏 API**：`YOMKRPC_DEBUG_*` 宏（定义于 `YomkRpcAPI.h`，链接 `YomkRpc::YomkRpc YomkServer::YomkServer`），库用户在代码中调用；
- **CLI**：`yomkrpc topic/node/interface` 命令经 `YomkRpcDebugService` 实现，各命令小节附等价宏调用。

## 能力清单

按能力分组，每组给出宏与 CLI 的对照（CLI 命令形态与第 1 章宏 API 表均为权威口径）：

### 组 1：主题观察

| 入口 | 能力 | 关键行为 |
|---|---|---|
| 宏 `YOMKRPC_DEBUG_TOPIC_PRINT(topicName, output)` | 登记调试主题 | 发现匹配的远端 DataWriter 后自动解析类型建立订阅（类型无关，无需 IDL 生成代码），消息文本逐条投递 output（用户自定义回调，服务层不打印） |
| CLI `yomkrpc topic print [--qos-ros \| --qos-KEY VAL ... \| --qos-file FILE] <主题名>` | 订阅指定主题，消息 JSON 文本逐条直出控制台 | QoS 经 --qos-ros/--qos-*/--qos-file 可调（缺省 BEST_EFFORT+VOLATILE depth 1） |
| CLI `yomkrpc topic hz [--window N] [--qos-ros \| --qos-KEY VAL ... \| --qos-file FILE] <主题名>` | 订阅主题测量接收频率（每秒一行滚动窗口统计） | 复用 topic print 的登记订阅链路，回调只记时间戳不打印消息（无独立宏）；`--window` 为窗口样本数上限，默认 10000；对接高频流建议 --qos-ros 防 depth 1 丢帧偏频 |

### 组 2：主题/节点列表

| 入口 | 能力 | 关键行为 |
|---|---|---|
| 宏 `YOMKRPC_DEBUG_TOPIC_LIST(stableRounds, intervalMs)` | 列出域内主题 | 返回 StringArray 每行一个 topicName（仅主题名，不含类型）按主题名排序；远端 DataWriter 与 DataReader 均记录（仅有订阅者而无发布者的主题同样列出）；列表可为空（域内无 writer/reader） |
| 宏 `YOMKRPC_DEBUG_TOPIC_LIST_T(stableRounds, intervalMs)` | 列出域内主题与类型名 | 与 `YOMKRPC_DEBUG_TOPIC_LIST` 同端点同收敛语义，返回 StringArray 每行 `主题名 [类型名]`（单空格 + 方括号，类型名原样输出） |
| 宏 `YOMKRPC_DEBUG_NODE_LIST(stableRounds, intervalMs)` | 列出域内节点 | 独立收敛查询域内已发现的命名参与者（不依赖 list_topics/topic_info）；participant_name 非空且非 "/" 才列出（空名与 ROS2 参与者默认占位名 "/" 跳过，不输出 GUID 串），按名称排序；调试节点自身不在自身发现回调中，天然不列出；列表可为空 |
| CLI `yomkrpc topic list` / `yomkrpc topic list [-t]` | 列出域内全部已发现主题 | `-t`（`--types`）进入类型名模式，每行输出 `主题名 [类型名]`（对齐 `ros2 topic list -t` 形态） |
| CLI `yomkrpc node list` | 列出域内全部已发现的命名参与者 | 同域其他进程的调试节点与 bag 节点分别以唯一名 `yomkrpc_debug_<uid>` / `yomkrpc_bag_<uid>` 列出（uid 为 12 位随机 hex，多进程各自的调试/录制节点可区分） |

### 组 3：主题/节点详情

| 入口 | 能力 | 关键行为 |
|---|---|---|
| 宏 `YOMKRPC_DEBUG_TOPIC_INFO(topicName, stableRounds, intervalMs)` | 查询主题详情 | 独立收敛查询单个主题的发现详情（不依赖 list_topics）；命中返回 StringArray 三行：`Type: 原始 DDS 类型名`（不做任何风格转换）、`Publisher count: N`、`Subscription count: N` |
| 宏 `YOMKRPC_DEBUG_TOPIC_INFO_V(topicName, stableRounds, intervalMs)` | 查询主题端点详情 | 与 `YOMKRPC_DEBUG_TOPIC_INFO` 同端点同收敛语义，verbose=true：在计数外逐端点输出 Node name（归属参与者名）/ Endpoint type（PUBLISHER/SUBSCRIPTION）/ GUID（FastDDS 原生格式）/ QoS profile 详情段（count 为 0 的端点类型无清单段） |
| 宏 `YOMKRPC_DEBUG_NODE_INFO(nodeName, stableRounds, intervalMs)` | 查询节点详情 | 独立收敛查询指定节点名参与者的发布/订阅主题清单（归属判定基于 RTPS 规范保证的"端点 GUID 前缀 == 所属参与者 GUID 前缀"）；返回节点名行 + `Subscribers:`/`Publishers:` 两段（每行 `    <主题名>: <类型名>`，类型名原样输出，空段仅打段头，同名多参与者端点合并） |
| CLI `yomkrpc topic info <主题名>` / `yomkrpc topic info -v <主题名>` | 查询单个主题详情 | `-v`（`--verbose`）端点详情模式：逐端点列出 Node name/GUID/QoS profile（ROS2 风格键值行） |
| CLI `yomkrpc node info <节点名>` | 查询指定节点的发布/订阅主题清单 | 形态对齐 `ros2 node info` |

### 组 4：类型反查

| 入口 | 能力 | 关键行为 |
|---|---|---|
| 宏 `YOMKRPC_DEBUG_TOPIC_FIND(typeName, stableRounds, intervalMs)` | 按数据类型反查主题 | 独立收敛查询（语义同 list_topics），类型名精确匹配；命中返回 StringArray 每行一个主题名（按主题名排序），无匹配主题返回错误（可发现类型名拼写错误） |
| CLI `yomkrpc topic type <主题名>` | 查询单主题数据类型名 | topic info 的单值快捷方式（复用同一 `/topic_info` 端点与收敛语义），输出单行裸类型名（对齐 `ros2 topic type`，便于脚本取用） |
| CLI `yomkrpc topic find <类型名>` | 按数据类型名反查域内主题列表 | 精确匹配，对齐 `ros2 topic find` |

### 组 5：类型内省

| 入口 | 能力 | 关键行为 |
|---|---|---|
| 宏 `YOMKRPC_DEBUG_INTERFACE_SHOW(typeName, stableRounds, intervalMs)` | 按类型名输出 IDL 结构 | 经 TypeInformation → TypeObject → DynamicType 纯类型内省（不建订阅），命中返回 StringArray 多行 IDL 源语法（struct 头 + 字段行 + 结尾）；仅支持顶层为 struct 的类型 |
| 宏 `YOMKRPC_DEBUG_INTERFACE_LIST(stableRounds, intervalMs)` | 列出全部类型名 | 返回发现缓存中出现的全部类型名（去重字典序排序，每行一个，interface show 配套导航）；域内无任何类型返回错误 |
| CLI `yomkrpc interface show <类型名>` / `yomkrpc interface list` | 同上 | 输出内容与宏一致 |

### 组 6：发布辅助与 JSON 发布

| 入口 | 能力 | 关键行为 |
|---|---|---|
| 宏 `YOMKRPC_DEBUG_TOPIC_EXAMPLE(topicName, stableRounds, intervalMs)` | 查询主题发布示例 | 返回三段行集：`Type: <类型名>`、`IDL:` + IDL 行集、`example:` + 可复制发布命令（JSON 默认值模板整体包单引号，改字段值即可发布） |
| 宏 `YOMKRPC_DEBUG_TOPIC_MSG(topicName, stableRounds, intervalMs)` | 查询消息描述两要素 | 返回 StringArray 恰 2 行：d[0]=类型名、d[1]=紧凑 msg JSON（单行，可直接作为发布载荷模板，`topic pub -ef` 导出文件的数据源） |
| 宏 `YOMKRPC_DEBUG_TOPIC_PUB(topicName, json, stableRounds, intervalMs, repeatIntervalMs, requiredSubscribers, maxTimes[, qos])` | 发布 JSON 载荷消息 | 发布链路：首轮发现收敛 → 类型重建 + `json_deserialize` 解析载荷 → 临时 writer（QoS 按 qos 尾参，缺省 KEEP_LAST(1)+RELIABLE+TRANSIENT_LOCAL）匹配收敛 → write → 存在 RELIABLE 订阅者时 `wait_for_acknowledgments` 确认送达（未全部确认返回错误，全 BEST_EFFORT 或无订阅者退化尽力而为） |
| CLI `yomkrpc topic pub -e <主题名>` | 按主题名输出发布示例三段行集 | 类型名 / IDL / JSON 发布载荷模板 |
| CLI `yomkrpc topic pub -ef <主题名> [-o 目录]` | 按主题名导出消息描述 JSON 文件 | 内容即默认值模板 JSON 本身，多行缩进可直接 `$(cat)` 填进发布命令；仅发描述不发布 |
| CLI `yomkrpc topic pub [--qos-ros \| --qos-KEY VAL ... \| --qos-file FILE] <主题名> <JSON数据>` / `yomkrpc topic pub [...] <主题名> -f <文件>` | 发布 JSON 载荷消息（文件形态整体作为载荷） | 缺省 1Hz 持续发送，Ctrl+C 停止并输出统计；`-w N` 期望建匹配订阅端数、`-r N` 持续发布频率、`-t N` 条数上限（三者独立识别、顺序任意叠加），分别对应宏侧 requiredSubscribers/repeatIntervalMs/maxTimes；CLI 侧 qos 尾参经 `--qos-ros`/`--qos-*`/`--qos-file` 指定 |

### 退出

| 宏 | 能力 |
|---|---|
| `YOMKRPC_DEBUG_QUIT()` | 无参宏（载荷 nullptr）；删除调试节点并销毁其全部 DDS 实体，未创建时返回错误 |

## 使用方式概述

调试节点生命周期三步：创建调试节点 → 登记查询/登记主题/发布 → 退出前显式清理（规避 FastDDS 静态析构期段错误）。完整示例（对应 CLI `topic print` 的等价用户代码）：

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

收敛查询类宏的等价调用序列（以 `yomkrpc topic list` 为例，流程同构）：

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

CLI 侧一次运行即自动完成"创建调试节点 → 查询 → 删除"全程（如 `yomkrpc topic list`、`yomkrpc topic print hello_world`）。

## 参数与约束

### 调试节点参数（`YOMKRPC_DEBUG_NODE`）

| 参数 | 约束 |
|---|---|
| 形参形态 | 全可变参 `[domainId[, udpDiscoverServerLocator[, tcpDiscoverServerLocator]]]`；显式实参完全兼容旧调用 |
| 单节点模型 | 一个进程至多一个，重复创建须先删除 |
| domainId | 合法范围 [0,232]；仅同域节点的主题可被调试；缺省（无参形态）读环境变量 `YOMKRPC_DDS_DOMAIN_ID`（未设置/为空/非法/超范围回落 0） |
| 两 serverLocator | UDP/TCP 发现服务器直配聚合串，语义同 `YOMKRPC_NODE` 形态 3/4（CLIENT 协议强制，非法串创建失败回显），详见 [discovery-server.md](./discovery-server.md) |

### 收敛查询语义

查询类能力（列表/详情/反查/内省/示例/描述，及 `YOMKRPC_DEBUG_TOPIC_PUB` 的首轮发现收敛）共用同一收敛模型：

- 每 intervalMs（默认 200ms）轮询一次发现缓存快照，连续 stableRounds 次集合不变即返回；
- 0 值钳制默认 5 次/200ms，最长阻塞约 stableRounds*intervalMs；
- **宏侧**：stableRounds/intervalMs 为显式形参（如 `YOMKRPC_DEBUG_TOPIC_LIST(5, 200)`）；
- **CLI 侧**：收敛判定次数经环境变量 `YOMKRPC_DDS_DISCOVER_ROUNDS` 配置（默认 5；值须为 >=1 的整数，非法报错退出；yomkrpc 启动时无该变量则自动创建并默认 5，同时幂等写入 `~/.bashrc`）。生效命令：`topic list`、`topic info`、`topic type`、`topic find`、`interface show`、`interface list`、`topic pub -e`、`topic pub -ef`、`topic pub`、`node list` 与 `node info`；`topic print`/`topic hz` 为持续订阅/测量型不用。该环境变量为收敛次数唯一配置入口，详见[环境变量与公共配置参考](../manual/env-vars-reference.md)。

### `YOMKRPC_DEBUG_TOPIC_PUB` 专有参数

| 参数 | 语义 |
|---|---|
| `requiredSubscribers` | >0 为期望建匹配订阅端数门槛（matched 达标才发布否则一直等待，0=自动收敛；对应 CLI `-w/--wait N`） |
| `repeatIntervalMs` | >0 进入持续发布模式（首轮校验后按该周期重复 write 尽力而为，经 `yomk::debugPubStop` 无锁置位退出，再次发起持续发布前宜先 `yomk::debugPubReset()` 复位停止标志（防上次会话残留置位导致循环秒退）），对应 CLI `-r/--rate N`（CLI 侧频率换算：发送间隔 = round(1000/rate) ms，下限 1ms） |
| `maxTimes` | >0 为条数上限（发满排空 200ms 后返回；对应 CLI `-t/--times N`，含首轮） |
| `qos`（可缺省尾参） | 临时发布 writer 的 QoS 描述 `DDSQosProfile`，全哨兵=既有固定值 KEEP_LAST(1)+RELIABLE+TRANSIENT_LOCAL，ROS2 底座建议 `ros2DefaultPublisher()`；CLI 侧经 `--qos-ros`/`--qos-*`/`--qos-file` 指定 |

持续模式成功返回 StringArray {"total=N","failed=M"}。

## 错误与边界行为

- **前置约束**：查询/登记/发布类宏须先创建调试节点，失败时 `m_msg` 携具体拒绝原因；`YOMKRPC_DEBUG_QUIT()` 未创建时返回错误；
- **登记被拒**（`YOMKRPC_DEBUG_TOPIC_PRINT` 失败原因枚举）：`debug node not created` / `subscriber not created` / `empty topic name` / `already registered`；CLI 侧输出形态为 `register debug topic failed: subscribeTopic [<主题名>] failed: <具体原因>`（重复登记为 `topic [<主题名>] already registered`）；
- **延迟登记语义**：登记为发现驱动的延迟行为，主题尚未被发布者上线时仍返回成功（登记约 3s 后仍无发布者则打一次性等待提示 `[FastDDSDebugNode] topic [<主题名>] not discovered yet, waiting for publisher...`——仅提示，不阻塞不改返回码，发布者上线后自动建订）；
- **类型不可重建告警**：主题已发现但类型不可重建（典型为 ROS2 rmw_fastrtps 端点：编译期类型映射，不提供 XTypes TypeObject）时打 `warning: TypeObject not ready, keep pending. topic=... type=... -- the remote endpoint does not publish XTypes TypeInformation/TypeObject ...`——首次立即打印、之后按 5s 节流；同类告警覆盖 DynamicType 构建失败 / `create_topic` 失败 / `create_datareader` 失败 / 建订异常四类；
- **查询未命中**：`topic [<主题名>] not found`（topic info/-v/type）、`type [<类型名>] not found`（topic find）、`node [<节点名>] not found`（node info）；
- **空域行为**：主题列表可为空（域内无 writer/reader），节点列表可为空（域内无有效命名参与者）；CLI 对应提示 `no topics discovered on domain N` / `no nodes discovered on domain <N>`；`interface list` 域内无任何类型返回错误；
- **`interface show` 失败分层**（错误文本按卡点归类）：

| 错误文本 | 卡点 |
|---|---|
| `type [<类型名>] not found` | 类型名未出现在发现缓存（拼写错误或端点不在本域） |
| `type object for [<类型名>] not available` | 对端未提供 XTypes TypeObject（**ROS2 rmw_fastrtps 端点即此情形**）或 TypeLookup 尚未完成 |
| `type [<类型名>] is not a struct, interface display unsupported` | 顶层为 enum/alias 等非 struct 类型 |
| `interface introspection failed for [<类型名>]` | DynamicType 成员枚举失败 |

- **`topic pub -e`/`-ef` 失败分层**（按节点层三步骤归类：步骤 1 发现 → 步骤 2 类型重建 → 步骤 3 示例生成）：

| 错误文本 | 卡点 |
|---|---|
| `topic [<主题名>] not found` | 步骤 1：主题在收敛窗口内始终未被发现（主题名拼错 / 未加 `rt/` 前缀 / 对端未发布 / 域号不一致） |
| `type [<类型名>] not found` | 步骤 2：类型名未入发现缓存（`interface show` 同款语义） |
| `type object for [<类型名>] not available` | 步骤 2：对端未提供 XTypes TypeObject（**ROS2 rmw_fastrtps 端点即此情形**）或 TypeLookup 尚未完成 |
| `type [<类型名>] is not a struct, interface display unsupported` | 步骤 2：顶层非 struct，IDL 段无法拼装 |
| `type rebuild failed for [<类型名>]` | 步骤 3：DynamicType 构建失败 |
| `example generation failed for [<类型名>]: create_data` / `: json_serialize` | 步骤 3：默认值样本创建或 JSON 序列化失败 |

- **`topic pub` 发布失败**：未发现主题/载荷不合法/送达未确认返回错误，错误文本枚举：`invalid json for type [...]`（非法 JSON）、`not all subscribers acknowledged`（未全部确认送达）、`subscribers exist but not all matched (suspended or offline)`（订阅者在场却无一与发布端匹配）、`cannot open json file`/`json file is empty`（`-f` 文件问题）、`非法频率 "..."`/`非法订阅者数量 "..."`/`非法发布条数 "..."`/`waiting for subscribers interrupted` 等。ack 确认覆盖语义：RELIABLE 订阅者运行中 → ack 协议级确认送达；中途挂起 → ack 超时报错退出不假成功；发布端启动前已离线的订阅者无从期待、照常返回成功（DDS 发现机制的客观边界）；BEST_EFFORT 订阅者不参与 ack 与 matched 校验，始终尽力而为。

> 除 `YOMKRPC_VERSION()` 外，其余宏均返回 `YomkResponse`，调用后须判 `m_status == YomkResponse::eOk`；失败时可读 `m_msg` 获取错误信息。

## 与其他功能的关系

- **核心通信**：调试服务是独立服务域（`YomkRpcDebugService`），与业务节点互不干扰；节点名经 `YOMKRPC_NODE(nodeName[, domainId[, serverLocator]])` 下沉到 DDS 参与者（随 SPDP 发现传播给同域对端），`node list`/`node info` 查到的即这些参与者，见 [core-communication.md](./core-communication.md)；
- **发现服务器**：调试节点支持 UDP/TCP 直配尾参（同域才可调试，跨网段调试须经 server），见 [discovery-server.md](./discovery-server.md)；
- **QoS**：`topic print`/`topic hz`/`topic pub` 的 QoS 参数族（`--qos-ros`/`--qos-*`/`--qos-file` 优先级链）与宏侧 `DDSQosProfile` 语义见 [qos-profiles.md](./qos-profiles.md)；
- **ROS2 互通**：ROS2 rmw_fastrtps 端点不提供 XTypes TypeObject，观察其主题会触发类型不可重建告警、`interface show` 报 `type object for [...] not available`（互通判定以数据到达为准），见 [ros2-interop.md](./ros2-interop.md)；
- **Bag 服务**：bag 节点（`YOMKRPC_BAG_NODE`）模型与调试节点同构（单节点、全可变参、直配尾参），见 [bag-service.md](./bag-service.md)。

## 教程链接

- 观察类命令逐条实操（print/list/info/type/find/hz、node list/info，含示例输出与 QoS 参数对接高频流）：[CLI 观察手册](../manual/cli-observe-handbook.md)
- 发布与内省实操（topic pub -e/-ef/直发/-f、-w/-r/-t 组合、interface show/list 排查类型、JSON 载荷构造）：[CLI 发布与内省手册](../manual/cli-publish-handbook.md)
- 环境变量全集（域号、收敛次数、发现服务器门控、`.bashrc` 幂等写入行为）：[环境变量与公共配置参考](../manual/env-vars-reference.md)
