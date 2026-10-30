# 功能文档：QoS 配置

> **本文档覆盖范围**：端点 QoS 描述 `DDSQosProfile` 的 8 项配置与哨兵/底座/覆盖三层语义、三个 ROS2 预置底座；宏侧 qos 可缺省尾参（`YOMKRPC_PUB_TOPIC` / `YOMKRPC_SUB_TOPIC` / `YOMKRPC_DEBUG_TOPIC_PUB`）与 `yomkrpc` CLI 侧 `--qos-ros` / `--qos-*` / `--qos-file` 参数族的用法与优先级链。

## 功能定位

QoS（Quality of Service）决定 DDS 端点的可靠性、持久化、历史深度等行为，直接影响发布端与订阅端能否匹配、消息是否丢失。YomkRpc 在两侧提供同一套 8 项 QoS 配置能力：编程侧经 `DDSQosProfile` 尾参随主题注册下发；命令行侧经 `yomkrpc` topic 命令的 QoS 参数族在不改代码的情况下调整 QoS。

## 能力清单

| 能力 | 入口 | 说明 |
|---|---|---|
| 8 项 QoS 独立设置 | `DDSQosProfile`（宏 qos 尾参）；CLI `--qos-KEY VAL` | history/depth/reliability/durability/liveliness/deadline/lifespan/liveliness lease；未设置项为哨兵保持底座值 |
| ROS2 预置底座 | `DDSQosProfile::ros2DefaultPublisher()` / `ros2DefaultSubscriber()` / `ros2SensorData()`；CLI `--qos-ros` | 整包切换底座，可在其上逐项覆盖 |
| JSON 文件逐项覆盖 | CLI `--qos-file FILE` | 扁平对象，键为 8 项 |

## 使用方式概述

### 宏侧（编程）

qos 为 `YOMKRPC_PUB_TOPIC` / `YOMKRPC_SUB_TOPIC` / `YOMKRPC_DEBUG_TOPIC_PUB` 的可缺省尾参，传入 `DDSQosProfile` 实例即可。摘自 README 5.6 的发布端用法（ROS2 互通场景使用 ROS2 发布端默认底座）：

```cpp
// DDS 层话题名带 rt/ 前缀（rmw_fastrtps 对语义名 /chatter 的实际 DDS 名），QoS 用 ROS2 发布端默认底座
YOMKRPC_PUB_TOPIC("interop_pub", "rt/chatter", type, DDSQosProfile::ros2DefaultPublisher());
```

底座上逐项覆盖（如只设 depth=10，其余项保持底座值；字段定义见公共头 `include/YomkRpcService.h` 的 `DDSQosProfile`）：

```cpp
DDSQosProfile qos = DDSQosProfile::ros2DefaultPublisher();
qos.depth = 10;   // 未设置项为哨兵保持底座值，仅覆盖 depth
YOMKRPC_PUB_TOPIC("node0", "my_topic", new YomkRpc::MStringPubSubType(), qos);
```

### CLI 侧（yomkrpc 命令行）

QoS 参数族仅 `topic pub` / `topic print` / `topic hz` 生效，命令行形态 `[--qos-ros | --qos-KEY VAL ... | --qos-file FILE]`：

```bash
# 整体切 ROS2 底座 QoS
yomkrpc topic print --qos-ros <主题名>
# 底座上逐项覆盖（--qos-ros 与 --qos-* 叠加时，逐项覆盖底座）
yomkrpc topic pub --qos-ros --qos-reliability reliable <主题名> <JSON数据>
# 从 JSON 文件加载逐项覆盖
yomkrpc topic pub --qos-file qos.json <主题名> <JSON数据>
```

`--qos-file FILE` 从 JSON 文件加载逐项覆盖——扁平对象，键为 reliability/durability/history/depth/deadline/lifespan/liveliness/liveliness_lease_duration，值形态同对应 `--qos-*` 选项（README 示例）：

```json
{"reliability":"reliable","depth":10,"deadline":0.5}
```

## 参数与约束

### 三层语义

1. **缺省全哨兵**：qos 尾参缺省（或 8 项均未设置）时保持 Fast DDS 端点默认底座（现状行为）。
2. **ROS2 预置底座**（整包切换，8 项全非哨兵）：
   - `DDSQosProfile::ros2DefaultPublisher()`：ROS2 默认底座（KEEP_LAST(10)+RELIABLE+VOLATILE+AUTOMATIC+∞×3）。
   - `DDSQosProfile::ros2DefaultSubscriber()`：KEEP_LAST(10)+BEST_EFFORT+VOLATILE+AUTOMATIC+∞×3；订阅端推荐（BEST_EFFORT requested 对任意 offered 均可匹配）。
   - `DDSQosProfile::ros2SensorData()`：传感器档（KEEP_LAST(5)+BEST_EFFORT+VOLATILE+AUTOMATIC+∞×3）。
3. **底座上逐项覆盖**：在底座（缺省底座或 ROS2 底座）上只设置需要的项，未设置项为哨兵保持底座值（如只设 depth=10）。

### 8 项参数表

逐项覆盖的值大小写不敏感，ros2 小写与 DDS 大写均收。宏侧字段名为公共头 `include/YomkRpcService.h` 中 `DDSQosProfile` 的成员（未设置哨兵为 `Unset` / `-1`）：

| 配置项 | 宏侧字段 | CLI 选项 | 取值（CLI） | 说明 |
|---|---|---|---|---|
| history | `historyKind` | `--qos-history HIST` | keep_last \| keep_all | 历史种类 |
| depth | `depth` | `--qos-depth N` | 须 >=1 整数 | history depth；keep_all 时忽略；未设 `--qos-history` 时底座即 keep_last，直接生效 |
| reliability | `reliability` | `--qos-reliability REL` | reliable \| best_effort | 可靠性 |
| durability | `durability` | `--qos-durability DUR` | transient_local \| volatile | 持久化 |
| liveliness | `liveliness` | `--qos-liveliness KIND` | automatic \| manual_by_topic | 存活确认种类 |
| deadline | `deadlineNs` | `--qos-deadline SEC` | 非负小数，0=∞ | deadline 周期秒 |
| lifespan | `lifespanNs` | `--qos-lifespan SEC` | 非负小数，0=∞ | lifespan 秒（writer 侧策略，print/hz 订阅端忽略） |
| liveliness lease | `livelinessLeaseNs` | `--qos-liveliness-lease-duration SEC` | 非负小数，0=∞ | liveliness 租约秒 |

### 宏侧约束

- qos 为可缺省尾参：`YOMKRPC_PUB_TOPIC(nodeName, topicName, type[, qos])`、`YOMKRPC_SUB_TOPIC(nodeName, topicName, type, callback[, qos])`、`YOMKRPC_DEBUG_TOPIC_PUB(topicName, json, stableRounds, intervalMs, repeatIntervalMs, requiredSubscribers, maxTimes[, qos])`。
- `YOMKRPC_DEBUG_TOPIC_PUB` 的 qos：临时发布 writer 的 QoS 描述 `DDSQosProfile`，全哨兵=既有固定值 KEEP_LAST(1)+RELIABLE+TRANSIENT_LOCAL，ROS2 底座建议 `ros2DefaultPublisher()`。
- 订阅端（`YOMKRPC_SUB_TOPIC`）推荐 `DDSQosProfile::ros2DefaultSubscriber()`（BEST_EFFORT requested 对任意 offered 均可匹配）。

### CLI 侧约束

- 仅 `topic pub` / `topic print` / `topic hz` 生效。
- `--qos-ros`：整体切 ROS2 底座 QoS——pub 端 KEEP_LAST(10)+RELIABLE+VOLATILE，print/hz 端 KEEP_LAST(10)+BEST_EFFORT+VOLATILE（时间项 ∞）；与 `--qos-*`/`--qos-file` 叠加时作底座被逐项覆盖。
- `--qos-file FILE`：JSON 扁平对象（键为 8 项，值形态同对应 `--qos-*` 选项）。
- **优先级链**：命令行逐项 > 文件逐项 > `--qos-ros` 底座 > 默认（pub 临时 writer 缺省 RELIABLE+TRANSIENT_LOCAL depth 1；print/hz 缺省 BEST_EFFORT+VOLATILE depth 1）。

## 错误与边界行为

- **depth 取值**：须 >=1 整数；keep_all 时被忽略。
- **lifespan 端点方向**：writer 侧策略，print/hz 订阅端忽略（设置了不生效）。
- **匹配方向**：QoS 匹配遵循 requested≤offered——`ros2DefaultSubscriber()` 的 BEST_EFFORT requested 对任意 offered 均可匹配；反之 RELIABLE 订阅端收不到 BEST_EFFORT 发布端，不当配置可能静默断链。
- **哨兵即「未设置」**：未设置项不会覆盖底座；全哨兵等价于不传 qos 尾参（Fast DDS 端点默认底座）。
- **CLI 多来源取值**：同项被多处设置时按优先级链取值（命令行逐项 > 文件逐项 > `--qos-ros` 底座 > 默认）。

## 与其他功能的关系

- 宏侧 qos 尾参挂在主题注册上（`YOMKRPC_PUB_TOPIC` / `YOMKRPC_SUB_TOPIC`），节点与发布订阅本身见 [core-communication.md](./core-communication.md)。
- CLI 侧 QoS 参数族属于 `yomkrpc` 的 `topic pub` / `topic print` / `topic hz` 命令，命令全集见 [debug-service.md](./debug-service.md)。

## 教程链接

- [QoS 配置教程](../manual/qos-tutorial.md)：ROS2 底座选择、逐项覆盖示例、--qos-file 编写。
