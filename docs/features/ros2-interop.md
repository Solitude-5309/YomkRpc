# 功能文档：ROS2 互通

> **本文档覆盖范围**：YomkRpc 与 ROS2 在同一 DDS 域内互收发同类型消息的实现机制（`set_name` 类型名直改）、互通前提与代码写法、互通三要素、限制与副作用。QoS 底座取值见 [qos-profiles.md](./qos-profiles.md)；节点/主题注册基础用法见 [core-communication.md](./core-communication.md)。

## 功能定位

YomkRpc 与 ROS2 在同一 DDS 域内互通（YomkRpc 节点发布、`ros2` 节点订阅，反之亦然）经**类型名直改**实现：构造类型实例后用 `set_name` 把注册类型名改成 ROS2 约定形态，再传给注册宏——链路内 `register_type` 以新名注册、发现数据 `type_name` 即新名，ROS2 侧同名匹配即通。

该能力是编程约定（不引入新宏/新端点），零额外运行时开销；代价与限制见下文「限制与注意事项」。

## 能力清单

| 能力 | 说明 |
|---|---|
| 类型名直改 | `set_name` 改为 ROS2 命名空间形态（如 `std_msgs::msg::dds_::String_`） |
| 与 ROS2 双向收发 | YomkRpc 发布 → `ros2` 订阅；`ros2 topic pub` → YomkRpc 订阅 |
| ROS2 默认 QoS 底座 | `DDSQosProfile::ros2DefaultPublisher()` / `ros2DefaultSubscriber()` |
| 真机回归脚本 | `./test/run_ros2_interop.sh`（双向收发判定） |

## 使用方式概述

互通写法（完整代码片段）：

```cpp
#include <YomkRpc/YomkRpcAPI.h>
#include <YomkRpcMsg/YomkRpcMsg.hpp>
#include <YomkRpcMsg/YomkRpcMsgPubSubTypes.hpp>

auto *type = new YomkRpc::MStringPubSubType();
type->set_name("std_msgs::msg::dds_::String_");   // ROS2 命名（须在移交前改）
YOMKRPC_NODE("interop_pub", 0);
// DDS 层话题名带 rt/ 前缀（rmw_fastrtps 对语义名 /chatter 的实际 DDS 名），QoS 用 ROS2 发布端默认底座
YOMKRPC_PUB_TOPIC("interop_pub", "rt/chatter", type, DDSQosProfile::ros2DefaultPublisher());
// YOMKRPC_PUB_MSG("interop_pub", "rt/chatter", &msg);   // 之后正常发布（话题名与注册名逐字节一致）
```

**使用注意**：`set_name` 必须在类型对象移交（传宏）之前调用——宏实参位内联 `new` 无法先改名。

示例（发布/订阅独立二进制，无命令行参数，均常驻运行、Ctrl+C/SIGTERM 优雅退出）：`examples/ExampleRos2InteropPub.cpp`（1Hz 持续发布 `rt/chatter_interop`）、`examples/ExampleRos2InteropSub.cpp`（持续监听 `rt/chatter_interop` 逐条打印，退出时打印总收条数）；两端同主题互为对端，也可与 ROS2 CLI 互通。

ROS2 侧验证：

```bash
ros2 topic echo /chatter std_msgs/msg/String
```

## 参数与约束

### 互通三要素（任一不一致即不匹配）

| 要素 | 说明 |
|---|---|
| 域 id | ROS2 默认域 0（`ROS_DOMAIN_ID`），YomkRpc 侧对齐（`YOMKRPC_DDS_DOMAIN_ID` 或宏显式 domainId） |
| topic 名 | 逐字节一致，且须为 **rmw_fastrtps 的 DDS 层话题名**——ROS2 语义名加 `rt/` 前缀（`ros2 topic pub /chatter` 的实际 DDS 名为 `rt/chatter`），YomkRpc 端直接用 `rt/chatter` 注册；无前缀名在 DDS 层与 ROS2 端永不匹配（真机实测：`/chatter_interop` 双向收不到，改 `rt/chatter_interop` 双向即通，见 run_ros2_interop.sh） |
| 类型名 + CDR 布局 | 类型名一致（`set_name` 直改为 `pkg::msg::dds_::Type_` 形态）且两端消息 CDR 编码布局一致（ROS2 msg 严格转 idl 生成即满足；字段结构不同的类型即使同名也不兼容） |

### 真机回归脚本

`./test/run_ros2_interop.sh`（ctest 树不含，手动执行；无参默认取 test 树构建产物 `test/build/Ros2Interop/TestRos2InteropPub|Sub`，可选传参覆盖；examples 侧示例仅面向用户教学，不承担判定职责）。两项判定均以**数据到达**为准并逐行列出两侧收发消息流：方向 1 YomkRpc pub 发 30 条 `hello ros2 N` → `ros2 topic echo` 6s 窗口收到多条；方向 2 `ros2 topic pub` 三路各 `-r 1` 发 `hello yomkrpc N`（N=1..3）→ YomkRpc sub 收满 3 条 `[RECV]` 即退。退出码 0=两项全过；前置 humble 环境与 test 树构建产物。

## 错误与边界行为（限制与注意事项）

副作用提示（按 README 原文逐条）：

- **原自名失效**：改名后原类型名（如 `YomkRpc::MString`）不再注册——同进程内 YomkRpc 原生话题与互通话题需两个类型实例两个名；
- **匹配语义**：ROS2 Humble 不注册 TypeObject、不发 TypeInformation，匹配退化为端点 `type_name` 字符串比较，改名全链路一致即通；Fast DDS 3.x 内部对跑：对称/不对称改名均匹配正常（两端均携带 TypeInformation，EDP 按 tid/TypeObject 兼容性匹配而 tid 与名字无关，实测 TestYomkRpcSetNaming S2 固化，不对称改名不隔离）；Jazzy+ 两端均发 TypeInformation 走类型一致性检查，未验证；
- **图可见性与数据匹配相互独立**：`ros2 topic list`/`node list` 看不到 YomkRpc 端点属正常（机制差异），互通判定以数据到达为准（run_ros2_interop.sh 两项实测 PASS）；
- **QoS 兼容**：两端默认 RELIABLE+VOLATILE 自然满足 requested≤offered；自定义 QoS 时注意匹配规则（如 RELIABLE 订阅端收不到 BEST_EFFORT 发布端），不当配置可能静默断链；真机实测口径：pub 端 `DDSQosProfile::ros2DefaultPublisher()`（RELIABLE offered）+ sub 端 `ros2DefaultSubscriber()`（BEST_EFFORT requested）对脚本两侧 `ros2 topic echo/pub` 全兼容；
- **bag 节点零影响**：bag 录制/回放为透传设计（按发现名动态建类型、payload 字节搬运），与类型名无关。

**根治路径**（可选）：idl 内直接用 ROS2 命名（`module std_msgs { module msg { struct String_ {...} } }`）重新生成——字面量从源头即新名，全链路一致，连 `set_name` 都不需要；代价是该类型以 ROS2 名存在（YomkRpc 原生名与 ROS2 名双类型并存需两份平行 idl）。禁止直接修改生成产物（重新生成必覆盖）。

不匹配的典型症状：双向收不到数据——先按三要素逐项排查（域号、`rt/` 前缀、类型名/CDR 布局）。

## 与其他功能的关系

- **QoS 配置**：互通推荐 ROS2 预置底座（`ros2DefaultPublisher()`/`ros2DefaultSubscriber()`/`ros2SensorData()`），8 项独立覆盖语义见 [qos-profiles.md](./qos-profiles.md)；
- **核心通信**：互通仅改类型名与话题名写法，节点创建/主题注册/发布订阅/回调消费机制完全不变，见 [core-communication.md](./core-communication.md)；
- **调试服务**：ROS2 rmw_fastrtps 端点不提供 XTypes TypeObject，`topic print` 对其会打类型不可重建告警、`interface show` 报 `type object for [<类型名>] not available`，见 [debug-service.md](./debug-service.md)；
- **Bag 服务**：录制/回放为透传设计，与类型名无关（互通场景可直接录制/回放），见 [bag-service.md](./bag-service.md)。

## 教程链接

- 分步实操（与 ros2 CLI 双向收发，含 yomkrpc 作为桥接观测）：[ROS2 互通教程](../manual/ros2-interop-tutorial.md)
