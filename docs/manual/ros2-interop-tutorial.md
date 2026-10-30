# 使用手册：ROS2 互通教程

> **本文档覆盖范围**：让 YomkRpc 程序与 ROS2 节点（rclcpp / ros2 CLI）就同名类型互相收发——互通前提（互通三要素与 QoS 建议）、YomkRpc 侧 `set_name` 类型名直改代码走读（examples 两示例）、ROS2 侧用 ros2 CLI 观察与反向发布验证、预期输出，以及类型名/QoS/域号三类不通的排障。功能原理与限制详见[功能文档](../features/ros2-interop.md)。

## 目标

完成本文档后，你将：

1. 跑通方向 1：YomkRpc 发布端持续发布，ROS2 侧 `ros2 topic echo` 逐条收到；
2. 跑通方向 2：ROS2 侧 `ros2 topic pub` 持续发布，YomkRpc 订阅端逐条收到；
3. 掌握互通三要素（域 id、话题名、类型名 + CDR 布局）与 `set_name` 直改的时机约束。

## 前提与准备

- YomkRpc 已按 [setup-and-build.md](./setup-and-build.md) 编译安装，examples 已构建（构建方式同 [discovery-server-tutorial.md](./discovery-server-tutorial.md) 前提一节）：

```bash
cmake -S examples -B examples/build -DCMAKE_PREFIX_PATH="$YOMK_PREFIX_PATH"
cmake --build examples/build
```

- 已安装 ROS2 并 source 环境（README 真机回归以 Humble 为验证口径；Ubuntu 22.04 示例：`sudo apt install ros-humble-desktop`）：

```bash
source /opt/ros/humble/setup.bash
```

- 消息类型对应：示例使用 `YomkRpc::MString` 与 ROS2 `std_msgs/msg/String`——两者同为单 string 字段，CDR 编码布局一致（ROS2 msg 严格转 idl 生成即满足布局前提）；
- **互通三要素**（任一不一致即不匹配，README 5.6 原文）：

| 要素 | 说明 |
|---|---|
| 域 id | ROS2 默认域 0（`ROS_DOMAIN_ID`），YomkRpc 侧对齐（`YOMKRPC_DDS_DOMAIN_ID` 或宏显式 domainId） |
| topic 名 | 逐字节一致，且须为 **rmw_fastrtps 的 DDS 层话题名**——ROS2 语义名加 `rt/` 前缀（`ros2 topic pub /chatter` 的实际 DDS 名为 `rt/chatter`），YomkRpc 端直接用 `rt/chatter` 注册；无前缀名在 DDS 层与 ROS2 端永不匹配（真机实测：`/chatter_interop` 双向收不到，改 `rt/chatter_interop` 双向即通，见 run_ros2_interop.sh） |
| 类型名 + CDR 布局 | 类型名一致（`set_name` 直改为 `pkg::msg::dds_::Type_` 形态）且两端消息 CDR 编码布局一致（ROS2 msg 严格转 idl 生成即满足；字段结构不同的类型即使同名也不兼容） |

- **QoS 建议**（README 5.6 真机实测口径）：pub 端 `DDSQosProfile::ros2DefaultPublisher()`（RELIABLE offered）+ sub 端 `ros2DefaultSubscriber()`（BEST_EFFORT requested），对脚本两侧 `ros2 topic echo/pub` 全兼容；自定义 QoS 时注意匹配规则（如 RELIABLE 订阅端收不到 BEST_EFFORT 发布端），不当配置可能静默断链。

## 分步操作

### 第 1 步：YomkRpc 侧代码走读（发布端）

机制：构造类型实例后用 `set_name` 把注册类型名改成 ROS2 约定形态，再传给注册宏——链路内 `register_type` 以新名注册、发现数据 `type_name` 即新名，ROS2 侧同名匹配即通。发布端关键代码（摘自 `examples/ExampleRos2InteropPub.cpp`，1Hz 持续发布直至 Ctrl+C/SIGTERM）：

```cpp
YOMK_INIT();
YOMK_NEW_SERVICE(YomkRpcService);

// 1. 创建节点（域 0 对齐 ROS2 默认域）
auto resp = YOMKRPC_NODE("interop_pub", 0);
if (resp.m_status != YomkResponse::eOk)
{
    YOMK_ERROR_TAG("ExampleRos2InteropPub", "create node failed: ", resp.m_msg);
    return 1;
}

// 2. 类型对象 new 后、移交前 set_name 成 ROS2 约定名（宏实参位内联 new 无法先改名）
auto *type = new YomkRpc::MStringPubSubType();
type->set_name("std_msgs::msg::dds_::String_");
// QoS 用 ROS2 发布端默认底座：RELIABLE offered 对 ros2 topic echo 任意 requested 可匹配
resp = YOMKRPC_PUB_TOPIC("interop_pub", "rt/chatter_interop", type,
                         DDSQosProfile::ros2DefaultPublisher());
if (resp.m_status != YomkResponse::eOk)
{
    YOMK_ERROR_TAG("ExampleRos2InteropPub", "register pub topic failed: ", resp.m_msg);
    YOMKRPC_DEL_NODE("interop_pub");
    return 1;
}

// 3. 1Hz 持续发布（Ctrl+C/SIGTERM 置位 g_stop 后跳出，清理退出）
YomkRpc::MString msg;
msg.data("interop hello " + std::to_string(seq++));
resp = YOMKRPC_PUB_MSG("interop_pub", "rt/chatter_interop", &msg);
```

两个关键约束：

- `set_name` 必须在类型对象**移交（传宏）之前**调用——宏实参位内联 `new` 无法先改名；
- **原自名失效**：改名后原类型名（如 `YomkRpc::MString`）不再注册——同进程内 YomkRpc 原生话题与互通话题需两个类型实例两个名。

### 第 2 步：YomkRpc 侧代码走读（订阅端）

订阅端关键代码（摘自 `examples/ExampleRos2InteropSub.cpp`，持续监听逐条打印，Ctrl+C/SIGTERM 优雅退出）：

```cpp
// 创建节点（域 0 对齐 ROS2 默认域）
auto resp = YOMKRPC_NODE("interop_sub", 0);

// 类型对象 new 后、移交前 set_name 成 ROS2 约定名（宏实参位内联 new 无法先改名）
std::atomic<int> received{0};
auto* type = new YomkRpc::MStringPubSubType();
type->set_name("std_msgs::msg::dds_::String_");
auto onMessage = [&](const void* data)
{
    auto* msg = static_cast<const YomkRpc::MString*>(data);
    received++;
    YOMK_INFO_TAG("ExampleRos2InteropSub", "[RECV] ", msg->data());
};
// QoS 用 ROS2 订阅端默认底座：BEST_EFFORT requested 对任意 offered（含 ros2 topic
// pub 与 YomkRpc pub 的 RELIABLE）均可匹配
resp =
    YOMKRPC_SUB_TOPIC("interop_sub", "rt/chatter_interop", type, onMessage, DDSQosProfile::ros2DefaultSubscriber());
```

### 第 3 步：方向 1 验证——YomkRpc 发布、ROS2 订阅

```bash
# 终端 1（ROS2 环境）
source /opt/ros/humble/setup.bash
ros2 topic echo /chatter_interop std_msgs/msg/String
```

```bash
# 终端 2
./examples/build/ExampleRos2InteropPub
```

ROS2 侧观察注意：`ros2 topic list`/`node list` 看不到 YomkRpc 端点属正常（机制差异）——图可见性与数据匹配相互独立，互通判定以数据到达为准。

### 第 4 步：方向 2 验证——ROS2 发布、YomkRpc 订阅

```bash
# 终端 1（先启动订阅端：VOLATILE 不回放，订阅端先行匹配后再发布保证送达）
./examples/build/ExampleRos2InteropSub
```

```bash
# 终端 2（ROS2 环境）
source /opt/ros/humble/setup.bash
ros2 topic pub /chatter_interop std_msgs/msg/String "{data: hello ros2}" -r 1
```

### 自动化回归（可选）

README 5.6 提供真机回归脚本 `./test/run_ros2_interop.sh`（ctest 树不含，手动执行；无参默认取 test 树构建产物 `test/build/Ros2Interop/TestRos2InteropPub|Sub`，可选传参覆盖）：两项判定均以数据到达为准并逐行列出两侧收发消息流——方向 1 YomkRpc pub 发 30 条 `hello ros2 N` 后 `ros2 topic echo` 在 6s 窗口收到多条；方向 2 `ros2 topic pub` 三路各 `-r 1` 发 `hello yomkrpc N`（N=1..3），YomkRpc sub 收满 3 条 `[RECV]` 即退；退出码 0 = 两项全过；前置 humble 环境与 test 树构建产物。

## 预期输出

- 方向 1：终端 2（YomkRpc 发布端）每秒打印 `[SEND] interop hello 0`、`[SEND] interop hello 1` …（发布失败时打印 `[SEND] publish failed`）；终端 1 `ros2 topic echo` 逐条输出 `data:` 行（如 `data: interop hello 0`）；
- 方向 2：终端 1（YomkRpc 订阅端）逐条打印 `[RECV] hello ros2`；Ctrl+C 退出时打印 `[QUIT] received total=N`；
- 两个示例均常驻运行，Ctrl+C/SIGTERM 优雅退出（发布端退出前显式销毁节点，无崩溃告警）。

## 常见错误与排障

| 现象 | 原因与处理 |
|---|---|
| 双向都收不到 | 按互通三要素逐一核对：域 id（ROS2 侧 `echo $ROS_DOMAIN_ID`，YomkRpc 侧 `echo $YOMKRPC_DDS_DOMAIN_ID` 或查宏显式 domainId，两端须一致，ROS2 默认 0）；话题名（须为 rmw_fastrtps 的 DDS 层话题名——ROS2 语义名加 `rt/` 前缀，逐字节一致；无前缀名在 DDS 层与 ROS2 端永不匹配）；类型名（`set_name` 是否生效且与 ROS2 侧一致） |
| 类型名不匹配 | `set_name` 未调用、名字拼错或时机错误——必须在类型对象移交（传宏）之前调用，宏实参位内联 `new` 无法先改名；名字须为 ROS2 约定形态 `pkg::msg::dds_::Type_` |
| RELIABLE 订阅端收不到 BEST_EFFORT 发布端 | QoS 不兼容：匹配规则 requested≤offered——自定义 QoS 时注意匹配规则（如 RELIABLE 订阅端收不到 BEST_EFFORT 发布端），不当配置可能静默断链；建议先用示例口径（pub `ros2DefaultPublisher()` + sub `ros2DefaultSubscriber()`，对 `ros2 topic echo/pub` 全兼容） |
| 类型名一致仍收不到 | 两端消息 CDR 编码布局不一致：字段结构不同的类型即使同名也不兼容——ROS2 msg 严格转 idl 生成即满足（示例的 MString 与 std_msgs/msg/String 同为单 string 字段） |
| 同进程内 YomkRpc 原生话题失效 | 原自名失效副作用：改名后原类型名（如 `YomkRpc::MString`）不再注册——原生话题与互通话题需两个类型实例两个名 |
| `ros2 topic list`/`node list` 看不到 YomkRpc 端点 | 属正常（机制差异）：图可见性与数据匹配相互独立，互通判定以数据到达为准 |
| ROS2 侧 `ros2 topic pub` 发出后 YomkRpc 订阅端收不到 | 先启动订阅端再发布：VOLATILE 不回放历史，订阅端先行匹配后再发布保证送达（方向 2 验证步骤已按此排序） |

## 延伸阅读

- [../features/ros2-interop.md](../features/ros2-interop.md)——set_name 直改原理、匹配语义与副作用（原自名失效/图可见性）的完整说明
- [./qos-tutorial.md](./qos-tutorial.md)——DDSQosProfile 8 项独立设置、ROS2 三个预置底座的选择与逐项覆盖
- [返回手册入口](./README.md)
