# 使用手册：QoS 配置教程

> **本文档覆盖范围**：为发布/订阅端点配置 QoS——三层语义（缺省底座、ROS2 预置底座、逐项覆盖）的宏 API 实操、8 项 QoS 参数逐项说明、`yomkrpc` 命令行侧的 QoS 参数（`--qos-ros` / `--qos-*` / `--qos-file`）与优先级链、QoS 匹配常识与「收不到消息先查 QoS」的排障方法。XML 文件级集中配置（`YOMKRPC_LOAD_XML`）不在本文档范围内。

## 目标

完成本文档后，你将能够：

1. 理解并选用三层 QoS 语义：什么都不传（Fast DDS 默认底座）、预置底座（`ros2DefaultPublisher()` / `ros2DefaultSubscriber()` / `ros2SensorData()`）、在底座上逐项覆盖；
2. 在宏 API 与 `yomkrpc` 命令行两侧按需配置 8 项 QoS 参数；
3. 用 QoS 匹配常识排查「订阅端收不到消息」一类问题。

## 前提与准备

- 已按 [setup-and-build.md](./setup-and-build.md) 完成安装，并跑通 [quick-start.md](./quick-start.md)；
- 知道注册宏的可缺省尾参：`YOMKRPC_PUB_TOPIC(nodeName, topicName, type[, qos])` 与 `YOMKRPC_SUB_TOPIC(nodeName, topicName, type, callback[, qos])` 的 `qos` 为端点 QoS 描述 `DDSQosProfile`（定义于 `include/YomkRpcService.h`，头文件已随 `YomkRpc/YomkRpcAPI.h` 引入）。

## 分步操作

### 第 1 步：理解三层语义

1. **缺省（不传 qos）**：全哨兵 = 保持 Fast DDS 端点默认底座（现状行为）——quick-start 中的示例即此形态；
2. **预置底座**：`DDSQosProfile` 提供三个 ROS2 底座工厂：
   - `DDSQosProfile::ros2DefaultPublisher()`——发布端 ROS2 默认底座（KEEP_LAST(10)+RELIABLE+VOLATILE+AUTOMATIC+∞×3）；
   - `DDSQosProfile::ros2DefaultSubscriber()`——订阅端 ROS2 默认底座（KEEP_LAST(10)+BEST_EFFORT+VOLATILE+AUTOMATIC+∞×3）；
   - `DDSQosProfile::ros2SensorData()`——传感器档（KEEP_LAST(5)+BEST_EFFORT+VOLATILE+AUTOMATIC+∞×3，两端通用）；
3. **逐项覆盖**：在底座上仅改某几项（如只设 depth=10），未设置项保持底座值。

底座选取的匹配依据（requested<=offered 匹配方向）：publisher RELIABLE offered 对任意 ROS2 侧 requested 可匹配；subscriber BEST_EFFORT requested 对任意 offered 可匹配（RELIABLE 版订阅 BEST_EFFORT 话题不匹配收不到）。

### 第 2 步：宏 API 实操（完整可编译示例）

发布端用 `ros2DefaultPublisher()` 底座并逐项覆盖 depth=10，订阅端用 `ros2DefaultSubscriber()`（BEST_EFFORT requested 对任意 offered 均可匹配）：

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

    // 2. 创建节点（domainId 缺省取环境变量 YOMKRPC_DDS_DOMAIN_ID）
    YOMKRPC_NODE("qos_node");

    // 3. 注册订阅主题：ROS2 默认订阅底座（BEST_EFFORT requested 对任意 offered 均可匹配）
    std::atomic<int> received{0};
    auto onMessage = [&](const void *data)
    {
        auto *msg = static_cast<const YomkRpc::MString *>(data);
        received++;
        YOMK_INFO_TAG("QosExample", "[RECV] ", msg->data());
    };
    YOMKRPC_SUB_TOPIC("qos_node", "qos_topic", new YomkRpc::MStringPubSubType(), onMessage,
                      DDSQosProfile::ros2DefaultSubscriber());

    // 4. 注册发布主题：ROS2 默认发布底座上逐项覆盖（如只设 depth=10）
    DDSQosProfile pubQos = DDSQosProfile::ros2DefaultPublisher();
    pubQos.depth = 10;
    YOMKRPC_PUB_TOPIC("qos_node", "qos_topic", new YomkRpc::MStringPubSubType(), pubQos);

    // 5. 等待 DDS discovery 完成后发布
    std::this_thread::sleep_for(std::chrono::seconds(1));
    for (int i = 0; i < 5; ++i)
    {
        YomkRpc::MString msg;
        msg.data("hello " + std::to_string(i));
        YOMKRPC_PUB_MSG("qos_node", "qos_topic", &msg);
        YOMK_INFO_TAG("QosExample", "[SEND] ", msg.data());
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    // 6. 等待接收完成后删除节点
    std::this_thread::sleep_for(std::chrono::seconds(1));
    YOMK_INFO_TAG("QosExample", "received=", received.load(), "/5");
    YOMKRPC_DEL_NODE("qos_node");
    return 0;
}
```

只想要传感器档时，把两端 qos 尾参换成 `DDSQosProfile::ros2SensorData()` 即可（KEEP_LAST(5)+BEST_EFFORT，两端通用）。

### 第 3 步：8 项 QoS 参数逐项说明

`DDSQosProfile` 的 8 项（history/depth/reliability/durability/liveliness/deadline/lifespan/liveliness lease）可独立设置，未设置项为哨兵保持底座值：

| 参数项 | 含义 | DDSQosProfile 成员 | CLI 选项与取值（仅 topic pub / topic print / topic hz 生效） |
|---|---|---|---|
| history | 历史策略 | `historyKind`（`DDSHistoryKind::KEEP_LAST` / `KEEP_ALL`） | `--qos-history HIST`：keep_last \| keep_all |
| depth | KEEP_LAST 的 history depth（KEEP_ALL 时忽略） | `depth` | `--qos-depth N`：须 >=1 整数；keep_all 时忽略；未设 `--qos-history` 时底座即 keep_last，直接生效 |
| reliability | 可靠性 | `reliability`（`DDSReliability::RELIABLE` / `BEST_EFFORT`） | `--qos-reliability REL`：reliable \| best_effort |
| durability | 持久性 | `durability`（`DDSDurability::TRANSIENT_LOCAL` / `VOLATILE`） | `--qos-durability DUR`：transient_local \| volatile |
| liveliness | 存活判定的报告方式 | `liveliness`（`DDSLiveliness::AUTOMATIC` / `MANUAL_BY_TOPIC`） | `--qos-liveliness KIND`：automatic \| manual_by_topic |
| deadline | deadline 周期 | `deadlineNs`（纳秒，-1=未设置=底座默认） | `--qos-deadline SEC`：deadline 周期秒（非负小数，0=∞；下同） |
| lifespan | 消息生存期（writer 侧策略，reader 翻译时忽略；CLI 侧 print/hz 订阅端忽略） | `lifespanNs`（纳秒，-1=未设置=底座默认） | `--qos-lifespan SEC`：lifespan 秒 |
| liveliness lease | liveliness 租约时长 | `livelinessLeaseNs`（纳秒，-1=未设置=底座默认） | `--qos-liveliness-lease-duration SEC`：liveliness 租约秒 |

CLI 侧取值大小写不敏感（ros2 小写与 DDS 大写均收）。

### 第 4 步：命令行侧 QoS 配置（yomkrpc）

topic 命令 QoS 参数仅 `topic pub` / `topic print` / `topic hz` 生效，命令行形态为 `[--qos-ros | --qos-KEY VAL ... | --qos-file FILE]`：

```bash
# 缺省：print/hz 缺省 BEST_EFFORT+VOLATILE depth 1
yomkrpc topic print hello_world

# 整体切 ROS2 底座：pub 端 KEEP_LAST(10)+RELIABLE+VOLATILE，print/hz 端 KEEP_LAST(10)+BEST_EFFORT+VOLATILE（时间项 ∞）
yomkrpc topic print hello_world --qos-ros

# 底座上逐项覆盖（如 depth 10）
yomkrpc topic print hello_world --qos-ros --qos-depth 10

# 对接高频流测量频率：建议 --qos-ros 防 depth 1 丢帧偏频
yomkrpc topic hz --qos-ros hello_world

# 从 JSON 文件加载逐项覆盖
yomkrpc topic print hello_world --qos-file qos.json

# 发布端指定 QoS（临时 writer 缺省 RELIABLE+TRANSIENT_LOCAL depth 1）
yomkrpc topic pub --qos-ros hello_world '{"data":"hello yomkrpc"}'
```

`--qos-file` 的 JSON 为扁平对象，键为 reliability/durability/history/depth/deadline/lifespan/liveliness/liveliness_lease_duration，值形态同对应 `--qos-*` 选项：

```json
{"reliability":"reliable","depth":10,"deadline":0.5}
```

优先级链：**命令行逐项 > 文件逐项 > `--qos-ros` 底座 > 默认**（pub 临时 writer 缺省 RELIABLE+TRANSIENT_LOCAL depth 1；print/hz 缺省 BEST_EFFORT+VOLATILE depth 1）。

### 第 5 步：QoS 匹配常识

两端 QoS 按 requested<=offered 方向裁定，收不到消息时先按下表核对两端组合：

| 发布端 offered | 订阅端 requested | 结果 |
|---|---|---|
| RELIABLE | RELIABLE 或 BEST_EFFORT | 匹配 |
| BEST_EFFORT | BEST_EFFORT | 匹配 |
| BEST_EFFORT | RELIABLE | **不匹配（收不到）** |

- 这就是「发布端用 `ros2DefaultPublisher()`（RELIABLE offered）+ 订阅端用 `ros2DefaultSubscriber()`（BEST_EFFORT requested）全兼容」的原因；
- durability 同样按 requested<=offered 方向裁定：TRANSIENT_LOCAL offered 满足任意 requested；TRANSIENT_LOCAL requested 无法从 VOLATILE offered 端得到满足——两端 durability 需按此方向核对（`--qos-durability` 的 transient_local | volatile 两值即对应此维度）；
- 自定义 QoS 时注意匹配规则，不当配置可能静默断链。

## 预期输出

运行第 2 步的示例程序（时间戳前缀以实际环境为准）：

- `[SEND] hello 0` 至 `[SEND] hello 4` 共 5 条发送日志；
- 交错的 `[RECV] hello 0` 至 `[RECV] hello 4` 接收日志；
- 结尾 `received=5/5`（首条消息可能因 discovery 时序丢失，偶尔为 4/5）。

命令行侧验证（另开终端，与示例程序同域）：

```bash
yomkrpc topic info qos_topic
```

应输出主题详情三行（类型名 / 发布者数 / 订阅者数），其中发布者数与订阅者数均不为 0，说明两端 QoS 已互相匹配。

## 常见错误与排障

| 现象 | 原因与处理 |
|---|---|
| 订阅端收不到消息 | **先查两端 QoS 兼容性**（第 5 步表格）：最典型是 RELIABLE 订阅端收不到 BEST_EFFORT 发布端；把订阅端换成 `ros2DefaultSubscriber()`（BEST_EFFORT requested 对任意 offered 均可匹配）或把发布端换成 `ros2DefaultPublisher()`（RELIABLE offered） |
| 排查 QoS 时高丢帧/统计频率明显偏低 | print/hz 缺省 BEST_EFFORT+VOLATILE **depth 1**，高频流下丢帧偏频：加 `--qos-ros` 切到 KEEP_LAST(10) 底座（`yomkrpc topic hz --qos-ros <主题名>`） |
| `--qos-depth` 不生效 | depth 只在 keep_last 下生效：未设 `--qos-history` 时底座即 keep_last 直接生效；若显式设了 keep_all 则 depth 被忽略（须 >=1 整数） |
| `--qos-file` 报错 | 文件须为扁平 JSON 对象，键为 reliability/durability/history/depth/deadline/lifespan/liveliness/liveliness_lease_duration，值形态同对应 `--qos-*` 选项（见第 4 步示例） |
| 两项 QoS 配置互相冲突不知哪个生效 | 按优先级链：命令行逐项 > 文件逐项 > `--qos-ros` 底座 > 默认 |
| 想按主题/节点集中配置而非逐处写代码 | 宏 API 逐项覆盖适合少量端点；participant 级集中配置走 XML profiles（`YOMKRPC_LOAD_XML`，须在节点创建之前调用） |

## 延伸阅读

- [../features/qos-profiles.md](../features/qos-profiles.md)——QoS 配置的能力边界、哨兵/底座/覆盖语义与参数详表
- [返回手册入口](./README.md)
