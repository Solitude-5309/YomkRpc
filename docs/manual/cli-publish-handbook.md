# 使用手册：yomkrpc 发布与类型内省手册（topic pub/interface）

> **本文档覆盖范围**：用 `yomkrpc` 命令行工具不写代码向任意 DDS 主题发布消息——topic pub -e 查看发布示例、-ef 导出载荷文件、JSON 直发、-f 文件发布、-w/-r/-t 组合控制与临时 writer QoS 调参（README 6.1 与 6.4 topic pub 部分），以及类型内省 interface show / interface list（探明消息结构）。观察类命令（print/list/info/hz/node）见 [cli-observe-handbook.md](./cli-observe-handbook.md)。

## 目标

完成本文档后，你将：

1. 用 `interface list` / `interface show` 在不写代码的情况下探明域内消息类型的 IDL 结构；
2. 用 `topic pub -e` 拿到某主题的可复制发布命令与载荷模板，用 `topic pub -ef` 导出多行缩进的载荷描述文件；
3. 用 `topic pub` 直发 JSON 载荷或 `-f` 从文件发布，理解「首轮收敛 + matched 校验 + ack 确认」的送达保障；
4. 用 `-w N`（期望建匹配订阅端数）、`-r N`（持续发布频率）、`-t N`（条数上限）任意叠加控制发布节奏；
5. 用 `--qos-ros`/`--qos-*`/`--qos-file` 调整临时 writer QoS（缺省 `RELIABLE+TRANSIENT_LOCAL` depth 1）；
6. 能独立排查主题未发现、JSON 载荷不合法、送达未确认三类发布侧问题。

## 前提与准备

- 已按 [setup-and-build.md](./setup-and-build.md) 完成编译安装：`yomkrpc` 命令行工具位于 `<安装路径>/bin`，可直接运行；
- 发布是**类型无关**的：工具经 `YomkRpcDebugService` 调试服务从发现数据重建 DynamicType 并解析 JSON 载荷，无需 IDL 生成代码；目标主题须有端点已在域内被发现（或使用 `-w N` 先等主题出现，见下文）；
- **域号**由环境变量 `YOMKRPC_DDS_DOMAIN_ID` 指定（合法范围 [0,232]，默认 0）：yomkrpc 启动时无该变量则自动创建并默认 0，幂等写入 `~/.bashrc`（带 `# added by yomkrpc` 注释），已打开的终端须 `source ~/.bashrc` 或重开才生效；单次临时换域可用显式选项 `-D/--domain <N>`（优先于环境变量，详见 [env-vars-reference.md](./env-vars-reference.md)）；
- **收敛次数**由环境变量 `YOMKRPC_DDS_DISCOVER_ROUNDS` 配置（默认 5，值须为 >=1 的整数，非法报错退出）：topic pub 全形态与 interface show/list 均按「每 ~200ms 轮询快照、连续 N 次不变即收敛」输出/发布；
- 一个用于验证的订阅端（可选）：安装自带的 `ExampleYomkRpcSub`（节点 `sub_node`，订阅 `hello_world`），或另开终端跑 `yomkrpc topic print hello_world`（见 [cli-observe-handbook.md](./cli-observe-handbook.md)）。

## 分步操作

topic pub 共五种形态（互相独立，均为「先解析再发布」）：`-e` 看示例、`-ef` 导出文件、直发 JSON、`-f` 文件发布；`-w`/`-r`/`-t` 与 QoS 参数只对后两种发布形态生效。典型工作流见本章末尾「典型工作流串联」。

### topic pub -e：查看主题的发布示例

- **用途**：按主题名输出发布示例三段行集（类型名 / IDL / JSON 发布载荷模板）——同类型重建 DynamicType 取默认值样本生成，与发布输入格式对称。
- **完整命令行**：

```bash
yomkrpc topic pub -e <主题名>        # 等价长名 --example
```

- **示例输出**：

```
Type: YomkRpc::MString

IDL:
struct YomkRpc::MString {
    string data;
};

example:
yomkrpc topic pub hello_world '{"data":""}'
```

- **要点**：example 段的发布命令里 JSON 已整体包好单引号（裸传会被 shell 剥掉内层双引号导致解析失败），改字段值即可直接发布；IDL 段同 `interface show` 的结构描述。
- **失败报错**：按节点层三步骤归类（步骤 1 发现 → 步骤 2 类型重建 → 步骤 3 示例生成），不再统一报 `topic [...] not found`：

| 错误文本 | 卡点 |
|---|---|
| `topic [<主题名>] not found` | 步骤 1：主题在收敛窗口内始终未被发现（主题名拼错 / 未加 `rt/` 前缀 / 对端未发布 / 域号不一致） |
| `type [<类型名>] not found` | 步骤 2：类型名未入发现缓存（`interface show` 同款语义） |
| `type object for [<类型名>] not available` | 步骤 2：对端未提供 XTypes TypeObject（**ROS2 rmw_fastrtps 端点即此情形**）或 TypeLookup 尚未完成 |
| `type [<类型名>] is not a struct, interface display unsupported` | 步骤 2：顶层非 struct，IDL 段无法拼装 |
| `type rebuild failed for [<类型名>]` | 步骤 3：DynamicType 构建失败 |
| `example generation failed for [<类型名>]: create_data` / `: json_serialize` | 步骤 3：默认值样本创建或 JSON 序列化失败 |

- **等价宏名**：`YOMKRPC_DEBUG_TOPIC_EXAMPLE(topicName, stableRounds, intervalMs)`。

### topic pub -ef：导出消息描述 JSON 文件

- **用途**：按主题名导出消息描述 JSON 文件（内容即默认值模板 JSON 本身，多行缩进可直接 `$(cat)` 填进发布命令；**仅导出不发布**，查完即退）。
- **完整命令行**：

```bash
yomkrpc topic pub -ef <主题名> [-o 目录]        # 等价长名 --example-file / --output
```

- **关键选项**：`-o <目录>` / `--output <目录>` 指定生成目录（相对/绝对路径均可），缺省当前目录；目录不存在报错退出，不自动创建（`bag record` 的 `-o` 语义不同——创建 bag 目录名）。
- **示例输出**：

```
message description written: hello_world_msg_2026-09-28-12-14-42-626.json (topic: hello_world, type: YomkRpc::MString)
```

文件名 `<主题名>_msg_<时间戳>.json`，时间戳格式为 年-月-日-时-分-秒-毫秒。文件内容（整体就是一份合法发布载荷）：

```json
{
  "data": ""
}
```

- **要点**：主题名/类型名不在文件内，从文件名与 stdout 提示行追溯；编辑文件改字段值后按文件发布（见下文 `-f` 形态）——`"$(cat 文件)"` 命令替换结果不再经历引号删除，内层双引号与换行原样保留，多行缩进 JSON 解析无碍。
- **失败报错**：按节点层两步骤归类——步骤 1 `topic [<主题名>] not found` 主题未发现；步骤 2 类型重建与 JSON 模板生成侧原因（同 `topic pub -e` 的步骤 2/3 错误文本）。
- **等价宏名**：`YOMKRPC_DEBUG_TOPIC_MSG(topicName, stableRounds, intervalMs)`（CLI 侧由两要素拼装导出文件）。

### topic pub 直发：发布 JSON 载荷

- **用途**：按主题名发布 JSON 载荷消息——缺省按 1 Hz 持续发送直到 Ctrl+C（首轮发布成功后每秒重发，Ctrl+C 停止并输出统计）。
- **完整命令行**：

```bash
yomkrpc topic pub [--qos-ros | --qos-KEY VAL ... | --qos-file FILE] <主题名> <JSON数据>
```

- **发布链路**（理解后即明白「为什么首发慢半拍、为什么缺订阅端会报错」）：

1. **发现收敛**：轮询主题类型名 + 订阅者数快照，连续 N 轮（默认 5，环境变量 `YOMKRPC_DDS_DISCOVER_ROUNDS` 可调）不变即收敛——保证不早发（避免订阅者还没被发现就漏收）；未发现主题报错退出（`-w N` 时改为先等待主题出现）；
2. **类型重建 + 载荷解析**：由发现的类型重建 DynamicType，`json_deserialize` 解析载荷；解析失败报错退出，不建任何发布实体；
3. **匹配收敛 + 首轮发布**：临时 RELIABLE + TRANSIENT_LOCAL writer（请求 ≤ 提供，最大兼容既有订阅者 QoS）匹配收敛后 write 首轮消息；`-w N` 时匹配收敛前先阻塞等待 matched 计数达到 N；随后执行 **matched 校验**（writer 实际匹配的订阅端 GUID 集与发现缓存中该主题 RELIABLE 订阅者逐一比对，缓存里有订阅者却不在匹配集则不发布、报错退出）；
4. **ack 自适应确认送达（首轮）**：存在 RELIABLE 订阅者时以 `wait_for_acknowledgments` 协议级确认——每个 RELIABLE 订阅者确认样本已入 reader history 才返回成功（未全部确认报错退出）；全 BEST_EFFORT 或无订阅者退化尽力而为（保底窗后返回成功）；首轮成功后进入持续发布循环（缺省 1 Hz），Ctrl+C 停止并输出统计（`-t N` 时发满即停，排空 200ms 后退出）。

- **示例输出**：

```
publishing to topic "hello_world" at 1.000000 Hz, press Ctrl+C to stop
publishing #2 to topic hello_world
^Cpublished total=5 failed=0 to topic hello_world
```

逐条回执附带消息 JSON 内容（从实际待发数据转出，与 `topic print` 输出同源），发布期间可看到每条载荷。
- **等价宏名**：`YOMKRPC_DEBUG_TOPIC_PUB(topicName, json, stableRounds, intervalMs, repeatIntervalMs, requiredSubscribers, maxTimes[, qos])`。

### topic pub -f：从文件读取载荷发布

- **用途**：按主题名发布 JSON 载荷文件——文件内容整体作为载荷，与 `topic pub -ef` 导出文件对接（导出 → 改值 → 发布闭环）。
- **完整命令行**：

```bash
yomkrpc topic pub [--qos-ros | --qos-KEY VAL ... | --qos-file FILE] <主题名> -f <文件>
```

- **关键选项**：`-f <文件>` / `--file <文件>` 指定载荷文件（文件不存在报错退出 `cannot open json file`，文件内容为空报错退出 `json file is empty`）；缺省 1 Hz 持续发送，`-w N` 等待订阅者匹配、`-r N` 持续发布、`-t N` 条数上限，语义同直发；临时 writer QoS 经 `--qos-ros`/`--qos-*`/`--qos-file` 可调。
- **示例输出**（先导出再按文件发布）：

```bash
$ yomkrpc topic pub -ef hello_world
message description written: hello_world_msg_2026-09-28-12-14-42-626.json (topic: hello_world, type: YomkRpc::MString)
# 编辑文件改字段值后按文件发布（同一发布链路，多行缩进 JSON 直接可发）
$ yomkrpc topic pub hello_world -f hello_world_msg_2026-09-28-12-14-42-626.json
publishing to topic "hello_world" at 1.000000 Hz, press Ctrl+C to stop
^Cpublished total=3 failed=0 to topic hello_world
```

### -w / -r / -t：等待匹配、持续频率与条数上限

三个参数独立识别、**任意顺序叠加**（`-w 2 -r 10` 等价 `-r 10 -w 2`），语义分别如下。

**`-w N | --wait N` 期望建匹配订阅端数**（N 为 >=1 整数）——关闭「自动收敛可能漏掉尚未上线订阅者」的缺口，以 N 作为收敛结束：

- **主题未发现时先等待主题出现**：域内尚无该主题任何端点（典型为「先发布后订阅」场景）时，先无限轮询等待主题被发现（订阅端上线即经 SPDP/EDP 宣告主题与类型），每 ~200ms 轮询、首轮打印 `waiting for topic <主题名> to be discovered`、约每 5 秒重复打印防静默；主题出现后自动进入类型重建与匹配等待；
- **计数达标才发布**：临时 writer 建立后轮询 `PublicationMatchedStatus.current_count`（含 BEST_EFFORT 订阅端，与 `ros2 topic pub -w` 口径一致），达到 N 才继续发布；未达到则打印等待进度（计数变化时立即打印，长期无变化约每 5 秒重复）：`waiting for subscribers: 1/2 matched on topic hello_world`；
- **一直等待不设超时**：N 大于实际订阅端总数、或主题迟迟未出现将无限等待；Ctrl+C 中断等待并报错退出（exit=1，`waiting for subscribers interrupted`），不发布任何数据；
- **达标后仍走完整安全网**：N 是「至少 N 个」的门槛，已发现但未匹配的 RELIABLE 订阅者（挂起/异常退出）仍会如实报错；
- 省略 `-w`（或 N=0）行为与旧版一致：发现稳定即发（自动收敛）。

**`-r N | --rate N` 持续发布频率**（单位 Hz，支持小数如 `-r 0.5`）——缺省即 1 Hz 持续发送（间隔 1000ms）：

- **首轮完整强校验**：发现收敛 → 类型重建 → 匹配收敛 → matched 校验 → write → ack 确认，失败即报错退出；
- **周期重发尽力而为**：首轮成功后按 N Hz 周期重复 write（RELIABLE 的 NACK 重传由协议自主完成，不再同步等 ack）；每轮输出 `publishing #N to topic <主题名>`；write 失败仅计入 failed 不中断（订阅者中途离场不停止发布，重新上线后可继续收到后续数据）；
- **绝对节拍网格**：每轮唤醒时刻按 `首发时刻 + k×间隔` 的绝对网格调度（sleep_until），平均频率精确贴合 N Hz；单轮调度毛刺超过周期时网格重锚到未来最近节拍点（不追发）；频率换算：发送间隔 = round(1000/rate) ms（下限 1ms）；
- **Ctrl+C 优雅停止**：输出统计 `published total=N failed=M to topic <主题名>`（total 含首轮），退出码 0。

**`-t N | --times N` 条数上限**（N 为 >=1 整数，含首轮）——发满 N 条自动停止：

- 发满即停，销毁发布链前保底排空 200ms 等待异步传输落地，随后输出统计并退出（exit=0），无需 Ctrl+C；
- 发送期间 Ctrl+C：立即停止（不排空），输出已发统计，exit=0；
- 短名 `-t` 复用判定：`-t` 下一参数为整数时视为条数（topic pub 语境），否则为 `topic list` 的类型名模式开关（`--times` 长名恒为条数）。

```bash
# 持续发布：首轮校验发布后按 10 Hz 周期重发，Ctrl+C 停止并输出统计
yomkrpc topic pub -r 10 hello_world '{"data":"hello yomkrpc"}'

# 等待订阅者匹配：期望建匹配 2 个订阅端，计数达标才开始发布
yomkrpc topic pub -w 2 hello_world '{"data":"hello yomkrpc"}'

# 先发布后订阅：主题未发现时先等待主题出现，上线后自动进入匹配等待
yomkrpc topic pub -w 1 hello_world '{"data":"hello yomkrpc"}'

# -w 与 -r 叠加（顺序任意）：先等 2 个订阅端匹配，再按 10 Hz 持续发布
yomkrpc topic pub -r 10 -w 2 hello_world '{"data":"hello yomkrpc"}'

# 条数上限：发满 5 条自动停止（排空 200ms 后退出，无需 Ctrl+C）
yomkrpc topic pub -t 5 hello_world '{"data":"hello yomkrpc"}'

# 三者叠加（顺序任意）：先等 1 个订阅端匹配，再按 5 Hz 发满 5 条退出
yomkrpc topic pub -t 5 -r 5 -w 1 hello_world '{"data":"hello yomkrpc"}'
```

### 临时 writer QoS 参数

topic pub 发布形态的临时 writer 缺省 **`RELIABLE+TRANSIENT_LOCAL` depth 1**（请求 ≤ 提供，最大兼容既有订阅者 QoS），命令行形态 `[--qos-ros | --qos-KEY VAL ... | --qos-file FILE]` 三层叠加：

- **`--qos-ros`**：整体切 ROS2 底座 QoS——pub 端 KEEP_LAST(10)+RELIABLE+VOLATILE（时间项 ∞）；与 `--qos-*`/`--qos-file` 叠加时作底座被逐项覆盖；
- **8 项逐项覆盖**（值大小写不敏感，ros2 小写与 DDS 大写均收）：
  - `--qos-reliability REL`：reliable | best_effort
  - `--qos-durability DUR`：transient_local | volatile
  - `--qos-history HIST`：keep_last | keep_all
  - `--qos-depth N`：history depth（须 >=1 整数；keep_all 时忽略；未设 `--qos-history` 时底座即 keep_last，直接生效）
  - `--qos-deadline SEC`：deadline 周期秒（非负小数，0=∞；下同）
  - `--qos-lifespan SEC`：lifespan 秒（writer 侧策略）
  - `--qos-liveliness KIND`：automatic | manual_by_topic
  - `--qos-liveliness-lease-duration SEC`：liveliness 租约秒
- **`--qos-file FILE`**：从 JSON 文件加载逐项覆盖（扁平对象，键为 reliability/durability/history/depth/deadline/lifespan/liveliness/liveliness_lease_duration，值形态同对应 `--qos-*` 选项，如 `{"reliability":"reliable","depth":10,"deadline":0.5}`）
- **优先级**：命令行逐项 > 文件逐项 > `--qos-ros` 底座 > 默认（pub 临时 writer 缺省 RELIABLE+TRANSIENT_LOCAL depth 1）。

```bash
yomkrpc topic pub --qos-ros hello_world '{"data":"hi"}'              # ROS2 底座发布
yomkrpc topic pub --qos-reliability best_effort hello_world '{"data":"hi"}'
yomkrpc topic pub --qos-file qos.json hello_world '{"data":"hi"}'    # 文件逐项覆盖发布 QoS
```

### interface show：查看类型的 IDL 结构

- **用途**：按数据类型名输出该类型的 IDL 结构描述（struct 头 + 字段行 + 结尾）——类型名精确匹配（同 `topic find` 的匹配语义），复用发现链路 TypeInformation → TypeObject → DynamicType 做纯类型内省（**不建订阅**），输出可直接粘回 .idl 文件复用（对齐 Protobuf DebugString、grpcurl describe、ros2 interface show 同族 schema 形态）。
- **完整命令行**：

```bash
yomkrpc interface show <类型名>
```

- **示例输出**：

```
struct YomkRpc::MString {
    string data;
};
```

- **要点**：字段类型名为 ROS2/IDL4 风格映射（bool/int32/uint32/float32/string 等；有界 `string<N>`；`sequence<T>`/`sequence<T, N>`；数组 `T[N]`；嵌套 struct/enum/alias 显示成员子类型名不递归展开）；仅支持顶层为 struct 的类型。
- **失败报错**（可直接判定问题在哪一层）：

| 错误文本 | 卡点 |
|---|---|
| `type [<类型名>] not found` | 类型名未出现在发现缓存（拼写错误或端点不在本域） |
| `type object for [<类型名>] not available` | 对端未提供 XTypes TypeObject（**ROS2 rmw_fastrtps 端点即此情形**）或 TypeLookup 尚未完成 |
| `type [<类型名>] is not a struct, interface display unsupported` | 顶层为 enum/alias 等非 struct 类型 |
| `interface introspection failed for [<类型名>]` | DynamicType 成员枚举失败 |

- **等价宏名**：`YOMKRPC_DEBUG_INTERFACE_SHOW(typeName, stableRounds, intervalMs)`。

### interface list：列出全部已发现类型

- **用途**：列出发现缓存中出现的全部数据类型名（去重、字典序排序，每行一个；同类型多主题仅出一行），是 `interface show` 的配套导航——先 list 拿到类型名全集，再 show 查看具体结构。
- **完整命令行**：

```bash
yomkrpc interface list
```

- **示例输出**：

```
YomkRpc::MString
```

- **要点**：域内无任何类型报错退出（find 族语义）。
- **等价宏名**：`YOMKRPC_DEBUG_INTERFACE_LIST(stableRounds, intervalMs)`。

### 典型工作流串联

五个命令串成一条「不写代码」的发布闭环（按顺序执行）：

```bash
# 1. 发现主题（拿到主题名全集；-t 可同时看类型名）
yomkrpc topic list -t

# 2. 看消息结构（确认字段名与类型，便于构造 JSON）
yomkrpc interface show YomkRpc::MString

# 3. 拿载荷模板（example 段是可直接改值发布的完整命令）
yomkrpc topic pub -e hello_world

# 4. 发布（发满 5 条自动退出；参数替换成自己的主题名与字段值）
yomkrpc topic pub -t 5 hello_world '{"data":"hello yomkrpc"}'

# 5. 验证（另开终端订阅直出，确认数据到达）
yomkrpc topic print hello_world
```

第 4 步若订阅端尚未上线，改用 `yomkrpc topic pub -w 1 -t 5 hello_world '{"data":"hello yomkrpc"}'` 先等订阅端匹配再发布。

## 预期输出

完整会话（终端 1 为订阅端 `ExampleYomkRpcSub` 先启动，终端 2 依次执行；前缀以实际环境为准）：

```bash
# 终端 1（先启动订阅端，提供匹配对象与验证载体）
ExampleYomkRpcSub
```

```bash
# 终端 2
yomkrpc interface list
# YomkRpc::MString

yomkrpc interface show YomkRpc::MString
# struct YomkRpc::MString {
#     string data;
# };

yomkrpc topic pub -e hello_world
# Type: YomkRpc::MString
#
# IDL:
# struct YomkRpc::MString {
#     string data;
# };
#
# example:
# yomkrpc topic pub hello_world '{"data":""}'

yomkrpc topic pub -t 5 hello_world '{"data":"hello yomkrpc"}'
# publishing to topic "hello_world" at 1.000000 Hz, limit 5 messages, press Ctrl+C to stop
# publishing #2 to topic hello_world
# publishing #5 to topic hello_world
# published total=5 failed=0 to topic hello_world
```

终端 1（订阅端）同步打印 5 条 `[RECV] hello yomkrpc`；Ctrl+C 退出时打印累计接收条数。将第 4 步换成 `-r 10 -w 1 hello_world '{"data":"hello yomkrpc"}'`，则先打印 `waiting for subscribers: 1/1 matched on topic hello_world`，随后以 10 Hz 持续发布，Ctrl+C 停止并输出统计。

## 常见错误与排障

| 现象 | 原因与处理 |
|---|---|
| 报 `topic [<主题名>] not found` | 主题在收敛窗口内始终未被发现（主题名拼错 / 未加 `rt/` 前缀 / 对端未发布 / 域号不一致）：先 `yomkrpc topic list` 确认主题已在域内、核对域号 `echo $YOMKRPC_DDS_DOMAIN_ID`；确要先发布后订阅就用 `-w N`（先等主题出现，订阅端上线即宣告主题与类型），或等订阅端上线后再发 |
| 报 `invalid json for type [...]` | JSON 载荷不合法（与类型不匹配或格式错误）：载荷解析失败报错退出、不建任何发布实体；先用 `topic pub -e <主题名>` 拿同构模板改值，或用 `-ef` 导出文件改值后 `-f` 发布；命令行裸传 JSON 双引号会被 shell 剥掉，务必整体包单引号（`'{"data":"..."}'`），多行文件用 `"$(cat 文件)"` |
| 报 `not all subscribers acknowledged` | 首轮送达未确认：存在 RELIABLE 订阅者时以 `wait_for_acknowledgments` 协议级确认，RELIABLE 订阅者中途挂起（如 SIGSTOP）即 ack 超时报错退出，不假成功；恢复订阅端运行后重试 |
| 报 `subscribers exist but not all matched (suspended or offline)` | 订阅者在场却无一与发布端匹配：发现缓存有记录但 QoS 不兼容（如订阅端请求 PERSISTENT），或订阅者异常死亡（kill -9、断网等无 graceful goodbye，lease 过期前的短暂区间内可能误报一次，重试即恢复）；正常退出（graceful dispose）的订阅端已被发现缓存清理，不影响发布 |
| 订阅端明明在线却收到「订阅者未匹配」报错 / 或发布「成功」但订阅端没收到 | 订阅者在发布端启动前已离线/挂起（发布端从未发现过该订阅者）→ 发布端无从期待照常成功——DDS 发现机制的客观边界；BEST_EFFORT 订阅者不参与 ack 与 matched 校验，始终尽力而为（无协议保证，深度不足即丢帧，可用 `--qos-ros`/`--qos-depth` 加大深度） |
| 报 `cannot open json file` / `json file is empty` | `-f` 文件不存在或内容为空：核对路径与文件内容 |
| 报 `非法频率 "..."` / `非法订阅者数量 "..."` / `非法发布条数 "..."` | `-r` 须为数值且 >0；`-w`/`-t` 须为 >=1 整数（0、负数、溢出或非数值均报错）；参数后缺值同用法错误退出 |
| `-w` 等待被打断 | Ctrl+C 中断等待报错退出（exit=1，`waiting for subscribers interrupted`），不发布任何数据；确认订阅端数量后再发布，或去掉 `-w` 改自动收敛 |
| `interface show` 报 `type object for [...] not available` | 对端未提供 XTypes TypeObject（**ROS2 rmw_fastrtps 端点即此情形**）或 TypeLookup 尚未完成：等对端类型发现完成后重试；ROS2 端点类型当前无法内省 |
| 收敛/等待感觉偏慢或偏快 | 调整 `YOMKRPC_DDS_DISCOVER_ROUNDS`（默认 5，值须为 >=1 的整数，非法报错退出）：发现频繁变动想多等调大，想快速失败调小 |

## 延伸阅读

- [../features/debug-service.md](../features/debug-service.md)——调试服务能力全貌：调试节点模型、`YOMKRPC_DEBUG_*` 宏全集与 CLI 双入口对照
- [../features/qos-profiles.md](../features/qos-profiles.md)——QoS 配置能力全貌：DDSQosProfile 8 项、ROS2 预置底座与宏侧/CLI 侧优先级链
- [cli-observe-handbook.md](./cli-observe-handbook.md)——观察类命令（print/list/info/hz/node），与本文档构成调试闭环
- [env-vars-reference.md](./env-vars-reference.md)——`YOMKRPC_DDS_DOMAIN_ID`、`YOMKRPC_DDS_DISCOVER_ROUNDS` 等环境变量的作用域、默认值与优先级
