# 使用手册：Bag 录制回放手册

> **本文档覆盖范围**：用 `yomkrpc` 命令行工具把域内主题数据录制为 mcap bag、查看 bag 元信息、按录制时间轴回放为真实 DDS 流量、按配置对 bag 做过滤/裁剪/合并，以及从 mcap 分片重建缺失/损坏的 `metadata.json`——覆盖 bag 全部五个子命令（`bag record`/`bag info`/`bag play`/`bag convert`/`bag reindex`）的操作闭环。bag 功能的能力边界与参数总表见 [../features/bag-service.md](../features/bag-service.md)。

## 目标

完成本文档后，你将：

1. 用 `yomkrpc bag record` 把一个或多个主题（含通配模式）录制为 mcap bag，掌握目录命名、分片、写缓存、暂停态启动与交互键；
2. 用 `yomkrpc bag info` 查看 bag 元信息，用 `yomkrpc bag reindex` 修复被删/损坏的 `metadata.json`；
3. 用 `yomkrpc bag play` 把 bag 按录制时间轴回放为真实 DDS 流量（倍率/主题过滤/循环/起始偏移/开播延迟/退出确认/主题重映射/QoS 覆盖）；
4. 用 `yomkrpc bag convert` 按输出配置文件对 bag 做过滤、裁剪与合并；
5. 独立完成一次端到端演练：录制约 10 秒数据，查看元信息，回放并验证收到数据，最后裁剪转换出新 bag。

## 前提与准备

- 已按 [setup-and-build.md](./setup-and-build.md) 完成 YomkRpc 编译安装（`yomkrpc` 命令行工具与 `ExampleYomkRpcPub` 等示例程序可直接运行）；
- bag 命令与被录/被回放主题同处一个 DDS 域：域号经环境变量 `YOMKRPC_DDS_DOMAIN_ID` 选择（合法范围 [0,232]，默认 0，同其他 yomkrpc 命令；单次临时换域可用显式选项 `-D/--domain <N>`，优先于环境变量；详见 [env-vars-reference.md](./env-vars-reference.md)）；
- bag 产物为标准 mcap 容器：bag 目录内含 `bag_0.mcap` 数据分片与 `metadata.json` 元信息两文件，可用任意 mcap 标准读库读回；
- 通配模式在 shell 下须加引号：bash 会对未加引号的 `*` 做当前目录文件名 glob 预展开（清单项被替换为文件名），程序内无法防御，务必 `'hello_*'` 传参；
- 演练中的观察/发布命令（`topic print`/`topic pub`）用法详见 [cli-observe-handbook.md](./cli-observe-handbook.md) 与 [cli-publish-handbook.md](./cli-publish-handbook.md)。

## 分步操作

### 五个子命令总览

| 子命令 | 职责 | 是否需要 bag 节点 | 等价宏 |
|---|---|---|---|
| `yomkrpc bag record` | 录制主题列表为 mcap bag（长驻阻塞至 Ctrl+C 收尾） | 经 bag 节点实现（CLI 自动建删） | `YOMKRPC_BAG_RECORD`（宏形态须先 `YOMKRPC_BAG_NODE` 自建 bag 节点） |
| `yomkrpc bag info` | 查看 bag 元信息（输出对齐 `ros2 bag info`） | 纯文件操作，无须节点 | `YOMKRPC_BAG_INFO` |
| `yomkrpc bag play` | 按录制时间轴回放为真实 DDS 流量 | CLI 自动建删 bag 节点 | `YOMKRPC_BAG_PLAY`（宏形态须先自建 bag 节点） |
| `yomkrpc bag convert` | 按输出配置文件把一个或多个输入 bag 过滤/裁剪/合并写为新 bag | 纯文件操作，无须节点 | `YOMKRPC_BAG_CONVERT` |
| `yomkrpc bag reindex` | 从 mcap 分片重建被删/损坏的 `metadata.json` | 纯文件操作，无须节点 | `YOMKRPC_BAG_REINDEX` |

info/reindex/convert 为纯本地文件操作——不经 bag 节点、不创建任何 DDS 实体、不依赖域内状态，未建 bag 节点亦可调用。等价宏 `YOMKRPC_BAG_*` 定义于 `YomkRpcAPI.h`，链接 `YomkRpc::YomkRpc YomkServer::YomkServer`；本文以 CLI 为主线，宏等价形态见 [../features/bag-service.md](../features/bag-service.md)。

### 第 1 步：录制主题到 bag（bag record）

命令形态（`<主题名|模式>` 清单为位置参数，至少一项）：

```bash
yomkrpc bag record [-o <目录>|--output <目录>] [-b <字节>|--max-bag-size <字节>] [-d <秒>|--max-bag-duration <秒>] [-c <字节>|--max-cache-size <字节>] [--start-paused] <主题名|模式> [<主题名|模式> ...]
```

基础用法：

```bash
yomkrpc bag record hello_world           # 录制单主题（默认域 0）
yomkrpc bag record rt/chatter rt/tf      # 多主题清单
yomkrpc bag record 'hello_*' '*_world'   # 通配模式（清单项恰好含一个 *，引号防 shell glob 展开）
yomkrpc bag record -o my_session hello_world   # 指定 bag 目录名（相对/绝对路径均可）
```

**bag 目录命名**（`-o|--output`）：

- 经 `-o` 指定 bag 目录名，相对/绝对路径均可，父目录自动多级创建；
- 缺省按当前路径下时间戳命名：`bag_<YYYY-MM-DD_HH-MM-SS_mmm>`（毫秒精度防同秒重名）；
- `-o` 指定的目录已存在报错退出（`bag directory [x] already exists, remove it or choose another name`，fail-fast 于发现校验前，不落盘）。

**通配模式**（清单项恰好含一个 `*`）：

| 模式 | 匹配语义 |
|---|---|
| `hello_*` | 前缀匹配（以 `hello_` 开头的全部已发现主题） |
| `*_hello` | 后缀匹配 |
| `pre*suf` | 中间夹逼（前后缀之间夹任意片段，`*` 不与首尾共享字符，对齐 fnmatch） |
| `*` | 匹配全部已发现主题 |

- 清单项含 0 个 `*` 为精确主题名；恰好 1 个 `*` 为通配模式；≥2 个 `*` 输入有误报错退出（`topic [x] 中 '*' 出现多次，仅支持单个通配`）；
- 模式在启动校验时按发现缓存展开为实际主题集合，与精确项**去重合并**（`hello_*` 与精确 `hello_world` 同清单只订阅一次），展开清单升序——统计与 metadata 输出确定；展开于启动校验时一次定型，录制期间新上线的匹配主题不自动加入；
- 模式未命中任何主题报错退出（`模式 [x] 未匹配到任何主题，请检查通配输入（域 N，已等待 M ms）`）。

**分片录制**（`-b` 字节上限与 `-d` 时长上限，写满/到限即滚动 `bag_N.mcap` 分片）：

```bash
yomkrpc bag record -b 1048576 hello_world      # 单分片最大 1MB（写满滚动 bag_N.mcap 分片，0=不分片缺省）
yomkrpc bag record -d 60 hello_world          # 单分片最大 60s（-d/--max-bag-duration；时长达限滚动 bag_N.mcap 分片，0=不分片缺省）
```

- `-b|--max-bag-size <字节>`：单分片最大字节数，下限 1024 字节——非 0 且低于 1024 在启动校验报错退出（`max bag size [x] too small, minimum split file size is 1024 bytes (0 to disable splitting)`）；实际分片粒度不小于 chunk 落盘边界——默认 786432 字节，小于该值的 `-b` 实际每 chunk 一片（时长分片不受 chunk 影响）；
- `-d|--max-bag-duration <秒>`：单分片最大时长秒，0=不分片缺省（无下限校验）；时长为距本分片首条消息接收时刻的间隔，暂停期不计入；
- `-b` 与 `-d` 同用时**先到先分**：每条消息写盘前检查本分片已落盘字节与已录时长，任一达上限即先滚动下一分片 `bag_N.mcap` 再写本条（写前检查对齐 ros2：超限消息整体落新分片，不产生空片）。

**写缓存双缓冲**（`-c`）：

```bash
yomkrpc bag record -c 8388608 hello_world     # 写缓存双缓冲 8MB（-c/--max-cache-size；回调拷贝入队独立线程写盘，CLI 缺省 100MiB，0=每条直写）
```

`-c|--max-cache-size <字节>` 非零时启用写缓存双缓冲——数据回调仅将消息深拷贝入内存队列，独立消费线程逐条取出落盘（默认 100 MiB，0=每条直写，最坏占 2 倍内存）；缓存满丢弃新消息并按主题计数，停止收尾一次性打印丢条统计。

**暂停态启动与交互键**：

```bash
yomkrpc bag record --start-paused hello_world  # 暂停态启动（订阅照常、消息丢弃不写入，按 SPACE 开始录制）
```

`--start-paused` 暂停态启动——订阅与发现照常、收到的消息丢弃不写入。交互键（tty 下）：SPACE 开始录制（暂停态→运行态，输出 `recording started`）、p 暂停（输出 `recording paused`）、r 继续（输出 `recording resumed`），Ctrl+C 停止；非 tty（管道/重定向）时跳过键盘处理，Ctrl+C 退出不受影响。

**启动校验**（失败不建目录不产生文件）：先做清单快速校验（空清单/空名/重复/多通配/`-o` 目录已存在即报错），随后轮询发现缓存快照并逐轮展开模式项（校验参数固定，不经 `YOMKRPC_DDS_DISCOVER_ROUNDS` 调整）；收敛后逐项判定，任一精确主题既无发布者也无订阅者、或任一模式未命中任何主题即整体报错退出。**校验失败发生在建目录之前，不产生任何文件**。仅有订阅者的主题同样通过（类型名取自订阅端点公告，先建订阅，发布者上线匹配后自动开始录流）。

与发布端配合观察（另开两个终端）：

```bash
# 终端 1（先启动发布端）
ExampleYomkRpcPub
# 终端 2
yomkrpc bag record hello_world
```

**落盘产物**（bag 目录两文件）：

- `bag_0.mcap`：标准 mcap 容器，逐主题 Channel（encoding=cdr、无 schema——schemaless 对齐透传语义），Channel.metadata 记录该主题消息类型名与 QoS 串（`type=<类型名>`、`qos=<Reliability>,<Durability>`），消息 record 保存原始 CDR 字节；指定 `-b`/`-d` 时滚动产生同格式分片 `bag_1.mcap`、`bag_2.mcap`、...（各分片独立完整可读回，消息按写入顺序连续切分——不重复不空洞）；
- `metadata.json`：顶层平铺 bag 元信息（`version` 为 yomkrpc 自有格式版本，1 起步），样例如下（起始时间与时长各附 `_format` 可读伴生键；`topics_with_message_count` 的 `qos` 键记录录制时远端 writer 的 offered QoS 串，仅订阅者主题录制时为空串）：

```json
{
    "version": 1,
    "storage_identifier": "mcap",
    "relative_file_paths": ["bag_0.mcap"],
    "starting_time": {
        "nanoseconds_since_epoch": 1791953853726412000,
        "nanoseconds_since_epoch_format": "2026-10-02_19-37-33_726-412-000"
    },
    "duration": {
        "nanoseconds": 4012345678,
        "nanoseconds_format": "00-00-04_012-345-678"
    },
    "message_count": 12,
    "topics_with_message_count": [
        {
            "topic_metadata": {
                "name": "hello_world",
                "type": "YomkRpc::MString",
                "qos": "RELIABLE,TRANSIENT_LOCAL"
            },
            "message_count": 12
        }
    ]
}
```

### 第 2 步：查看 bag 元信息（bag info）

```bash
yomkrpc bag info <bag目录>
```

```bash
yomkrpc bag info bag_2026-10-02_19-37-33_726   # 查看第 1 步的录制产物
```

读取 bag 目录下 `metadata.json` 与目录递归总大小，输出与 `ros2 bag info`（humble）逐行一致的元信息文本（多行文本逐行直出 stdout；纯本地文件读——不经 bag 节点、不创建任何 DDS 实体、不依赖域内状态）。输出样例与逐行含义见下文「预期输出」。

失败报错退出（exit 1）：`bag path [x] does not exist`（路径不存在）、`could not find metadata.json in bag directory [x]`（目录存在但无 metadata.json，非 bag 目录）、`parse metadata.json failed: ...`（metadata.json 非法 JSON）。

### 第 3 步：回放 bag（bag play）

命令形态（`<bag目录>` 为单个位置参数，CLI 自动创建并在退出前删除 bag 节点，播放期间服务端长驻阻塞，播毕即退出）：

```bash
yomkrpc bag play [-r <rate>] [--topics T1 T2 ...] [--start-paused] [-l | --loop] [--start-offset SEC] [-d <sec> | --delay <sec>] [--wait-for-all-acked MS] [--remap old:=new ...] [--read-ahead-queue-size N] [--qos-profile-overrides-path FILE] <bag目录>
```

基础用法：

```bash
yomkrpc bag play bag_in                    # 全主题 1x 按录制时间轴回放
yomkrpc bag play -r 2.0 bag_in             # 2 倍速回放（0.5 半倍速慢放排查竞态）
yomkrpc bag play bag_in --topics hello_world          # 只回放指定主题
yomkrpc bag play bag_in --topics 'hello_*' /other     # 清单可混排通配与精确项
yomkrpc bag play bag_in --start-paused                # 暂停态启动，SPACE 开始回放
yomkrpc bag play bag_in --loop                        # 循环回放，到尾回开头无限播放
yomkrpc bag play bag_in --start-offset 5             # 从 bag 内第 5 秒起播，跳过前段
yomkrpc bag play bag_in -d 3                         # 开播前先等 3 秒，等订阅端就绪
yomkrpc bag play bag_in --wait-for-all-acked 0       # 播毕等订阅端确认收齐再退出
yomkrpc bag play bag_in --remap /chatter:=hello_world   # 旧数据喂新主题名（可多规则空格分隔）
yomkrpc bag play bag_in --read-ahead-queue-size 5000   # 加大预读队列防 IO 抖动致播放延迟
yomkrpc bag play bag_in --qos-profile-overrides-path qos.json   # 按主题覆盖发布 QoS（文件命中>快照>回退）
yomkrpc bag play ./bag_2026-10-08_09-52-22            # 回放指定录制产物目录
                                           # tty 下播放中按 +/- 键 ±10% 动态调率
```

逐项说明：

- **`-r|--rate <rate>`**：回放速率倍率，缺省 1.0，合法范围 >0（2.0 两倍速、0.5 半倍速）；时间轴按录制 logTime 相对间隔除以倍率定时发布（绝对锚定抗累积漂移）；≤0 报 `invalid playback rate` 拒播（CLI 侧报 `not in the valid range (> 0.0)`）；
- **`--topics T1 T2 ...`**：只回放清单内主题（空格分隔多值，缺省全部；支持 record 同款通配——模式项按 bag 主题清单展开）；精确项不在 bag 报 `topic [x] not found in bag`，模式未命中报 `topic pattern [x] matched no bag topic`，双 `*` 报 `topic pattern [x] 中 '*' 出现多次，仅支持单个通配`——清单对账失败快速返回不建任何 DDS 实体；
- **`--start-paused`**：暂停态启动（对齐 record 同名选项）——读线程照常预读、播放挂起零发出，按 SPACE 开始；
- **`-l|--loop`**：循环回放——到尾即回开头无限播放，收尾统计为各轮累计；Ctrl+C 随时终止；
- **`--start-offset SEC`**：从 bag 内第 SEC 秒起播——读侧跳过前段消息不入队不计收尾统计（偏移基准 = bag 全场首条 logTime，不受 `--topics` 过滤影响）；支持小数，负值非法（CLI 报 `not in the valid range (>= 0.0)`）；
- **`-d|--delay SEC`**：开播延迟秒——每次开播含每轮循环前先睡 SEC 秒等订阅端匹配就绪（首条发出不早于发布实体建立 + SEC）；支持小数，须 >0（0 也拒，CLI 报 `not in the valid range (> 0.0)`）；
- **`--wait-for-all-acked MS`**：退出前等全部 RELIABLE 订阅端确认收齐毫秒——`0`=永等（可 Ctrl+C 中断），`>0` 超时毫秒逐主题各计，**超时仅告警不失败**；BEST_EFFORT 主题不参与 ackNack 直接跳过；缺省不等待；
- **`--remap old:=new ... | -m ...`**：主题重映射（空格分隔多值）——把 bag 里 old 主题的数据发到 new 主题，发布主题名精确匹配；old 不在 bag 静默无效；old 重复报 `duplicate remap rule [x]` 拒播；QoS/类型按 bag 快照照旧（remap 只改发布主题名）；
- **`--read-ahead-queue-size N`**：预读内存队列深度——读侧吸收磁盘 IO 抖动保证确定性回放，大→占内存但防播放延迟；缺省 1000，须 ≥1（0 与负同拒，CLI 报 `not in the valid range (>= 1)`）；
- **`--qos-profile-overrides-path FILE`**：按主题覆盖发布 QoS 文件（JSON）——每主题独立三档优先级：**文件命中 > bag 快照 > 回退**（旧 bag 无快照回退 `RELIABLE,VOLATILE`）。

**QoS 按录制快照恢复**：回放端预扫 `metadata.json` 后按主题按需重建透传发布实体——消息 CDR 原始字节直发不反序列化，发布 QoS 按 `topic_metadata.qos` 串（录制时落盘的 offered 快照，格式 `<Reliability>,<Durability>`）恢复；`qos` 键缺失/空串回退 `RELIABLE,VOLATILE`；未知拼写整体拒播。覆盖文件 `qos.json` 形态——JSON 顶层键 = bag 内主题名（`bag info` 显示的原名，精确匹配，与 `--remap` 正交），值 = QoS 字段对象：仅 `{reliability, durability}` 两字段、可独立给（只覆盖给定档，未给的档保持快照恢复值）；枚举值为大写 DDS 标准拼写——reliability 取 `RELIABLE` / `BEST_EFFORT`，durability 取 `VOLATILE` / `TRANSIENT_LOCAL` / `TRANSIENT` / `PERSISTENT`：

```json
{
  "/chatter": {"reliability": "BEST_EFFORT", "durability": "VOLATILE"},
  "/battery": {"durability": "VOLATILE"}
}
```

上例 `/battery` 只给 durability——reliability 保持 bag 快照恢复值；未知字段键忽略，不在 bag 清单的主题键静默无效，非法枚举值启动报 `topic [x] invalid qos string [y]` 整体拒播；覆盖为 `BEST_EFFORT` 后该主题对 RELIABLE 订阅端匹配不建立（收不到该主题数据，如需照常收数先核订阅端档位）。

**匹配前丢样本**：offered durability 为 VOLATILE 时订阅端晚于消息发布建立的匹配必丢样本——先起订阅端再开播可保收满；快照为 TRANSIENT_LOCAL 时匹配建立后经 latched 补发可收满全场；或用 `-d|--delay` 开播延迟给足匹配窗口。

**交互键**（tty 下，与 record 键位一致）：SPACE 从暂停态开始（打印 `replay started`，仅暂停态生效——缺省启动即开播，运行态按 SPACE 无操作）、p 暂停（打印 `replay paused`）、r 恢复（打印 `replay resumed`），可反复切换；`+`/`-` 调节回放速率 ±10% 步进（成功打印 `rate set to <值>`、≤0 拒绝打印 `failed to set rate to <值>`）；Ctrl+C 终止并按已播出数收尾统计；非 tty（管道/重定向）自动跳过键盘监听。

### 第 4 步：转换 bag（bag convert）

```bash
yomkrpc bag convert -i <bag目录>|--input <bag目录> [-i <bag目录> ...] -o <配置文件>|--output <配置文件>
```

```bash
yomkrpc bag convert -i bag_in -o convert.json              # 单输入全缺省条目：全主题全时段合并输出
yomkrpc bag convert -i bag_a -i bag_b -o convert.json      # 多输入按 logTime 全局归并
```

按输出配置文件把一个或多个输入 bag 的消息过滤、裁剪、合并后写为新 bag（纯本地文件操作——不经 bag 节点、不创建任何 DDS 实体，未建 bag 节点亦可调用）。多输入按消息 logTime 全局升序 k-way 归并（同刻消息按输入序稳定先后）；转换透传原始 CDR 字节与消息时间戳/序号，不反序列化——产物仍为标准 mcap bag，可被 `bag info`、`bag reindex` 与任意 mcap 标准读库直接读取。

输出配置文件为 JSON（顶层 `output_bags` 键），每条目独立一组输出参数，一次调用可产出多个 bag：

```json
{
    "output_bags": [
        {
            "uri": "out_extract",
            "topics": ["hello_world", "hello_*"]
        },
        {
            "uri": "out_trimmed",
            "start_time": "2026-10-08_09-52-22_685-614-542",
            "end_time": "2026-10-08_09-53-22_685-614-542",
            "max_bagfile_size": 1048576,
            "max_bagfile_duration": 60
        }
    ]
}
```

条目字段（未知字段忽略）：

| 字段 | 语义 |
|---|---|
| `uri` | 输出 bag 目录（必填非空字符串，相对/绝对均可；已存在报错——防混入旧产物，同 record `-o` 口径；父目录自动多级创建） |
| `topics` | 输出主题清单（缺省 = 全部输入主题；元素为精确主题名或恰好一个 `*` 的通配模式——模式形态与前后缀夹逼语义同 record 清单通配，但展开对象是输入 bag 的主题集而非发现缓存；精确项须存在于输入主题集、模式须命中至少一个输入主题，否则报错；展开集去重升序） |
| `start_time` / `end_time` | 收纳区间（按消息 logTime，缺省 0 = 不限；含头含尾；start > end 报错）。两种写法：非负整数纳秒；或可读时间字符串 `"YYYY-MM-DD_HH-MM-SS_毫秒-微秒-纳秒"`（本地时区，与录制产物 metadata.json `starting_time.nanoseconds_since_epoch_format` 伴生键同构——从产物 metadata.json 直接拷贝即用） |
| `max_bagfile_size` | 单输出分片最大字节数（0=不分片缺省，>0 且 <1024 报错；实际分片粒度不小于 chunk 落盘边界，同 record `-b`） |
| `max_bagfile_duration` | 单输出分片最大时长秒（0=不分片缺省；与 size 同用先到先分，同 record `-d`） |

行为要点：

- 输入收集：`-i` 可重复、至少 1 个（相对/绝对均可）；逐输入校验存在/为目录/含分片，重复目录拒绝（同名目录重复归并会双写同一消息）；分片收集与编号升序同 reindex 口径；
- 类型预扫：逐分片读 Channel 映射收集输入主题全集与主题→类型名、主题→QoS 串；**同主题跨输入类型或 QoS 串不一致均报错**（空串不参与比较——旧格式 bag 的缺值不误报）；入选主题无类型名（旧格式 bag）拒转；
- 过滤与归并：主题 ∈ 展开集 且 logTime ∈ [start, end]（0=不限）双条件过滤；输出分片滚动为 `bag_0.mcap`、`bag_1.mcap`、...；
- 条目独立：逐条目重建游标与输出 writer；**任一条目失败整体报错退出（已产生的输出目录保留不回滚，便于排查）**；
- 成功输出：每条目一行 `converted <uri>: N messages / M bytes / K topics`（N/M 为实际写出消息数与字节，K 为展开主题数）。

### 第 5 步：修复 bag 元信息（bag reindex）

```bash
yomkrpc bag reindex bag_2026-10-02_19-37-33_726   # 重建第 1 步录制产物的 metadata.json
yomkrpc bag info bag_2026-10-02_19-37-33_726      # 重建后可正常查看
```

从 bag 目录内的 mcap 分片文件重建 `metadata.json` 并**无条件覆盖写出**（纯本地文件操作——不经 bag 节点、不创建任何 DDS 实体，未建 bag 节点亦可调用）。元信息文件缺失（中断录制 / 手工删除）或损坏时均可重建，行为对齐 `ros2 bag reindex`（"Reconstruct metadata file for a bag"）；成功打印一行 `reindexing complete`。

行为要点：

- **文件收集**：仅收集目录内匹配 `<前缀>_<编号>.mcap` 命名约定的 regular file，按**编号升序**处理（`bag_10.mcap` 排 `bag_2.mcap` 之后，非字典序）；其余条目（含子目录、非 .mcap 文件）跳过；
- **统计合并**：per-topic 条数跨分片累加、starting_time 取各分片最小、duration 为全局跨度（end − start）；主题清单按主题名升序排列；
- **type/qos 经 Channel.metadata 恢复**：录制侧把消息类型名与 QoS 串写入各分片 Channel 的 metadata，重建时读回分别填入 `topic_metadata.type` 与 `topic_metadata.qos`；旧格式 bag（无该元数据的既有分片）读不到则回退空串——文件里没有就不虚构；
- **无 summary 回退**：分片缺 summary（中断录制残片）或 summary 损坏时，走 mcap 库级 `AllowFallbackScan` 线性扫描重建统计——中断 bag 也能恢复元信息，非报错。

失败报错退出（exit 1）：`bag path [x] does not exist`（路径不存在）、`must specify a bag directory`（路径存在但不是目录，传了文件）、`empty directory`（目录无任何条目）、`no bag files found for reindexing`（目录有文件但无一匹配 `<前缀>_<编号>.mcap` 命名约定）、`open <名> failed: ...` / `read summary of <名> failed: ...`（分片打开失败 / summary 读取且回退扫描失败）。

### 端到端演练：录一段再回放

以下用一个完整闭环串联五个子命令（bag 目录名以实际输出为准，下文沿用 README 样例目录名 `bag_2026-10-02_19-37-33_726`）：

```bash
# 终端 1：启动发布端（ExampleYomkRpcPub 每 1s 发布一次 hello_world；
#         或用等价 CLI 发布端：yomkrpc topic pub hello_world '{"data":"hello bag"}'）
ExampleYomkRpcPub
```

```bash
# 终端 2：录制 hello_world 约 10 秒后按 Ctrl+C 停止
yomkrpc bag record hello_world
# [19:37:30.123456] recording topics from 1 list item(s) on domain 0, press p to pause, r to resume, Ctrl+C to stop
# recording topic=hello_world type=YomkRpc::MString
# ...等待约 10 秒...
# ^C
```

```bash
# 终端 2：查看录制产物
yomkrpc bag info bag_2026-10-02_19-37-33_726
```

```bash
# 终端 3（先起订阅观察端，再开播——防 VOLATILE 快照丢开头样本）
yomkrpc topic print hello_world
```

```bash
# 终端 2：回放（-d 3 给订阅端预留 3 秒匹配窗口），播毕自动退出
yomkrpc bag play -d 3 ./bag_2026-10-02_19-37-33_726
# 终端 3 同步逐条打印回放出的消息 JSON
```

```bash
# 终端 2：convert 裁剪——从录制产物裁出前 5 秒写为新 bag
cat > convert.json <<'EOF'
{
    "output_bags": [
        {
            "uri": "out_trimmed",
            "topics": ["hello_world"],
            "max_bagfile_duration": 5
        }
    ]
}
EOF
yomkrpc bag convert -i bag_2026-10-02_19-37-33_726 -o convert.json
yomkrpc bag info out_trimmed
```

> `max_bagfile_duration` 为单输出分片最大时长秒（本例 5 秒一片）；若想按时段精确裁剪，把条目换成 `start_time`/`end_time`（从产物 metadata.json 的 `_format` 伴生键直接拷贝即用）。

## 预期输出

### bag record

启动行显示清单项数与交互键提示——指定 `-o` 时追加 `output dir=<目录>` 回显，指定 `-b` 非零值时追加 `max bag size=<字节>` 回显，指定 `--max-bag-duration` 非零值时追加 `max bag duration=<秒>s` 回显，指定 `-c`/`--max-cache-size` 非零值时追加 `cache=<字节>` 回显；节点层逐主题输出实际录制清单——通配展开结果以此为准；Ctrl+C 触发收尾后输出统计——首行 bag 目录名，其后每主题一行统计，随后进程退出：

```
[19:37:30.123456] recording topics from 1 list item(s) on domain 0, press p to pause, r to resume, Ctrl+C to stop
recording topic=hello_world type=YomkRpc::MString
recording paused
recording resumed
^C
bag_2026-10-02_19-37-33_726
hello_world: 12 条 / 288 字节
```

（`recording paused`/`recording resumed` 两行为按下 p/r 键的回显，未用交互键时不会出现；缓存丢条时收尾还会按主题打印丢条统计 + `Total dropped` 总计，无丢弃不打印。）

### bag info

首行为空行（对齐参考实现输出形态）；`Bag size:` 为目录递归总大小（1024 进制换算 B/KiB/MiB/GiB/TiB）；`Topic information:` 每主题一行，多主题时续行缩进 19 空格对齐；`QoS` 段可选——metadata.json 有 qos 键且非空才显示：

```

Files:             bag_0.mcap
Bag size:          2.6 KiB
Storage id:        mcap
Duration:          4.012345678s
Start:             Oct  2 2026 19:37:33.726412000 (1791953853.726412000)
End:               Oct  2 2026 19:37:37.738757678 (1791953857.738757678)
Messages:          12
Topic information: Topic: hello_world | Type: YomkRpc::MString | QoS: RELIABLE,TRANSIENT_LOCAL | Count: 12 | Serialization Format: cdr
```

### bag play

开播首行打印交互键提示，其后逐主题打印类型与生效 QoS 串（`replaying topic=` 行主题名为 remap 后发布名）+ bag 元信息行；收尾按主题打印播出统计（topic 同为 remap 后新名，Ctrl+C 提前停止时未播主题计 0）。其余启动行变体：`--start-paused` 后缀为 `started paused, press SPACE to start, p/r to pause/resume, +/- rate, Ctrl+C to stop`；`-l/--loop` 后缀附 `, looping`；`--start-offset` 后缀附 `, offset=<SEC>`；`-d/--delay` 后缀附 `, delay=<SEC>`；`--wait-for-all-acked` 显式传参时后缀附 `, ack-wait=<MS>`；`--remap` 后缀附 `, remap=<old1>:=<new1>[,<old2>:=<new2>...]`；`--read-ahead-queue-size` 后缀附 `, queue=<N>`；`--qos-profile-overrides-path` 后缀附 `, qos-override=<FILE>`：

```text
replaying bag [ bag_in ] on domain 0 , rate=1 , press p to pause, r to resume, +/- rate, Ctrl+C to stop
replaying topic=/chatter type=YomkRpc::MString qos=RELIABLE,TRANSIENT_LOCAL
replaying topic=/battery type=YomkRpc::MInt32 qos=RELIABLE,TRANSIENT_LOCAL
replaying bag [bag_in]: 60 messages / duration 00-00-06_000-000-000 / start 2026-10-08_09-52-22_685-614-542
bag_in
/chatter: 30 条 / 150 字节
/battery: 30 条 / 120 字节
```

### bag convert / bag reindex

convert 成功每条目一行统计；reindex 成功一行完成提示（其后可用 `bag info` 正常查看）：

```text
converted out_trimmed: 6 messages / 144 bytes / 1 topics
```

```text
reindexing complete
```

### 演练核对清单

端到端演练完成后应能看到：

1. 终端 2 录制收尾打印 bag 目录名与 `hello_world: N 条 / M 字节`（N 约等于录制秒数，发布端 1 Hz）；
2. `bag info` 显示 `Storage id: mcap` 与该主题计数行；
3. 回放期间终端 3 的 `topic print` 逐条打印消息 JSON（形如 `[17:18:34.868398] topic=hello_world type=YomkRpc::MString seq=1` 与 `{"data":"hello world 0"}` 两行一组）；
4. `bag play` 播毕打印收尾统计行 `hello_world: N 条 / M 字节`；
5. convert 打印 `converted out_trimmed: ...` 一行，`bag info out_trimmed` 可正常查看（时长约 5 秒分片）。

## 常见错误与排障

| 现象 | 原因与处理 |
|---|---|
| 录制报 `bag directory [x] already exists, remove it or choose another name` | `-o` 指定的目录已存在（fail-fast 于发现校验前，不落盘）：删除旧目录或换名重试 |
| 录制报 `模式 [x] 未匹配到任何主题，请检查通配输入（域 N，已等待 M ms）` | 通配未命中：模式拼错或域内确无匹配主题；先 `yomkrpc topic list` 核对域内主题名，注意模式须加引号防 shell glob 展开 |
| 录制报 `主题 [x] 既无发布者也无订阅者，请检查主题名输入（域 N，已等待 M ms）` | 精确主题无任何端点：主题名拼错 / 对端未上线 / 域号不一致（`echo $YOMKRPC_DDS_DOMAIN_ID` 与发布端对齐）；先启动发布端再录制 |
| 录制报 `max bag size [x] too small, minimum split file size is 1024 bytes (0 to disable splitting)` | `-b` 分片上限过小（非 0 且低于 1024 字节）：调大或传 0 关闭分片 |
| 录制收尾打印丢条统计（按主题计数 + `Total dropped` 总计） | `-c` 写缓存满丢弃新消息：加大 `-c`/`--max-cache-size`（默认 100 MiB），或传 0 改每条直写（牺牲突发吞吐换零丢失） |
| 指定 `-b` 后分片大小不生效（单片偏大/偏小） | chunk 边界：受存储层 chunk 缓冲影响，`-b` 实际分片粒度不小于 chunk 落盘边界——默认 786432 字节，小于该值的 `-b` 实际每 chunk 一片；时长分片（`-d`）不受 chunk 影响 |
| 回放时订阅端一条都收不到 / 只有开头几条 | 回放无订阅端就绪：offered durability 为 VOLATILE 时晚建的订阅端必丢样本——先起订阅端再开播，或加 `-d <sec>` 开播延迟给足匹配窗口；RELIABLE 快照下可加 `--wait-for-all-acked 0` 播毕等确认收齐再退出 |
| 回放报 `topic [x] not found in bag` / `topic pattern [x] matched no bag topic` | `--topics` 清单对账失败（精确项不在 bag / 模式未命中）：先 `yomkrpc bag info <bag目录>` 核对 bag 内主题名，注意精确匹配大小写与 `/` 前缀 |
| 回放报 `topic [x] invalid qos string [y]` | QoS 快照或 `--qos-profile-overrides-path` 覆盖文件含未知枚举拼写（原串随报错回显）：按大写 DDS 标准拼写修正（`RELIABLE`/`BEST_EFFORT`、`VOLATILE`/`TRANSIENT_LOCAL`/`TRANSIENT`/`PERSISTENT`） |
| 回放报 `invalid remap rule [x] (expected old:=new)` / `duplicate remap rule [x]` | `--remap` 规则缺 `:=` 或两侧为空 / old 主题重复：按 `old:=new` 形态逐条修正 |
| 回放/转换/查看报 `could not find metadata.json in bag directory [x]` | bag 目录元信息缺失（中断录制/手工删除）：`yomkrpc bag reindex <bag目录>` 从 mcap 分片重建后重试 |
| convert 报 `output dir [x] already exists` | 输出条目 `uri` 目录已存在（防混入旧产物）：删除旧目录或换 uri |
| convert 报 `topic [x] has conflicting types [a] vs [b] across inputs` | 同主题跨输入类型不一致：核对多输入 bag 是否来自同一录制口径，或用条目 `topics` 缩小清单避开冲突主题 |

## 延伸阅读

- [../features/bag-service.md](../features/bag-service.md)——bag 功能能力清单、参数总表与 `YOMKRPC_BAG_*` 宏等价调用（含 `yomk::bagRecordStop/Pause/Resume`、`yomk::bagPlaySetRate` 库侧接口）
- [cli-observe-handbook.md](./cli-observe-handbook.md)——`topic print`/`topic list`/`topic info` 等观察命令，与回放验证配合使用
- [cli-publish-handbook.md](./cli-publish-handbook.md)——`topic pub` 手工发布消息，演练中可替代 `ExampleYomkRpcPub` 作发布端
- [env-vars-reference.md](./env-vars-reference.md)——域号等公共环境变量（bag 命令域号经 `YOMKRPC_DDS_DOMAIN_ID` 选择）
- [返回手册入口](./README.md)
