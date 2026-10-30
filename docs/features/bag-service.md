# 功能文档：Bag 录制与回放

> **本文档覆盖范围**：`YomkRpcBagService` bag 服务的完整能力——bag 节点模型、mcap 录制（分片/写缓存/暂停恢复/通配展开）、元信息查看与重建索引、多输入转换、按录制时间轴回放；以及"哪些操作须 bag 节点、哪些是纯文件操作"的能力边界。各命令的端到端实操见 [Bag 录制回放手册](../manual/bag-handbook.md)。

## 功能定位

Bag 服务把 DDS 域内流量录制为标准 mcap 格式 bag（类型无关，无需 IDL 生成代码——发现匹配远端 DataWriter 自动解析类型，透传订阅将原始 CDR 字节写入 mcap），并提供元信息查看、索引重建、多输入过滤/裁剪/归并转换与按录制时间轴回放为真实 DDS 流量的全套操作。命令形态对齐 `ros2 bag`（record/info/reindex/convert/play）。

## 能力清单

| 能力 | 宏 API | yomkrpc CLI | 是否须 bag 节点 |
|---|---|---|---|
| 创建/删除 bag 节点 | `YOMKRPC_BAG_NODE(...)` / `YOMKRPC_BAG_DEL_NODE()` | —（CLI 内部自动建删） | — |
| 录制主题列表为 mcap bag | `YOMKRPC_BAG_RECORD(topics, outputDir, maxBagSize, maxBagDurationSec, maxCacheSize)` | `bag record` | **须**（宏侧先 `YOMKRPC_BAG_NODE`） |
| 查看 bag 元信息 | `YOMKRPC_BAG_INFO(bagDir)` | `bag info` | 不须（纯文件读，无须 create_node） |
| 重建 bag 元信息 | `YOMKRPC_BAG_REINDEX(bagDir)` | `bag reindex` | 不须（纯文件操作，无须 create_node） |
| 转换 bag（过滤/裁剪/归并） | `YOMKRPC_BAG_CONVERT(inputs, cfgPath)` | `bag convert` | 不须（纯文件操作，无须 create_node） |
| 回放 bag | `YOMKRPC_BAG_PLAY(...)`（等价宏见 README 6.11，须先自建 bag 节点） | `bag play` | CLI 自动创建并在退出前删除 bag 节点 |

> 除 `YOMKRPC_VERSION()` 外，其余宏均返回 `YomkResponse`，调用后须判 `m_status == YomkResponse::eOk`；失败时可读 `m_msg` 获取错误信息。

## 使用方式概述

### bag 节点（`YOMKRPC_BAG_NODE`）

`YOMKRPC_BAG_NODE([domainId[, udpDiscoverServerLocator[, tcpDiscoverServerLocator]]])`（端点 `/YomkRpcBagService/create_node`）：

- 单节点模型（一个进程至多一个，重复创建须先删除）；`YOMKRPC_BAG_DEL_NODE()` 删除 bag 节点并销毁其全部 DDS 实体，未创建时返回错误；
- 全可变参——无参形态三字段均经 DDSBagNode 类内缺省初始化器读环境变量（domainId 读 `YOMKRPC_DDS_DOMAIN_ID`，两 serverLocator 按 5.7 开关门控读 `YOMKRPC_UDP/TCP_DISCOVER_SERVERS`，语义同 `YOMKRPC_NODE`）；显式实参完全兼容旧调用；
- domainId 合法范围 [0,232]，仅同域端点可被发现与录制；两 serverLocator 为 UDP/TCP 发现服务器直配聚合串，语义同 `YOMKRPC_NODE` 形态 3/4（CLIENT 协议强制，非法串创建失败回显），详见 [discovery-server.md](./discovery-server.md)。

### 录制（`YOMKRPC_BAG_RECORD` / `bag record`）

`YOMKRPC_BAG_RECORD(topics, outputDir, maxBagSize, maxBagDurationSec, maxCacheSize)`（端点 `/YomkRpcBagService/bag_record`）**长驻阻塞至 Ctrl+C**（SIGINT 经 `yomk::bagRecordStop` 无锁置位）收尾返回。CLI 形态：

```bash
yomkrpc bag record hello_world           # 录制单主题（默认域 0）
yomkrpc bag record rt/chatter rt/tf      # 多主题清单
yomkrpc bag record 'hello_*' '*_world'   # 通配模式（清单项恰好含一个 *，引号防 shell glob 展开）
yomkrpc bag record -o my_session hello_world   # 指定 bag 目录名（相对/绝对路径均可）
yomkrpc bag record -b 1048576 hello_world      # 单分片最大 1MB（写满滚动 bag_N.mcap 分片，0=不分片缺省）
yomkrpc bag record -d 60 hello_world          # 单分片最大 60s（-d/--max-bag-duration；时长达限滚动 bag_N.mcap 分片，0=不分片缺省）
yomkrpc bag record -c 8388608 hello_world     # 写缓存双缓冲 8MB（-c/--max-cache-size；回调拷贝入队独立线程写盘，CLI 缺省 100MiB，0=每条直写）
yomkrpc bag record --start-paused hello_world  # 暂停态启动（订阅照常、消息丢弃不写入，按 SPACE 开始录制）
```

等价宏调用（完整用户代码，`YOMKRPC_BAG_*` 宏定义于 `YomkRpcAPI.h`，SIGINT 处理与暂停控制声明于 `YomkRpcBagService.h`）：

```cpp
#include <YomkServer/YomkAPI.h>
#include <YomkRpc/YomkRpcAPI.h>
#include <YomkRpc/YomkRpcBagService.h> // bagRecordStop/bagRecordReset（停止标志读写端）

#include <csignal>
#include <iostream>
#include <vector>

using namespace yomk;

void onSignal(int) { bagRecordStop(); }  // 无锁原子置位，录制循环读它收尾

int main()
{
    YOMK_INIT();
    YOMK_NEW_SERVICE(YomkRpcBagService);

    // 1. 复位残留停止标志（防上次会话置位导致秒退）后创建 bag 节点（单节点模型）
    bagRecordReset();
    auto resp = YOMKRPC_BAG_NODE(0);
    if (resp.m_status != YomkResponse::eOk)
    {
        return 1;
    }

    // 2. Ctrl+C → bagRecordStop 置位；可选暂停态启动：录制前 bagRecordPause() 置位
    //    （订阅照常、消息丢弃不写入，等价 CLI --start-paused），bagRecordResume() 恢复写入
    std::signal(SIGINT, onSignal);

    // 3. 长驻阻塞：启动校验、订阅与录制、收尾落盘都在服务端完成，返回即录制结束
    std::vector<std::string> topics{"hello_world", "hello_*"};
    resp = YOMKRPC_BAG_RECORD(topics, "my_bag", 0, 0, 0);  // 第二参 bag 目录名（空串=缺省时间戳名），
    //                                                      第三参单分片最大字节数（0=不分片，>0 写满滚动 bag_N.mcap），
    //                                                      第四参单分片最大时长秒（0=不分片，>0 时长达限滚动；与字节上限同用时先到先分），
    //                                                      第五参写缓存双缓冲字节数（0=每条直写；>0 回调拷贝入队独立线程写盘，缓存满丢新并计数）
    if (resp.m_status != YomkResponse::eOk)
    {
        // m_msg 为逐项原因（校验类多行）；失败路径同样须删除已建 bag 节点
        YOMKRPC_BAG_DEL_NODE();
        return 2;
    }

    // 4. 成功回执 StringArray：首行 bag 目录名，其后每主题一行 "topic: N 条 / M 字节"
    YomkUnPackPkg(resp.m_data, StringArray, lines);
    if (lines != nullptr)
    {
        for (const auto &line : lines->d) { std::cout << line << "\n"; }
    }

    // 5. 退出前显式删除 bag 节点（确保 DDS 实体在 FastDDS 静态资源销毁前清理）
    YOMKRPC_BAG_DEL_NODE();
    return 0;
}
```

暂停恢复：库侧 `yomk::bagRecordPause()`/`bagRecordResume()`（等价 CLI 交互键 p/r 与 `--start-paused` 的 SPACE 启动）。落盘产物为 bag 目录两文件：`bag_0.mcap`（标准 mcap 容器，逐主题 Channel，`Channel.metadata` 记录 `type=<类型名>`、`qos=<Reliability>,<Durability>` 串，消息 record 保存原始 CDR 字节；指定分片时滚动产生 `bag_1.mcap`、`bag_2.mcap`、...，各分片独立完整可读回）与 `metadata.json`（顶层平铺 bag 元信息：`version`/`storage_identifier`/`relative_file_paths`/`starting_time`（含 `_format` 可读伴生键）/`duration`/`message_count`/`topics_with_message_count`）。

### 纯文件操作（无须 bag 节点）

```bash
yomkrpc bag info bag_2026-10-02_19-37-33_726     # 查看 bag 元信息（输出对齐 ros2 bag info 的多行文本）
yomkrpc bag reindex bag_2026-10-02_19-37-33_726  # 重建被删/损坏的 metadata.json（无条件覆盖写出）
yomkrpc bag convert -i bag_in -o convert.json              # 单输入全缺省条目：全主题全时段合并输出
yomkrpc bag convert -i bag_a -i bag_b -o convert.json      # 多输入按 logTime 全局归并
```

- **`YOMKRPC_BAG_INFO(bagDir)`**：读 metadata.json 与目录递归总大小输出多行元信息文本（首行空行）；纯文件读不经 bag 节点（无须 create_node）；成功返回 StringArray（每项一行，首项为空行）；
- **`YOMKRPC_BAG_REINDEX(bagDir)`**：从 mcap 分片（`<前缀>_<编号>.mcap` 编号升序）重建 metadata.json 并**无条件覆盖**写出（缺失/损坏均可重建；缺 summary 的中断残片回退 mcap 库级 `AllowFallbackScan` 线性扫描）；纯文件操作不经 bag 节点；成功返回 eOk（`m_msg == "ok"`，无数据载荷）；
- **`YOMKRPC_BAG_CONVERT(inputs, cfgPath)`**：读输入 bag 目录集与输出配置文件，把输入消息按配置过滤/裁剪/归并写为新 bag（多输入按 logTime 全局归并；转换透传原始 CDR 字节与消息时间戳/序号，不反序列化）；纯文件操作不经 bag 节点；成功返回 StringArray 每条目一行 `converted <uri>: N messages / M bytes / K topics` 统计。

### 回放（`bag play`）

`yomkrpc bag play <bag目录>` 按录制时间轴把 bag 回放为真实 DDS 流量：预扫 metadata.json 后按主题按需重建透传发布实体——消息 CDR 原始字节直发不反序列化，发布 QoS 按录制快照恢复；读播解耦（读线程从分片逐条预读入有界内存队列、播放线程按消息 logTime 相对间隔定时发布）。CLI 自动创建并在退出前删除 bag 节点（域号经环境变量 `YOMKRPC_DDS_DOMAIN_ID` 选择，同其他命令）。

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
```

交互键（tty 下）：SPACE 从暂停态开始、p 暂停、r 恢复、`+`/`-` ±10% 动态调率，Ctrl+C 停止。库侧同源控制函数（定义于 `YomkRpcBagService.h`，`namespace yomk`，无锁原子标志不经 DDS 协议传递）：

| 函数 | 语义 |
|---|---|
| `yomk::bagPlayStop()` | 置停止标志（播放线程各阻塞处只读，async-signal-safe，SIGINT 处理即用此） |
| `yomk::bagPlayPause()` / `yomk::bagPlayResume()` | 置暂停 / 解除暂停（暂停期播放线程冻结时间轴零发出，恢复后重锚时间基准继续按录制节奏发布） |
| `yomk::bagPlaySetRate(rate)` | 动态调率（对齐 ros2 `~/set_rate`：rate<=0 拒绝返回 false 保持原值，合法值即时生效于后续调度） |
| `yomk::bagPlayGetRate()` | 查询当前倍率（对齐 ros2 `~/get_rate`） |
| `yomk::bagPlayReset()` | 发起回放前调用：复位停止与暂停标志 + 倍率复位 1.0（防上次会话残留导致秒退/开播即冻住/倍率漂移） |

录制侧配套：`yomk::bagRecordStop()`（Ctrl+C 置位收尾）、`yomk::bagRecordPause()`/`yomk::bagRecordResume()`（暂停期照常 take 排空但丢弃不写）、`yomk::bagRecordReset()`（双复位停止+暂停，防上次会话残留表现为"何都不录"）。

## 参数与约束

### `YOMKRPC_BAG_RECORD` 五参数

| 参数 | 约束 |
|---|---|
| `topics` | 主题清单（`std::vector<std::string>`）；清单项支持通配模式（见下表）；模式项启动校验时按发现缓存展开为实际主题集合（去重升序、与精确项合并） |
| `outputDir` | bag 目录名/路径（相对/绝对均可，父目录自动多级创建，已存在报错；空串=缺省当前路径下 `bag_<YYYY-MM-DD_HH-MM-SS_mmm>`） |
| `maxBagSize` | 单分片最大字节数（>0 写满滚动 `bag_N.mcap` 分片，下限 1024 字节过小报错；0=不分片；受存储层 chunk 缓冲影响，实际分片粒度不小于 chunk 落盘边界——默认 786432 字节） |
| `maxBagDurationSec` | 单分片最大时长秒（0=不分片，无下限校验，与 maxBagSize 同用先到先分；暂停期不计入分片时长） |
| `maxCacheSize` | 写缓存双缓冲字节数（0=每条直写，>0 回调拷贝入队、独立消费线程写盘，缓存满丢弃新消息并按主题计数；CLI 缺省 100 MiB，最坏占 2 倍内存） |

录制为透传设计：每主题一个 reader（QoS 的 Reliability/Durability 跟随远端 writer offered 值，requested ≤ offered 恒成立）直写原始 CDR 字节到 mcap。仅有订阅者的主题同样通过校验（先建订阅，发布者上线匹配后自动开始录流）。

### 通配模式（清单项恰好含一个 `*`）

| 模式 | 匹配语义 |
|---|---|
| `hello_*` | 前缀匹配（以 `hello_` 开头的全部已发现主题） |
| `*_hello` | 后缀匹配 |
| `pre*suf` | 中间夹逼（前后缀之间夹任意片段，`*` 不与首尾共享字符，对齐 fnmatch） |
| `*` | 匹配全部已发现主题 |

- 清单项含 0 个 `*` 为精确主题名；**恰好 1 个 `*` 为通配模式；≥2 个 `*` 输入有误报错退出**（fail-fast）；
- 模式在启动校验时按发现缓存展开为实际主题集合，与精确项**去重合并**，展开清单升序——统计与 metadata 输出确定；
- 展开于启动校验时一次定型：录制期间新上线的匹配主题不自动加入；
- **shell 下模式须加引号**（bash 会对未加引号的 `*` 做当前目录文件名 glob 预展开）。

### `bag convert` 配置文件字段（cfg 顶层 `output_bags` 数组，每条目独立）

| 字段 | 语义 |
|---|---|
| `uri` | 输出 bag 目录（必填非空字符串，相对/绝对均可；已存在报错——防混入旧产物，同 record `-o` 口径；父目录自动多级创建） |
| `topics` | 输出主题清单（缺省 = 全部输入主题；元素为精确主题名或恰好一个 `*` 的通配模式——展开对象是输入 bag 的主题集而非发现缓存；精确项须存在于输入主题集、模式须命中至少一个输入主题，否则报错；展开集去重升序） |
| `start_time` / `end_time` | 收纳区间（按消息 logTime，缺省 0 = 不限；含头含尾；start > end 报错）。两种写法：非负整数纳秒；或可读时间字符串 `"YYYY-MM-DD_HH-MM-SS_毫秒-微秒-纳秒"`（本地时区，与录制产物 metadata.json 的 `_format` 伴生键同构——从产物直接拷贝即用） |
| `max_bagfile_size` | 单输出分片最大字节数（0=不分片缺省，>0 且 <1024 报错） |
| `max_bagfile_duration` | 单输出分片最大时长秒（0=不分片缺省；与 size 同用先到先分） |

配置文件示例：

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

### `bag play` 关键参数

| 参数 | 语义 |
|---|---|
| `-r <rate>` / `--rate <rate>` | 速率倍率，缺省 1.0，合法范围 >0（2.0 两倍速、0.5 半倍速） |
| `--topics T1 T2 ...` | 只回放清单内主题（空格分隔多值，缺省全部；精确项须在 bag 清单内否则报错，模式项按清单展开，支持 record 同款通配） |
| `--start-paused` | 暂停态启动（读线程照常预读、播放挂起零发出，暂停期不计入播放时间轴），按 SPACE 开始 |
| `-l` / `--loop` | 循环回放（到尾即回开头无限播放，收尾统计为各轮累计） |
| `--start-offset SEC` | 从 bag 内第 SEC 秒起播（读侧跳过前段消息不入队不计统计，支持小数，负值非法） |
| `-d <sec>` / `--delay <sec>` | 开播延迟秒（每次开播含每轮循环前先睡 SEC 秒等订阅端匹配就绪，支持小数，须 >0） |
| `--wait-for-all-acked MS` | 退出前等全部 RELIABLE 订阅端确认收齐毫秒（0=永等可 Ctrl+C 中断，>0 超时逐主题各计，超时仅告警不失败；BEST_EFFORT 主题跳过） |
| `--remap old:=new ...` / `-m ...` | 主题重映射（空格分隔多值，把 bag 里 old 主题的数据发到 new 主题——发布主题名精确匹配，old 不在 bag 静默无效） |
| `--read-ahead-queue-size N` | 预读内存队列深度（读侧吸收磁盘 IO 抖动保证确定性回放，大→占内存但防播放延迟，缺省 1000，须 ≥1） |
| `--qos-profile-overrides-path FILE` | 按主题覆盖发布 QoS 文件（JSON，键=bag 原主题名，值={reliability, durability} 枚举名可独立给，枚举值为大写 DDS 标准拼写）——每主题独立三档优先级：**文件命中 > bag 快照 > 回退**（旧 bag 回退 `RELIABLE,VOLATILE`） |

匹配前丢样本提示：offered durability 为 VOLATILE 时订阅端晚于消息发布建立的匹配必丢样本——先起订阅端再开播、或用 `-d/--delay` 开播延迟给足匹配窗口；快照为 TRANSIENT_LOCAL 时匹配建立后经 latched 补发可收满全场（writer history 深度为独立常量，不随 `--read-ahead-queue-size` 联动）。

## 错误与边界行为

### `bag record`（录制阶段失败 exit=2；校验失败不建目录不产生任何文件）

| 错误文本 | 卡点 |
|---|---|
| `no topics given` | 未给主题清单 |
| `topic [x] 中 '*' 出现多次，仅支持单个通配` | 清单项含 ≥2 个 `*` |
| `empty topic name in topic list` | 清单含空名主题 |
| `duplicate topic [x] in topic list` | 清单内精确主题名重复 |
| `bag directory [x] already exists, remove it or choose another name` | `-o` 指定的目录已存在（fail-fast 于发现校验前，不落盘） |
| `max bag size [x] too small, minimum split file size is 1024 bytes (0 to disable splitting)` | `-b` 分片上限过小（非 0 且低于 1024 字节） |
| `主题 [x] 既无发布者也无订阅者，请检查主题名输入（域 N，已等待 M ms）` | 校验收敛后该精确主题仍无任何端点（主题名拼错 / 对端未上线 / 域号不一致） |
| `模式 [x] 未匹配到任何主题，请检查通配输入（域 N，已等待 M ms）` | 校验收敛后该模式命中 0 主题 |

启动校验轮询发现缓存快照（每 200ms 一轮，连续 15 轮不变或全部清单项确认有端点即提前收敛；总窗不足 6s 自动提升轮数，防 SPDP 公告期误判；**校验参数固定，不经 `YOMKRPC_DDS_DISCOVER_ROUNDS` 调整**）。

### `bag info`（exit 1）

| 错误文本 | 卡点 |
|---|---|
| `bag path [x] does not exist` | 路径不存在 |
| `could not find metadata.json in bag directory [x]` | 目录存在但无 metadata.json（非 bag 目录） |
| `parse metadata.json failed: ...` | metadata.json 非法 JSON |

### `bag reindex`（exit 1）

| 错误文本 | 卡点 |
|---|---|
| `bag path [x] does not exist` | 路径不存在 |
| `must specify a bag directory` | 路径存在但不是目录（传了文件） |
| `empty directory` | 目录无任何条目 |
| `no bag files found for reindexing` | 目录有文件但无一匹配 `<前缀>_<编号>.mcap` 命名约定 |
| `open <名> failed: ...` / `read summary of <名> failed: ...` | 分片打开失败 / summary 读取且回退扫描失败 |

### `bag convert`（exit 1；任一条目失败整体报错，已产生的输出目录保留不回滚）

| 错误文本 | 卡点 |
|---|---|
| `no input bag dir given` / `no config file given` | 缺 `-i` 输入 / 缺 `-o` 配置文件 |
| `input bag path [x] does not exist` | 输入路径不存在 |
| `input must specify a bag directory: [x]` | 输入是文件非目录 |
| `duplicate input bag directory [x]` | 重复输入目录 |
| `input bag directory [x] is empty` / `no bag files found in [x]` | 输入目录无任何条目 / 无匹配分片 |
| `open cfg [x] failed` / `parse cfg [x] failed: ...` | 配置文件不可读 / 非法 JSON |
| `cfg [x] must contain a non-empty "output_bags" array` | 配置缺顶层键 / 非数组 / 空数组 |
| `cfg [x] output_bags entry requires non-empty string "uri"` | 条目缺 uri / uri 非字符串 / 空串 |
| `cfg [x] duplicate output uri [x]` | 条目 uri 重复 |
| `cfg [x] "topics" must be an array of strings` / `cfg [x] "topics" elements must be strings` / `cfg [x] "topics" contains empty name` | topics 字段形态错误 |
| `topic pattern [x] must contain at most one '*'` | 通配模式多于一个 `*` |
| `cfg [x] "max_bagfile_size"/"max_bagfile_duration" must be a non-negative integer` | 数值字段为负数或浮点 |
| `cfg [x] "start_time"/"end_time" must be a non-negative integer or a "YYYY-MM-DD_HH-MM-SS_mmm-uuu-nnn" string` | 时间字段既非非负整数也非字符串 |
| `cfg [x] "start_time"/"end_time" invalid time [原文], expected YYYY-MM-DD_HH-MM-SS_mmm-uuu-nnn` | 可读时间字符串非法（原文随报错回显） |
| `cfg [x] start_time must not exceed end_time` | 区间倒置 |
| `max_bagfile_size must be at least 1024 bytes (got x)` | 分片字节上限过小 |
| `topic [x] not found in input bags` | 精确主题不存在于输入 |
| `topic pattern [x] matched no input topic` | 模式未命中任何输入主题 |
| `topic [x] has conflicting types [a] vs [b] across inputs` | 同主题跨输入类型冲突 |
| `topic [x] has conflicting qos [a] vs [b] across inputs` | 同主题跨输入 QoS 串冲突（空串不参与比较） |
| `topic [x] has no type name (legacy bag without channel type metadata)` | 入选主题为旧格式 bag（无类型元数据） |
| `output dir [x] already exists` | 输出目录已存在 |
| `open <名> failed: ...` / `write to [x] failed ...` / `write metadata.json failed ...` | 输入分片读失败 / 输出写出失败 / 元信息写失败（输出目录保留） |

### `bag play`（exit 2）

| 错误文本 | 卡点 |
|---|---|
| `no bag dir given` | 缺 bag 目录位置参数（服务层前置） |
| `invalid playback rate` | 速率倍率 ≤0（CLI 侧报 `not in the valid range (> 0.0)`） |
| `invalid start offset` | 起始偏移 <0（CLI 侧报 `not in the valid range (>= 0.0)`） |
| `invalid delay` | 开播延迟 <0（CLI 侧报 `not in the valid range (> 0.0)`，0 也拒） |
| `bag node not created` | 未建 bag 节点（CLI 自动建删；宏直调须先 `YOMKRPC_BAG_NODE`） |
| `bag path [x] does not exist` / `must specify a bag directory` | 路径不存在 / 路径是文件非目录 |
| `could not find metadata.json in bag directory [x]` | 目录缺元信息文件 |
| `parse metadata.json failed: ...` / `invalid metadata.json: ...` | 元信息非 JSON / 必需键缺失或形态错误 |
| `topic [x] has no type info (legacy bag), cannot replay` | 旧格式 bag 拒播 |
| `topic [x] invalid qos string [x]` | qos 串缺逗号 / 多逗号 / 未知枚举拼写（原串随报错回显；覆盖文件值非法枚举同款报错） |
| `topic [x] not found in bag` | `--topics` 精确项不在 bag 主题清单 |
| `topic pattern [x] matched no bag topic` | `--topics` 通配项未命中任何 bag 主题 |
| `topic pattern [x] 中 '*' 出现多次，仅支持单个通配` | `--topics` 清单项含 ≥2 个 `*` |
| `invalid remap rule [x] (expected old:=new)` / `duplicate remap rule [x]` | `--remap` 规则缺 `:=` 或两侧为空 / old 主题重复 |
| `invalid read-ahead queue size` | 预读队列深度 0（CLI 侧报 `not in the valid range (>= 1)`，0 与负同拒） |
| `open qos overrides file [x] failed` / `parse qos overrides file [x] failed: ...` / `invalid qos overrides file [x]: top level must be an object` | QoS 覆盖文件打不开 / 非法 JSON / 顶层非对象 |
| `no topics found in metadata.json` / `no bag files found for playing` | 元信息主题清单为空 / 有元信息但无匹配分片 |
| `create publisher failed` / `create topic [x] failed` / `create datawriter [x] failed` | 发布侧 DDS 实体创建失败 |

边界要点：旧格式 bag（主题 type/qos 为空串）在回放与转换的入选主题上整体拒处理（有意偏离参考实现的告警跳过——类型名是透传落盘的唯一来源）；清单对账/格式校验失败快速返回不建任何 DDS 实体。

## 与其他功能的关系

- **核心通信/bag 节点定位串**：bag 节点的 domainId 与 UDP/TCP 直配尾参语义同 `YOMKRPC_NODE`（每节点一个独立 DDS 参与者），见 [core-communication.md](./core-communication.md) 与 [discovery-server.md](./discovery-server.md)；
- **调试服务**：调试节点与 bag 节点同为单节点全可变参模型（`YOMKRPC_DEBUG_NODE` 同款），见 [debug-service.md](./debug-service.md)；
- **ROS2 互通**：bag 录制/回放为透传设计（按发现名动态建类型、payload 字节搬运），与类型名无关——互通话题可直接录制/回放，见 [ros2-interop.md](./ros2-interop.md)；
- **环境变量**：域号 `YOMKRPC_DDS_DOMAIN_ID`、发现服务器直配三环境变量详见[环境变量与公共配置参考](../manual/env-vars-reference.md)。

## 教程链接

- 端到端实操（录制（通配/分片/缓存/暂停交互键）→ info → play（速率/重映射/QoS 覆盖）→ convert → reindex 修复演练）：[Bag 录制回放手册](../manual/bag-handbook.md)
- 环境变量全集与 `.bashrc` 幂等写入行为：[环境变量与公共配置参考](../manual/env-vars-reference.md)
