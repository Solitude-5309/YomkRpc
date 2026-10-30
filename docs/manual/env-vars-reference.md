# 使用手册：环境变量与公共配置参考

> **本文档覆盖范围**：YomkRpc 的 5 个环境变量（`YOMKRPC_DDS_DOMAIN_ID`、`YOMKRPC_DDS_DISCOVER_ROUNDS`、`YOMKRPC_USE_DISCOVER_SERVER`、`YOMKRPC_UDP_DISCOVER_SERVERS`、`YOMKRPC_TCP_DISCOVER_SERVERS`）逐项参考——作用与作用域（库侧宏 / yomkrpc CLI 侧 / 两侧）、默认值、取值约束、`~/.bashrc` 幂等写入行为、与显式参数的优先级关系，以及「改了不生效」等排障。发现服务器直配原理与跨域互通见 [../features/discovery-server.md](../features/discovery-server.md)。

## 目标

完成本文档后，你将：

1. 查清任一环境变量的作用、作用域、默认值与取值约束；
2. 知道哪些命令/宏受 `YOMKRPC_DDS_DISCOVER_ROUNDS` 收敛次数影响、哪些不受影响；
3. 掌握「不改代码、进程级配置」的环境变量用法（容器/CI 批量注入）与 `.bashrc` 幂等写入行为；
4. 排查「改了环境变量但不生效」「值非法报错」两类高频问题。

## 前提与准备

- 已安装 yomkrpc 命令行工具（见 [setup-and-build.md](./setup-and-build.md)）；库侧宏使用见 [quick-start.md](./quick-start.md)；
- `.bashrc` 幂等写入是 **yomkrpc CLI 侧**行为：`yomkrpc` 启动时无某变量则自动创建并设默认值，同时幂等写入 `~/.bashrc`（仅当其中无该变量时，带 `# added by yomkrpc` 注释便于识别）——新开任意终端可直接 `echo $<变量名>` 查看并自动继承；**已打开的终端须 `source ~/.bashrc` 或重开才生效**；
- 库侧宏（`YOMKRPC_NODE`/`YOMKRPC_DEBUG_NODE`/`YOMKRPC_BAG_NODE` 等）经类内缺省初始化器读环境变量，**不写 `.bashrc`**；
- 另有一组 XML profiles 双环境变量（`YOMKRPC_USE_XML_PROFILE`/`YOMKRPC_XML_PROFILE_PATH`）属于 XML 集中配置通道，机制同本文（开关+路径、幂等写入），详见 [../features/xml-profiles.md](../features/xml-profiles.md)。

## 分步操作

### 变量总表（作用域 × 默认值 × 缺省行为）

| 环境变量 | 作用域 | 默认值 | 缺省（未设置）行为 |
|---|---|---|---|
| `YOMKRPC_DDS_DOMAIN_ID` | 两侧（库侧宏缺省实参 + yomkrpc CLI 域号） | `0` | 域号取 0；yomkrpc 启动时自动创建默认 0 并幂等写入 `~/.bashrc` |
| `YOMKRPC_DDS_DISCOVER_ROUNDS` | CLI 侧（查询类命令收敛次数） | `5` | 收敛判定 5 次；yomkrpc 启动时自动创建默认 5 并幂等写入 `~/.bashrc` |
| `YOMKRPC_USE_DISCOVER_SERVER` | 两侧（`YOMKRPC_NODE`/`YOMKRPC_DEBUG_NODE`/`YOMKRPC_BAG_NODE` 缺省直配 + yomkrpc CLI 同机制） | `false` | 开关关闭；未设置/空/`false`/`0`/其余任意值均关闭 |
| `YOMKRPC_UDP_DISCOVER_SERVERS` | 两侧（同上三宏的 UDP 通道缺省直配 + CLI 同机制） | 空 | 开关关闭时被忽略恒空（=不直配，行为同未设置，回落多播发现） |
| `YOMKRPC_TCP_DISCOVER_SERVERS` | 两侧（同上三宏的 TCP 通道缺省直配 + CLI 同机制） | 空 | 同上 |

三个发现服务器变量的 `.bashrc` 写入机制与 `YOMKRPC_DDS_DOMAIN_ID` 同机制对齐：yomkrpc 启动时无该变量则自动创建并设默认值（`false`/空/空），幂等写入 `~/.bashrc`；**默认值与未设置行为完全一致（开关关闭/串空回落多播发现），仅保证可见性**；接发现服务器时 `export YOMKRPC_USE_DISCOVER_SERVER=true` 并填对应通道 server 串即可。

### YOMKRPC_DDS_DOMAIN_ID：DDS 域号

- **作用**：指定 DDS 域号（domainId）。仅同域端点可相互发现/收发——库侧 `YOMKRPC_NODE` 建的发布订阅节点、`YOMKRPC_DEBUG_NODE` 调试节点、`YOMKRPC_BAG_NODE` bag 节点，以及 yomkrpc 各命令均经它选择域。
- **作用域**：两侧。库侧——`YOMKRPC_NODE(nodeName[, domainId, ...])` 的 domainId 可缺省，缺省时经 DDSNode 类内缺省初始化器读环境变量 `YOMKRPC_DDS_DOMAIN_ID`（未设置/为空/非法/超范围回落 0）；`YOMKRPC_DEBUG_NODE`/`YOMKRPC_BAG_NODE` 全可变参，无参形态 domainId 同样读该变量。CLI 侧——`yomkrpc` 各命令的域号经该环境变量选择；亦可用显式选项 `-D/--domain <N>` 单次指定（合法范围 [0,232]，**显式选项优先于环境变量**，缺参数/非法值报错退出码 2）。
- **默认值**：`0`。
- **取值约束**：合法范围 [0,232]。
- **`.bashrc` 幂等写入**：yomkrpc 启动时无该变量则自动创建并默认 0，同时幂等写入 `~/.bashrc`（仅当其中无该变量时，带 `# added by yomkrpc` 注释便于识别）——新开任意终端可直接 `echo $YOMKRPC_DDS_DOMAIN_ID` 查看并自动继承；已打开的终端须 `source ~/.bashrc` 或重开才生效。
- **修改方式**：直接编辑 `.bashrc` 中该行，或临时 `export YOMKRPC_DDS_DOMAIN_ID=5`。
- **示例**：

```bash
echo $YOMKRPC_DDS_DOMAIN_ID        # 查看当前域号（新开终端自动继承 .bashrc 值）
yomkrpc -D 5 topic list            # 单次显式指定域号（优先于环境变量）
export YOMKRPC_DDS_DOMAIN_ID=5     # 临时切到域 5（仅当前终端有效，优先于 .bashrc）
yomkrpc topic list                 # 观察域 5；另开终端需重新 export 或 source ~/.bashrc
```

### YOMKRPC_DDS_DISCOVER_ROUNDS：查询类命令收敛次数

- **作用**：收敛判定次数——查询类命令内部每 ~200ms 轮询一次发现缓存快照，连续 N 次集合不变即认为发现收敛、立即输出（默认 5）。
- **作用域**：CLI 侧。该环境变量为收敛次数**唯一配置入口**；库侧宏（如 `YOMKRPC_DEBUG_TOPIC_LIST(stableRounds, intervalMs)`）以显式实参指定收敛次数，不经该变量。
- **默认值**：`5`。
- **取值约束**：值须为 >=1 的整数，非法报错退出。
- **生效命令清单**：`topic list`、`topic info`、`topic type`、`topic find`、`interface show`、`interface list`、`topic pub -e`、`topic pub -ef`、`topic pub`、`node list` 与 `node info` 生效；`topic print`/`topic hz` 为持续订阅/测量型不用。
- **`.bashrc` 幂等写入**：yomkrpc 启动时无该变量则自动创建并默认 5，同时幂等写入 `~/.bashrc`（仅当其中无该变量时，带 `# added by yomkrpc` 注释）——新开任意终端可直接 `echo $YOMKRPC_DDS_DISCOVER_ROUNDS` 查看并自动继承；已打开的终端须 `source ~/.bashrc` 或重开才生效。
- **修改方式**：直接编辑 `.bashrc` 中该行，或临时 `export YOMKRPC_DDS_DISCOVER_ROUNDS=7`。
- **注意**：`bag record` 的启动校验轮询参数固定，不经 `YOMKRPC_DDS_DISCOVER_ROUNDS` 调整。
- **示例**：

```bash
export YOMKRPC_DDS_DISCOVER_ROUNDS=10   # 大域/慢发现环境加大收敛次数（查询更稳、耗时略增）
yomkrpc node list                       # 连续 10 次快照不变才输出
```

### YOMKRPC_USE_DISCOVER_SERVER：发现服务器直配总开关

- **作用**：发现服务器直配三环境变量的总开关——开启后，节点创建时缺省（省略）的 serverLocator 尾参自动取对应通道环境变量值，participant 以 CLIENT 发现协议直配（禁用多播 PDP，发现流量单播直送 server）。
- **作用域**：两侧。库侧——`YOMKRPC_NODE` 缺省（省略）的 serverLocator 尾参自动取对应环境变量值；`YOMKRPC_DEBUG_NODE`/`YOMKRPC_BAG_NODE` 同款支持全缺省（无参）形态——三字段（含 domainId）一并经缺省初始化器读 env（domainId 读 `YOMKRPC_DDS_DOMAIN_ID`）。CLI 侧——yomkrpc 各命令创建调试/bag 节点同机制受其门控。
- **默认值**：`false`。
- **取值约束**：`true`/`1` 视为开启，大小写不敏感；未设置/空/`false`/`0`/其余任意值关闭。
- **`.bashrc` 幂等写入**：yomkrpc 启动时无该变量则自动创建并设默认值 `false`，幂等写入 `~/.bashrc`（带 `# added by yomkrpc` 注释）；默认值与未设置行为完全一致，仅保证可见性。
- **示例**：

```bash
export YOMKRPC_USE_DISCOVER_SERVER=true      # 开启直配总开关（大小写不敏感，true/1 均可）
yomkrpc topic list                           # CLI 节点按开关直配发现服务器
```
