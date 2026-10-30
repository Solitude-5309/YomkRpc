# YomkRpc 扩展

基于 [YomkServer](https://github.com/Solitude-5309/YomkServer) 框架的 RPC 分布式通信扩展，集成 Fast DDS 提供分布式发布订阅能力，配套类型无关的域内调试工具与 mcap 格式 Bag 录制回放。

## 1 文档

完整文档位于 [docs/](docs/README.md)（文档中心：体系说明 + 总导航），分两大册：

| 册 | 入口 | 内容 |
|---|---|---|
| 功能文档 | [docs/features/README.md](docs/features/README.md) | YomkRpc 提供哪些功能：能力清单、参数约束、错误与边界行为 |
| 用户使用手册 | [docs/manual/README.md](docs/manual/README.md) | 手把手教程：环境搭建、快速开始、进阶主题、CLI 工具手册 |

全部小文档索引：

| 功能文档（features/） | 用户使用手册（manual/） |
|---|---|
| [核心通信](docs/features/core-communication.md) | [环境搭建与编译安装](docs/manual/setup-and-build.md) |
| [Loan 借出零拷贝](docs/features/loan-zero-copy.md) | [快速开始](docs/manual/quick-start.md) |
| [QoS 配置](docs/features/qos-profiles.md) | [发布订阅进阶教程](docs/manual/pub-sub-tutorial.md) |
| [发现服务器](docs/features/discovery-server.md) | [Loan 零拷贝教程](docs/manual/loan-tutorial.md) |
| [XML profiles 配置](docs/features/xml-profiles.md) | [QoS 配置教程](docs/manual/qos-tutorial.md) |
| [ROS2 互通](docs/features/ros2-interop.md) | [发现服务器与跨域互通教程](docs/manual/discovery-server-tutorial.md) |
| [调试服务](docs/features/debug-service.md) | [XML profiles 配置教程](docs/manual/xml-profiles-tutorial.md) |
| [Bag 录制/回放](docs/features/bag-service.md) | [ROS2 互通教程](docs/manual/ros2-interop-tutorial.md) |
| | [yomkrpc 观察手册](docs/manual/cli-observe-handbook.md) |
| | [yomkrpc 发布与内省手册](docs/manual/cli-publish-handbook.md) |
| | [Bag 录制回放手册](docs/manual/bag-handbook.md) |
| | [环境变量与公共配置参考](docs/manual/env-vars-reference.md) |

## 2 核心特性

- **分布式发布订阅**：每节点一个独立 DDS 参与者，注册发布/订阅主题、同步发布、回调消费
- **Loan 借出零拷贝**：plain 类型借出 writer 池内样本直接填值发布，免序列化
- **QoS 可配**：`DDSQosProfile` 8 项独立设置 + ROS2 预置底座（含 ROS2 互通）
- **发现服务器**：UDP/TCP 双通道直配（CLIENT 协议），支持跨域互通与 XML profiles 叠加
- **XML profiles**：`YOMKRPC_LOAD_XML` 三级提取链（节点名精确匹配 → 默认档兜底 → 默认底座）
- **ROS2 互通**：set_name 类型名直改，与 ROS2 节点就同名类型互相收发
- **类型无关调试**：`yomkrpc` CLI 与 `YOMKRPC_DEBUG_*` 宏观察任意 DDS 域（无需 IDL 生成代码）：主题观察/频率测量、主题与节点详情、类型内省、JSON 载荷发布
- **Bag 录制回放**：mcap 格式录制（分片/缓存/暂停/通配）、元信息、重建索引、转换、按时间轴回放

## 3 宏 API 速览

用户经 `YOMKRPC_*` 宏（定义于 `include/YomkRpcAPI.h`）调用三个服务域，完整参数与约束见对应功能文档。

> 除 `YOMKRPC_VERSION()` 外，其余宏均返回 `YomkResponse`，调用后须判 `m_status == YomkResponse::eOk`；失败时可读 `m_msg` 获取错误信息。

**核心通信（YomkRpcService）** — 详见 [功能文档：核心通信](docs/features/core-communication.md)：

| 宏 | 简述 |
|---|---|
| `YOMKRPC_VERSION()` | 版本查询（宏内部自动解包并打印） |
| `YOMKRPC_LOAD_XML([xmlPath])` | 加载 Fast DDS XML profiles 文件（详见 [xml-profiles](docs/features/xml-profiles.md)） |
| `YOMKRPC_NODE(nodeName[, domainId[, udp/tcp serverLocator]])` | 创建节点（详见 [discovery-server](docs/features/discovery-server.md)） |
| `YOMKRPC_DEL_NODE(nodeName)` | 删除节点 |
| `YOMKRPC_PUB_TOPIC(nodeName, topicName, type[, qos])` | 注册发布主题（qos 详见 [qos-profiles](docs/features/qos-profiles.md)） |
| `YOMKRPC_SUB_TOPIC(nodeName, topicName, type, callback[, qos])` | 注册订阅主题 |
| `YOMKRPC_PUB_MSG(nodeName, topicName, data)` | 发布数据 |
| `YOMKRPC_LOAN(nodeName, topicName, outPtr)` | 借出发送缓冲（详见 [loan-zero-copy](docs/features/loan-zero-copy.md)） |
| `YOMKRPC_DISCARD_LOAN(nodeName, topicName, sample)` | 归还未发布的借出缓冲 |

**调试服务（YomkRpcDebugService）** — 详见 [功能文档：调试服务](docs/features/debug-service.md)，等价 CLI 见 `yomkrpc` 命令：

| 宏 | 简述 |
|---|---|
| `YOMKRPC_DEBUG_NODE([domainId[, udp/tcp serverLocator]])` | 创建调试节点（单节点模型） |
| `YOMKRPC_DEBUG_TOPIC_PRINT(topicName, output)` | 登记调试主题（消息文本逐条投递回调） |
| `YOMKRPC_DEBUG_TOPIC_LIST(_T)(stableRounds, intervalMs)` | 列出域内主题（_T 附类型名） |
| `YOMKRPC_DEBUG_TOPIC_INFO(_V)(topicName, stableRounds, intervalMs)` | 查询主题详情（_V 含逐端点 GUID/QoS） |
| `YOMKRPC_DEBUG_NODE_LIST(stableRounds, intervalMs)` | 列出域内节点 |
| `YOMKRPC_DEBUG_NODE_INFO(nodeName, stableRounds, intervalMs)` | 查询节点发布/订阅清单 |
| `YOMKRPC_DEBUG_TOPIC_FIND(typeName, stableRounds, intervalMs)` | 按数据类型反查主题 |
| `YOMKRPC_DEBUG_INTERFACE_SHOW/LIST(...)` | 按类型名输出 IDL 结构 / 列出全部类型名 |
| `YOMKRPC_DEBUG_TOPIC_EXAMPLE/MSG(...)` | 查询主题发布示例 / 消息描述两要素 |
| `YOMKRPC_DEBUG_TOPIC_PUB(topicName, json, ...)` | 发布 JSON 载荷消息（支持持续发布/条数上限/QoS 尾参） |
| `YOMKRPC_DEBUG_QUIT()` | 退出调试（删除调试节点） |

**Bag 服务（YomkRpcBagService）** — 详见 [功能文档：Bag 录制/回放](docs/features/bag-service.md)：

| 宏 | 简述 |
|---|---|
| `YOMKRPC_BAG_NODE([domainId[, udp/tcp serverLocator]])` | 创建 bag 节点（单节点模型） |
| `YOMKRPC_BAG_RECORD(topics, outputDir, maxBagSize, maxBagDurationSec, maxCacheSize)` | 录制主题列表为 mcap bag（长驻阻塞至 Ctrl+C） |
| `YOMKRPC_BAG_INFO(bagDir)` | 查看 bag 元信息（纯文件操作，无须 bag 节点） |
| `YOMKRPC_BAG_REINDEX(bagDir)` | 从 mcap 分片重建 metadata.json（纯文件操作） |
| `YOMKRPC_BAG_CONVERT(inputs, cfgPath)` | 按配置过滤/裁剪/归并转换 bag（纯文件操作） |
| `YOMKRPC_BAG_PLAY(bagDir, ...)` | 按录制时间轴回放 bag（须先自建 bag 节点；CLI `bag play` 自动建删） |
| `YOMKRPC_BAG_DEL_NODE()` | 删除 bag 节点 |

安装后可直接运行的 CLI 工具 **`yomkrpc`**（位于 `<安装路径>/bin`）：`topic print/list/info/type/find/hz/pub`、`interface show/list`、`node list/info`、`bag record/info/reindex/convert/play`，用法见 [yomkrpc 观察手册](docs/manual/cli-observe-handbook.md) 与 [yomkrpc 发布与内省手册](docs/manual/cli-publish-handbook.md)。

## 4 快速开始

1. **编译安装**（交互式脚本：依赖检测 → 主库 → msg 类型库 → 示例程序 → 动态库注册）：

```bash
source build_ubuntu.sh
```

详见 [docs/manual/setup-and-build.md](docs/manual/setup-and-build.md)。

2. **跑通第一个发布订阅**（安装后可直接运行示例，或照教程写第一个程序）：

```bash
<安装路径>/bin/ExampleYomkRpcTopic
```

详见 [docs/manual/quick-start.md](docs/manual/quick-start.md)。

3. **观察域内状态 / 按需进阶**：`yomkrpc topic list` 查看域内主题（详见 [cli-observe-handbook](docs/manual/cli-observe-handbook.md)）；跨域互通、QoS、Loan、ROS2 互通、Bag 录制回放按 [docs/manual/README.md](docs/manual/README.md) 场景导航进入对应教程。

## 5 测试

### 5.1 编译测试

测试树（`test/`）为独立 CMake 工程，只测 YomkRpc 自有源码（`src/`、`include/`），第三方（FastDDS / YomkServer / YomkRpcMsg）仅链接不插桩：

```bash
cmake -S test -B test/build -DCMAKE_PREFIX_PATH="${YOMK_PREFIX_PATH:-/opt/yomk}"
cmake --build test/build -j
```

构建产物为 20 个测试可执行（位于 `test/build/Harness/`、`test/build/YomkRpcService/`、`test/build/FastDDSDebugNode/`、`test/build/YomkRpcDebugService/`、`test/build/YomkRpcBagService/`、`test/build/FastDDSBagNode/`）：

| 模块 | 测试目标 |
|---|---|
| Harness (1) | TestHarnessSmoke（零 DDS 基座自检） |
| YomkRpcService (8) | TestYomkRpcServiceContract、TestYomkRpcNodeLifecycle、TestYomkRpcTopic、TestYomkRpcLoan、TestYomkRpcTypes、TestYomkRpcConcurrency、TestYomkRpcStressSerial、TestYomkRpcStressConcurrent |
| FastDDSDebugNode (1) | TestFastDDSDebugNode（节点层守卫 + 发现→动态类型→订阅→JSON 输出端到端） |
| YomkRpcDebugService (2) | TestYomkRpcDebugServiceContract（DDS-free 契约）、TestYomkRpcDebugServiceLifecycle（真实 DDS 生命周期） |
| YomkRpcBagService (2) | TestYomkRpcBagServiceContract（DDS-free 契约）、TestYomkRpcBagServiceLifecycle（真实 DDS 录制生命周期） |
| FastDDSBagNode (6) | TestFastDDSBagNodeValidation（启动校验与输入校验分支，含通配展开判定）、TestFastDDSBagNodeRecord（发布→录制→Ctrl+C 收尾→落盘断言端到端，含通配模式录制）、TestFastDDSBagNodeInfo（bag info 静态函数直测）、TestFastDDSBagNodeReindex（bag reindex 静态函数直测）、TestFastDDSBagNodeConvert（bag convert 静态函数直测）、TestFastDDSBagNodePlay（节点层回放端到端） |

可选构建开关（CMake cache 变量）：

- `-DYOMKRPC_TEST_SANITIZER=off/asan/tsan`：对自有源码插桩 sanitizer（默认 `off`；tsan 模式经 `setarch -R` 启动以兼容高 ASLR 内核）
- `-DSTRESS_ITERS=5000 -DSTRESS_CYCLES=30`：压测规模（编译期旋钮，闭环规模 100000/50）

### 5.2 一键运行全量测试

```bash
./test/run_tests.sh                # 全量运行（含 2 个 stress，编译期规模 5000/30）
./test/run_tests.sh --skip-stress  # 快速冒烟（跳过 2 个 stress）
./test/run_tests.sh --full         # 闭环规模：自动重配+重编 stress（100000/50）后全量运行
./test/run_tests.sh --bin DIR      # 指定测试可执行根目录（默认 test/build，自动定位子目录）
./test/run_tests.sh --timeout N    # 单测试超时秒数（默认 300；stress 默认 1800）
./test/run_tests.sh --sanitizer asan,tsan  # 插桩全量回归：asan、tsan 各一轮顺序执行
```

运行器行为：失败即停（含 SHM 残留复查）、日志落盘 `test/test_logs/<时间戳>/`、每测试独立临时目录运行、`timeout` 超时保护、sanitizer 插桩回归（suppressions 分别经 `LSAN_OPTIONS=test/lsan.supp` 与 `TSAN_OPTIONS=test/tsan.supp`，tsan 轮以 `setarch -R` 禁用 ASLR 包裹运行）。

### 5.3 单独运行与压测规模

每个测试为独立可执行（纯 `main()` + `CHECK` 断言，返回 0 = 全部通过），可直接单独运行：

```bash
./test/build/YomkRpcService/TestYomkRpcTopic
```

2 个 stress 测试的规模是**编译期**旋钮，由 CMake 变量 `STRESS_ITERS` / `STRESS_CYCLES` 注入（缺省 5000/30，闭环规模 100000/50），`./test/run_tests.sh --full` 会自动重配+重编后全量运行。

## 6 开发状态

- ✅ 基础框架搭建完成
- ✅ CMake 构建系统集成
- ✅ FastDDS 集成（FastDDSNode 发布订阅）
- ✅ YomkRpcService DDS 接口（节点管理/主题注册/发布）
- ✅ Loan 借出机制（订阅端透明自动切换，发布端 loan/discard 接口）
- ✅ 跨进程通信验证（ExampleYomkRpcPub/ExampleYomkRpcSub 双进程示例）
- ✅ 类型无关调试（FastDDSDebugNode + YomkRpcDebugService + yomkrpc 命令行工具）
- 🚧 更多数据类型支持

## 7 License

MIT License - 详见 [LICENSE](LICENSE)

## 8 链接

- [YomkServer 官方仓库](https://github.com/Solitude-5309/YomkServer)
