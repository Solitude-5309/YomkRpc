# 使用手册：环境搭建与编译安装

> **本文档覆盖范围**：从零完成 YomkRpc 的依赖确认、一键编译安装（`build_ubuntu.sh` 交互流程逐步说明）、安装布局确认与安装结果验证。完成本文档后，你的机器上会装好 YomkRpc 扩展库、msg 类型库（含 SWIG Python 绑定）与全部示例程序，并可直接运行第一个发布订阅演示。

## 目标

按本文档操作完后，你将得到：

1. 一台满足全部编译依赖的 Ubuntu 机器（C++17 编译器、CMake、swig、python3-dev 等）；
2. 已安装到指定路径的 YomkRpc 扩展（与 YomkServer 安装到一起），动态库已注册进系统缓存，新开任意终端即可使用；
3. `<安装路径>/bin` 下可直接运行的示例程序与 `yomkrpc` 命令行工具，并已通过运行 `ExampleYomkRpcTopic` 验证安装成功。

## 前提与准备

| 依赖 | 要求 | 说明 |
|---|---|---|
| C++17 编译器 | gcc 等 | 编译 YomkRpc 主库与示例 |
| CMake | >= 3.14 | 构建系统 |
| YomkServer | 已安装 | 通过其 `build_ubuntu.sh` 安装后会自动配置环境变量 `YOMK_PREFIX_PATH` 指向安装路径 |
| swig 与 python3-dev | 已安装 | msg 类型库 SWIG Python 绑定编译依赖 |

确认 `YOMK_PREFIX_PATH` 已指向 YomkServer 安装路径（未安装 YomkServer 时先完成 YomkServer 安装）：

```bash
echo $YOMK_PREFIX_PATH
```

## 分步操作

### 第 1 步：启动交互式编译脚本

在仓库根目录执行：

```bash
source build_ubuntu.sh
```

脚本启动后进入交互流程，逐步说明如下。

### 第 2 步：回答路径询问

脚本依次询问两个路径，默认均取环境变量 `$YOMK_PREFIX_PATH`，可直接回车确认或输入新路径修改：

1. **YomkServer 安装路径（前置路径）**——YomkRpc 编译与链接时的前置依赖查找路径；若未检测到环境变量 `YOMK_PREFIX_PATH`，脚本会提示「未检测到环境变量 YOMK_PREFIX_PATH，可能未通过 build_ubuntu.sh 安装 YomkServer，请手动输入安装路径」，此时必须手动输入；
2. **扩展安装路径**——YomkRpc 扩展的安装目录，默认装入 YomkServer 安装目录（扩展库与 YomkServer 安装到一起，头文件路径由 `YomkServer::YomkServer` 的 INTERFACE include 统一提供）。

两个路径任一为空脚本都会报「错误: 未指定安装路径」并终止。

### 第 3 步：选择是否编译 test 测试树（可选）

脚本随后询问 `是否编译 test 测试？(直接回车=否，输入 y=是)`。测试树仅供开发验证、不安装；普通用户直接回车跳过即可。

### 第 4 步：依赖自动检测与一键 apt 补齐

脚本自动检测 gcc / swig / python3-dev / build-essential / cmake 等编译依赖：

- 全部已安装：提示「所有依赖已安装，跳过」；
- 有缺失：列出缺失项并询问「是否现在安装这些依赖？(y/n，默认 y)」——直接回车即一键 `sudo apt update` + `sudo apt install` 补齐；回答 n 则脚本以「依赖不完整，脚本终止」退出。

### 第 5 步：三步编译安装

依赖就绪后脚本按三步完成编译安装：

| 步骤 | 内容 |
|---|---|
| 步骤 1/3 | 编译安装主库 YomkRpc |
| 步骤 2/3 | 编译安装 msg 类型库 YomkRpcMsg（含 SWIG Python 绑定） |
| 步骤 3/3 | 编译安装示例程序（随扩展一并安装到 `<安装路径>/bin`） |

任一步失败（如「cmake 配置失败」「编译失败」「msg 编译安装失败」「示例程序编译失败」）脚本立即中止，修复后重新 `source build_ubuntu.sh` 即可。

### 第 6 步：动态库注册（自动完成，无需手动配置）

安装完成后脚本自动完成动态库注册，无需手动设置 `LD_LIBRARY_PATH`：

1. 将 `${安装路径}/lib` 幂等注册到 `/etc/ld.so.conf.d/yomk.conf`（已存在则不重复写入）；
2. 执行 `sudo ldconfig` 刷新动态库缓存。

此后**新开任意终端**即可找到扩展 so 与示例程序。

## 预期输出

### 安装成功横幅

三步编译安装完成后，脚本打印安装结果汇总（示例，实际路径以你的输入为准）：

```
===========================================
 YomkRpc 扩展安装成功!
-------------------------------------------
 安装路径:       <安装路径>
 YomkServer 路径: <YomkServer 安装路径>
 动态库缓存:
 ...（ldconfig -p 过滤出的 YomkRpc 相关库）
 示例程序列表（安装于 <安装路径>/bin）:
   - ExampleYomkRpcTopic
   - ...
===========================================
编译完成，扩展库已注册到系统动态库缓存，新开任意终端即可使用
```

### 安装布局确认

安装后可直接运行的程序位于 `<安装路径>/bin`：

| 程序 | 用途 |
|---|---|
| `ExampleYomkRpcTopic` | 发布订阅完整流程演示 |
| `ExampleYomkRpcTopicLoan` | loan 借出机制演示 |
| `ExampleYomkRpcPub` | 跨进程发布端示例（每 1s 发布 hello world，Ctrl+C 退出） |
| `ExampleYomkRpcSub` | 跨进程订阅端示例（订阅 hello_world，Ctrl+C 退出） |
| `yomkrpc` | 命令行工具：观察任意 DDS 主题与频率（topic print/type/find/hz）、列出域内主题与类型（topic list）、查询主题与节点详情（topic info/node info）、查看类型 IDL 结构（interface show/list）、按 JSON 载荷发布消息（topic pub）、录制/查看/重建/转换 mcap bag（bag record/info/reindex/convert） |

确认程序就位：

```bash
ls $YOMK_PREFIX_PATH/bin
```

### 验证安装：运行第一个示例

新开一个终端，直接运行发布订阅完整流程演示：

```bash
ExampleYomkRpcTopic
```

预期观察到的关键输出（日志前缀与时间戳以实际运行环境为准）：

- 依次打印 `=== YomkRpcService::getVersion ===`、`=== YomkRpcService::createNode ===` 等阶段标记；
- `[SEND] 发布消息: Hello YomkRpcService 0` 至 `Hello YomkRpcService 4` 共 5 条发送日志；
- 交错的 `[RECV] 收到消息: ...` 接收日志（同进程内发布并被订阅回调收到）；
- 结尾 `[RESULT] 接收=5/5 最后一条=Hello YomkRpcService 4`（首条消息可能因 discovery 时序丢失，偶尔为 4/5，属正常现象，详见 [quick-start.md](./quick-start.md) 排障一节）。

程序自动退出、返回码为 0 即安装验证通过。

## 常见错误与排障

| 现象 | 原因与处理 |
|---|---|
| 脚本提示「未检测到环境变量 YOMK_PREFIX_PATH」 | 未通过 YomkServer 的 `build_ubuntu.sh` 安装或未生效：手动输入 YomkServer 安装路径；新终端需确认 YomkServer 安装时已写入的环境变量（重开终端或检查其安装文档） |
| 脚本询问依赖时选择 n 后提示「依赖不完整，脚本终止」 | 按提示先手动补齐缺失项（如 `sudo apt install build-essential cmake swig python3-dev`），或重跑脚本回车选择 y |
| 步骤 1/3 报「cmake 配置失败」 | YomkServer 安装路径输入错误（找不到 YomkServer 包）：重新 `source build_ubuntu.sh`，第 2 步输入正确的 YomkServer 安装路径 |
| 步骤 2/3 报「msg 编译安装失败」 | 多为 swig / python3-dev 缺失（msg 类型库 SWIG Python 绑定编译依赖）：回到第 4 步让脚本检测补齐后重跑 |
| 运行示例报 `error while loading shared libraries`（找不到扩展 so） | 动态库缓存未更新：检查 `/etc/ld.so.conf.d/yomk.conf` 是否包含 `<安装路径>/lib` 一行（缺失则重新运行安装脚本），随后执行 `sudo ldconfig` 刷新；或确认是在安装完成之后新开的终端中运行 |
| 安装路径输错，想重新安装 | 直接重新 `source build_ubuntu.sh`，在路径询问处输入正确路径即可 |
| `ExampleYomkRpcTopic` 命令未找到 | 安装路径的 `bin` 不在 `PATH` 中：用完整路径运行（`$YOMK_PREFIX_PATH/bin/ExampleYomkRpcTopic`），或先 `export PATH=$YOMK_PREFIX_PATH/bin:$PATH` |

## 延伸阅读

- [quick-start.md](./quick-start.md)——安装完成后 5 分钟跑通第一个发布订阅程序
- [返回手册入口](./README.md)——查看完整学习路径与场景导航
