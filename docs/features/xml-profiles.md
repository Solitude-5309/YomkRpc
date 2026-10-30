# 功能文档：XML profiles 配置

> **本文档覆盖范围**：`YOMKRPC_LOAD_XML` 加载 Fast DDS XML profiles 文件的完整语义——显式路径与无参缺省（环境变量自动加载）两种形态、participant 三级 profile 提取链、时序约束、重复调用替换语义与失败行为。与发现服务器直配的组合边界仅给出结论，叠加细节以 [discovery-server.md](./discovery-server.md) 为主责方。

## 功能定位

YomkRpc 节点的 participant 级**发现配置/传输/QoS** 可以不经代码、经 Fast DDS XML profiles 文件下发：用户编写符合 Fast DDS XML schema 的 profiles 文件，用 `YOMKRPC_LOAD_XML` 宏加载缓存，后续创建的节点（participant）按三级提取链叠加对应 profile。适合统一调整租约时长、注入传输描述符、打底 QoS 等进程级配置，且可与发现服务器直配组合。

## 能力清单

| 能力 | 说明 |
|---|---|
| 显式路径加载 | `YOMKRPC_LOAD_XML(xmlPath)` |
| 无参缺省加载（环境变量自动加载） | `YOMKRPC_LOAD_XML()` 读 `YOMKRPC_USE_XML_PROFILE` + `YOMKRPC_XML_PROFILE_PATH` |
| participant 三级提取链 | 节点名精确匹配 → 默认档兜底 → 保持默认底座 |
| 时序约束 | 必须在任何节点创建之前调用 |
| 重复调用替换 | 以最新成功文件整体替换 |
| 失败回显 | 路径不存在/文件为空/格式非法返回错误，不静默降级 |

## 使用方式概述

```cpp
YOMKRPC_LOAD_XML("/path/to/profiles.xml");   // 形态 1：显式路径；必须在任何节点创建之前调用！
YOMKRPC_LOAD_XML();                          // 形态 2：无参缺省，读下方两环境变量（总开关+路径）
YOMKRPC_NODE("xml_node", 0);                 // 创建时按三级提取链叠加 XML 配置
```

宏内部读文件经 `load_XML_profiles_file` 解析校验（返回码显式检查）后缓存，供后续 create_node 的 participant 按三级提取链叠加。

participant 档 XML 片段示例（格式经 Fast DDS XML schema 校验）：

```xml
<?xml version="1.0" encoding="UTF-8" ?>
<profiles xmlns="http://www.eprosima.com/XMLSchemas/fastRTPS_Profiles">
    <participant profile_name="xml_node" is_default_profile="false">
        <rtps>
            <builtin>
                <discovery_config>
                    <leaseDuration>
                        <sec>120</sec>
                    </leaseDuration>
                </discovery_config>
            </builtin>
        </rtps>
    </participant>
</profiles>
```

### 环境变量自动加载（无参形态）

与发现服务器自动直配同机制对齐，进程级无需改代码：双环境变量控制 XML profiles 缺省加载——

| 环境变量 | 作用 | 行为 |
|---|---|---|
| `YOMKRPC_USE_XML_PROFILE` | 总开关 | 默认 `false`，`true`/`1` 视为开启（大小写不敏感）；未设置/空/`false`/`0`/其余任意值关闭 |
| `YOMKRPC_XML_PROFILE_PATH` | profiles 文件路径 | 默认空，相对/绝对均可 |

- 开关开启时 `YOMKRPC_LOAD_XML` 无参形态经 `DDSXmlLoad` 类内缺省初始化器自动取该路径（显式传参覆盖 env 缺省，语义同 serverLocator 尾参）；
- 路径空静默不加载（同发现服务器空串=不直配），路径不存在/文件为空/格式非法失败回显不静默降级；
- 开关关闭时路径变量被忽略恒空（服务端报 `xml path is empty`）；
- `yomkrpc` CLI 与该双环境变量对齐：启动时无则创建默认值并幂等写入 `~/.bashrc`（仅可见性，行为同未设置），且各子命令创建调试/bag 节点前自动加载（开关关闭或路径空行为不变），适合容器/CI 批量注入：

```bash
export YOMKRPC_USE_XML_PROFILE=true
export YOMKRPC_XML_PROFILE_PATH="/path/to/profiles.xml"
```

## 参数与约束

### 三级提取链

| 级别 | 命中条件 | 说明 |
|---|---|---|
| 1. 节点名精确匹配 | 按节点名 `profile_name` 精确匹配 | 优先级最高 |
| 2. 默认档兜底 | `is_default_profile="true"` 默认档 | 同文件多默认档取首个 |
| 3. 保持默认底座 | 均未命中 | Fast DDS 端点默认底座 |

命中后节点名仍最后覆盖（profile 不改变节点名）。可配置项范围为 participant 级的发现配置/传输/QoS。

### 时序约束

- **必须在任何节点创建之前调用**——participant 的发现配置/传输/QoS 在创建时固化，先 `YOMKRPC_LOAD_XML` 再 `YOMKRPC_NODE`；
- 节点创建后加载仅影响后续新建实体，对已建节点不生效；
- 再次调用以最新成功文件整体替换。

## 错误与边界行为

- 失败（路径不存在/文件为空/格式非法）返回错误，`m_msg` 携原因与返回码，不静默降级；
- 开关关闭时无参形态不加载（路径变量被忽略恒空，服务端报 `xml path is empty`）；
- 开关开启但路径空：静默不加载（同发现服务器空串=不直配）。

> 除 `YOMKRPC_VERSION()` 外，其余宏均返回 `YomkResponse`，调用后须判 `m_status == YomkResponse::eOk`；失败时可读 `m_msg` 获取错误信息。

## 与其他功能的关系

- **发现服务器直配**：直配可单独使用或配合 `YOMKRPC_LOAD_XML`——XML 打底，直配 server 项覆盖 XML `discoveryServersList`；叠加规则以 [discovery-server.md](./discovery-server.md) 为主责方，本文不重复展开；
- **节点创建**：加载结果作用于 `YOMKRPC_NODE` 创建的 participant（每节点一个独立参与者），节点生命周期见 [core-communication.md](./core-communication.md)；
- **环境变量**：`YOMKRPC_USE_XML_PROFILE`/`YOMKRPC_XML_PROFILE_PATH` 及 CLI 侧 `.bashrc` 幂等写入行为详见[环境变量与公共配置参考](../manual/env-vars-reference.md)。

## 教程链接

- 分步实操（编写 profiles XML → 加载 → 验证命中 → 时序错误排障）：[XML profiles 配置教程](../manual/xml-profiles-tutorial.md)
