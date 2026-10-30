# 使用手册：XML profiles 配置教程

> **本文档覆盖范围**：用 Fast DDS XML profiles 文件集中配置 YomkRpc 节点的发现/传输/QoS——编写一份含具名 profile 与默认档的最小可用 profiles XML、用 `YOMKRPC_LOAD_XML` 宏加载并检查返回值、验证三级提取链的 profile 命中规则（节点名精确匹配优先、默认档兜底、命中后节点名最后覆盖），以及加载时序红线与失败排障。功能原理与能力边界详见[功能文档](../features/xml-profiles.md)。

## 目标

完成本文档后，你将：

1. 编写一份最小可用的 profiles XML（具名 participant 档 + `is_default_profile="true"` 默认档）；
2. 在任何节点创建之前用 `YOMKRPC_LOAD_XML` 加载，并正确判错（读 `m_msg`）；
3. 验证 profile 命中：节点名精确匹配优先、默认档兜底、命中后节点名仍最后覆盖。

## 前提与准备

- 已按 [setup-and-build.md](./setup-and-build.md) 完成编译安装；C++ 工程骨架与 [quick-start.md](./quick-start.md) 相同（`find_package(YomkServer REQUIRED)` + `find_package(YomkRpc REQUIRED)`，链接 `YomkRpc::YomkRpc YomkServer::YomkServer`——本教程示例未使用消息类型，无需 `YomkRpcMsg`）；
- 加载语义（README 5.7 口径）：`YOMKRPC_LOAD_XML` 读文件经 `load_XML_profiles_file` 解析校验（返回码显式检查）后缓存，供后续 create_node 的 participant 按**三级提取链**叠加：按节点名 `profile_name` 精确匹配 → `is_default_profile="true"` 默认档兜底（同文件多默认档取首个）→ 均未命中保持默认底座；命中后节点名仍最后覆盖。可配置范围为 participant 级发现/传输/QoS 配置，字段写法经 Fast DDS XML schema 校验；
- 宏返回值约定：除 `YOMKRPC_VERSION()` 外，其余宏均返回 `YomkResponse`，调用后须判 `m_status == YomkResponse::eOk`，失败时读 `m_msg` 获取错误信息。

## 分步操作

### 第 1 步：编写 profiles XML

新建 `profiles.xml`，包含两种命中形态：一个 `profile_name` 与节点名对应的具名档，一个 `is_default_profile="true"` 的默认档兜底（字段写法与 README 5.7 示例同源，本文不展开 README 之外的 Fast DDS 配置项细节）：

```xml
<?xml version="1.0" encoding="UTF-8" ?>
<profiles xmlns="http://www.eprosima.com/XMLSchemas/fastRTPS_Profiles">
    <!-- 具名档：创建名为 xml_node 的节点时精确命中 -->
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
    <!-- 默认档：具名档未命中的其余节点兜底命中 -->
    <participant profile_name="default_participant" is_default_profile="true">
        <rtps>
            <builtin>
                <discovery_config>
                    <leaseDuration>
                        <sec>60</sec>
                    </leaseDuration>
                </discovery_config>
            </builtin>
        </rtps>
    </participant>
</profiles>
```

要点：同一文件可共存多档；同文件多个默认档只取首个，避免写多个 `is_default_profile="true"`。

### 第 2 步：YOMKRPC_LOAD_XML 加载并检查返回值

宏的两种形态（README 5.7 原文）：

```cpp
YOMKRPC_LOAD_XML("/path/to/profiles.xml");   // 形态 1：显式路径；必须在任何节点创建之前调用！
YOMKRPC_LOAD_XML();                          // 形态 2：无参缺省，读下方两环境变量（总开关+路径）
```

形态 2 依赖的双环境变量（进程级，无需改代码）：

```bash
export YOMKRPC_USE_XML_PROFILE=true
export YOMKRPC_XML_PROFILE_PATH="/path/to/profiles.xml"
```

- 总开关 `YOMKRPC_USE_XML_PROFILE` 默认 `false`，`true`/`1` 视为开启（大小写不敏感；未设置/空/`false`/`0`/其余任意值关闭）；路径 `YOMKRPC_XML_PROFILE_PATH` 默认空，相对/绝对均可；
- 开关开启时无参形态自动取该路径（显式传参覆盖 env 缺省）；路径空静默不加载；路径不存在/文件为空/格式非法失败回显不静默降级；
- `yomkrpc` CLI 与该双环境变量对齐：启动时无则创建默认值并幂等写入 `~/.bashrc`（仅可见性，行为同未设置），且各子命令创建调试/bag 节点前自动加载（开关关闭或路径空行为不变），适合容器/CI 批量注入。

验证加载与命中（把第 1 步的 XML 路径替换为实际路径）：

```cpp
#include <YomkRpc/YomkRpcAPI.h>
#include <YomkServer/YomkAPI.h>

using namespace yomk;

int main()
{
    YOMK_INIT();
    YOMK_NEW_SERVICE(YomkRpcService);
    YOMKRPC_VERSION();

    // 1. 加载 XML profiles——必须在任何节点创建之前调用！
    auto resp = YOMKRPC_LOAD_XML("/path/to/profiles.xml");
    if (resp.m_status != YomkResponse::eOk)
    {
        YOMK_ERROR_TAG("XmlProfilesDemo", "load xml failed: ", resp.m_msg);
        return 1;
    }

    // 2. 创建节点：xml_node 精确命中具名档；other_node 具名档无匹配走默认档兜底
    resp = YOMKRPC_NODE("xml_node", 0);
    if (resp.m_status != YomkResponse::eOk)
    {
        YOMK_ERROR_TAG("XmlProfilesDemo", "create xml_node failed: ", resp.m_msg);
        return 1;
    }

    resp = YOMKRPC_NODE("other_node", 0);
    if (resp.m_status != YomkResponse::eOk)
    {
        YOMK_ERROR_TAG("XmlProfilesDemo", "create other_node failed: ", resp.m_msg);
        YOMKRPC_DEL_NODE("xml_node");
        return 1;
    }

    // 3. 退出前删除节点（判错此处从略，正式代码建议逐宏判断）
    YOMKRPC_DEL_NODE("xml_node");
    YOMKRPC_DEL_NODE("other_node");
    return 0;
}
```

编译运行（工程 CMake 同 quick-start 第 1 步，目标名可换为 `xml_profiles_demo`）：

```bash
cmake -B build -DCMAKE_PREFIX_PATH="$YOMK_PREFIX_PATH"
cmake --build build
./build/xml_profiles_demo
```

### 第 3 步：验证 profile 命中

按三级提取链逐项核对：

1. **具名档精确匹配优先**：节点名 `xml_node` 与 XML 中 `profile_name="xml_node"` 精确匹配，命中具名档；
2. **默认档兜底**：节点名 `other_node` 在具名档中无匹配，命中 `is_default_profile="true"` 的默认档；若 XML 未写默认档，则保持默认底座（即不叠加任何 XML 配置）；
3. **命中后节点名仍最后覆盖**：命中 profile 只提取 participant 的发现/传输/QoS 配置，participant 最终仍以 `YOMKRPC_NODE` 传入的节点名命名——`xml_node` 节点的名字不会变成 profile 的 `profile_name` 以外的东西，`other_node` 也不会因命中默认档而改名。

命中效果的观察方式：

- 加载与建节点均返回 `eOk` 是命中生效的前提（任何一步失败都会回显，见「常见错误与排障」）；
- 若 XML 配置了可观察的端点 QoS 项，可用 `yomkrpc topic info <主题名> -v` 的逐端点 QoS 详情段对照（CLI 用法见 README 6.1 与 [cli-observe-handbook.md](./cli-observe-handbook.md)）。

### 时序红线（务必遵守）

- **必须在任何节点创建之前调用**：participant 的发现配置/传输/QoS 在创建时固化——先 `YOMKRPC_LOAD_XML` 再 `YOMKRPC_NODE`；
- 节点创建后加载仅影响后续新建实体，**对已建节点不生效**；
- 再次调用以**最新成功文件整体替换**（注意：替换同样只影响之后新建的节点）。

## 预期输出

- 程序启动先打印版本号（`YOMKRPC_VERSION` 输出）；
- `YOMKRPC_LOAD_XML` 成功返回 `eOk`，无额外打印；
- 两次 `YOMKRPC_NODE` 均返回 `eOk`（框架日志前缀以实际环境为准）；两个节点按各自命中的档运行——本示例的 leaseDuration 数值差异不产生可见打印，命中效果体现在 participant 配置上（观察方式见第 3 步）；
- 失败路径示例：把路径改成不存在的文件重跑，程序打印 `load xml failed: <m_msg 携原因与返回码>` 后以退出码 1 退出（失败回显，不静默降级）。

## 常见错误与排障

| 现象 | 原因与处理 |
|---|---|
| `YOMKRPC_LOAD_XML` 返回错误，`m_msg` 携原因与返回码 | 三类失败：路径不存在/文件为空/格式非法——核对路径（相对/绝对均可）与文件内容，确认 XML 经 Fast DDS XML schema 校验合法后重试 |
| 形态 2 无参加载报 `xml path is empty` | 总开关 `YOMKRPC_USE_XML_PROFILE` 关闭时路径变量被忽略恒空（服务端报 `xml path is empty`）——先 `export YOMKRPC_USE_XML_PROFILE=true` 再设置 `YOMKRPC_XML_PROFILE_PATH` |
| 「改了 XML 不生效」 | 时序原因：节点创建后才加载——participant 的发现配置/传输/QoS 在创建时固化，对已建节点不生效，仅影响后续新建实体；把 `YOMKRPC_LOAD_XML` 挪到第一个 `YOMKRPC_NODE` 之前，或删除相关节点后重建 |
| 具名档没命中、走了默认档 | 三级提取链第一级按节点名 `profile_name` **精确匹配**——检查 XML 的 `profile_name` 与 `YOMKRPC_NODE` 第一参是否逐字节一致 |
| 想让多个节点共用不同配置却只命中同一档 | 同文件多默认档只取首个：需要差异化配置时为每个节点写独立具名档（`profile_name` 等于节点名），默认档只作兜底 |
| 节点名与预期不符 | 命中后节点名仍最后覆盖：profile 只提供 participant 配置，participant 仍以 `YOMKRPC_NODE` 传入的节点名命名，属正常 |

## 延伸阅读

- [../features/xml-profiles.md](../features/xml-profiles.md)——三级提取链、重复调用替换语义与能力边界的完整说明
- [./discovery-server-tutorial.md](./discovery-server-tutorial.md)——XML 打底与发现服务器直配的组合（直配 server 项覆盖 XML `discoveryServersList`）
- [./env-vars-reference.md](./env-vars-reference.md)——`YOMKRPC_USE_XML_PROFILE`/`YOMKRPC_XML_PROFILE_PATH` 等环境变量总参考
