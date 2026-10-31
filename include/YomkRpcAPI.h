#pragma once
#include <YomkRpc/YomkRpcDebugService.h>
#include <YomkRpc/YomkRpcService.h>

// YomkRpc 对外 API 宏：每个宏封装一次对 /YomkRpcService/*（RPC 服务）或 /YomkRpcDebugService/*
// （调试服务）端点的 YOMK_REQUEST 调用。
// 除 YOMKRPC_VERSION（无返回值）外，其余宏均返回 YomkResponse，调用后须判 m_status==YomkResponse::eOk。

/// @brief 加载 Fast DDS XML profiles（经 /YomkRpcService/load_xml 缓存 raw 串；须在 create_node
///   前调用，再次调用以最新成功文件整体替换）。
/// @param xmlPath 可缺省：显式路径（相对/绝对均可）；缺省时读环境变量 YOMKRPC_XML_PROFILE_PATH
///   （总开关 YOMKRPC_USE_XML_PROFILE=true 才生效）。
/// @retval eOk 文件可解析并缓存（仅证明文件可解析；后续 create_node 按三级提取链叠加：
///   节点名 profile → is_default_profile 默认档 → 均未命中保持默认底座）。
/// @retval eNo 路径不存在/文件为空/格式非法（m_msg 携带原因与 rc）。
/// 详见 docs/features/xml-profiles.md
#define YOMKRPC_LOAD_XML(...) \
    YOMK_REQUEST("/YomkRpcService/load_xml", YomkMkPtr(DDSXmlLoad, DDSXmlLoad{##__VA_ARGS__}))

/// @brief 创建 DDS 节点（每节点一个独立 DDS 参与者；nodeName 唯一，仅同域节点可互通）。
/// @param nodeName 节点唯一名。
/// @param domainId 可缺省：DDS 域号 [0,232]；缺省时读环境变量 YOMKRPC_DDS_DOMAIN_ID
///   （无/非法/越界回落 0）。
/// @param udpDiscoverServerLocator 可缺省尾参：UDP 发现服务器直配聚合串 "ip[:port]"（多 server
///   分号分隔，port 缺省 11811），participant 以 CLIENT 发现协议直配；非法串创建失败回显。
/// @param tcpDiscoverServerLocator 可再缺省尾参：TCP 发现服务器直配聚合串（port 为 server 的
///   TCP 监听口），participant 补注入 TCPv4 传输；两通道可并用，任一通道 server 在场即完成
///   发现（仅 TCP 直配时 UDP 通道传空串占位）。
/// 详见 docs/features/core-communication.md、discovery-server.md
#define YOMKRPC_NODE(nodeName, ...) \
    YOMK_REQUEST("/YomkRpcService/create_node", YomkMkPtr(DDSNode, DDSNode{nodeName, ##__VA_ARGS__}))

/// @brief 查询扩展版本并打印（成功走 YOMK_INFO_TAG、失败走 YOMK_ERROR_TAG）；无返回值。
#define YOMKRPC_VERSION()                                                          \
    do                                                                             \
    {                                                                              \
        auto __resp = YOMK_REQUEST("/YomkRpcService/version", nullptr);            \
        if (__resp.m_status == YomkResponse::eOk)                                  \
        {                                                                          \
            YomkUnPackPkg(__resp.m_data, String, __ver);                           \
            if (__ver)                                                             \
            {                                                                      \
                YOMK_INFO_TAG("YomkRpcService", __ver->d);                         \
            }                                                                      \
        }                                                                          \
        else                                                                       \
        {                                                                          \
            YOMK_ERROR_TAG("YomkRpcService", "getVersion failed: ", __resp.m_msg); \
        }                                                                          \
    } while (0)

/// @brief 在指定节点上注册发布主题。
/// @param type 消息类型实例（new XxxPubSubType()），所有权移交服务端——调用后 caller 不再
///   持有/释放（无论成功或失败）。
/// @param qos 可缺省尾参：端点 QoS 描述 DDSQosProfile；缺省全哨兵=保持 Fast DDS 端点默认
///   底座；传 DDSQosProfile::ros2DefaultPublisher() 即 ROS2 默认底座
///   （RELIABLE+KEEP_LAST(10)+VOLATILE），也可在底座上逐项覆盖（如只设 depth）。
/// 详见 docs/features/qos-profiles.md
#define YOMKRPC_PUB_TOPIC(nodeName, topicName, type, ...) \
    YOMK_REQUEST("/YomkRpcService/register_pub_topic", YomkMkPtr(DDSTopic, DDSTopic{nodeName, topicName, type, ##__VA_ARGS__}))

/// @brief 在指定节点上注册订阅主题。
/// @param type 同 YOMKRPC_PUB_TOPIC，所有权移交服务端。
/// @param callback std::function<void(const void*)>，每收到一条消息调用一次（消息实例指针
///   仅回调期间有效）。
/// @param qos 可缺省尾参：语义同 YOMKRPC_PUB_TOPIC 的 qos；订阅端推荐
///   DDSQosProfile::ros2DefaultSubscriber()（BEST_EFFORT requested 对任意 offered 均可匹配）。
/// 详见 docs/features/qos-profiles.md
#define YOMKRPC_SUB_TOPIC(nodeName, topicName, type, callback, ...) \
    YOMK_REQUEST(                                              \
        "/YomkRpcService/register_sub_topic",                  \
        YomkMkPtr(DDSSubRequest, DDSSubRequest{nodeName, topicName, type, callback, ##__VA_ARGS__}))

/// @brief 向指定节点的已注册发布主题发送一条消息。
/// @param data 消息实例指针（&msg），借用（非所有权）——publish 同步写入，caller 保留并在
///   调用后自行管理其生命周期。
#define YOMKRPC_PUB_MSG(nodeName, topicName, data) \
    YOMK_REQUEST("/YomkRpcService/publish", YomkMkPtr(DDSPublish, DDSPublish{nodeName, topicName, data}))

/// @brief 借出发布缓冲（仅 plain 类型）：成功时 outPtr 指向 writer 池内待发样本，直接在池内
///   填值后经 YOMKRPC_PUB_MSG 发布免序列化；类型不支持 loan（非 plain）或池耗尽时 outPtr
///   保持 nullptr，回退普通发布路径。每次 write 后指针即被中间件收回，须重新借出。
/// 详见 docs/features/loan-zero-copy.md
#define YOMKRPC_LOAN(nodeName, topicName, outPtr)                                                                      \
    do                                                                                                                 \
    {                                                                                                                  \
        (outPtr) = nullptr;                                                                                            \
        auto __resp = YOMK_REQUEST("/YomkRpcService/loan", YomkMkPtr(DDSLoan, DDSLoan{nodeName, topicName, nullptr})); \
        if (__resp.m_status == YomkResponse::eOk && __resp.m_data)                                                     \
        {                                                                                                              \
            YomkUnPackPkg(__resp.m_data, DDSLoanResult, __loan);                                                       \
            if (__loan)                                                                                                \
            {                                                                                                          \
                (outPtr) = __loan->msg.sample;                                                                         \
            }                                                                                                          \
        }                                                                                                              \
    } while (0)

/// @brief 归还未发布的借出样本（sample 为 YOMKRPC_LOAN 借出的指针），避免 writer 池泄漏。
#define YOMKRPC_DISCARD_LOAN(nodeName, topicName, sample) \
    YOMK_REQUEST("/YomkRpcService/discard_loan", YomkMkPtr(DDSLoan, DDSLoan{nodeName, topicName, sample}))

/// @brief 删除节点并销毁其全部 DDS 实体；nodeName 不存在时返回错误。
#define YOMKRPC_DEL_NODE(nodeName) YOMK_REQUEST("/YomkRpcService/delete_node", YomkMkPtr(String, nodeName))

// ===== YomkRpcDebugService 调试 API（端点定义见 YomkRpcDebugService.h） =====

/// @brief 创建调试节点（单节点模型：一个进程至多一个，重复创建须先删除）。
/// @param 可变参 [domainId[, udpDiscoverServerLocator[, tcpDiscoverServerLocator]]]：
///   domainId 为 DDS 域号 [0,232]（仅同域节点的主题可被调试），缺省时读环境变量
///   YOMKRPC_DDS_DOMAIN_ID；两个 server 直配聚合串语义同 YOMKRPC_NODE 尾参，缺省时读
///   YOMKRPC_USE_DISCOVER_SERVER 门控的双 server 环境变量（调试节点无用户名，XML 提取链
///   仅默认档可兜底）。
/// 调试节点以唯一名 yomkrpc_debug_<uid> 入域（uid 为 12 位随机 hex，可被 yomkrpc node list 列出）。
#define YOMKRPC_DEBUG_NODE(...) \
    YOMK_REQUEST("/YomkRpcDebugService/create_node", YomkMkPtr(DDSDebugNode, DDSDebugNode{__VA_ARGS__}))

/// @brief 登记调试主题：发现匹配的远端 DataWriter 后自动解析类型并建立订阅，消息 JSON 文本
///   逐条投递 output（DDSDebugOutputFunc，用户自定义，服务层不打印）；须先创建调试节点。
/// @param qos 可缺省尾参：动态订阅 reader QoS DDSQosProfile；缺省全哨兵=Fast DDS reader
///   默认底座（KEEP_LAST(1)+BEST_EFFORT+VOLATILE）；对接 ROS2 高频流建议
///   DDSQosProfile::ros2DefaultSubscriber()（KEEP_LAST(10) 防丢样本）。
/// 登记是发现驱动的延迟行为：主题尚无发布者时仍返回成功，订阅待 writer 出现后自动建立；
/// 登记约 3s 后仍无 writer 由节点层工作线程打一次性等待提示（宽限窗避开发现未完成时的误报）。
/// 重复登记同一主题返回错误。失败 m_msg 归因：debug node not created / subscriber not
/// created / empty topic name / topic [...] already registered。
/// 详见 docs/features/debug-service.md
#define YOMKRPC_DEBUG_TOPIC_PRINT(topicName, output, ...) \
    YOMK_REQUEST(                              \
        "/YomkRpcDebugService/topic_print",    \
        YomkMkPtr(DDSDebugTopic, DDSDebugTopic{topicName, output, ##__VA_ARGS__}))

/// @brief 列出当前域内已发现的全部主题（仅主题名，按名排序，每行一项，列表可为空）；
///   须先创建调试节点。仅有订阅者而无发布者的主题同样列出。
/// 收敛语义（本组 list/info/find 接口同用，后文以「收敛语义同上」引用）：轮询发现缓存快照，
/// 连续 stableRounds 次集合不变即返回；stableRounds=0 钳制为默认 5 次/200ms，=1 即单次快照
/// 免等待，最长阻塞约 stableRounds*intervalMs。
#define YOMKRPC_DEBUG_TOPIC_LIST(stableRounds, intervalMs)                   \
    YOMK_REQUEST(                                                            \
        "/YomkRpcDebugService/list_topics",                                  \
        YomkMkPtr(DDSDebugList, DDSDebugList{stableRounds, intervalMs}))

/// @brief 同 YOMKRPC_DEBUG_TOPIC_LIST 的 types 模式：每行输出 "主题名 [类型名]"（单空格 +
///   方括号，对齐 ros2 topic list -t 形态，类型名原样输出）；收敛语义同上。须先创建调试节点。
#define YOMKRPC_DEBUG_TOPIC_LIST_T(stableRounds, intervalMs)                 \
    YOMK_REQUEST(                                                            \
        "/YomkRpcDebugService/list_topics",                                  \
        YomkMkPtr(DDSDebugList, DDSDebugList{stableRounds, intervalMs, true}))

/// @brief 查询单主题的发现详情（独立收敛，语义同上）：命中返回 StringArray 三行——
///   Type: <原始 DDS 类型名>、Publisher count: <N>、Subscription count: <N>（类型名原样
///   输出，无任何风格转换）；未发现主题返回错误。须先创建调试节点。
#define YOMKRPC_DEBUG_TOPIC_INFO(topicName, stableRounds, intervalMs)        \
    YOMK_REQUEST(                                                            \
        "/YomkRpcDebugService/topic_info",                                   \
        YomkMkPtr(DDSDebugInfo, DDSDebugInfo{topicName, stableRounds, intervalMs}))

/// @brief 单主题端点详情（verbose，与 topic_info 同端点同收敛语义）：另逐端点输出 Node name
///   （归属参与者名）、Endpoint type（PUBLISHER/SUBSCRIPTION）、GUID（FastDDS 原生格式）与
///   QoS profile（ROS2 风格键值行，核心组恒输出、扩展组按发现数据携带情况追加）；count=0
///   的端点类型无清单段；未发现主题返回错误。须先创建调试节点。
#define YOMKRPC_DEBUG_TOPIC_INFO_V(topicName, stableRounds, intervalMs)      \
    YOMK_REQUEST(                                                            \
        "/YomkRpcDebugService/topic_info",                                   \
        YomkMkPtr(DDSDebugInfo, DDSDebugInfo{topicName, stableRounds, intervalMs, true}))

/// @brief 按数据类型名反查域内主题列表（类型名精确匹配，收敛语义同上）：命中返回 StringArray
///   每行一个主题名（按名排序）；无匹配主题返回错误（type [...] not found）。
///   须先创建调试节点。
#define YOMKRPC_DEBUG_TOPIC_FIND(typeName, stableRounds, intervalMs)         \
    YOMK_REQUEST(                                                            \
        "/YomkRpcDebugService/topic_find",                                   \
        YomkMkPtr(DDSDebugFind, DDSDebugFind{typeName, stableRounds, intervalMs}))

/// @brief 按数据类型名输出该类型的 IDL 结构描述（精确匹配，收敛语义同上）：命中返回
///   StringArray 多行（IDL 源语法：struct 头 + 四空格缩进字段行 + 结尾 };）。失败 m_msg
///   归因：type [...] not found（类型名未出现在发现缓存）/ type object for [...] not
///   available（对端未提供 XTypes TypeObject，ROS2 rmw_fastrtps 端点即此情形）/ type [...]
///   is not a struct, interface display unsupported / interface introspection failed
///   for [...]。须先创建调试节点。
#define YOMKRPC_DEBUG_INTERFACE_SHOW(typeName, stableRounds, intervalMs)     \
    YOMK_REQUEST(                                                            \
        "/YomkRpcDebugService/interface_show",                               \
        YomkMkPtr(DDSDebugInterfaceShow, DDSDebugInterfaceShow{typeName, stableRounds, intervalMs}))

/// @brief 列出发现缓存中出现的全部数据类型名（去重字典序，interface show 的配套导航；
///   收敛语义同上）：域内无任何类型返回错误（no interface types discovered）。
///   须先创建调试节点。
#define YOMKRPC_DEBUG_INTERFACE_LIST(stableRounds, intervalMs)              \
    YOMK_REQUEST(                                                            \
        "/YomkRpcDebugService/interface_list",                               \
        YomkMkPtr(DDSDebugInterfaceList, DDSDebugInterfaceList{stableRounds, intervalMs}))

/// @brief 按主题名查询发布示例三段行集（收敛语义同上）："Type: <类型名>" + "IDL:" + IDL
///   行集 + "example:" + 可复制发布命令（JSON 默认值模板整体包单引号，改字段值即可发布）。
///   失败 m_msg 按三步归因：topic [...] not found → 类型侧（同 interface_show）→ 示例
///   生成侧（type rebuild failed for [...] / example generation failed for [...]:
///   create_data 或 json_serialize）。须先创建调试节点。
#define YOMKRPC_DEBUG_TOPIC_EXAMPLE(topicName, stableRounds, intervalMs)    \
    YOMK_REQUEST(                                                            \
        "/YomkRpcDebugService/topic_example",                                \
        YomkMkPtr(DDSDebugTopicExample, DDSDebugTopicExample{topicName, stableRounds, intervalMs}))

/// @brief 按主题名查询消息描述（供导出消息描述文件；收敛语义同上）：命中返回 StringArray
///   恰 2 行——d[0]=类型名、d[1]=紧凑 msg JSON（单行，可直接作为发布载荷模板）。失败 m_msg
///   按两步归因：topic [...] not found → JSON 模板生成侧（同 topic_example 步骤 2/3）。
///   须先创建调试节点。
#define YOMKRPC_DEBUG_TOPIC_MSG(topicName, stableRounds, intervalMs)        \
    YOMK_REQUEST(                                                            \
        "/YomkRpcDebugService/topic_msg",                                    \
        YomkMkPtr(DDSDebugTopicMsg, DDSDebugTopicMsg{topicName, stableRounds, intervalMs}))

/// @brief 按主题名发布 JSON 载荷消息：发现收敛 → 类型重建 + json_deserialize 解析载荷 →
///   建临时 RELIABLE+TRANSIENT_LOCAL writer 匹配收敛 → write 一次 → 存在 RELIABLE 订阅者时
///   wait_for_acknowledgments 确认送达；全 BEST_EFFORT 或无订阅者退化尽力而为。
///   须先创建调试节点。
/// @param requiredSubscribers 期望建匹配订阅端数门槛（>0 达标才发布并打印等待进度，直至达标
///   或停止标志置位中断；=0 自动收敛）。
/// @param repeatIntervalMs >0 为持续发布模式：首轮校验成功后按该周期重复 write（尽力而为：
///   write 失败仅计数不返回，不再等 ack），直到 yomk::g_debugPubStop 置位（CLI Ctrl+C 经
///   debugPubStop 置，async-signal-safe）或发满 maxTimes 条（>0 时，含首轮）才退出清理。
/// @param maxTimes 见 repeatIntervalMs；发满达标在销毁发布链前保底排空 200ms 后返回。
/// @param qos 可缺省尾参：临时发布 writer QoS DDSQosProfile；缺省全哨兵=Fast DDS writer
///   默认底座（KEEP_LAST(1)+RELIABLE+TRANSIENT_LOCAL）；ROS2 底座建议
///   DDSQosProfile::ros2DefaultPublisher()。
/// @retval eNo 未发现主题（topic [...] not found）/ 载荷不合法（invalid json for type [...]）/
///   送达未确认（not all subscribers acknowledged）。
/// 详见 docs/features/debug-service.md
#define YOMKRPC_DEBUG_TOPIC_PUB(topicName, json, stableRounds, intervalMs, repeatIntervalMs, requiredSubscribers, maxTimes, ...)  \
    YOMK_REQUEST(                                                            \
        "/YomkRpcDebugService/topic_pub",                                    \
        YomkMkPtr(DDSDebugTopicPub, DDSDebugTopicPub{topicName, json, stableRounds, intervalMs, repeatIntervalMs, requiredSubscribers, maxTimes, ##__VA_ARGS__}))

/// @brief 列出当前域内已发现的全部命名参与者（独立收敛，语义同上）：返回 StringArray 每行
///   一个节点名（participant_name 非空才列出，空名参与者跳过，不输出 GUID 串），按名称排序，
///   列表可为空。须先创建调试节点。
#define YOMKRPC_DEBUG_NODE_LIST(stableRounds, intervalMs)                    \
    YOMK_REQUEST(                                                            \
        "/YomkRpcDebugService/list_nodes",                                   \
        YomkMkPtr(DDSNodeList, DDSNodeList{stableRounds, intervalMs}))

/// @brief 查询指定节点名的发布/订阅主题清单（独立收敛，语义同上）：归属判定基于 RTPS 规范
///   「端点 GUID 前缀 == 所属参与者 GUID 前缀」，同名多参与者端点合并。命中返回 StringArray：
///   节点名行、"  Subscribers:" 段 + 每行 "    <topic>: <type>"、"  Publishers:" 段同形态
///   （类型名原样输出；空段仅打段头）；未发现节点名返回错误。须先创建调试节点。
#define YOMKRPC_DEBUG_NODE_INFO(nodeName, stableRounds, intervalMs)          \
    YOMK_REQUEST(                                                            \
        "/YomkRpcDebugService/node_info",                                    \
        YomkMkPtr(DDSNodeInfo, DDSNodeInfo{nodeName, stableRounds, intervalMs}))

/// @brief 退出调试：删除调试节点并销毁其全部 DDS 实体（未创建时返回错误）。
#define YOMKRPC_DEBUG_QUIT() YOMK_REQUEST("/YomkRpcDebugService/delete_node", nullptr)

// ===== YomkRpcBagService 录制 API（端点定义见 YomkRpcBagService.h） =====

/// @brief 创建 bag 节点（单节点模型：一个进程至多一个，重复创建须先删除）。
/// @param 可变参 [domainId[, udpDiscoverServerLocator[, tcpDiscoverServerLocator]]]：语义同
///   YOMKRPC_DEBUG_NODE（缺省时经 DDSBagNode 类内缺省初始化器读同一组环境变量；bag 节点无
///   用户名，XML 提取链仅默认档可兜底）。domainId 合法范围 [0,232]。
/// bag 节点以唯一名 yomkrpc_bag_<uid> 入域（uid 为 12 位随机 hex，可被 yomkrpc node list 列出）。
#define YOMKRPC_BAG_NODE(...) \
    YOMK_REQUEST("/YomkRpcBagService/create_node", YomkMkPtr(DDSBagNode, DDSBagNode{__VA_ARGS__}))

/// @brief 录制主题（长驻阻塞：调用线程阻塞至录制收尾完成，SIGINT 经 yomk::bagRecordStop
///   无锁置位后收尾返回）；须先创建 bag 节点。
/// @param topics 清单项支持通配（恰好一个 '*'：前缀 pre* / 后缀 *suf / 中间 pre*suf，"*"
///   匹配全部；≥2 个 '*' 返回错误），启动时按发现缓存全表展开为实际主题集合（去重升序，
///   与精确项合并）；模式未命中、既无发布者也无订阅者的精确主题、清单重复/空名均整体报错
///   （m_msg 逐项列出，不建 bag）。
/// @param outputDir bag 目录：相对/绝对均可，父目录自动多级创建，已存在报错；为空缺省当前
///   路径 bag_<YYYY-MM-DD_HH-MM-SS_mmm>（含 bag_0.mcap 与 metadata.json）。
/// @param maxBagSize 单分片最大字节数（>0 写满即滚动 bag_N.mcap 分片；下限 1024 字节过小
///   报错；0=不分片）。
/// @param maxBagDurationSec 单分片最大时长秒（0=不分片，无下限校验；与 maxBagSize 同用先到
///   先分）。
/// @param maxCacheSize 写缓存双缓冲字节数（0=直写；>0 回调拷贝入队、服务端独立消费线程写盘，
///   缓存满丢弃新消息并按主题计数）。
/// 透传订阅：通过校验的主题以透传方式订阅，原始 CDR 字节直写 mcap。落盘元信息：每分片
/// Channel.metadata 记 type+qos 两键（qos 取首见发布者 offered 的 Reliability/Durability 拼
/// "<RELIABILITY>,<DURABILITY>" 串，仅订阅者主题空串），metadata.json 的 topic_metadata 内
/// name/type/qos 并列（供 bag info 显示与 reindex 重建）。
/// 成功 data 为 StringArray：首行 bag 目录名，其后每主题一行 "topic: N 条 / M 字节" 统计。
/// 详见 docs/features/bag-service.md
#define YOMKRPC_BAG_RECORD(topics, outputDir, maxBagSize, maxBagDurationSec, maxCacheSize) \
    YOMK_REQUEST("/YomkRpcBagService/bag_record", YomkMkPtr(DDSBagRecord, DDSBagRecord{(topics), (outputDir), (maxBagSize), (maxBagDurationSec), (maxCacheSize)}))

/// @brief 查看 bag 元信息（纯文件读，不经 bag 节点，无须 create_node）：读 bagDir 的
///   metadata.json 与目录递归总大小，输出对齐 ros2 bag info 形态的元信息文本（Files/Bag
///   size/Storage id/Duration/Start/End/Messages/Topic information 各一行，多主题/多分片
///   续行缩进对齐；Serialization Format 固定 cdr；topic_metadata 含非空 qos 键时 Topic
///   information 行在 Type 后插 QoS 段，旧 bag/仅订阅者主题不显示）。成功 data 每项一行
///   （首项为空行）；路径不存在/无 metadata.json/解析失败返回错误。
#define YOMKRPC_BAG_INFO(bagDir) \
    YOMK_REQUEST("/YomkRpcBagService/bag_info", YomkMkPtr(DDSBagInfo, DDSBagInfo{(bagDir)}))

/// @brief 重建 bag 元信息（纯文件操作，不经 bag 节点）：读 bagDir 全部 mcap 分片
///   （<前缀>_<编号>.mcap 按编号升序）重建 metadata.json 并无条件覆盖写出（元信息文件缺失/
///   损坏均可重建；对齐 ros2 bag reindex 行为语义）。主题 type 经各分片 Channel.metadata 的
///   type 键恢复；旧格式 bag（无该元数据的既有分片）读不到则回退空串。路径不存在/非目录/
///   无 mcap 分片/读取失败返回错误。
#define YOMKRPC_BAG_REINDEX(bagDir) \
    YOMK_REQUEST("/YomkRpcBagService/bag_reindex", YomkMkPtr(DDSBagReindex, DDSBagReindex{(bagDir)}))

/// @brief 转换 bag（纯文件操作，不经 bag 节点）：读输入 bag 目录集与输出配置文件，把输入
///   消息按配置过滤合并写为新 bag；多输入按 logTime 全局归并，同主题跨输入类型不一致报错。
/// @param inputs 输入 bag 目录清单（至少 1 个，相对/绝对均可，重复目录拒绝）。
/// @param cfgPath 输出配置文件路径（JSON，顶层 output_bags 键为对象序列，每条目独立一组输出
///   参数）：uri 输出目录必填非空（已存在报错）；topics 输出主题清单（缺省全部，支持恰好
///   一个 '*' 的通配模式，精确项不存在/模式未命中报错）；start_time/end_time 收纳区间
///   （缺省 0=不限，start>end 报错，含头含尾；非负整数纳秒，或可读时间串
///   YYYY-MM-DD_HH-MM-SS_毫秒-微秒-纳秒，本地时区）；max_bagfile_size 单分片最大字节数
///   （0=不分片，>0 且 <1024 报错）；max_bagfile_duration 单分片最大时长秒（0=不分片，与
///   size 同用先到先分）；未知字段忽略。任一条目失败整体报错（已产生的输出目录保留不回滚）。
/// 成功 data 每条目一行 "converted <uri>: N messages / M bytes / K topics" 统计。
#define YOMKRPC_BAG_CONVERT(inputs, cfgPath) \
    YOMK_REQUEST("/YomkRpcBagService/bag_convert", YomkMkPtr(DDSBagConvert, DDSBagConvert{(inputs), (cfgPath)}))

/// @brief 回放 bag（长驻阻塞：调用线程阻塞至全部消息播毕或 SIGINT 经 yomk::bagPlayStop
///   无锁置位后收尾返回）；须先创建 bag 节点。读播解耦：读线程逐条预读原始 CDR 字节入有界
///   内存队列，播放线程按相邻时间戳差值定时发布；writer 按需创建，发布 QoS 按录制快照恢复
///   （空串/缺失回退 RELIABLE+VOLATILE；旧格式 bag——任一主题 type 空串——报错拒播）；
///   暂停/恢复经 yomk::bagPlayPause/bagPlayResume 键盘通道控制，不经协议传递。
/// @param bagDir bag 目录（相对/绝对均可，须含 metadata.json）。
/// @param playbackRate 回放速率倍率（对齐 ros2 --rate：2.0 两倍速、0.5 半倍速；<=0 报
///   "invalid playback rate"；运行时经 yomk::bagPlaySetRate/bagPlayGetRate 动态调率，换率
///   即时生效且已播进度连续）。
/// @param topics 回放清单（空=全部，对齐 ros2 --topics；精确项须在 bag 清单否则报
///   "not found in bag"；含一个 '*' 为通配按 bag 清单展开，未命中报 "matched no bag topic"；
///   入选集合 = 精确 ∪ 通配命中，回显/发布实体/收尾统计只含入选主题）。
/// @param loop 循环回放（对齐 ros2 -l/--loop：到尾即回开头无限播放；收尾统计为各轮累计；
///   Ctrl+C 终止）。
/// @param startOffset 起始偏移秒（对齐 ros2 --start-offset：从 bag 开始第 SEC 秒起播——读
///   线程跳过 logTime 早于全场首条+偏移的消息，不入队不计收尾统计；与 loop 叠加每轮保持；
///   负值报 "invalid start offset"；超全场时长则零消息播出正常播毕返回）。
/// @param delay 开播延迟秒（对齐 ros2 -d/--delay：播放线程在每轮轮边界重锚前先睡 SEC 秒再
///   播出，发布实体已建，为订阅端留匹配窗口；与 loop 叠加每轮保持；负值报 "invalid delay"；
///   0 不延迟）。
/// @param waitForAllAckedMs 退出前等全部 RELIABLE 订阅端确认收齐毫秒（对齐 ros2
///   --wait-for-all-acked：回放结束后、销毁发布实体前，逐 RELIABLE writer 等待，BEST_EFFORT
///   跳过；-1 不等待，0 永等（Ctrl+C 可中断），>0 逐主题各计超时；超时/中断仅告警不改变
///   返回值）。
/// @param topicRemaps 主题重映射 "old:=new" 原始串列表（对齐 ros2 --remap：发布主题名精确
///   匹配改名，QoS/类型按 bag 快照照旧，topics 过滤在读取层按原名先过滤后改名；old 不在
///   bag 清单则规则静默无效；空=不重映射）。
/// @param readAheadQueueSize 预读内存队列深度（对齐 ros2 --read-ahead-queue-size：读侧吸收
///   磁盘 IO 抖动保证确定性回放；<1 报 "invalid read-ahead queue size" 快速返回；发布端
///   history 上限为独立常量不随其联动）。
/// @param qosOverridesPath QoS 覆盖文件路径（JSON：键=bag 原主题名精确匹配，值={reliability,
///   durability} 两枚大写 DDS 标准拼写枚举名可独立给，未给档保持 bag 快照恢复值；非法枚举值
///   报 "invalid qos string" 拒播；不在 bag 的主题键静默无效；优先级：文件命中 > bag 内
///   qos 快照 > 旧 bag 回退 RELIABLE+VOLATILE；空串=不加载）。
/// 成功 data 为 StringArray：首行 bag 目录名，其后每主题一行 "topic: N 条 / M 字节" 统计。
/// 详见 docs/features/bag-service.md
#define YOMKRPC_BAG_PLAY(bagDir, playbackRate, topics, loop, startOffset, delay, waitForAllAckedMs, topicRemaps, readAheadQueueSize, qosOverridesPath) \
    YOMK_REQUEST("/YomkRpcBagService/bag_play", \
        YomkMkPtr(DDSBagPlay, DDSBagPlay{(bagDir), (playbackRate), (topics), (loop), (startOffset), (delay), (waitForAllAckedMs), (topicRemaps), (readAheadQueueSize), (qosOverridesPath)}))

/// @brief 退出录制：删除 bag 节点并销毁其全部 DDS 实体（未创建时返回错误）。
#define YOMKRPC_BAG_DEL_NODE() YOMK_REQUEST("/YomkRpcBagService/delete_node", nullptr)
