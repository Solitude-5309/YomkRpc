#pragma once
#include <YomkRpc/YomkRpcDebugService.h>
#include <YomkRpc/YomkRpcService.h>

// YomkRpc 对外 API 宏：每个宏封装一次对 /YomkRpcService/*（RPC 服务）或 /YomkRpcDebugService/*
// （调试服务）端点的 YOMK_REQUEST 调用。
// 除 YOMKRPC_VERSION（无返回值）外，其余宏均返回 YomkResponse，调用后须判 m_status==YomkResponse::eOk。

// 创建 DDS 节点（每节点对应一个独立的 DDS 参与者）。
// domainId：DDS 域号，合法范围 [0,232]，仅同域节点可互通；nodeName：节点唯一名。
#define YOMKRPC_NODE(domainId, nodeName) \
    YOMK_REQUEST("/YomkRpcService/create_node", YomkMkPtr(DDSNode, DDSNode{domainId, nodeName}))

// 查询扩展版本：宏内部自动解包并打印（成功走 YOMK_INFO_TAG、失败走 YOMK_ERROR_TAG），无返回值。
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

// 在指定节点上注册发布主题。
// type：消息类型实例（new XxxPubSubType()），所有权移交服务端——调用后 caller 不再持有/释放（无论成功或失败）。
#define YOMKRPC_PUB_TOPIC(nodeName, topicName, type) \
    YOMK_REQUEST("/YomkRpcService/register_pub_topic", YomkMkPtr(DDSTopic, DDSTopic{nodeName, topicName, type}))

// 在指定节点上注册订阅主题。
// type：同 YOMKRPC_PUB_TOPIC，所有权移交服务端；callback：std::function<void(const void*)>，每次收到消息时被调用。
#define YOMKRPC_SUB_TOPIC(nodeName, topicName, type, callback) \
    YOMK_REQUEST(                                              \
        "/YomkRpcService/register_sub_topic",                  \
        YomkMkPtr(DDSSubRequest, DDSSubRequest{nodeName, topicName, type, callback}))

// 向指定节点的已注册发布主题发送一条消息。
// data：消息实例指针（&msg），借用（非所有权）——publish 同步写入，caller 保留并在调用后自行管理其生命周期。
#define YOMKRPC_PUB_MSG(nodeName, topicName, data) \
    YOMK_REQUEST("/YomkRpcService/publish", YomkMkPtr(DDSPublish, DDSPublish{nodeName, topicName, data}))

// 借出发布缓冲（仅 plain 类型）：outPtr 为输出参数，成功时指向 writer 池内待发样本，直接在池内填值后
// 经 YOMKRPC_PUB_MSG 发布免序列化；类型不支持 loan（非 plain）或池耗尽时 outPtr 保持 nullptr，回退普通发布路径。
// 每次 write 后指针即被中间件收回，须重新借出。
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

// 归还未发布的借出样本（sample 为 YOMKRPC_LOAN 借出的指针），避免 writer 池泄漏。返回 YomkResponse。
#define YOMKRPC_DISCARD_LOAN(nodeName, topicName, sample) \
    YOMK_REQUEST("/YomkRpcService/discard_loan", YomkMkPtr(DDSLoan, DDSLoan{nodeName, topicName, sample}))

// 删除节点并销毁其全部 DDS 实体；nodeName 不存在时返回错误。返回 YomkResponse。
#define YOMKRPC_DEL_NODE(nodeName) YOMK_REQUEST("/YomkRpcService/delete_node", YomkMkPtr(String, nodeName))

// ===== YomkRpcDebugService 调试 API（端点定义见 YomkRpcDebugService.h） =====

// 创建调试节点（单节点模型：一个进程至多一个，重复创建须先删除）。
// domainId：DDS 域号，合法范围 [0,232]，仅同域节点的主题可被调试。返回 YomkResponse。
#define YOMKRPC_DEBUG_NODE(domainId) \
    YOMK_REQUEST("/YomkRpcDebugService/create_node", YomkMkPtr(DDSDebugNode, DDSDebugNode{domainId}))

// 登记调试主题：发现匹配的远端 DataWriter 后自动解析类型并建立订阅，消息 JSON 文本逐条投递
// output（DDSDebugOutputFunc，用户自定义，服务层不打印）；须先创建调试节点，重复登记同一主题返回错误。
// 登记为发现驱动的延迟行为：主题尚未被任何发布者上线时仍返回成功，订阅待 writer 出现后
// 自动建立；登记约 3s 后仍无 writer 时由节点层工作线程打一次性等待提示（宽限窗避开
// 刚入域发现未完成时对"发布者早已在线"的误报）。失败时 m_msg 携带节点层归类的具体拒绝原因
// （debug node not created / subscriber not created / empty topic name /
// topic [...] already registered）。返回 YomkResponse。
#define YOMKRPC_DEBUG_TOPIC_PRINT(topicName, output) \
    YOMK_REQUEST(                              \
        "/YomkRpcDebugService/topic_print",    \
        YomkMkPtr(DDSDebugTopic, DDSDebugTopic{topicName, output}))

// 列出当前域内已发现的全部主题与数据类型名（自适应收敛）：轮询发现缓存快照，连续 stableRounds
// 次集合不变即收敛返回（0 值钳制为默认 5 次/200ms；stableRounds=1 即单次快照免等待；最长阻塞约
// stableRounds*intervalMs）。远端 DataWriter 与 DataReader 均记录——仅有订阅者而无发布者的主题
// 同样列出。返回 StringArray 包，每行一个 topicName（仅主题名，不含类型），按主题名排序；
// 列表可为空（域内无 writer/reader）。须先创建调试节点。返回 YomkResponse。
#define YOMKRPC_DEBUG_TOPIC_LIST(stableRounds, intervalMs)                   \
    YOMK_REQUEST(                                                            \
        "/YomkRpcDebugService/list_topics",                                  \
        YomkMkPtr(DDSDebugList, DDSDebugList{stableRounds, intervalMs}))

// 列出域内主题与数据类型名（types 模式，与 topic list 同端点同收敛语义）：每行输出
// "主题名 [类型名]"（单空格 + 方括号，对齐 ros2 topic list -t 形态，类型名原样输出）；
// 收敛参数语义同 topic list。须先创建调试节点。返回 YomkResponse。
#define YOMKRPC_DEBUG_TOPIC_LIST_T(stableRounds, intervalMs)                 \
    YOMK_REQUEST(                                                            \
        "/YomkRpcDebugService/list_topics",                                  \
        YomkMkPtr(DDSDebugList, DDSDebugList{stableRounds, intervalMs, true}))

// 查询单个主题的发现详情（独立收敛，与 list_topics 完全分开）：轮询该主题详情快照（存在标志 +
// 类型名 + 端点计数），连续 stableRounds 次不变即收敛返回（0 值钳制为默认 5 次/200ms；
// stableRounds=1 即单次快照免等待；最长阻塞约 stableRounds*intervalMs）。命中返回 StringArray
// 三行：Type: <原始 DDS 类型名>、Publisher count: <N>、Subscription count: <N>（类型名原样输出，
// 无任何风格转换）；未发现主题返回错误。须先创建调试节点。返回 YomkResponse。
#define YOMKRPC_DEBUG_TOPIC_INFO(topicName, stableRounds, intervalMs)        \
    YOMK_REQUEST(                                                            \
        "/YomkRpcDebugService/topic_info",                                   \
        YomkMkPtr(DDSDebugInfo, DDSDebugInfo{topicName, stableRounds, intervalMs}))

// 查询单主题端点详情（verbose，与 topic_info 同端点同收敛语义）：除 Type 与两端点计数外，
// 逐端点输出 Node name（归属参与者名）、Endpoint type（PUBLISHER/SUBSCRIPTION）、GUID
// （FastDDS 原生格式）与 QoS profile（ROS2 风格键值行，核心组恒输出、扩展组按发现数据
// 携带情况追加）；count 为 0 的端点类型无清单段；未发现主题返回错误。须先创建调试节点。
#define YOMKRPC_DEBUG_TOPIC_INFO_V(topicName, stableRounds, intervalMs)      \
    YOMK_REQUEST(                                                            \
        "/YomkRpcDebugService/topic_info",                                   \
        YomkMkPtr(DDSDebugInfo, DDSDebugInfo{topicName, stableRounds, intervalMs, true}))

// 按数据类型名反查域内主题列表（独立收敛，语义同 list_topics）：类型名精确匹配，命中返回
// StringArray 每行一个主题名（按主题名排序）；无匹配主题返回错误（type [...] not found）。
// 须先创建调试节点。返回 YomkResponse。
#define YOMKRPC_DEBUG_TOPIC_FIND(typeName, stableRounds, intervalMs)         \
    YOMK_REQUEST(                                                            \
        "/YomkRpcDebugService/topic_find",                                   \
        YomkMkPtr(DDSDebugFind, DDSDebugFind{typeName, stableRounds, intervalMs}))

// 按数据类型名输出该类型的 IDL 结构描述（独立收敛，语义同 topic_find）：类型名精确匹配，
// 命中返回 StringArray 多行（IDL 源语法：struct 头 + 四空格缩进字段行 + 结尾 };）。失败时 m_msg
// 携带节点层归类的具体原因，不再是单一 not found：type [...] not found（类型名未出现在发现
// 缓存）/ type object for [...] not available（对端未提供 XTypes TypeObject，ROS2
// rmw_fastrtps 端点即此情形）/ type [...] is not a struct, interface display unsupported
// （顶层非 struct）/ interface introspection failed for [...]（字段内省失败）。
// 须先创建调试节点。返回 YomkResponse。
#define YOMKRPC_DEBUG_INTERFACE_SHOW(typeName, stableRounds, intervalMs)     \
    YOMK_REQUEST(                                                            \
        "/YomkRpcDebugService/interface_show",                               \
        YomkMkPtr(DDSDebugInterfaceShow, DDSDebugInterfaceShow{typeName, stableRounds, intervalMs}))

// 列出发现缓存中出现的全部数据类型名（去重字典序排序，每行一个，interface show 的配套导航；
// 独立收敛，同 interface_show）：轮询发现缓存快照，连续 stableRounds 次不变即返回（0 值钳制
// 为默认）。命中返回 StringArray 多行；域内无任何类型返回错误（no interface types discovered）。
// 须先创建调试节点。返回 YomkResponse。
#define YOMKRPC_DEBUG_INTERFACE_LIST(stableRounds, intervalMs)              \
    YOMK_REQUEST(                                                            \
        "/YomkRpcDebugService/interface_list",                               \
        YomkMkPtr(DDSDebugInterfaceList, DDSDebugInterfaceList{stableRounds, intervalMs}))

// 按主题名查询发布示例三段行集（独立收敛，同 interface_show）："Type: <类型名>" + "IDL:"
// + IDL 行集 + "example:" + 可复制发布命令（JSON 默认值模板整体包单引号，改字段值即可
// 发布）。命中返回 StringArray 多行。失败时 m_msg 按节点层三步骤归类携带具体原因：
// 步骤 1 topic [...] not found（主题未发现）；步骤 2 类型侧原因（同 interface_show：
// type [...] not found / type object for [...] not available / 非 struct）；步骤 3 示例
// 生成侧原因（type rebuild failed for [...] / example generation failed for [...]:
// create_data 或 json_serialize）。须先创建调试节点。返回 YomkResponse。
#define YOMKRPC_DEBUG_TOPIC_EXAMPLE(topicName, stableRounds, intervalMs)    \
    YOMK_REQUEST(                                                            \
        "/YomkRpcDebugService/topic_example",                                \
        YomkMkPtr(DDSDebugTopicExample, DDSDebugTopicExample{topicName, stableRounds, intervalMs}))

// 按主题名查询消息描述两要素（供导出消息描述文件，独立收敛同 interface_show）：命中返回
// StringArray 恰 2 行：d[0]=类型名、d[1]=紧凑 msg JSON（单行，可直接作为发布载荷模板）。
// 失败时 m_msg 按节点层两步骤归类携带具体原因：步骤 1 topic [...] not found（主题未发现）；
// 步骤 2 JSON 模板生成侧原因（同 topic_example 步骤 2/3：类型名未出现 / TypeObject 不可得 /
// 非 struct / type rebuild failed for [...] / example generation failed for [...]）。
// 须先创建调试节点。返回 YomkResponse。
#define YOMKRPC_DEBUG_TOPIC_MSG(topicName, stableRounds, intervalMs)        \
    YOMK_REQUEST(                                                            \
        "/YomkRpcDebugService/topic_msg",                                    \
        YomkMkPtr(DDSDebugTopicMsg, DDSDebugTopicMsg{topicName, stableRounds, intervalMs}))

// 按主题名发布 JSON 载荷消息（首轮：节点层先发现收敛 → 类型重建 + json_deserialize 解析
// 载荷 → 建临时 RELIABLE+TRANSIENT_LOCAL writer 并匹配收敛 → write 一次 → 存在 RELIABLE
// 订阅者时 wait_for_acknowledgments 确认送达（超时报错）not all subscribers acknowledged），
// 全 BEST_EFFORT 或无订阅者退化尽力而为。requiredSubscribers>0 为期望建匹配订阅端数门槛
// （CLI -w N）：matched 计数达到该值才继续发布，否则一直等待（节点层打印等待进度日志）
// 直至达标或停止标志置位中断（不设超时，报错 waiting for subscribers interrupted）；=0
// 自动收敛。repeatIntervalMs>0 为持续发布模式：首轮校验
// 成功后保留发布链按该周期重复 write（尽力而为：write 失败仅计数不返回，不再等 ack），
// 直到 yomk::g_debugPubStop 置位（CLI Ctrl+C 经 debugPubStop 置，async-signal-safe）或
// 发满 maxTimes 条（>0 时，含首轮；发满达标在销毁发布链前保底排空 200ms 后返回，Ctrl+C
// 中断不排空）才退出清理；成功响应 data 为 StringArray {"total=N", "failed=M"}（仅持续
// 模式，含首轮）。未发现主题（topic [...] not found）/载荷不合法（invalid json for type [...]）/
// 送达未确认返回 eNo。须先创建调试节点。返回 YomkResponse。
#define YOMKRPC_DEBUG_TOPIC_PUB(topicName, json, stableRounds, intervalMs, repeatIntervalMs, requiredSubscribers, maxTimes)  \
    YOMK_REQUEST(                                                            \
        "/YomkRpcDebugService/topic_pub",                                    \
        YomkMkPtr(DDSDebugTopicPub, DDSDebugTopicPub{topicName, json, stableRounds, intervalMs, repeatIntervalMs, requiredSubscribers, maxTimes}))

// 列出当前域内已发现的全部命名参与者（独立收敛，与 list_topics/topic_info 完全分开）：轮询
// 参与者发现缓存快照，连续 stableRounds 次不变即收敛返回（0 值钳制为默认 5 次/200ms；
// stableRounds=1 即单次快照免等待；最长阻塞约 stableRounds*intervalMs）。返回 StringArray，
// 每行一个节点名（participant_name 非空才列出，空名参与者跳过，不输出 GUID 串），按名称
// 排序；列表可为空（域内无命名参与者）。须先创建调试节点。返回 YomkResponse。
#define YOMKRPC_DEBUG_NODE_LIST(stableRounds, intervalMs)                    \
    YOMK_REQUEST(                                                            \
        "/YomkRpcDebugService/list_nodes",                                   \
        YomkMkPtr(DDSNodeList, DDSNodeList{stableRounds, intervalMs}))

// 查询指定节点名（participant_name）的发布/订阅主题清单（独立收敛，与 list_nodes 完全
// 分开）：轮询归属快照（存在标志 + 发布/订阅 (topicName, typeName) 清单），连续 stableRounds
// 次不变即收敛返回（0 值钳制为默认 5 次/200ms；stableRounds=1 即单次快照免等待；最长阻塞约
// stableRounds*intervalMs）。归属判定基于 RTPS 规范保证的"端点 GUID 前缀 == 所属参与者 GUID
// 前缀"。命中返回 StringArray 输出行：节点名行、"  Subscribers:" 段 + 每行 "    <topic>:
// <type>"、"  Publishers:" 段同形态（类型名原样输出，无任何风格转换；空段仅打段头）；未发现
// 节点名返回错误。同名多参与者端点合并。须先创建调试节点。返回 YomkResponse。
#define YOMKRPC_DEBUG_NODE_INFO(nodeName, stableRounds, intervalMs)          \
    YOMK_REQUEST(                                                            \
        "/YomkRpcDebugService/node_info",                                    \
        YomkMkPtr(DDSNodeInfo, DDSNodeInfo{nodeName, stableRounds, intervalMs}))

// 退出调试：删除调试节点并销毁其全部 DDS 实体（未创建时返回错误）。返回 YomkResponse。
#define YOMKRPC_DEBUG_QUIT() YOMK_REQUEST("/YomkRpcDebugService/delete_node", nullptr)

// ===== YomkRpcBagService 录制 API（端点定义见 YomkRpcBagService.h） =====

// 创建 bag 节点（单节点模型：一个进程至多一个，重复创建须先删除）。
// domainId：DDS 域号，合法范围 [0,232]。返回 YomkResponse。
#define YOMKRPC_BAG_NODE(domainId) \
    YOMK_REQUEST("/YomkRpcBagService/create_node", YomkMkPtr(DDSBagNode, DDSBagNode{domainId}))

// 录制主题列表（长驻阻塞：调用线程阻塞至录制收尾完成，SIGINT 经 yomk::bagRecordStop
// 无锁置位后收尾返回）。清单项支持通配模式（恰好一个 '*'：前缀 pre* / 后缀 *suf /
// 中间 pre*suf，"*" 匹配全部主题；≥2 个 '*' 返回错误），模式项启动时按发现缓存全表
// 匹配展开为实际主题集合（去重升序，与精确项合并），未命中任何主题的模式与既无发布者
// 也无订阅者的精确主题同样整体报错（m_msg 逐项列出，不建 bag）；通过校验的主题以透传方式订阅并将
// 原始 CDR 字节直写 mcap（bag 目录由 outputDir 指定——目录名/路径相对/绝对均可，父目录自动
// 多级创建，已存在报错；为空缺省当前路径下 bag_<YYYY-MM-DD_HH-MM-SS_mmm>，含 bag_0.mcap
// 与 metadata.json）。须先创建 bag 节点；清单内重复/空名主题返回错误。
// 成功返回 StringArray 包：首行 bag 目录名，其后每主题一行 "topic: N 条 / M 字节"统计。
// 返回 YomkResponse。
#define YOMKRPC_BAG_RECORD(topics, outputDir) \
    YOMK_REQUEST("/YomkRpcBagService/bag_record", YomkMkPtr(DDSBagRecord, DDSBagRecord{(topics), (outputDir)}))

// 退出录制：删除 bag 节点并销毁其全部 DDS 实体（未创建时返回错误）。返回 YomkResponse。
#define YOMKRPC_BAG_DEL_NODE() YOMK_REQUEST("/YomkRpcBagService/delete_node", nullptr)
