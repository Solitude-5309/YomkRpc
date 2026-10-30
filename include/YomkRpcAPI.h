#pragma once
#include <YomkRpc/YomkRpcDebugService.h>
#include <YomkRpc/YomkRpcService.h>

// YomkRpc 对外 API 宏：每个宏封装一次对 /YomkRpcService/*（RPC 服务）或 /YomkRpcDebugService/*
// （调试服务）端点的 YOMK_REQUEST 调用。
// 除 YOMKRPC_VERSION（无返回值）外，其余宏均返回 YomkResponse，调用后须判 m_status==YomkResponse::eOk。

// 加载 Fast DDS XML profiles 文件（/YomkRpcService/load_xml 端点）：读文件内容经
// load_XML_profiles_file 解析校验后缓存 raw 串（成功仅证明文件可解析；须在节点创建前调用，
// 后续本进程内 create_node 的 participant 均按三级提取链叠加——按节点名 profile →
// is_default_profile 默认档 → 均未命中保持默认底座）。再次调用以最新成功文件整体替换。
// 形态 1：YOMKRPC_LOAD_XML(xmlPath) —— 显式路径（相对/绝对均可）。
// 形态 2：YOMKRPC_LOAD_XML()        —— 无参缺省，经 DDSXmlLoad 类内缺省初始化器读环境
//            变量：总开关 YOMKRPC_USE_XML_PROFILE=true 时取 YOMKRPC_XML_PROFILE_PATH，
//            否则空串（服务端报 xml path is empty）。
// 失败（路径不存在/文件为空/格式非法）返回 eNo，m_msg 携带原因与 rc。返回 YomkResponse。
#define YOMKRPC_LOAD_XML(...) \
    YOMK_REQUEST("/YomkRpcService/load_xml", YomkMkPtr(DDSXmlLoad, DDSXmlLoad{##__VA_ARGS__}))

// 创建 DDS 节点（每节点对应一个独立的 DDS 参与者）。
// 形态 1：YOMKRPC_NODE(nodeName)            —— domainId 缺省，经 DDSNode 类内缺省初始化器
//            读环境变量 YOMKRPC_DDS_DOMAIN_ID（无/非法/超 [0,232] 回落 0）。
// 形态 2：YOMKRPC_NODE(nodeName, domainId)  —— 显式指定，合法范围 [0,232]。
// 形态 3：YOMKRPC_NODE(nodeName, domainId, udpDiscoverServerLocator) —— 尾参传 UDP 发现服务器
//            直配聚合串 "ip[:port]"（多 server 分号分隔，port 缺省 11811）：participant 以
//            CLIENT 发现协议直配（可单独使用或配合 YOMKRPC_LOAD_XML；非法串创建失败回显）。
// 形态 4：YOMKRPC_NODE(nodeName, domainId, udpDiscoverServerLocator, tcpDiscoverServerLocator)
//            —— 再传 TCP 发现服务器直配聚合串（port 为 server 的 TCP 监听口）：participant
//            补注入 TCPv4 传输并以 TCPv4 locator 直配；两通道可同时使用，任一通道 server
//            在场即完成发现（仅 TCP 直配时 UDP 通道传空串占位）。
// nodeName：节点唯一名。仅同域节点可互通。
#define YOMKRPC_NODE(nodeName, ...) \
    YOMK_REQUEST("/YomkRpcService/create_node", YomkMkPtr(DDSNode, DDSNode{nodeName, ##__VA_ARGS__}))

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
// qos（可缺省尾参）：端点 QoS 描述 DDSQosProfile（include/YomkRpcService.h）；缺省（不传）
// 为全哨兵=保持 Fast DDS 端点默认底座（现状行为）；传 DDSQosProfile::ros2DefaultPublisher()
// 即 ROS2 默认底座（RELIABLE+KEEP_LAST(10)+VOLATILE），也可在底座上逐项覆盖（如只设 depth）。
#define YOMKRPC_PUB_TOPIC(nodeName, topicName, type, ...) \
    YOMK_REQUEST("/YomkRpcService/register_pub_topic", YomkMkPtr(DDSTopic, DDSTopic{nodeName, topicName, type, ##__VA_ARGS__}))

// 在指定节点上注册订阅主题。
// type：同 YOMKRPC_PUB_TOPIC，所有权移交服务端；callback：std::function<void(const void*)>，每次收到消息时被调用。
// qos（可缺省尾参）：端点 QoS 描述 DDSQosProfile；缺省为全哨兵=现状行为；订阅端推荐
// DDSQosProfile::ros2DefaultSubscriber()（BEST_EFFORT requested 对任意 offered 均可匹配）。
#define YOMKRPC_SUB_TOPIC(nodeName, topicName, type, callback, ...) \
    YOMK_REQUEST(                                              \
        "/YomkRpcService/register_sub_topic",                  \
        YomkMkPtr(DDSSubRequest, DDSSubRequest{nodeName, topicName, type, callback, ##__VA_ARGS__}))

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
// 全可变参 ([domainId[, udpDiscoverServerLocator[, tcpDiscoverServerLocator]]])：无参形态三字段
// 均经 DDSDebugNode 类内缺省初始化器读环境变量（YOMKRPC_DDS_DOMAIN_ID 与开关
// YOMKRPC_USE_DISCOVER_SERVER 门控的双 server 串，语义同 YOMKRPC_NODE）；显式 domainId
// 的旧调用完全兼容。domainId：DDS 域号，合法范围 [0,232]，仅同域节点的主题可被调试。
// 调试节点以唯一名 yomkrpc_debug_<uid> 入域（uid 为 12 位随机 hex，可被 yomkrpc node list 列出）。
// udpDiscoverServerLocator（可缺省尾参）：UDP 发现服务器直配聚合串，语义同 YOMKRPC_NODE 形态 3 尾参；
// tcpDiscoverServerLocator（可再缺省尾参）：TCP 发现服务器直配聚合串，语义同形态 4 第 4 参
// （调试节点无用户名，XML 提取链仅默认档可兑底）。返回 YomkResponse。
#define YOMKRPC_DEBUG_NODE(...) \
    YOMK_REQUEST("/YomkRpcDebugService/create_node", YomkMkPtr(DDSDebugNode, DDSDebugNode{__VA_ARGS__}))

// 登记调试主题：发现匹配的远端 DataWriter 后自动解析类型并建立订阅，消息 JSON 文本逐条投递
// output（DDSDebugOutputFunc，用户自定义，服务层不打印）；须先创建调试节点，重复登记同一主题返回错误。
// qos（可缺省尾参）：动态订阅 reader QoS DDSQosProfile；缺省全哨兵=Fast DDS reader 默认底座
// （KEEP_LAST(1)+BEST_EFFORT+VOLATILE，现状行为）；对接 ROS2 高频流建议
// DDSQosProfile::ros2DefaultSubscriber()（KEEP_LAST(10) 防丢样本）。
// 登记为发现驱动的延迟行为：主题尚未被任何发布者上线时仍返回成功，订阅待 writer 出现后
// 自动建立；登记约 3s 后仍无 writer 时由节点层工作线程打一次性等待提示（宽限窗避开
// 刚入域发现未完成时对"发布者早已在线"的误报）。失败时 m_msg 携带节点层归类的具体拒绝原因
// （debug node not created / subscriber not created / empty topic name /
// topic [...] already registered）。返回 YomkResponse。
#define YOMKRPC_DEBUG_TOPIC_PRINT(topicName, output, ...) \
    YOMK_REQUEST(                              \
        "/YomkRpcDebugService/topic_print",    \
        YomkMkPtr(DDSDebugTopic, DDSDebugTopic{topicName, output, ##__VA_ARGS__}))

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
// 送达未确认返回 eNo。qos（可缺省尾参）：临时发布 writer QoS DDSQosProfile；缺省全哨兵=
// Fast DDS writer 默认底座（KEEP_LAST(1)+RELIABLE+TRANSIENT_LOCAL，≡既有写死值，现状行为）；
// ROS2 底座建议 DDSQosProfile::ros2DefaultPublisher()。须先创建调试节点。返回 YomkResponse。
#define YOMKRPC_DEBUG_TOPIC_PUB(topicName, json, stableRounds, intervalMs, repeatIntervalMs, requiredSubscribers, maxTimes, ...)  \
    YOMK_REQUEST(                                                            \
        "/YomkRpcDebugService/topic_pub",                                    \
        YomkMkPtr(DDSDebugTopicPub, DDSDebugTopicPub{topicName, json, stableRounds, intervalMs, repeatIntervalMs, requiredSubscribers, maxTimes, ##__VA_ARGS__}))

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
// 全可变参 ([domainId[, udpDiscoverServerLocator[, tcpDiscoverServerLocator]]])：无参形态三字段
// 均经 DDSBagNode 类内缺省初始化器读环境变量（语义同 YOMKRPC_DEBUG_NODE）；显式 domainId
// 的旧调用完全兼容。domainId：DDS 域号，合法范围 [0,232]。bag 节点以唯一名
// yomkrpc_bag_<uid> 入域（uid 为 12 位随机 hex，可被 yomkrpc node list 列出。
// udpDiscoverServerLocator（可缺省尾参）：UDP 发现服务器直配聚合串，语义同 YOMKRPC_NODE 形态 3 尾参；
// tcpDiscoverServerLocator（可再缺省尾参）：TCP 发现服务器直配聚合串，语义同形态 4 第 4 参
// （bag 节点无用户名，XML 提取链仅默认档可兑底）。返回 YomkResponse。
#define YOMKRPC_BAG_NODE(...) \
    YOMK_REQUEST("/YomkRpcBagService/create_node", YomkMkPtr(DDSBagNode, DDSBagNode{__VA_ARGS__}))

// 录制主题列表（长驻阻塞：调用线程阻塞至录制收尾完成，SIGINT 经 yomk::bagRecordStop
// 无锁置位后收尾返回）。清单项支持通配模式（恰好一个 '*'：前缀 pre* / 后缀 *suf /
// 中间 pre*suf，"*" 匹配全部主题；≥2 个 '*' 返回错误），模式项启动时按发现缓存全表
// 匹配展开为实际主题集合（去重升序，与精确项合并），未命中任何主题的模式与既无发布者
// 也无订阅者的精确主题同样整体报错（m_msg 逐项列出，不建 bag）；通过校验的主题以透传方式订阅并将
// 原始 CDR 字节直写 mcap（bag 目录由 outputDir 指定——目录名/路径相对/绝对均可，父目录自动
// 多级创建，已存在报错；为空缺省当前路径下 bag_<YYYY-MM-DD_HH-MM-SS_mmm>，含 bag_0.mcap
// 与 metadata.json）。maxBagSize 单分片最大字节数（>0 时写满即滚动 bag_N.mcap 分片录制，
// 下限 1024 字节过小报错；0=不分片）。
// 须先创建 bag 节点；清单内重复/空名主题返回错误。maxBagDurationSec 单分片最大时长秒
// （0=不分片，无下限校验，与 maxBagSize 同用先到先分）。maxCacheSize 写缓存双缓冲字节数
// （0=直写；>0 回调拷贝入队、服务端独立消费线程写盘，缓存满丢弃新消息并按主题计数）。
// 落盘元信息：每分片 mcap Channel.metadata 记 type+qos 两键（qos 取首见发布者 offered 的
// Reliability/Durability 拼 "<RELIABILITY>,<DURABILITY>" 串，仅订阅者主题空串），
// metadata.json 的 topic_metadata 内 name/type/qos 并列（供 bag info 显示与 reindex 重建）。
// 成功返回 StringArray 包：首行 bag 目录名，其后每主题一行 "topic: N 条 / M 字节"统计。
// 返回 YomkResponse。
#define YOMKRPC_BAG_RECORD(topics, outputDir, maxBagSize, maxBagDurationSec, maxCacheSize) \
    YOMK_REQUEST("/YomkRpcBagService/bag_record", YomkMkPtr(DDSBagRecord, DDSBagRecord{(topics), (outputDir), (maxBagSize), (maxBagDurationSec), (maxCacheSize)}))

// 查看 bag 元信息：读 bag 目录 metadata.json 与目录递归总大小，输出对齐参考实现 ros2 bag
// info 形态的元信息文本（首行空行；Files/Bag size/Storage id/Duration/Start/End/Messages/
// Topic information 各一行，多主题/多分片续行缩进对齐；Serialization Format 固定 cdr；
// Topic information 行在 Type 后可选插 QoS 段——topic_metadata 含非空 qos 键时显示，
// 旧 bag/仅订阅者主题不显示该段）。
// bagDir 相对/绝对路径均可；纯文件读不经 bag 节点（无须 create_node）。成功返回 StringArray
// 包（每项一行，首项为空行）；路径不存在/无 metadata.json/解析失败返回错误。返回 YomkResponse。
#define YOMKRPC_BAG_INFO(bagDir) \
    YOMK_REQUEST("/YomkRpcBagService/bag_info", YomkMkPtr(DDSBagInfo, DDSBagInfo{(bagDir)}))

// 重建 bag 元信息：读 bag 目录内全部 mcap 分片（<前缀>_<编号>.mcap 按编号升序）重建
// metadata.json 并无条件覆盖写出（元信息文件缺失/损坏均可重建；对齐参考实现 ros2 bag
// reindex 行为语义）。bagDir 相对/绝对路径均可；纯文件操作不经 bag 节点（无须
// create_node）。主题 type 经各分片 Channel.metadata 的 type 键恢复（录制侧落盘的消息类型名）；
// 旧格式 bag（无该元数据的既有分片）读不到则回退空串。
// 成功返回 YomkResponse(eOk)；路径不存在/非目录/无 mcap 分片/读取失败返回错误。
#define YOMKRPC_BAG_REINDEX(bagDir) \
    YOMK_REQUEST("/YomkRpcBagService/bag_reindex", YomkMkPtr(DDSBagReindex, DDSBagReindex{(bagDir)}))

// 转换 bag：读输入 bag 目录集与输出配置文件，把输入消息按配置过滤合并写为新 bag
// （纯文件操作不经 bag 节点，无须 create_node）。inputs 输入 bag 目录清单（至少 1 个，
// 相对/绝对均可，重复目录拒绝；多输入按 logTime 全局归并，同主题跨输入类型不一致报错）。
// cfgPath 输出配置文件路径（JSON，顶层 output_bags 键为对象序列，每条目独立一组输出参数）：
// uri 输出目录必填非空（已存在报错）；topics 输出主题清单（缺省全部，元素支持恰好一个 '*'
// 的通配模式，精确项不存在/模式未命中报错）；start_time/end_time 收纳区间（缺省 0=不限，
// start>end 报错，含头含尾）两种写法：非负整数纳秒，或可读时间字符串（YYYY-MM-DD_HH-MM-SS_
// 毫秒-微秒-纳秒，本地时区，产物 metadata starting_time 的 `_format` 伴生键同款拷贝即用）；
// max_bagfile_size 单分片最大字节数（0=不分片，>0 且 <1024 报错）；
// max_bagfile_duration 单分片最大时长秒（0=不分片，与 size 同用先到先分）；未知字段忽略。
// 任一条目失败整体报错（已产生的输出目录保留不回滚）。成功返回 StringArray 包（每条目一行
// "converted <uri>: N messages / M bytes / K topics" 统计）。返回 YomkResponse。
#define YOMKRPC_BAG_CONVERT(inputs, cfgPath) \
    YOMK_REQUEST("/YomkRpcBagService/bag_convert", YomkMkPtr(DDSBagConvert, DDSBagConvert{(inputs), (cfgPath)}))

// 回放 bag：打开 bag 目录预扫 metadata.json（主题 type/qos 快照）后按录制时间轴回放入选
// 主题（长驻阻塞：调用线程阻塞至全部消息播毕或 SIGINT 经 yomk::bagPlayStop 无锁置位后
// 收尾返回；读播解耦——读线程逐条预读原始 CDR 字节入有界内存队列，播放线程按相邻时间
// 戳差值定时发布，writer 按需创建，发布 QoS 按录制快照恢复，空串/缺失回退 RELIABLE+
// VOLATILE；旧格式 bag——任一主题 type 空串——报错拒播；暂停/恢复由 yomk::bagPlayPause /
// bagPlayResume 键盘通道控制，不经协议传递）。playbackRate 回放速率倍率（>0：2.0 两倍速、
// 0.5 半倍速，对齐 ros2 --rate；<=0 报 "invalid playback rate"；运行时经
// yomk::bagPlaySetRate/bagPlayGetRate 动态调率，换率即时生效且已播进度连续）。
// topics 只回放清单内主题（空=全部，对齐 ros2 --topics 缺省语义；精确项须在 bag 清单
// 否则报 "not found in bag"；元素含一个 '*' 为通配，按 bag 内主题清单匹配展开，未命中
// 报 "matched no bag topic"、双 '*' 报错，对齐 convert topics 语义；入选集合 = 精确 ∪
// 通配命中，回显/发布实体/收尾统计只含入选主题）。
// loop 循环回放（对齐 ros2 -l/--loop：到尾即回开头无限播放——读线程每轮回卷重新预读、
// 播放线程轮边界重锚无倾泻；收尾统计为各轮累计；Ctrl+C 终止）。
// startOffset 起始偏移秒（对齐 ros2 --start-offset "Start the playback player this many
// seconds into the bag file"：从 bag 开始第 SEC 秒起播——读线程跳过 logTime 早于全场首
// 条+偏移的消息，不入队不计收尾统计；与 loop 叠加每轮保持偏移；负值报 "invalid start
// offset"；超全场时长则零消息播出正常播毕返回）。
// delay 开播延迟秒（对齐 ros2 -d/--delay，PlayOptions 注释 "Sleep before play. Negative
// durations invalid. Will delay at the beginning of each loop."：播放线程在每轮轮边界
// （首轮首条与每轮回卷轮）重锚前先睡 SEC 秒再播出；sleep 期间发布实体已建，首条发出
// 不早于实体建立+SEC，为订阅端留匹配窗口；与 loop 叠加每轮保持延迟；负值报
// "invalid delay"；0 不延迟）。
// waitForAllAckedMs 退出前等全部 RELIABLE 订阅端确认收齐毫秒（对齐 ros2
// --wait-for-all-acked MS：回放结束——自然播毕或 Ctrl+C 停止——后、销毁发布实体前，
// 对每个 RELIABLE writer 等全部订阅端 ACK，BEST_EFFORT 跳过；RELIABLE 下保证
// "播完"升级为"确认送达"，补齐大批量高速回放的退出语义；-1 不等待缺省恒等现行为，
// 0 永等（Ctrl+C 可中断），>0 超时毫秒逐主题各计；超时/中断仅告警不改变返回值，
// 对齐 ros2 WARN 继续）。
// topicRemaps 主题重映射规则（对齐 ros2 --remap/-m "list of topics to be remapped: in
// the form old_topic1:=new_topic1"："old:=new" 原始串列表，发布主题名精确匹配映射——
// old 经发布实体命名层精确比较（无通配），QoS/类型按 bag 快照照旧，--topics 过滤在
// 读取层按原名先过滤后改名；old 不在 bag 清单则规则静默无效（对齐 ros2 rcl remap
// 语义），空=不重映射恒等现行为）。
// readAheadQueueSize 预读内存队列深度（对齐 ros2 --read-ahead-queue-size N "size of
// message queue rosbag tries to hold in memory to help deterministic playback"：读侧
// 吸收磁盘 IO 抖动保证确定性回放，大→占内存但防播放延迟；<1 报 "invalid read-ahead
// queue size" 快速返回；发布端 history 上限为独立常量不随其联动）。
// qosOverridesPath QoS 覆盖文件路径（对齐 ros2 --qos-profile-overrides-path "Path to a
// yaml file defining overrides of the QoS profile for specific topics"——本仓 JSON 形态：
// 键=bag 原主题名精确匹配（与 --remap 正交，remap 只改发布主题名），值={reliability,
// durability} 两枚大写 DDS 标准拼写枚举名可独立给（只覆盖给定档，未给档保持 bag 快照
// 恢复值）；未知字段键忽略，非法枚举值报 "invalid qos string" 拒播，不在 bag 的主题键
// 静默无效（对齐 remap 先例）；三档优先级：文件命中 > bag 内 qos 快照 > 旧 bag 回退
// RELIABLE+VOLATILE；空串=不加载恒等现行为）。
// 须先创建 bag 节点（无 domainId 参数——节点模型先 YOMKRPC_BAG_NODE，同 DDSBagRecord
// 裁定）；bagDir 相对/绝对路径均可，须含 metadata.json。
// 成功返回 StringArray 包：首行 bag 目录名，其后每主题一行 "topic: N 条 / M 字节"统计。
// 返回 YomkResponse。
#define YOMKRPC_BAG_PLAY(bagDir, playbackRate, topics, loop, startOffset, delay, waitForAllAckedMs, topicRemaps, readAheadQueueSize, qosOverridesPath) \
    YOMK_REQUEST("/YomkRpcBagService/bag_play", \
        YomkMkPtr(DDSBagPlay, DDSBagPlay{(bagDir), (playbackRate), (topics), (loop), (startOffset), (delay), (waitForAllAckedMs), (topicRemaps), (readAheadQueueSize), (qosOverridesPath)}))

// 退出录制：删除 bag 节点并销毁其全部 DDS 实体（未创建时返回错误）。返回 YomkResponse。
#define YOMKRPC_BAG_DEL_NODE() YOMK_REQUEST("/YomkRpcBagService/delete_node", nullptr)
