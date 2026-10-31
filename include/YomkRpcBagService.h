#pragma once
// YomkRpcBagService 对外头：定义 YOMKRPC_BAG_* 宏（见 YomkRpcAPI.h）所打包的请求结构
// （DDSBagNode/DDSBagRecord/DDSBagInfo/DDSBagReindex/DDSBagConvert/DDSBagPlay），以及
// 服务类 YomkRpcBagService 的声明。录制能力由内部的
// FastDDSBagNode 提供（类型无关：透传原始 CDR 字节直写 mcap，schema_id=0 无 schema 通道，
// 零消息类型依赖）；回放能力同节点（读 mcap 分片重建透传 writer，按录制时间轴发布）。
#include <YomkServer/YomkAPI.h>
// 引 DDSBagNode 三字段缺省初始化器 env helper（YomkRpcDefaultDomainId 等）；平铺引号
// 三环境命中：安装树同目录/测试树平铺/主库平铺
#include "YomkRpcService.h"

#include <atomic>
#include <memory>
#include <mutex>
#include <string>
#include <vector>
class FastDDSBagNode;
using namespace yomk;

class YomkRpcBagService : public YomkService
{
public:
    YomkRpcBagService(YomkServer* server);
    virtual ~YomkRpcBagService();
    virtual int init() override;

private:
    YomkResponse getVersion(YomkPkgPtr pkg);
    YomkResponse createNode(YomkPkgPtr pkg);
    YomkResponse bagRecord(YomkPkgPtr pkg);
    YomkResponse deleteNode(YomkPkgPtr pkg);
    YomkResponse bagInfo(YomkPkgPtr pkg);
    YomkResponse bagReindex(YomkPkgPtr pkg);
    YomkResponse bagConvert(YomkPkgPtr pkg);
    YomkResponse bagPlay(YomkPkgPtr pkg);

private:
    // 单 bag 节点：一个进程至多一个实例，重复 create 须先 delete；服务析构时自动销毁节点
    // 并按节点内部顺序清理全部 DDS 实体（推荐进程退出前经 /delete_node 显式清理）。
    std::unique_ptr<FastDDSBagNode> node_;
    std::mutex mtx_;  // 串行化 node_ 的增删查改与节点入口调用
};

// bag record 录制停止标志：录制循环（FastDDSBagNode::record 内部，每 100ms 轮询）只读，
// SIGINT 处理函数（CLI onSignal）经 bagRecordStop 写——atomic 无锁 store 保证
// async-signal-safe（不可经 YOMK_REQUEST 停止：请求链路含锁与内存分配，handler 内禁用）。
// 调用方在发起录制前应先 bagRecordReset 复位，避免上次会话残留置位导致秒退；
// bag 服务 handler 录制前亦兜底复位 stop（只复位 stop 不动 paused——暂停是调用方启动意图）。
//
// bag record 暂停标志（--start-paused）：暂停态由 CLI --start-paused 或库用户
// bagRecordPause 置位，回调写路径（BagSubListener）每条消息检查——置位期间照常 take
// 排空（防恢复后旧数据涌入）但丢弃不写，bagRecordResume 后开始写入。订阅与落盘路径
// 不受影响；不经 DDSBagRecord 协议传递（CLI 与 bag 服务同进程，全局标志直读）。
namespace yomk
{
inline std::atomic<bool> g_bagRecordStop{false};
inline std::atomic<bool> g_bagRecordPaused{false};
inline void bagRecordStop()
{
    g_bagRecordStop.store(true);
}
inline void bagRecordPause()
{
    g_bagRecordPaused.store(true);
}
inline void bagRecordResume()
{
    g_bagRecordPaused.store(false);
}
inline void bagRecordReset()
{
    g_bagRecordStop.store(false);
    g_bagRecordPaused.store(false);  // 双复位：防上次会话残留暂停态（表现为何都不录）
}
}  // namespace yomk

// bag play 回放停止标志：播放线程（读线程/播放线程各阻塞处）只读，SIGINT 处理函数
//（CLI onSignal）经 bagPlayStop 写——atomic 无锁 store 保证 async-signal-safe（同
// bagRecordStop 约定：不可经 YOMK_REQUEST 停止）。调用方在发起回放前应先 bagPlayReset
// 复位，避免上次会话残留置位导致秒退；bag 服务 handler 回放前亦兜底复位 stop（只复位
// stop 不动 paused——暂停是调用方运行意图）。
//
// bag play 暂停标志：置位后播放线程冻结时间轴不再发布新消息（读线程被队列满反压自然
// 停住），恢复后重锚时间基准继续按录制节奏发布。暂停/恢复由 CLI 键盘交互键（p/SPACE
// 暂停、r 继续）或库用户 bagPlayPause/bagPlayResume 置位；不经 DDSBagPlay 协议传递
//（CLI 与 bag 服务同进程，全局标志直读）。
//
// bag play 回放速率标志：播放线程每条消息调度前读取（对齐 ros2 PlayerClock 的
// set_rate 原语——倍率缩放消息间调度间隔，2.0 两倍速、0.5 半倍速）。初值由 play 入口
// 注入（playbackRate 参数校验通过后 store），此后播放线程只认原子；运行时经
// bagPlaySetRate 动态调节（换率即时作用于后续调度，已播进度不跳变）；不经 DDSBagPlay
// 协议传递（CLI 与 bag 服务同进程，全局标志直读）。
namespace yomk
{
inline std::atomic<bool> g_bagPlayStop{false};
inline std::atomic<bool> g_bagPlayPaused{false};
inline std::atomic<double> g_bagPlayRate{1.0};  // 回放速率倍率（初值由 play 入口注入）
inline void bagPlayStop()
{
    g_bagPlayStop.store(true);
}
inline void bagPlayPause()
{
    g_bagPlayPaused.store(true);
}
inline void bagPlayResume()
{
    g_bagPlayPaused.store(false);
}
// 设置回放速率倍率（对齐 ros2 ~/set_rate 服务语义）：rate<=0 拒绝返回 false 并保持
// 原值（调用方可打印失败提示），合法值即时生效于后续消息调度
inline bool bagPlaySetRate(double rate)
{
    if (rate <= 0.0)
    {
        return false;
    }
    g_bagPlayRate.store(rate, std::memory_order_relaxed);
    return true;
}
// 查询当前回放速率倍率（对齐 ros2 ~/get_rate 服务语义）
inline double bagPlayGetRate()
{
    return g_bagPlayRate.load(std::memory_order_relaxed);
}
inline void bagPlayReset()
{
    g_bagPlayStop.store(false);
    g_bagPlayPaused.store(false);  // 双复位：防上次会话残留暂停态（表现为开播即冻住）
    g_bagPlayRate.store(1.0, std::memory_order_relaxed);  // 速率复位：防上次会话残留倍率
}
}  // namespace yomk

// create_node 请求负载。
struct DDSBagNode
{
    // DDS 域号，合法范围 [0,232]；缺省（YOMKRPC_BAG_NODE 无参形态）经类内缺省初始化器读
    // 环境变量 YOMKRPC_DDS_DOMAIN_ID（无/非法/超范围回落 0），语义同 DDSNode。
    uint32_t domainId = YomkRpcDefaultDomainId();
    // 发现服务器直配聚合串（可缺省尾参，UDP 通道）：空串=不直配；格式 "ip[:port]"，
    // 多 server 分号分隔，port 缺省 11811。非空时 participant 以 CLIENT 发现协议直配
    // （覆盖 XML discoveryServersList）；非法串（空项/空 ip/越界 port）创建失败回显。
    // 缺省（YOMKRPC_BAG_NODE 省略该尾参）经类内缺省初始化器读环境变量：总开关
    // YOMKRPC_USE_DISCOVER_SERVER=true 时取 YOMKRPC_UDP_DISCOVER_SERVERS，否则空串。
    std::string udpDiscoverServerLocator = YomkRpcDefaultUdpDiscoverServers();
    // 发现服务器直配聚合串（可缺省尾参，TCP 通道）：格式同上，port 为 server 的 TCP 监听口，
    // 非空时 participant 补注入 TCPv4 传输并以 TCPv4 locator 直配。语义同 DDSNode。
    // 缺省读 YOMKRPC_TCP_DISCOVER_SERVERS，门控同 UDP 通道。
    std::string tcpDiscoverServerLocator = YomkRpcDefaultTcpDiscoverServers();
};

// bag_record 请求负载。
struct DDSBagRecord
{
    std::vector<std::string> topics;  // 待录制主题清单（至少 1 个，清单内重复/空名拒绝）
    std::string outputDir;            // bag 目录名/路径（相对/绝对均可，父目录自动创建，
                                      // 已存在报错）；空 = 缺省时间戳名 bag_<YYYY-MM-DD_HH-MM-SS_mmm>
    uint64_t maxBagSize = 0;          // 单分片最大字节数；0=不分片（尾部缺省字段）
    uint64_t maxBagDurationSec = 0;   // 单分片最大时长秒；0=不分片（尾部缺省字段，同用先到先分）
    uint64_t maxCacheSize = 0;        // 写缓存双缓冲字节数；0=直写（尾部缺省字段，CLI 默认传 100MiB）
};

// bag_info 请求负载。
struct DDSBagInfo
{
    std::string bagDir;  // bag 目录名/路径（相对/绝对均可，须含 metadata.json）
};

// bag_reindex 请求负载。
struct DDSBagReindex
{
    std::string bagDir;  // bag 目录名/路径（相对/绝对均可；metadata.json 缺失/损坏均可重建）
};

// bag_convert 请求负载。
struct DDSBagConvert
{
    std::vector<std::string> inputs;  // 输入 bag 目录清单（至少 1 个，相对/绝对均可，
                                      // 重复目录拒绝；多输入按 logTime 全局归并）
    std::string cfgPath;              // 输出配置文件路径（JSON，顶层 output_bags 键为
                                      // 对象序列，每条目独立一组输出参数）
};

// bag play 预读内存队列深度（--read-ahead-queue-size N，对齐 ros2 同名选项：读侧吸收
// 磁盘 IO 抖动保证确定性回放，大→占内存但防播放延迟；<1 拒播，writer QoS 不随其联动；
// 缺省对齐参考实现同款默认 1000）
constexpr size_t kBagPlayReadAheadQueueSize = 1000;

// bag_play 请求负载。
struct DDSBagPlay
{
    std::string bagDir;  // bag 目录名/路径（相对/绝对均可，须含 metadata.json）
    double playbackRate = 1.0;  // 回放速率倍率（>0；2.0 两倍速，0.5 半倍速，对齐 ros2 --rate；
                                // 尾部缺省字段）
    std::vector<std::string> topics;  // 只回放清单内主题（空=全部，对齐 ros2 --topics 缺省；
                                      // 精确项须在 bag 清单否则报错；元素含一个 '*' 为通配，
                                      // 按 bag 内主题清单匹配展开，对齐 convert topics 语义）
    bool loop = false;  // 循环回放（对齐 ros2 -l/--loop：到尾即回开头无限播放，Ctrl+C 终止；
                        // 尾部缺省字段）
    double startOffset = 0.0;  // 起始偏移秒（对齐 ros2 --start-offset：从 bag 开始第 SEC 秒
                               // 起播，读侧跳过前段消息不入队不计收尾统计；负值非法；
                               // 尾部缺省字段）
    double delay = 0.0;  // 开播延迟秒（对齐 ros2 -d/--delay "Will delay at the beginning of
                         // each loop"：每次开播含每轮回卷前先睡 SEC 秒等订阅端匹配就绪；
                         // 负值非法；尾部缺省字段）
    int64_t waitForAllAckedMs = -1;  // 退出前等全部 RELIABLE 订阅端确认收齐毫秒（对齐 ros2
                                     // --wait-for-all-acked MS：-1 不等待（缺省），0 永等，
                                     // >0 超时毫秒逐主题各计；尾部缺省字段）
    std::vector<std::string> topicRemaps;  // 主题重映射规则（"old:=new" 原始串，对齐 ros2
                                           // --remap old:=new ...：发布主题名精确映射，QoS/类型
                                           // 按 bag 快照照旧，--topics 过滤按原名；尾部缺省字段）
    size_t readAheadQueueSize = kBagPlayReadAheadQueueSize;  // 预读内存队列深度（尾部缺省
                                           // 字段；深度语义见常量注释）
    std::string qosOverridesPath;  // QoS 覆盖文件路径（JSON，键=bag 原主题名，值={reliability,
                                   // durability} 枚举名可独立给——文件命中>快照>回退三档优先
                                   // 级；空=缺省不加载；尾部缺省字段）
};

// clang-format off
// YomkMsg 是 YomkServer 第三方宏，cppcheck 未 --library 配置识别（unknownMacro 属工具配置需求，非自有源码缺陷）
// cppcheck-suppress unknownMacro
YomkMsg(DDSBagNode, DDSBagNode, msg)
YomkMsg(DDSBagRecord, DDSBagRecord, msg)
YomkMsg(DDSBagInfo, DDSBagInfo, msg)
YomkMsg(DDSBagReindex, DDSBagReindex, msg)
YomkMsg(DDSBagConvert, DDSBagConvert, msg)
YomkMsg(DDSBagPlay, DDSBagPlay, msg)
