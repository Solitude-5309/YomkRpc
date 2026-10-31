#ifndef FASTDDSBAGNODE_H
#define FASTDDSBAGNODE_H

// 引 ParticipantQosSpec（XML 配置通道 + serverLocator 直配 spec；缺省实参需完整类型，
// 自身引公共头与 fastdds）
#include "FastDDSXmlHelper.h"

#include <cstdint>
#include <fastdds/dds/builtin/topic/PublicationBuiltinTopicData.hpp>
#include <fastdds/dds/builtin/topic/SubscriptionBuiltinTopicData.hpp>
#include <fastdds/dds/domain/DomainParticipant.hpp>
#include <fastdds/dds/subscriber/DataReader.hpp>
#include <fastdds/dds/subscriber/Subscriber.hpp>
#include <fastdds/dds/topic/Topic.hpp>
#include <fastdds/dds/topic/TypeSupport.hpp>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

// bag 录制节点：加入域后经 DDS 发现校验待录制主题（既无发布者也无订阅者视为输入有误
// 整体报错），通过校验的主题以透传 TopicDataType（不解析数据，原始 CDR 字节含
// encapsulation header 原样落盘 mcap：schema_id=0 无 schema 通道、encoding="cdr"、
// thirdparty/mcap v2.1.3，实现仅在本节点 .cpp 单译元编译）建立订阅，收尾删除全部 reader
// 后关闭 writer 补写 summary 并生成 metadata.json（顶层平铺元信息 + _format 可读时间伴生
// 键）。停止/暂停标志由 YomkRpcBagService 提供（yomk::g_bagRecordStop/g_bagRecordPaused）。
// 回放（play）为反向链路：预扫 metadata.json（主题 type/qos 快照）后按读播解耦架构发布
// ——读线程经 mcap 索引逐条预读原始 CDR 字节入有界内存队列，播放线程按录制时间轴还原
// 节奏，writer 按 bag 内快照按需创建（缺键回退 RELIABLE+VOLATILE）；停止/暂停标志
// yomk::g_bagPlayStop/g_bagPlayPaused（暂停冻结时间轴，恢复后重锚继续）。
// 线程模型：发现回调仅缓存发现信息（seenMtx_ 叶子锁，锁序倒置防护同 FastDDSDebugNode）；
// 数据回调两路：maxCacheSize=0 同步直写（DDS 接收线程逐条写盘，payloadMtx_ 串行多
// reader 并发写同一 McapWriter），>0 写缓存双缓冲（回调深拷贝入队即返回、独立消费线程
// 写盘——仅把写盘 IO 挪出接收线程，不改变消息内容与时间戳口径）。公开方法以 mtx_ 串行
// 化；record/play 均为长驻阻塞调用（record 成功返回后节点定格须重建，play 播完实体全清
// 可再次调用）。
class FastDDSBagNode
{
    // 参与者监听器：writer/reader 发现回调 → onWriterDiscovered/onReaderDiscovered
    class BagParticipantListener;
    // 数据监听器：take 透传样本 → mcap write
    class BagSubListener;
    // 已建立订阅登记项。
    struct BagSub
    {
        eprosima::fastdds::dds::TypeSupport type;  // 持有透传 TopicDataType（shared_ptr 所有权，按类型名共享）
        eprosima::fastdds::dds::TopicDataType* topicType =
            nullptr;  // 指向 type 所辖同一对象，仅供 create/delete_data，不单独持有所有权
        void* data = nullptr;  // create_data() 创建的 Blob 接收缓冲（收尾/回滚经 topicType->delete_data 释放）
        eprosima::fastdds::dds::Topic* topic = nullptr;  // 订阅主题，析构时 delete_topic
        eprosima::fastdds::dds::DataReader* reader = nullptr;
        std::unique_ptr<BagSubListener> listener;  // reader 的数据监听器，引用 data 缓冲并持写盘上下文
        std::string qos;  // 落盘 QoS 串 "<RELIABILITY>,<DURABILITY>"（首见 writer offered；
                          // 仅订阅者主题空串）——Channel.metadata 与 metadata.json 同源单次取值
    };

public:
    FastDDSBagNode();
    ~FastDDSBagNode();
    FastDDSBagNode(const FastDDSBagNode&) = delete;
    FastDDSBagNode& operator=(const FastDDSBagNode&) = delete;

public:
    // record 校验收敛默认参数：连续 15 次快照不变即收敛 × 200ms 轮询（不足最短窗自动提升）
    static constexpr uint32_t kDefaultStableRounds = 15;
    static constexpr uint32_t kDefaultIntervalMs = 200;
    // 校验最短总窗（毫秒）：rounds*intervalMs 不足时向上提升 rounds（防 SPDP 误判）。SPDP
    // 参与者公告周期默认 3s，bag 节点与远端端点近同时上线时，校验窗必须覆盖 SPDP+EDP
    // 一整轮互通，否则"刚上线的发布者"会被误判为无端点——取 2 倍公告周期
    static constexpr uint32_t kMinValidateWindowMs = 6000;
    // play 预读队列缺省深度（--read-ahead-queue-size 缺省，对齐参考实现同款默认；读侧
    // PlayQueue 背压边界，writer QoS 独立常量不随其联动）
    static constexpr size_t kPlayReadAheadQueueSizeDefault = 1000;
    // 单主题录制统计（record 出参单元）。
    struct BagTopicStat
    {
        std::string topic;     // 主题名（用户输入原样）
        std::string type;      // 发现的远端数据类型名（原始 DDS 类型名，无转换）
        uint64_t count = 0;    // 录制消息条数
        uint64_t bytes = 0;    // 录制字节数（CDR 全量，含 encapsulation header）
    };
    // 设置 DDS 域并创建 participant（挂发现监听）与 subscriber；仅可成功一次，失败/重复调用返回 false。
    // spec/err 语义同 FastDDSNode::setDomainId（XML 三级提取链——本节点无用户名，按名 profile
    // 无意义仅默认档可兜底；serverLocator 直配强制 CLIENT，节点唯一名在提取后置入）。
    bool setDomainId(uint32_t domainId, const yomkrpc_xml::ParticipantQosSpec& spec =
            yomkrpc_xml::ParticipantQosSpec{}, std::string* err = nullptr);
    // 录制主题列表（长驻阻塞：调用线程阻塞至 yomk::g_bagRecordStop 置位并完成收尾）。
    // ①启动校验——轮询发现缓存快照至收敛（0 值钳制默认 15 次/200ms，总窗不足
    //   kMinValidateWindowMs 自动提升防 SPDP 误判；清单项支持单 '*' 通配，模式项按发现
    //   缓存全表展开为实际录制清单）；任一精确主题既无发布者也无订阅者、或模式未命中
    //   即整体报错 false（error 逐项列出，不建目录不产生任何文件；仅订阅者主题同样通过）。
    // ②建 bag 目录（outputDir 空=缺省时间戳名 bag_<YYYY-MM-DD_HH-MM-SS_mmm>；已存在报错）
    //   + mcap writer；maxBagSize/maxBagDurationSec 先到先分片，maxCacheSize>0 走写缓存
    //   双缓冲（缓存满丢弃新消息并按主题计数，停止先排空再关）。
    // ③逐主题注册透传类型并建订阅（reader QoS 跟随远端 writer offered）；每主题一个
    //   mcap Channel（schema_id=0，metadata 落 type+qos）；成功即输出 recording topic= 行。
    // ④每 100ms 轮询停止标志；g_bagRecordPaused 置位期间照常 take 但丢弃（不计入时间统计）。
    // ⑤停止序列：delete 全部 reader → writer.close 补写 summary → 写 metadata.json →
    //   回填 stats。
    // 成功返回 true 且节点定格（再次 record 报错须重建）；失败（未入域/重复/输入有误/资源
    // 创建失败）返回 false 并经 error 出参回填原因，未定格可修正输入后重试。
    bool record(const std::vector<std::string>& topics, std::vector<BagTopicStat>& stats,
            std::string* error = nullptr,
            uint32_t stableRounds = kDefaultStableRounds,
            uint32_t intervalMs = kDefaultIntervalMs,
            const std::string& outputDir = "",
            uint64_t maxBagSize = 0,
            uint64_t maxBagDurationSec = 0,
            uint64_t maxCacheSize = 0);
    // 最近一次成功录制的 bag 目录名（当前路径相对名，如 bag_2026-01-01_12-00-00_000）；未录制过为空。
    const std::string& bagDir() const
    {
        return bagDir_;
    }
    // 回放 bag 目录（长驻阻塞：阻塞至播毕或停止标志置位；不引入定格——播完发布实体全清，
    // 可再次调用）。①预扫 metadata.json（name/type/qos 三键快照 + 分片收集；type 空串的
    // 旧格式 bag 拒播，qos 空/缺失回退 RELIABLE+VOLATILE；失败即整体返回 false 不建任何
    // DDS 实体）；②读线程逐条预读原始 CDR 入有界队列（满则背压，loop 到尾回卷重预读）；
    // ③播放线程 writer 按需创建（history depth=预读深度支持 TRANSIENT_LOCAL 补发），按
    // 相邻时间戳差值/速率倍率绝对锚定还原录制节奏，暂停置位冻结时间轴；④停止序列：播放
    // 线程 → 读线程 → join → 等 ACK → 删发布实体 → 回填 stats。读流中途失败记错误推 EOS，
    // 已读消息照常播完后返回 false。参数语义（引号为服务端回显错误串原文）：
    // playbackRate 回放速率倍率（对齐 ros2 --rate）：>0 生效（2.0 两倍速、0.5 半倍速），
    // <=0 报 "invalid playback rate"。
    // topics 只回放清单内主题（对齐 ros2 --topics）：空=全部；精确项不在 bag 报
    // "not found in bag"；单 '*' 模式按 bag 清单展开（对齐 convert topics 语义），未命中
    // 报 "matched no bag topic"；入选集合=精确∪通配命中（去重升序）。
    // loop 循环回放（对齐 ros2 -l/--loop）：到尾即回开头无限播放，轮边界重锚无倾泻无跳变；
    // 收尾统计各轮累计；与 playbackRate/topics 正交叠加。
    // startOffset 起始偏移秒（对齐 ros2 --start-offset）：读侧跳过早于全场首条（过滤前
    // 基准）+偏移的消息（bag 级偏移不受 topics 过滤影响，与 loop 叠加每轮保持）；负值报
    // "invalid start offset"。
    // delay 开播延迟秒（对齐 ros2 -d/--delay）：播放侧每轮轮边界前分段轮询睡 SEC（读侧睡
    // 不挡播出起点；分段轮询保证大延迟下 Ctrl+C 即退）；负值报 "invalid delay"。
    // waitForAllAckedMs 播毕/停止后对 RELIABLE writer 分片等 ACK（对齐 ros2
    // --wait-for-all-acked）：0 永等（片间可中断）、>0 超时逐主题各计、超时仅告警；
    // -1 不等待（缺省）。
    // topicRemaps 主题重映射（"old:=new"，对齐 ros2 --remap）：发布主题名精确映射，QoS/
    // 类型按 bag 快照照旧，--topics 按原名先过滤后改名；格式/重复报 "invalid remap rule"/
    // "duplicate remap rule"；old 不在 bag 清单静默无效；回显/统计显示 remap 后新名。
    // readAheadQueueSize 预读内存队列深度（对齐 ros2 同名选项：读侧吸收磁盘 IO 抖动）；
    // <1 报 "invalid read-ahead queue size"；writer history 上限 kPlayWriterHistorySize
    // 独立不联动；队列空致播放持续等待打一次性 starved 告警。
    // qosOverridesPath QoS 覆盖文件（JSON，键=bag 原主题名，值={reliability,durability}
    // 可独立给；三档优先级：文件>bag 快照>回退，命中回填生效口径；非法枚举报
    // "invalid qos string"，不在 bag 清单的主题键静默无效）；空串=不加载。
    bool play(const std::string& bagDir, std::vector<BagTopicStat>& stats,
            std::string* error = nullptr, double playbackRate = 1.0,
            const std::vector<std::string>& topics = {}, bool loop = false,
            double startOffset = 0.0, double delay = 0.0,
            int64_t waitForAllAckedMs = -1,
            const std::vector<std::string>& topicRemaps = {},
            size_t readAheadQueueSize = kPlayReadAheadQueueSizeDefault,
            const std::string& qosOverridesPath = {});
    // 读 bag 目录 metadata.json 组装 info 文本（纯文件操作 DDS-free，不经节点实例；输出
    // 形态对齐 ros2 bag info：首行空行、标签列宽 19、多主题/多分片续行缩进 19 空格、Bag
    // size 为目录递归总大小的人类可读换算、时间含 9 位纳秒小数；主题行 Type 后可选 QoS 段
    // ——旧 bag/仅订阅者主题不显示该段）。outLines 每项一行（不含换行符，首项为空行）；
    // 失败返回 false 并经 error 出参回填原因（路径不存在/无 metadata.json/解析失败）。
    // bagDir 相对/绝对路径均可。
    static bool bagInfoText(const std::string& bagDir, std::vector<std::string>& outLines,
            std::string* error = nullptr);
    // 从 bag 目录内 mcap 分片文件重建 metadata.json（纯文件操作 DDS-free，不经节点实例；
    // 对齐 ros2 bag reindex：<前缀>_<编号>.mcap 按编号升序收集、逐文件读 mcap summary
    // 统计、主题名升序合并、无条件覆盖写出）。metadata.json 缺失/损坏均可重建；bagDir
    // 相对/绝对路径均可。失败返回 false 并经 error 出参回填原因。
    static bool bagReindex(const std::string& bagDir, std::string* error = nullptr);
    // 将输入 bag 目录集按输出配置文件转换合并为新 bag（纯文件操作 DDS-free，不经节点实
    // 例；输出配置语义对齐 ros2 bag convert：顶层 output_bags 键为对象序列，每条目独立
    // uri/topics/start_time/end_time/max_bagfile_size/max_bagfile_duration，未知字段宽容
    // 忽略；start_time/end_time 支持非负整数纳秒或可读时间串——本地时区，与产物 metadata
    // starting_time 的 `_format` 伴生键同款，拷贝即用）。逐输入收集分片预扫主题与类型名
    // （同主题跨输入类型冲突报错；入选主题无类型名的旧格式 bag 拒转），逐条目重建游标
    // k-way 归并（logTime 全局升序）过滤写盘，写前分片检查同 record 口径（size/duration
    // 先到先分），输出 metadata.json（reindex 同构）。inputs 至少 1 个（重复目录拒绝；
    // 多输入按 logTime 全局归并），cfgPath 为配置文件路径。outLines 每条目一行
    // "converted <uri>: N messages / M bytes / K topics"；任一条目失败返回 false 并经
    // error 回填原因（已产生的输出目录保留不回滚）。
    static bool bagConvert(const std::vector<std::string>& inputs, const std::string& cfgPath,
            std::vector<std::string>& outLines, std::string* error = nullptr);

private:
    // 发现线程回调入口（BagParticipantListener 转发）：新见 writer/reader 追加进发现缓存。
    // 仅拿 seenMtx_ 叶子锁：回调在 Fast DDS 持有 PDP/EDP 内部锁的临界区内被调用，拿 mtx_ 会
    // 与 record（持 mtx_ 建 reader 内部等 PDP 锁）形成锁序倒置死锁（防护同 FastDDSDebugNode）。
    bool onWriterDiscovered(const std::string& topicName,
                            const eprosima::fastdds::rtps::PublicationBuiltinTopicData& info);
    bool onReaderDiscovered(const std::string& topicName,
                            const eprosima::fastdds::rtps::SubscriptionBuiltinTopicData& info);

private:
    eprosima::fastdds::dds::DomainParticipant* participant_ = nullptr;
    eprosima::fastdds::dds::Subscriber* subscriber_ = nullptr;  // 订阅者，拥有全部 reader
    std::unique_ptr<BagParticipantListener> listener_;          // participant 的发现监听器
    // 已发现 writer/reader 缓存（topic → 发现信息列表，同主题每端点各占一项，首项供类型名
    // 与 QoS 读取；发现事件不重发，缓存单调累积——录制校验只看有无，不处理端点离线）
    std::map<std::string, std::vector<eprosima::fastdds::rtps::PublicationBuiltinTopicData>> seen_;
    std::map<std::string, std::vector<eprosima::fastdds::rtps::SubscriptionBuiltinTopicData>> seenReaders_;
    std::map<std::string, BagSub> subs_;  // 已建立订阅
    std::mutex mtx_;  // 串行化公开方法（record 长驻阻塞期间本节点全部入口排队）
    // 发现缓存专用叶子锁（保护 seen_/seenReaders_）：锁序固定 mtx_ → seenMtx_，seenMtx_
    // 永不反向嵌套（锁序倒置防护同 FastDDSDebugNode::seenMtx_）
    std::mutex seenMtx_;
    bool recorded_ = false;   // 录制定格标志（成功收尾后置位；mtx_ 保护）
    std::string bagDir_;      // 最近一次录制的 bag 目录名（recorded_ 置位后有效）
};

#endif  // FASTDDSBAGNODE_H
