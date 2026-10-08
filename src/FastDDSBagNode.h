#ifndef FASTDDSBAGNODE_H
#define FASTDDSBAGNODE_H

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

// bag 录制节点：加入域后经 DDS 发现机制校验待录制主题（既无发布者也无订阅者视为输入有误
// 整体报错），通过校验的主题以透传 TopicDataType（不解析数据，原始 CDR 字节含 encapsulation
// header 原样取出）建立订阅，落盘 mcap 文件（schema_id=0 无 schema 通道，encoding="cdr"，
// 存储层 thirdparty/mcap v2.1.3，实现仅在本节点 .cpp 单译元编译）；Ctrl+C 停止标志由
// YomkRpcBagService 提供（yomk::g_bagRecordStop），收尾删除全部 reader 后关闭 writer 补写
// summary 并生成 metadata.json（顶层平铺元信息 + _format 可读时间伴生键）。
// 线程模型：发现回调仅缓存发现信息（seenMtx_ 叶子锁，锁序倒置防护同 FastDDSDebugNode）；
// 数据回调两路：maxCacheSize=0 同步直写（DDS 接收线程逐条写盘，payloadMtx_ 串行多 reader
// 并发写同一 McapWriter）；>0 写缓存双缓冲（回调深拷贝入队后即返回，缓存满丢弃新消息并
// 按主题计数、独立消费线程逐条写盘——仅把写盘 IO 挪出接收线程，不改变消息内容与时间戳
// 口径；停止先排空缓存再关 writer）。公开方法以 mtx_ 串行化；record 为长驻阻塞调用（调用
// 线程阻塞至录制收尾完成），成功返回后节点定格（再次 record 报错，须重建节点）。
class FastDDSBagNode
{
    class BagParticipantListener;  // 参与者监听器：writer/reader 发现回调 → onWriterDiscovered/onReaderDiscovered
    class BagSubListener;          // 数据监听器：take 透传样本 → mcap write
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
    // 单主题录制统计（record 出参单元）。
    struct BagTopicStat
    {
        std::string topic;     // 主题名（用户输入原样）
        std::string type;      // 发现的远端数据类型名（原始 DDS 类型名，无转换）
        uint64_t count = 0;    // 录制消息条数
        uint64_t bytes = 0;    // 录制字节数（CDR 全量，含 encapsulation header）
    };
    // 设置 DDS 域并创建 participant（挂发现监听）与 subscriber；仅可成功一次，失败/重复调用返回 false。
    bool setDomainId(uint32_t domainId);
    // 录制主题列表（长驻阻塞：调用线程阻塞至 Ctrl+C 停止标志置位并完成收尾）：
    // ①启动校验——轮询发现缓存快照（各主题 writer/reader 有无），连续 stableRounds 次不变或
    //   全部主题有端点即收敛（总窗不足 kMinValidateWindowMs 自动提升 rounds；0 值钳制默认
    //   15 次/200ms）；清单项支持通配模式（恰好一个 '*'：前缀 pre* / 后缀 *suf / 中间
    //   pre*suf，"*" 匹配全部主题；≥2 个 '*' 输入有误报错），模式项按发现缓存全表匹配
    //   展开为实际主题集合（去重升序，与精确项合并，展开于启动校验时定型）；收敛后逐项
    //   判定，任一精确主题既无发布者也无订阅者、或任一模式未命中任何主题即整体报错返回
    //   false（error 逐项列出，不建 bag 目录，不产生任何文件）；仅有订阅者的主题同样通过
    //   （类型名取自订阅端点公告，建 reader 等发布者匹配后自动开始录流）。
    // ②建 bag 目录（outputDir 非空用指定目录名/路径——相对/绝对均可、父目录自动多级创建、
    //   已存在即报错；为空缺省当前路径下 bag_<YYYY-MM-DD_HH-MM-SS_mmm>，毫秒精度防同秒重名）
    //   + mcap writer（bag_0.mcap）；maxBagSize > 0 时单分片写满即滚动 bag_N.mcap 分片录制
    //   （0=不分片；下限 1024 字节，过小输入有误报错）；maxBagDurationSec > 0 时单分片
    //   时长达限即滚动 bag_N.mcap（0=不分片，无下限校验；与 maxBagSize 同用先到先分；
    //   暂停期消息不计入分片时长）；maxCacheSize > 0 时写缓存双缓冲（回调拷贝入队、独立
    //   消费线程写盘、缓存满丢弃新消息并按主题计数、停止先排空缓存再关闭；0=每条直写）。
    // ③逐主题注册透传类型并建立订阅（reader QoS 的 Reliability/Durability 跟随远端 writer
    //   offered 值——requested ≤ offered 恒成立；仅有订阅者的主题无 offered 可跟随，用默认
    //   QoS 由用户保证与后续发布者兼容）；每主题一个 mcap Channel（schema_id=0，encoding="cdr"）；
    //   每主题建订成功即输出一行 recording topic=<名> type=<类型>（stdout，通配展开后的实际
    //   录制清单由此可见——CLI 启动行仅显示清单项数）。
    // ④等待 yomk::g_bagRecordStop（每 100ms 轮询，SIGINT 经 bagRecordStop 置位）；
    //   g_bagRecordPaused 置位期间（--start-paused / bagRecordPause）回调照常 take 但丢弃，
    //   bagRecordResume 后开始写入（订阅与落盘路径照常，暂停期不计入 starting_time/duration）。
    // ⑤停止序列：delete 全部 reader（杜绝并发回调）→ writer.close()（补写 summary 索引）→
    //   写 metadata.json（storage_identifier=mcap，relative_file_paths 列全部分片，起始时间/
    //   时长/条数统计保持全局累计）→ 回填 stats。
    // 成功返回 true 且节点定格（再次 record 报错须重建）；失败（未入域/重复/输入有误/资源创建
    // 失败）返回 false 并经 error 出参回填原因，未定格可修正输入后重试。
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
    // 读 bag 目录 metadata.json 组装 info 文本（纯文件操作 DDS-free，不经节点实例；对齐参考
    // 实现 ros2 bag info 输出形态：首行空行、标签列宽 19、多主题/多分片续行缩进 19 空格、
    // Bag size 为目录递归总大小的人类可读换算、时间含 9 位纳秒小数）。outLines 每项一行
    // （不含换行符，首项为空行）；失败返回 false 并经 error 出参回填原因（路径不存在/
    // 无 metadata.json/解析失败）。bagDir 相对/绝对路径均可。
    static bool bagInfoText(const std::string& bagDir, std::vector<std::string>& outLines,
            std::string* error = nullptr);
    // 从 bag 目录内 mcap 分片文件重建 metadata.json（纯文件操作 DDS-free，不经节点实例；
    // 对齐参考实现 ros2 bag reindex 行为语义：收集 <前缀>_<编号>.mcap 按编号升序、逐文件
    // 读 mcap summary 统计、主题名升序合并、无条件覆盖写出）。metadata.json 缺失/损坏均可
    // 重建；bagDir 相对/绝对路径均可。失败返回 false 并经 error 出参回填原因。
    static bool bagReindex(const std::string& bagDir, std::string* error = nullptr);
    // 将输入 bag 目录集按输出配置文件转换合并为新 bag（纯文件操作 DDS-free，不经节点实例；
    // 对齐参考实现 ros2 bag convert 的输出配置语义：cfg 顶层 output_bags 键为对象序列，每
    // 条目独立 uri/topics/start_time/end_time/max_bagfile_size/max_bagfile_duration，未知
    // 字段宽容忽略；start_time/end_time 收纳区间支持两种写法——非负整数纳秒，或可读时间
    // 字符串 YYYY-MM-DD_HH-MM-SS_mmm-uuu-nnn（本地时区，与产物 metadata starting_time 的
    // `_format` 伴生键同款，拷贝即用））。逐输入收集分片预扫主题与类型名（同主题跨输入类型
    // 冲突报错；入选主题无类型名的旧格式 bag 拒转），逐条目重建游标 k-way 归并（logTime
    // 全局升序）过滤写盘，
    // 写前分片检查同 record 口径（size/duration 先到先分），输出 metadata.json（reindex
    // 同构组装）。inputs 至少 1 个（重复目录拒绝；多输入按 logTime 全局归并），cfgPath 为
    // 配置文件路径。outLines 每条目一行 "converted <uri>: N messages / M bytes / K topics"；
    // 任一条目失败返回 false 并经 error 回填原因（已产生的输出目录保留不回滚）。
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
