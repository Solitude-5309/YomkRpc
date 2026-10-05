// FastDDSBagNode 实现：发现校验 → 透传订阅 → mcap 同步直写 → Ctrl+C 收尾（详见头注释）。
//
// 透传类型设计：PassthroughPubSubType 不携带 TypeObject（不覆写 register_type_object_representation），
// 发现层仅按主题名 + 类型名字符串匹配，与远端类型定义零耦合；deserialize 把接收 payload
// （含 4 字节 encapsulation header）原样存入 Blob，录制即字节搬运，对任意 DDS 消息类型通用。
//
// 线程模型（防锁序倒置，同 FastDDSDebugNode）：发现回调仅拿 seenMtx_ 叶子锁；record 持 mtx_
// 建 reader（内部等 PDP/EDP 锁）→ 锁序固定 mtx_ → seenMtx_；数据回调在 DDS 接收线程仅拿
// payloadMtx_（McapWriter 非线程安全，多 reader 并发写须串行），不触 mtx_。
#include "FastDDSBagNode.h"

#include "YomkRpcBagService.h" // yomk::g_bagRecordStop（录制停止标志，API 头内联定义）

#define MCAP_IMPLEMENTATION  // mcap header-only：实现随本 TU 编译（全库唯一单译元）
#include <mcap/reader.hpp>   // 读回侧实现同译元编译（读回断言经链接本库取实现）
#include <mcap/writer.hpp>

#include <nlohmann/json.hpp> // metadata.json 组装（vendored 于 thirdparty/nlohmann_json）

#include <algorithm>
#include <array>
#include <atomic>
#include <charconv>
#include <chrono>
#include <condition_variable>
#include <cstring>
#include <ctime>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <set>
#include <sstream>
#include <thread>
#include <unordered_map>
#include <utility>

#include <fastdds/dds/core/policy/QosPolicies.hpp>
#include <fastdds/dds/core/status/StatusMask.hpp>
#include <fastdds/dds/domain/DomainParticipantFactory.hpp>
#include <fastdds/dds/domain/DomainParticipantListener.hpp>
#include <fastdds/dds/subscriber/DataReaderListener.hpp>
#include <fastdds/dds/subscriber/SampleInfo.hpp>
#include <fastdds/dds/subscriber/Subscriber.hpp>
#include <fastdds/rtps/reader/ReaderDiscoveryStatus.hpp>
#include <fastdds/rtps/writer/WriterDiscoveryStatus.hpp>

using namespace eprosima::fastdds::dds;

namespace
{
constexpr uint32_t kEncapsulationBytes = 4;   // CDR encapsulation header 长度
constexpr uint32_t kMaxPayloadBytes = 65536;  // serialize 预判上限：64KB 覆盖常规消息
constexpr size_t kTimeBufBytes = 32;          // 时间戳 strftime 格式化缓冲（目录名与 metadata 共用）
constexpr uint32_t kRecordPollMs = 100;       // 录制循环停止标志轮询间隔
constexpr int kMsDigits = 3;                  // 定宽 3 位段位数（毫秒/微秒/纳秒）
constexpr int kClockDigits = 2;               // 定宽 2 位段位数（时/分/秒）
constexpr uint32_t kBagMetadataVersion = 1;   // yomkrpc 元信息格式自有版本号（1 起步，与参考来源 rosbag2 的 v5 无关）
constexpr uint64_t kMinSplitFileSize = 1024;  // -b 分片下限（对齐 rosbag2 mcap 插件 kMinimumSplitFileSize）
constexpr uint64_t kNsPerUs = 1000;                     // 纳秒每微秒
constexpr uint64_t kNsPerMs = kNsPerUs * kNsPerUs;      // 纳秒每毫秒
constexpr uint64_t kNsPerSecond = kNsPerMs * kNsPerUs;  // 纳秒每秒
constexpr uint64_t kNsPerMinute = 60 * kNsPerSecond;    // 纳秒每分
constexpr uint64_t kNsPerHour = 60 * kNsPerMinute;      // 纳秒每小时

// ---- metadata 可读时间伴生键（_format）的格式化 ----
// 毫秒-微秒-纳秒三段 → "mmm-uuu-nnn"（各 3 位补零，'-' 分隔）
std::string formatSubSecondNs(uint64_t subNs)
{
    const uint64_t ms = subNs / kNsPerMs;
    const uint64_t us = (subNs % kNsPerMs) / kNsPerUs;
    const uint64_t ns = subNs % kNsPerUs;
    std::ostringstream out;
    out << std::setw(kMsDigits) << std::setfill('0') << ms << '-'
        << std::setw(kMsDigits) << std::setfill('0') << us << '-'
        << std::setw(kMsDigits) << std::setfill('0') << ns;
    return out.str();
}

// 纳秒时长 → "HH-MM-SS_mmm-uuu-nnn"（时-分-秒_毫秒-微秒-纳秒）
std::string formatDurationNs(uint64_t totalNs)
{
    const uint64_t hours = totalNs / kNsPerHour;
    const uint64_t minutes = (totalNs % kNsPerHour) / kNsPerMinute;
    const uint64_t seconds = (totalNs % kNsPerMinute) / kNsPerSecond;
    std::ostringstream out;
    out << std::setw(kClockDigits) << std::setfill('0') << hours << '-'
        << std::setw(kClockDigits) << std::setfill('0') << minutes << '-'
        << std::setw(kClockDigits) << std::setfill('0') << seconds << '_'
        << formatSubSecondNs(totalNs % kNsPerSecond);
    return out.str();
}

// epoch 纳秒 → "YYYY-MM-DD_HH-MM-SS_mmm-uuu-nnn"（本地时区）
std::string formatEpochNs(uint64_t epochNs)
{
    const std::time_t secs = static_cast<std::time_t>(epochNs / kNsPerSecond);
    std::tm localNow{};
    localtime_r(&secs, &localNow);
    std::array<char, kTimeBufBytes> timeBuf{};
    std::strftime(timeBuf.data(), timeBuf.size(), "%Y-%m-%d_%H-%M-%S", &localNow);
    std::ostringstream out;
    out << timeBuf.data() << '_' << formatSubSecondNs(epochNs % kNsPerSecond);
    return out.str();
}

// ---- bag info 输出格式化（bagInfoText 用，对齐参考实现 ros2 bag info 输出形态）----
constexpr int kNsDigits = 9;             // 时间小数定宽位数（纳秒）
constexpr double kBytesPerUnit = 1024.0; // 人类可读大小换算进制
constexpr int kSizeUnitCount = 5;        // B/KiB/MiB/GiB/TiB 单位数（索引上限 4）
constexpr int kInfoLabelWidth = 19;      // info 标签列宽（"Topic information: " 宽度）

// 字节数 → 人类可读大小：1024 进制逐级换算，B 零小数、其余 1 位小数
std::string formatBagFileSize(uint64_t bytes)
{
    double size = static_cast<double>(bytes);
    static const std::array<const char*, kSizeUnitCount> kSizeUnits = {"B", "KiB", "MiB", "GiB", "TiB"};
    int index = 0;
    while (size >= kBytesPerUnit && index < kSizeUnitCount - 1)
    {
        size /= kBytesPerUnit;
        ++index;
    }
    std::ostringstream out;
    out << std::fixed << std::setprecision(index == 0 ? 0 : 1) << size << " "
        << kSizeUnits.at(static_cast<std::size_t>(index));
    return out.str();
}

// epoch 纳秒 → "Sep 30 2026 19:06:10.123456789 (1789652770.123456789)"（本地时区；
// 人类可读段 %b %e %Y %H:%M:%S + 9 位纳秒，括号内 epoch 秒同小数位）
std::string formatBagTimePoint(uint64_t epochNs)
{
    const uint64_t secs = epochNs / kNsPerSecond;
    const uint64_t subNs = epochNs % kNsPerSecond;
    const std::time_t timeVal = static_cast<std::time_t>(secs);
    std::tm localTime{};
    localtime_r(&timeVal, &localTime);
    std::array<char, kTimeBufBytes> timeBuf{};
    std::strftime(timeBuf.data(), timeBuf.size(), "%b %e %Y %H:%M:%S", &localTime);
    std::ostringstream out;
    out << timeBuf.data() << "." << std::setw(kNsDigits) << std::setfill('0') << subNs
        << " (" << secs << "." << std::setw(kNsDigits) << std::setfill('0') << subNs << ")";
    return out.str();
}

// 纳秒时长 → "12.000000000s"（秒 + 9 位纳秒小数）
std::string formatBagDurationSec(uint64_t durationNs)
{
    std::ostringstream out;
    out << durationNs / kNsPerSecond << "." << std::setw(kNsDigits) << std::setfill('0')
        << durationNs % kNsPerSecond << "s";
    return out.str();
}

// info 行组装：标签补齐 19 列后接内容（多主题/多分片续行由调用方再前缀同宽空格）
std::string bagInfoLabeledLine(const std::string& label, const std::string& content)
{
    std::string line = label;
    if (line.size() < static_cast<size_t>(kInfoLabelWidth))
    {
        line.append(static_cast<size_t>(kInfoLabelWidth) - line.size(), ' ');
    }
    return line + content;
}

// ---- bag reindex 分片文件名收集（bagReindex 用）----
// 文件名 → 分片编号：去掉 .mcap 后缀后尾部 '_' 之后须全为数字（"bag_0.mcap" → 0，
// 前导零按十进制折算），对齐参考实现 <前缀>_<编号>.<扩展名> 命名约定；
// 无 .mcap 后缀/无尾段/尾段含非数字返回 false（调用方跳过该条目）
bool parseBagFileNumber(const std::string& fileName, uint64_t& outNumber)
{
    constexpr const char* kMcapSuffix = ".mcap";
    constexpr std::size_t kMcapSuffixLen = 5;
    if (fileName.size() <= kMcapSuffixLen ||
        fileName.compare(fileName.size() - kMcapSuffixLen, kMcapSuffixLen, kMcapSuffix) != 0)
    {
        return false;
    }
    const std::string stem = fileName.substr(0, fileName.size() - kMcapSuffixLen);
    const auto underscore = stem.rfind('_');
    if (underscore == std::string::npos || underscore + 1 >= stem.size())
    {
        return false;
    }
    const std::string digits = stem.substr(underscore + 1);
    uint64_t number = 0;
    // from_chars 仅提供指针区间接口：尾指针经一次显式指针算术构造（NOLINT 豁免，同透传
    // deserialize 的 payload.data + length 先例），后续比较复用尾指针不再算术
    const char* const digitsEnd = digits.data() + digits.size(); // NOLINT(cppcoreguidelines-pro-bounds-pointer-arithmetic)
    const auto [endPtr, errc] = std::from_chars(digits.data(), digitsEnd, number);
    // 全数字才合法：endPtr 未达尾说明含非数字字符；errc 覆盖溢出
    if (errc != std::errc() || endPtr != digitsEnd)
    {
        return false;
    }
    outNumber = number;
    return true;
}

// 通配模式匹配：'*' 拆前缀/后缀夹逼（* 匹配任意主题名片段，不与首尾共享字符，对齐
// fnmatch 语义）；pattern 已由输入校验保证恰好含一个 '*'——"pre*" 前缀、"*suf" 后缀、
// "pre*suf" 中间，"*"（前后均空）即匹配全部主题。
bool matchTopicPattern(const std::string& pattern, const std::string& topic)
{
    const std::size_t star = pattern.find('*');
    const std::string prefix = pattern.substr(0, star);
    const std::string suffix = pattern.substr(star + 1);
    return topic.size() >= prefix.size() + suffix.size() &&
           topic.compare(0, prefix.size(), prefix) == 0 &&
           topic.compare(topic.size() - suffix.size(), suffix.size(), suffix) == 0;
}

// 透传数据载体：原始 CDR 字节（含 encapsulation header）。
struct Blob
{
    std::vector<uint8_t> bytes;
};

// 透传 TopicDataType：不解析数据，serialize/deserialize 均为原样字节拷贝。构造按远端发现的
// 类型名命名（同名主题共享同一实例），使 create_topic 的类型名与远端端点公告一致——
// DDS 匹配只比主题名 + 类型名字符串 + QoS 兼容，无需真实类型定义。
class PassthroughPubSubType : public TopicDataType
{
public:
    explicit PassthroughPubSubType(const std::string& typeName)
    {
        set_name(typeName);
        // 上限仅约束本端 serialize 的预判（本节点不发布，录制路径不触达）；接收侧
        // deserialize 按实际 payload.length 拷贝不受限。64KB 覆盖常规消息。
        max_serialized_type_size = kEncapsulationBytes + kMaxPayloadBytes;
        is_compute_key_provided = false;
    }
    ~PassthroughPubSubType() override = default;

    bool serialize(const void* const data, eprosima::fastdds::rtps::SerializedPayload_t& payload,
                   DataRepresentationId_t data_representation) override
    {
        static_cast<void>(data_representation);
        const auto* blob = static_cast<const Blob*>(data);
        if (payload.max_size < blob->bytes.size())
        {
            return false;
        }
        std::memcpy(payload.data, blob->bytes.data(), blob->bytes.size());
        payload.length = static_cast<uint32_t>(blob->bytes.size());
        return true;
    }
    bool deserialize(eprosima::fastdds::rtps::SerializedPayload_t& payload, void* data) override
    {
        auto* blob = static_cast<Blob*>(data);
        // 全量透传（含 encapsulation header）：mcap 内 encoding="cdr" 语义即完整 CDR，
        // 读回方可直接交任何 CDR 反序列化器使用
        blob->bytes.assign(payload.data,
                payload.data + payload.length);  // NOLINT(cppcoreguidelines-pro-bounds-pointer-arithmetic)
        return true;
    }
    uint32_t calculate_serialized_size(const void* const data,
                                       DataRepresentationId_t data_representation) override
    {
        static_cast<void>(data);
        static_cast<void>(data_representation);
        return max_serialized_type_size;
    }
    bool compute_key(eprosima::fastdds::rtps::SerializedPayload_t& payload,
                     eprosima::fastdds::rtps::InstanceHandle_t& ihandle, bool force_md5) override
    {
        static_cast<void>(payload);
        static_cast<void>(ihandle);
        static_cast<void>(force_md5);
        return true;  // 无键类型：不做实例归属
    }
    bool compute_key(const void* const data, eprosima::fastdds::rtps::InstanceHandle_t& ihandle,
                     bool force_md5) override
    {
        static_cast<void>(data);
        static_cast<void>(ihandle);
        static_cast<void>(force_md5);
        return true;
    }
    void* create_data() override
    {
        return new Blob();
    }
    void delete_data(void* data) override
    {
        delete static_cast<Blob*>(data);
    }
};

// 写缓存条目：双缓冲模式下回调深拷贝的透传消息（Blob 为复用接收缓冲，入队须拷出独立
// 副本供消费线程写盘）。
struct CachedMsg
{
    mcap::ChannelId channelId = 0;
    uint32_t sequence = 0;
    mcap::Timestamp logTime = 0;  // 回调内采样的接收时刻（与直写模式同口径）
    std::vector<uint8_t> data;

    // 组装 mcap 写入消息（uint8_t 与 std::byte 同为单字节原始存储，别名转换安全）
    mcap::Message toMessage() const
    {
        mcap::Message msg;
        msg.channelId = channelId;
        msg.sequence = sequence;
        msg.logTime = logTime;
        msg.publishTime = logTime;
        msg.data = reinterpret_cast<const std::byte*>(data.data());  // NOLINT(cppcoreguidelines-pro-type-reinterpret-cast)
        msg.dataSize = data.size();
        return msg;
    }
};

// 写缓存双缓冲（参考实现 MessageCache/CacheConsumer 同构最小自建）：producer（DDS 接收
// 回调，多 reader 并发）push 深拷贝消息；consumer（record 内独立消费线程）wait→swap→
// 逐条写盘→clear 贪婪轮转。单侧 buffer 字节上限 maxBytes：超限置丢弃标志（本条仍入队，
// 超限至多一条的量），标志置位期间后续新消息丢弃并按 channelId 计数（收尾统一告警），
// swap 后复位——满载丢弃不阻塞：阻塞仅把丢弃点转移到 DDS 接收队列，样本仍会丢。
class BagMessageCache
{
public:
    explicit BagMessageCache(uint64_t maxBytes) : maxBytes_(maxBytes)
    {
    }

    // producer API：入队一条；返回 false 表示缓存满被丢弃（内部已按 channelId 计数）
    bool push(CachedMsg&& msg, mcap::ChannelId channelId)
    {
        const std::size_t bytes = msg.data.size();
        bool pushed = false;
        {
            std::lock_guard<std::mutex> lock(producerMtx_);
            if (dropNew_)
            {
                ++dropped_[channelId];
            }
            else
            {
                if (bufferBytes_ + bytes > maxBytes_)
                {
                    dropNew_ = true;  // 本条仍入队（超限至多一条量），后续新消息丢弃直至 swap 复位
                }
                producer_.push_back(std::move(msg));
                bufferBytes_ += bytes;
                pushed = true;
            }
        }
        notifyDataReady();  // 每条 push 都唤醒（参考实现同款，含丢弃路径）
        return pushed;
    }

    // consumer API：阻塞至有数据或进入排空态，然后 swap 双缓冲（贪婪轮转：有数据即换）
    void waitAndSwap()
    {
        std::unique_lock<std::mutex> lock(producerMtx_);
        if (!flushing_.load(std::memory_order_relaxed))
        {
            dataCv_.wait(lock, [this] { return dataReady_ || flushing_.load(std::memory_order_relaxed); });
            dataReady_ = false;
        }
        consumer_.swap(producer_);  // 原 producer 成为空 buffer，字节量归零
        bufferBytes_ = 0;
        dropNew_ = false;  // 丢弃标志随 swap 复位（参考实现同款）
    }

    std::vector<CachedMsg>& consumerBuffer()
    {
        return consumer_;  // 仅消费线程访问（swap 后独占）
    }

    void clearConsumerBuffer()
    {
        consumer_.clear();
    }

    // 停止排空：置排空态并唤醒消费线程做最后一轮（残余全部落盘后退出）
    void beginFlush()
    {
        {
            std::lock_guard<std::mutex> lock(producerMtx_);
            flushing_.store(true, std::memory_order_relaxed);
        }
        dataCv_.notify_one();
    }

    bool flushing() const
    {
        return flushing_.load(std::memory_order_relaxed);
    }

    // 丢条统计（channelId → 条数；收尾消费线程已 join 后直读）
    const std::unordered_map<mcap::ChannelId, uint64_t>& dropped() const
    {
        return dropped_;
    }

private:
    void notifyDataReady()
    {
        {
            std::lock_guard<std::mutex> lock(producerMtx_);
            dataReady_ = true;
        }
        dataCv_.notify_one();
    }

    const uint64_t maxBytes_;                                // 单侧 buffer 字节上限
    std::vector<CachedMsg> producer_;                        // producerMtx_ 保护
    std::vector<CachedMsg> consumer_;                        // 仅消费线程访问
    std::unordered_map<mcap::ChannelId, uint64_t> dropped_;  // producerMtx_ 保护（多回调并发）
    std::mutex producerMtx_;
    std::condition_variable dataCv_;
    bool dataReady_ = false;        // producerMtx_ 保护
    std::atomic<bool> flushing_{false};  // 置位后不再接受新数据（仅停止序列置位一次）
    std::size_t bufferBytes_ = 0;   // producer 侧当前字节量（producerMtx_ 保护）
    bool dropNew_ = false;          // producerMtx_ 保护（满载丢弃标志，swap 复位）
};

// 数据监听器共享的写盘上下文（record 局部对象所有，生命周期覆盖全部 listener）。
struct McapWriteCtx
{
    mcap::McapWriter* writer = nullptr;
    std::mutex* payloadMtx = nullptr;   // 串行多 reader 并发 write（McapWriter 非线程安全）
    mcap::ChannelId channelId = 0;
    std::atomic<uint64_t>* firstNs = nullptr;  // 全局首条消息时间戳（metadata starting_time，0=未录到）
    std::atomic<uint64_t>* lastNs = nullptr;   // 全局末条消息时间戳（metadata duration）
    // -b/-d 分片滚动共享状态（滚动仅在 payloadMtx 锁内串行执行，fileIndex/filePaths/
    // shardStartNs 无需原子），全部指向 record 局部对象；maxBagSize/maxBagDurationSec 按值
    // 持入（只读，0=该条件禁用）
    const mcap::McapWriterOptions* options = nullptr;  // 滚动重开新分片沿用同一 options（compression 等）
    const std::string* dirPath = nullptr;              // bag 目录路径（滚动 open 完整路径前缀）
    std::vector<std::string>* filePaths = nullptr;     // 全部分片相对名（metadata relative_file_paths）
    uint64_t* fileIndex = nullptr;                     // 当前分片序号（bag_<N>.mcap 命名）
    uint64_t* shardStartNs = nullptr;                  // 当前分片首条消息时间戳（0=尚无首条）
    std::atomic<bool>* splitBroken = nullptr;          // 滚动失败置位：提示一次且不再重试（防多主题刷屏）
    uint64_t maxBagSize = 0;
    uint64_t maxBagDurationSec = 0;
    BagMessageCache* cache = nullptr;  // 非空=双缓冲模式（回调入队/消费线程写盘）；空=同步直写
};

mcap::Timestamp steadyToSystemNs()
{
    return mcap::Timestamp(std::chrono::duration_cast<std::chrono::nanoseconds>(
                               std::chrono::system_clock::now().time_since_epoch())
                               .count());
}

// 写缓存丢条收尾告警（参考实现 MessageCache::log_dropped 同款语义）：仅在有丢条时一次性
// 打印，按主题名升序逐行列出（channelId 经 channels 表反查主题名）
void logCacheDropped(const BagMessageCache& cache,
                     const std::map<std::string, mcap::ChannelId>& channels)
{
    uint64_t total = 0;
    for (const auto& kv : cache.dropped())
    {
        total += kv.second;
    }
    if (total == 0)
    {
        return;
    }
    std::map<std::string, uint64_t> byTopic;
    for (const auto& kv : cache.dropped())
    {
        for (const auto& channel : channels)
        {
            if (channel.second == kv.first)
            {
                byTopic[channel.first] = kv.second;
                break;
            }
        }
    }
    std::cout << "bag cache dropped messages per topic:";
    for (const auto& kv : byTopic)
    {
        std::cout << "\n\t" << kv.first << ": " << kv.second;
    }
    std::cout << "\nTotal dropped: " << total << std::endl;
}
}  // namespace

// 数据监听器：take_next_sample 取透传 Blob → mcap write。同步直写模式录制发生在 DDS
// 接收线程；双缓冲模式回调仅拷贝入队，写盘由独立消费线程完成（见 record 消费线程）。
class FastDDSBagNode::BagSubListener : public DataReaderListener
{
    friend class FastDDSBagNode;  // record 内消费线程调用 shardCheckAndWrite 落盘

public:
    BagSubListener(void* data, const McapWriteCtx& ctx) : data_(data), ctx_(ctx) {}

    void on_data_available(DataReader* reader) override
    {
        SampleInfo info;
        while (RETCODE_OK == reader->take_next_sample(data_, &info))
        {
            if (!info.valid_data || info.instance_state != ALIVE_INSTANCE_STATE)
            {
                continue;
            }
            // --start-paused 暂停态：照常 take 排空（防恢复后旧数据涌入），不写不计数，
            // sequence 不递增（恢复后首条消息 sequence 从 0 起）
            if (yomk::g_bagRecordPaused.load(std::memory_order_relaxed))
            {
                continue;
            }
            auto* blob = static_cast<Blob*>(data_);
            const auto nowNs = steadyToSystemNs();
            mcap::Message msg;
            msg.channelId = ctx_.channelId;
            msg.sequence = sequence_++;
            msg.logTime = nowNs;      // 接收时刻（系统时钟纳秒），即消息接收时间戳语义
            msg.publishTime = nowNs;
            // uint8_t 与 std::byte 同为单字节原始存储，别名转换安全（mcap 接口要求数据指针）
            msg.data = reinterpret_cast<const std::byte*>(blob->bytes.data());  // NOLINT(cppcoreguidelines-pro-type-reinterpret-cast)
            msg.dataSize = blob->bytes.size();
            if (ctx_.cache != nullptr)
            {
                // 双缓冲模式：深拷贝入队（Blob 为复用接收缓冲，须拷出独立副本供消费线程
                // 写盘）；满载丢弃不入统计——首末时间戳只反映实际进入写路径的消息
                CachedMsg cached;
                cached.channelId = ctx_.channelId;
                cached.sequence = msg.sequence;
                cached.logTime = nowNs;
                cached.data = blob->bytes;
                if (ctx_.cache->push(std::move(cached), ctx_.channelId))
                {
                    recordRecvTs(nowNs);
                }
                continue;
            }
            // 同步直写模式：接收线程锁内写前分片检查 + 落盘
            {
                std::lock_guard<std::mutex> lock(*ctx_.payloadMtx);
                shardCheckAndWrite(msg, nowNs);
            }
            recordRecvTs(nowNs);
        }
    }

    uint64_t count() const
    {
        return count_;
    }
    uint64_t bytes() const
    {
        return bytes_;
    }

private:
    // 分片检查（参考实现同款写前检查点，在写本条之前）+ 落盘 + 统计。仅限持有
    // payloadMtx 调用：直写路径在回调线程，双缓冲路径在消费线程（两路各自串行）。
    void shardCheckAndWrite(const mcap::Message& msg, mcap::Timestamp nowNs)
    {
        // size 与时长双条件先到先分（任一满足即滚动）——size 条件读上一条写后的落盘量
        // （写前/写后逐条等价，达上限本条落新分片），时长条件用本条接收时间戳判断
        // （超时本条落新分片）；新分片起始由其首条消息时间戳确定（0=尚无首条不判定）。
        // size 仅在 chunk 落盘时增长（mcap 默认 chunk 缓冲批量 IO），size 分片触发点
        // 落在 chunk 落盘边界；0 值任一=该条件禁用；暂停期消息不入写路径不计入分片时长
        if (!ctx_.splitBroken->load(std::memory_order_relaxed))
        {
            bool shouldSplit = false;
            if (ctx_.maxBagSize > 0)
            {
                mcap::IWritable* sink = ctx_.writer->dataSink();
                shouldSplit = sink != nullptr && sink->size() >= ctx_.maxBagSize;
            }
            if (!shouldSplit && ctx_.maxBagDurationSec > 0 && *ctx_.shardStartNs != 0)
            {
                // 严格大于（参考实现同款）：距本分片首条消息超过上限即滚动
                shouldSplit = nowNs - *ctx_.shardStartNs >
                              kNsPerSecond * ctx_.maxBagDurationSec;
            }
            // close→open 后 channelId 跨分片稳定（mcap write 首见 channelId 自动
            // 补写 Channel 记录），ctx 零改动
            if (shouldSplit && tryRollShard())
            {
                *ctx_.shardStartNs = 0;
            }
        }
        if (ctx_.writer->write(msg).ok())
        {
            ++count_;
            bytes_ += msg.dataSize;
            if (*ctx_.shardStartNs == 0)
            {
                *ctx_.shardStartNs = nowNs;  // 分片首条：确定本分片时长起点
            }
        }
    }

    // 首末时间戳 CAS 维护（metadata 起始时间与时长）；双缓冲模式在 push 成功后调用
    // （丢弃消息不维护），直写模式无条件调用（对齐现状）
    void recordRecvTs(mcap::Timestamp nowNs)
    {
        uint64_t expected = ctx_.firstNs->load(std::memory_order_relaxed);
        while ((expected == 0 || nowNs < expected) &&
               !ctx_.firstNs->compare_exchange_weak(expected, nowNs, std::memory_order_relaxed))
        {
        }
        expected = ctx_.lastNs->load(std::memory_order_relaxed);
        while (nowNs > expected &&
               !ctx_.lastNs->compare_exchange_weak(expected, nowNs, std::memory_order_relaxed))
        {
        }
    }

    void* data_;          // create_data() 创建的 Blob 接收缓冲（BagSub 持有所有权）
    McapWriteCtx ctx_;
    uint64_t count_ = 0;  // 录制条数（持 payloadMtx 累加：直写=回调线程/双缓冲=消费线程，
                          // 收尾 reader 已删且消费线程已 join，可直读）
    uint64_t bytes_ = 0;  // 录制字节数（同步口径同 count_）
    uint32_t sequence_ = 0;  // per-topic 递增序号

    // 滚动新分片：close 旧文件（补写 summary，内部含残留 chunk flush）→ open bag_<N+1>.mcap。
    // 成功返回 true 且 filePaths/fileIndex 已推进；失败置 splitBroken（提示一次不再重试），
    // 后续 write 全部失败丢弃。仅限 payloadMtx 锁内调用（McapWriter 非线程安全）
    bool tryRollShard()
    {
        ctx_.writer->close();
        const uint64_t nextIndex = *ctx_.fileIndex + 1;
        const std::string nextName = "bag_" + std::to_string(nextIndex) + ".mcap";
        const mcap::Status splitStatus =
            ctx_.writer->open(*ctx_.dirPath + "/" + nextName, *ctx_.options);
        if (!splitStatus.ok())
        {
            // 滚动失败（磁盘满/权限等）：writer 已 closed，后续 write 全部失败丢弃；
            // 提示一次且不再重试（splitBroken 跨 listener 共享）
            if (!ctx_.splitBroken->exchange(true, std::memory_order_relaxed))
            {
                std::cout << "bag split failed: " << splitStatus.message
                          << ", further messages dropped" << std::endl;
            }
            return false;
        }
        ctx_.filePaths->push_back(nextName);
        *ctx_.fileIndex = nextIndex;
        return true;
    }
};

// 参与者监听器：writer/reader 发现回调仅缓存发现信息（不建订阅——订阅统一在 record 校验
// 收敛后建立），回调内仅拿 seenMtx_ 叶子锁（锁序倒置防护见成员注释）。
class FastDDSBagNode::BagParticipantListener : public DomainParticipantListener
{
public:
    explicit BagParticipantListener(FastDDSBagNode& node) : node_(node) {}

    // 仅处理 DISCOVERED_WRITER（QoS 变更/移除忽略）
    void on_data_writer_discovery(
            DomainParticipant* /*participant*/,
            eprosima::fastdds::rtps::WriterDiscoveryStatus reason,
            const PublicationBuiltinTopicData& info,
            bool& should_be_ignored) override
    {
        should_be_ignored = false;
        if (reason == eprosima::fastdds::rtps::WriterDiscoveryStatus::DISCOVERED_WRITER)
        {
            node_.onWriterDiscovered(std::string(info.topic_name.to_string()), info);
        }
    }

    // 仅处理 DISCOVERED_READER（REMOVED 忽略——录制校验只看有无，端点离线不回滚判定）
    void on_data_reader_discovery(
            DomainParticipant* /*participant*/,
            eprosima::fastdds::rtps::ReaderDiscoveryStatus reason,
            const SubscriptionBuiltinTopicData& info,
            bool& should_be_ignored) override
    {
        should_be_ignored = false;
        if (reason == eprosima::fastdds::rtps::ReaderDiscoveryStatus::DISCOVERED_READER)
        {
            node_.onReaderDiscovered(std::string(info.topic_name.to_string()), info);
        }
    }

private:
    FastDDSBagNode& node_;  // 反向引用宿主节点（仅转发发现事件）
};

FastDDSBagNode::FastDDSBagNode()
    : listener_(std::make_unique<BagParticipantListener>(*this))
{
}

FastDDSBagNode::~FastDDSBagNode()
{
    if (participant_ == nullptr)
    {
        return;
    }

    if (subscriber_ != nullptr)
    {
        for (auto& kv : subs_)
        {
            if (kv.second.reader != nullptr)
            {
                subscriber_->delete_datareader(kv.second.reader);
            }
            if (kv.second.topicType != nullptr && kv.second.data != nullptr)
            {
                kv.second.topicType->delete_data(kv.second.data);
            }
            if (kv.second.topic != nullptr)
            {
                participant_->delete_topic(kv.second.topic);
            }
        }
        participant_->delete_subscriber(subscriber_);
    }

    DomainParticipantFactory::get_instance()->delete_participant(participant_);
}

bool FastDDSBagNode::setDomainId(uint32_t domainId)
{
    std::lock_guard<std::mutex> lock(mtx_);
    if (participant_ != nullptr)
    {
        return false;
    }

    // listener 仅服务发现回调（RTPS 层直接派发，不受 mask 控制）；mask 用 none() 对齐
    // FastDDSNode/FastDDSDebugNode——实测 all() 会使动态 reader 匹配成功但数据不交付。
    // bag 节点固定名：入域即被同域 nodeList 类工具辨识，创建后不可改
    DomainParticipantQos qos = PARTICIPANT_QOS_DEFAULT;
    qos.name(std::string("yomkrpc-bag"));
    participant_ = DomainParticipantFactory::get_instance()->create_participant(
        domainId, qos, listener_.get(), StatusMask::none());
    if (participant_ == nullptr)
    {
        return false;
    }

    subscriber_ = participant_->create_subscriber(SUBSCRIBER_QOS_DEFAULT, nullptr, StatusMask::none());
    if (subscriber_ == nullptr)
    {
        DomainParticipantFactory::get_instance()->delete_participant(participant_);
        participant_ = nullptr;
        return false;
    }
    return true;
}

bool FastDDSBagNode::onWriterDiscovered(const std::string& topicName,
        const eprosima::fastdds::rtps::PublicationBuiltinTopicData& info)
{
    std::lock_guard<std::mutex> lock(seenMtx_);
    seen_[topicName].push_back(info);
    return true;
}

bool FastDDSBagNode::onReaderDiscovered(const std::string& topicName,
        const eprosima::fastdds::rtps::SubscriptionBuiltinTopicData& info)
{
    std::lock_guard<std::mutex> lock(seenMtx_);
    seenReaders_[topicName].push_back(info);
    return true;
}

bool FastDDSBagNode::record(const std::vector<std::string>& topics, std::vector<BagTopicStat>& stats,
        std::string* error,
        uint32_t stableRounds,
        uint32_t intervalMs,
        const std::string& outputDir,
        uint64_t maxBagSize,
        uint64_t maxBagDurationSec,
        uint64_t maxCacheSize)
{
    std::lock_guard<std::mutex> lock(mtx_);
    auto fail = [&error](const std::string& msg)
    {
        if (error != nullptr)
        {
            *error = msg;
        }
        return false;
    };

    if (participant_ == nullptr || subscriber_ == nullptr)
    {
        return fail("bag node not created");
    }
    if (recorded_)
    {
        return fail("record already finished, recreate node to record again");
    }
    if (topics.empty())
    {
        return fail("no topics given");
    }
    // 通配模式：清单项支持恰好一个 '*'（前缀 pre* / 后缀 *suf / 中间 pre*suf），出现
    // 两次及以上按输入有误拒绝
    for (const auto& topic : topics)
    {
        if (std::count(topic.begin(), topic.end(), '*') > 1)
        {
            return fail("topic [" + topic + "] 中 '*' 出现多次，仅支持单个通配");
        }
    }
    // 重复主题名会导致同主题重复订阅与 channel 注册，按输入有误拒绝
    for (std::size_t i = 0; i < topics.size(); ++i)
    {
        if (topics[i].empty())
        {
            return fail("empty topic name in topic list");
        }
        for (std::size_t j = i + 1; j < topics.size(); ++j)
        {
            if (topics[i] == topics[j])
            {
                return fail("duplicate topic [" + topics[i] + "] in topic list");
            }
        }
    }
    // 目录名检查 fail-fast 于发现校验前（纯输入错误）：outputDir 非空且已存在即拒绝，
    // 对齐 ros2 避免混入旧数据；父目录不存在由②段 create_directories 自动创建，不在此拦
    if (!outputDir.empty() && std::filesystem::exists(outputDir))
    {
        return fail("bag directory [" + outputDir + "] already exists, remove it or choose another name");
    }
    // -b 分片上限校验 fail-fast 于发现校验前（纯输入错误，不等待不落盘）：非 0 须不低于
    // 最小分片文件大小（ros2 同款启动校验，mcap 存储层下限 1024 字节）；0=不分片不校验
    if (maxBagSize != 0 && maxBagSize < kMinSplitFileSize)
    {
        return fail("max bag size [" + std::to_string(maxBagSize) +
                    "] too small, minimum split file size is 1024 bytes (0 to disable splitting)");
    }

    // ---- ①启动校验：轮询发现缓存，精确主题全部有端点即收敛；清单含通配模式（单个
    // '*'，前缀/后缀/中间夹逼匹配）时，模式项按当轮发现缓存全表匹配展开为实际主题，
    // 展开集连续 stableRounds 次不变也收敛（收敛后仍缺端点的精确主题、未命中任何主题
    // 的模式即输入有误）。总窗不足 kMinValidateWindowMs 时提升 rounds——SPDP 参与者
    // 公告周期约 3s，短窗会把"发布者早已在线但发现未完成"误判为无端点
    std::vector<std::string> exactTopics;  // 精确名子集（无端点时按输入有误报错）
    std::vector<std::string> patterns;     // 通配模式子集（已保证恰好一个 '*'）
    for (const auto& topic : topics)
    {
        (topic.find('*') == std::string::npos ? exactTopics : patterns).push_back(topic);
    }
    const bool hasPatterns = !patterns.empty();
    uint32_t rounds = stableRounds == 0 ? kDefaultStableRounds : stableRounds;
    uint32_t interval = intervalMs == 0 ? kDefaultIntervalMs : intervalMs;
    if (static_cast<uint64_t>(rounds) * interval < kMinValidateWindowMs)
    {
        rounds = static_cast<uint32_t>(kMinValidateWindowMs / interval) + 1;
    }
    const auto validateStart = std::chrono::steady_clock::now();
    std::vector<std::string> missing;       // 收敛后仍无任何端点的精确主题
    std::vector<std::string> recordTopics;  // 展开后的实际录制清单（精确项 ∪ 模式命中项）
    {
        std::vector<std::string> prevSnapshot;
        uint32_t stableCount = 0;
        while (true)
        {
            std::set<std::string> known;  // 发现缓存全表主题名（锁内拷 key，短临界区）
            {
                // 仅读发现缓存：seenMtx_ 叶子锁短临界区
                std::lock_guard<std::mutex> seenLock(seenMtx_);
                for (const auto& kv : seen_)
                {
                    known.insert(kv.first);
                }
                for (const auto& kv : seenReaders_)
                {
                    known.insert(kv.first);
                }
            }
            // 锁外展开：精确项 ∪ 模式命中项（set 去重升序，多模式命中同一主题合并）
            std::set<std::string> effective(exactTopics.begin(), exactTopics.end());
            for (const auto& topic : known)
            {
                for (const auto& pattern : patterns)
                {
                    if (matchTopicPattern(pattern, topic))
                    {
                        effective.insert(topic);
                        break;
                    }
                }
            }
            const bool allExactPresent = std::all_of(exactTopics.begin(), exactTopics.end(),
                [&known](const std::string& topic) { return known.count(topic) > 0; });
            bool allPatternsHit = true;  // 每个模式在展开集中均有命中
            for (const auto& pattern : patterns)
            {
                bool hit = false;
                for (const auto& topic : effective)
                {
                    if (matchTopicPattern(pattern, topic))
                    {
                        hit = true;
                        break;
                    }
                }
                if (!hit)
                {
                    allPatternsHit = false;
                    break;
                }
            }
            std::vector<std::string> snapshot(effective.begin(), effective.end());
            const bool sameAsPrev = snapshot == prevSnapshot;
            if (sameAsPrev)
            {
                ++stableCount;
            }
            else
            {
                stableCount = 1;
            }
            if (stableCount >= rounds)
            {
                // 慢路径收敛：展开集已稳定仍不满足快路径，结算缺端点主题后按输入有误报错
                recordTopics = hasPatterns ? std::move(snapshot) : exactTopics;
                for (const auto& topic : exactTopics)
                {
                    if (known.count(topic) == 0)
                    {
                        missing.push_back(topic);
                    }
                }
                break;
            }
            if (allExactPresent && allPatternsHit && (!hasPatterns || sameAsPrev))
            {
                // 快路径收敛：全部精确项在线且模式全部命中（无模式保留原"全在线立即收敛"；
                // 含模式再多等一轮确认展开集不再增长——匹配集开放无"到齐"信号，一轮无新增防抖）
                recordTopics = hasPatterns ? std::move(snapshot) : exactTopics;
                break;
            }
            prevSnapshot = std::move(snapshot);
            std::this_thread::sleep_for(std::chrono::milliseconds(interval));
        }
    }
    // 模式空命中结算：展开集里查不到该模式命中即输入有误（与缺端点主题合并逐行列出）
    std::vector<std::string> unmatchedPatterns;
    for (const auto& pattern : patterns)
    {
        bool hit = false;
        for (const auto& topic : recordTopics)
        {
            if (matchTopicPattern(pattern, topic))
            {
                hit = true;
                break;
            }
        }
        if (!hit)
        {
            unmatchedPatterns.push_back(pattern);
        }
    }
    if (!missing.empty() || !unmatchedPatterns.empty())
    {
        const auto waitedMs =
            std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - validateStart)
                .count();
        std::string msg;
        for (const auto& topic : missing)
        {
            if (!msg.empty())
            {
                msg += "\n";
            }
            msg += "主题 [" + topic + "] 既无发布者也无订阅者，请检查主题名输入（域 " +
                   std::to_string(participant_->get_domain_id()) + "，已等待 " +
                   std::to_string(waitedMs) + " ms）";
        }
        for (const auto& pattern : unmatchedPatterns)
        {
            if (!msg.empty())
            {
                msg += "\n";
            }
            msg += "模式 [" + pattern + "] 未匹配到任何主题，请检查通配输入（域 " +
                   std::to_string(participant_->get_domain_id()) + "，已等待 " +
                   std::to_string(waitedMs) + " ms）";
        }
        return fail(msg);
    }

    // ---- ②建 bag 目录 + mcap writer（校验通过才落盘，输入有误不产生任何文件）
    // outputDir 非空：用指定目录名/路径（相对/绝对均可，父目录由 create_directories 自动
    // 多级创建；已存在已在①段拒绝）；为空：按时间戳生成——目录名精确到毫秒（同秒录制不
    // 重名）：strftime 无毫秒，取 epoch 毫秒低 3 位手拼补零；日期与时间段格式与 metadata
    // 可读时间伴生键（_format）保持一致
    std::string bagDirName;
    if (!outputDir.empty())
    {
        bagDirName = outputDir;
    }
    else
    {
        const auto dirTime = std::chrono::system_clock::now();
        std::time_t now = std::chrono::system_clock::to_time_t(dirTime);
        const int msPart = static_cast<int>(
                std::chrono::duration_cast<std::chrono::milliseconds>(dirTime.time_since_epoch())
                    .count() %
                1000);
        std::array<char, kTimeBufBytes> timeBuf{};
        std::tm localNow{};
        localtime_r(&now, &localNow);
        std::strftime(timeBuf.data(), timeBuf.size(), "%Y-%m-%d_%H-%M-%S", &localNow);
        std::ostringstream dirName;
        dirName << "bag_" << timeBuf.data() << '_' << std::setw(kMsDigits) << std::setfill('0')
                << msPart;
        bagDirName = dirName.str();
    }
    std::error_code fsError;
    if (!std::filesystem::create_directories(bagDirName, fsError))
    {
        return fail("create bag directory [" + bagDirName + "] failed: " + fsError.message());
    }
    bagDir_ = bagDirName;

    mcap::McapWriter writer;
    mcap::McapWriterOptions options("");  // 空 profile：不绑定任何框架约定
    options.compression = mcap::Compression::None;  // 最小实现不压缩，明文可直查
    mcap::Status status = writer.open(bagDirName + "/bag_0.mcap", options);
    if (!status.ok())
    {
        return fail("open mcap file failed: " + status.message);
    }

    // -b/-d 分片滚动共享状态（bag_0 已打开即首分片；滚动仅在 payloadMtx 锁内串行执行，
    // 无需原子；生命周期覆盖全部 listener，经 McapWriteCtx 指针共享）
    std::vector<std::string> filePaths{"bag_0.mcap"};
    uint64_t fileIndex = 0;
    std::atomic<bool> splitBroken{false};
    uint64_t shardStartNs = 0;  // 当前分片首条消息时间戳（0=尚无首条；新分片由其首条重新确定）

    // 写缓存双缓冲（maxCacheSize>0 启用）：回调深拷贝入队、独立消费线程逐条落盘；
    // 空 = 同步直写（回调线程逐条写盘）
    std::unique_ptr<BagMessageCache> cache;
    if (maxCacheSize > 0)
    {
        cache = std::make_unique<BagMessageCache>(maxCacheSize);
    }

    // ---- ③逐主题建订阅（类型注册按类型名共享；失败回滚已建订阅与 writer）
    std::map<std::string, TypeSupport> types;   // typeName → 已注册透传类型（同名主题共享）
    std::map<std::string, mcap::ChannelId> channels;  // topic → mcap channel
    std::map<mcap::ChannelId, BagSubListener*> byChannel;  // channel → listener（消费线程按 channelId 定位写盘入口）
    std::mutex payloadMtx;
    std::atomic<uint64_t> firstNs{0};
    std::atomic<uint64_t> lastNs{0};
    auto rollback = [&]()
    {
        for (auto& kv : subs_)
        {
            subscriber_->delete_datareader(kv.second.reader);
            if (kv.second.topicType != nullptr && kv.second.data != nullptr)
            {
                kv.second.topicType->delete_data(kv.second.data);
            }
            participant_->delete_topic(kv.second.topic);
        }
        subs_.clear();
        writer.terminate();  // 异常收尾：不写 summary，文件为无效残片（可删目录）
    };

    for (const auto& topic : recordTopics)
    {
        // 类型名：writer 端点公告优先，无 writer 时取 reader 端点公告（仅订阅者场景）
        std::string typeName;
        eprosima::fastdds::rtps::PublicationBuiltinTopicData writerInfo;
        bool hasWriter = false;
        {
            std::lock_guard<std::mutex> seenLock(seenMtx_);
            auto w = seen_.find(topic);
            if (w != seen_.end() && !w->second.empty())
            {
                writerInfo = w->second.front();
                hasWriter = true;
            }
            if (!hasWriter)
            {
                auto r = seenReaders_.find(topic);
                if (r != seenReaders_.end() && !r->second.empty())
                {
                    typeName = r->second.front().type_name.to_string();
                }
            }
            else
            {
                typeName = writerInfo.type_name.to_string();
            }
        }
        if (typeName.empty())
        {
            rollback();
            return fail("topic [" + topic + "] discovered without type name");
        }

        // 透传类型按类型名共享注册（register_type 幂等；TypeSupport 以 shared_ptr 持所有权）
        const auto ins = types.emplace(typeName, TypeSupport(new PassthroughPubSubType(typeName)));
        if (ins.second)
        {
            ins.first->second.register_type(participant_);
        }

        Topic* topicHandle = participant_->create_topic(topic, typeName, TOPIC_QOS_DEFAULT);
        if (topicHandle == nullptr)
        {
            rollback();
            return fail("create topic [" + topic + "] failed");
        }

        // reader QoS：Reliability/Durability 跟随远端 writer offered（requested ≤ offered 恒
        // 成立）；仅订阅者场景无 offered 可跟随，用默认 QoS 由用户保证与后续发布者兼容
        DataReaderQos rqos = DATAREADER_QOS_DEFAULT;
        if (hasWriter)
        {
            rqos.reliability().kind = writerInfo.reliability.kind;
            rqos.durability().kind = writerInfo.durability.kind;
        }

        McapWriteCtx ctx;
        ctx.writer = &writer;
        ctx.payloadMtx = &payloadMtx;
        ctx.firstNs = &firstNs;
        ctx.lastNs = &lastNs;
        ctx.options = &options;
        ctx.dirPath = &bagDirName;
        ctx.filePaths = &filePaths;
        ctx.fileIndex = &fileIndex;
        ctx.splitBroken = &splitBroken;
        ctx.shardStartNs = &shardStartNs;
        ctx.maxBagSize = maxBagSize;
        ctx.maxBagDurationSec = maxBagDurationSec;
        ctx.cache = cache.get();
        // 首见主题注册 mcap Channel：schema_id=0 无 schema 通道，encoding 记 "cdr"
        // （原始 CDR 字节透传落盘的直接落点）；metadata 记类型名——schemaless 无 schema
        // 可承载，bag reindex 据此重建 metadata.json 的 topics type（分片滚动后 mcap
        // write 首见 channelId 自动补写 Channel 记录时一并携带）
        auto channelIt = channels.find(topic);
        if (channelIt == channels.end())
        {
            mcap::Channel channel(topic, "cdr", 0, mcap::KeyValueMap{{"type", typeName}});
            writer.addChannel(channel);
            channels[topic] = channel.id;
            ctx.channelId = channel.id;
        }
        else
        {
            ctx.channelId = channelIt->second;
        }

        BagSub sub;
        sub.type = types[typeName];
        sub.topicType = types[typeName].get();
        sub.data = sub.topicType->create_data();
        sub.topic = topicHandle;
        sub.listener = std::make_unique<BagSubListener>(sub.data, ctx);
        sub.reader = subscriber_->create_datareader(topicHandle, rqos, sub.listener.get());
        if (sub.data == nullptr || sub.reader == nullptr)
        {
            if (sub.data != nullptr)
            {
                sub.topicType->delete_data(sub.data);
            }
            rollback();
            return fail("create datareader for [" + topic + "] failed");
        }
        subs_[topic] = std::move(sub);
        byChannel[ctx.channelId] = subs_[topic].listener.get();
        // 逐主题启动回执（裸 cout 对齐 FastDDSDebugNode 的 subscribed 提示惯例）：CLI 启动行
        // 只显示清单项数，通配展开后的实际录制清单由此逐条可见
        std::cout << "recording topic=" << topic << " type=" << typeName << std::endl;
    }

    // ---- ③.5 双缓冲消费线程（maxCacheSize>0 时启动）：wait→swap→锁内逐条落盘→清空
    // 贪婪轮转；停止序列 beginFlush 后最后一轮排空残余退出（先删 reader 保证此后无新消息）
    std::thread consumerThread;
    if (cache != nullptr)
    {
        consumerThread = std::thread([&cache, &payloadMtx, &byChannel]()
        {
            while (true)
            {
                cache->waitAndSwap();
                if (!cache->consumerBuffer().empty())
                {
                    std::lock_guard<std::mutex> lock(payloadMtx);
                    for (const auto& cached : cache->consumerBuffer())
                    {
                        const auto it = byChannel.find(cached.channelId);
                        if (it != byChannel.end())
                        {
                            it->second->shardCheckAndWrite(cached.toMessage(), cached.logTime);
                        }
                    }
                }
                cache->clearConsumerBuffer();
                if (cache->flushing())
                {
                    break;
                }
            }
        });
    }

    // ---- ④录制循环：每 100ms 轮询停止标志（SIGINT 经 yomk::bagRecordStop 置位，
    // atomic store async-signal-safe）
    while (!yomk::g_bagRecordStop.load(std::memory_order_relaxed))
    {
        std::this_thread::sleep_for(std::chrono::milliseconds(kRecordPollMs));
    }

    // ---- ⑤停止序列：先删全部 reader 杜绝并发回调 → [双缓冲模式：排空写缓存] →
    //      close 补写 summary 索引 → metadata → 统计
    for (auto& kv : subs_)
    {
        subscriber_->delete_datareader(kv.second.reader);
        kv.second.reader = nullptr;
    }
    if (cache != nullptr)
    {
        // 排空写缓存（reader 已删保证此后无新消息）：置排空态唤醒消费线程，残余全部
        // 落盘后线程退出；随后一次性打印丢条统计（参考实现 log_dropped 同款收尾告警）
        cache->beginFlush();
        consumerThread.join();
        logCacheDropped(*cache, channels);
    }
    writer.close();  // 收尾写 summary（三层索引 + 统计）；close() 无返回值，索引异常经后续读回路径暴露

    // metadata.json：顶层平铺 bag 元信息（storage_identifier=mcap；起始时间/时长各附
    // _format 可读伴生键便于人工阅读；最小字段集：起始时间/时长/主题清单与计数），
    // nlohmann json 组装后 dump(4) 美化落盘
    uint64_t totalMessages = 0;
    nlohmann::json topicsWithCount = nlohmann::json::array();
    for (const auto& topic : recordTopics)
    {
        const auto& sub = subs_[topic];
        totalMessages += sub.listener->count();
        topicsWithCount.push_back(
            {{"topic_metadata", {{"name", topic}, {"type", sub.type->get_name()}}},
             {"message_count", sub.listener->count()}});
    }
    const uint64_t startNs = firstNs.load();
    const uint64_t durationNs = startNs == 0 ? 0 : lastNs.load() - startNs;
    nlohmann::json info;
    info["version"] = kBagMetadataVersion;
    info["storage_identifier"] = "mcap";
    info["relative_file_paths"] = filePaths;  // 全部分片相对名（bag_0.mcap, bag_1.mcap, ...）
    info["starting_time"]["nanoseconds_since_epoch"] = startNs;
    info["starting_time"]["nanoseconds_since_epoch_format"] = formatEpochNs(startNs);
    info["duration"]["nanoseconds"] = durationNs;
    info["duration"]["nanoseconds_format"] = formatDurationNs(durationNs);
    info["message_count"] = totalMessages;
    info["topics_with_message_count"] = topicsWithCount;
    {
        std::ofstream meta(bagDirName + "/metadata.json");
        meta << info.dump(4) << "\n";
    }

    // 统计回填（按录制清单顺序：纯精确清单为输入顺序，含模式为展开后去重升序；
    // reader 已删，listener 计数稳定可直读）
    stats.clear();
    stats.reserve(recordTopics.size());
    for (const auto& topic : recordTopics)
    {
        const auto& sub = subs_[topic];
        BagTopicStat stat;
        stat.topic = topic;
        stat.type = sub.type->get_name();
        stat.count = sub.listener->count();
        stat.bytes = sub.listener->bytes();
        stats.push_back(std::move(stat));
    }

    // 收尾清理：reader 已删，补删 topic 释放发现层端点；listener 持有的接收缓冲一并释放
    for (auto& kv : subs_)
    {
        if (kv.second.topicType != nullptr && kv.second.data != nullptr)
        {
            kv.second.topicType->delete_data(kv.second.data);
            kv.second.data = nullptr;
        }
        participant_->delete_topic(kv.second.topic);
        kv.second.topic = nullptr;
    }
    subs_.clear();

    recorded_ = true;
    return true;
}

// 读 bag 目录 metadata.json 组装 info 文本行（实现见头注释）。纯文件读：不触任何 DDS 实体
// 与节点状态，静态调用；JSON 字段缺失/类型不符经 nlohmann 异常统一转报错回填。
bool FastDDSBagNode::bagInfoText(const std::string& bagDir, std::vector<std::string>& outLines,
    std::string* error)
{
    outLines.clear();
    auto fail = [&error](const std::string& reason) {
        if (error != nullptr)
        {
            *error = reason;
        }
        return false;
    };

    const std::filesystem::path dirPath(bagDir);
    std::error_code ec;
    if (!std::filesystem::exists(dirPath, ec))
    {
        return fail("bag path [" + bagDir + "] does not exist");
    }
    std::ifstream metaFile(dirPath / "metadata.json");
    if (!metaFile)
    {
        return fail("could not find metadata.json in bag directory [" + bagDir + "]");
    }
    // 目录递归总大小（对齐参考实现语义：bag 目录全部文件字节数累加，含 metadata.json 自身）
    uint64_t dirBytes = 0;
    for (const auto& entry : std::filesystem::recursive_directory_iterator(dirPath, ec))
    {
        if (entry.is_regular_file(ec))
        {
            const std::uintmax_t fileSize = entry.file_size(ec);
            if (!ec)
            {
                dirBytes += static_cast<uint64_t>(fileSize);
            }
        }
    }

    nlohmann::json info;
    try
    {
        metaFile >> info;
        const uint64_t startNs =
            info.at("starting_time").at("nanoseconds_since_epoch").get<uint64_t>();
        const uint64_t durationNs = info.at("duration").at("nanoseconds").get<uint64_t>();
        const uint64_t messages = info.at("message_count").get<uint64_t>();
        const std::string storageId = info.at("storage_identifier").get<std::string>();
        const auto files = info.at("relative_file_paths").get<std::vector<std::string>>();
        const auto topics = info.at("topics_with_message_count");

        outLines.emplace_back("");  // 首行空行（对齐参考实现输出形态）
        // Files：首文件跟标签，多分片续行缩进 19 空格
        if (files.empty())
        {
            outLines.emplace_back(bagInfoLabeledLine("Files:", ""));
        }
        else
        {
            outLines.emplace_back(bagInfoLabeledLine("Files:", files.front()));
            for (std::size_t i = 1; i < files.size(); ++i)
            {
                outLines.emplace_back(
                    std::string(static_cast<size_t>(kInfoLabelWidth), ' ') + files[i]);
            }
        }
        outLines.emplace_back(bagInfoLabeledLine("Bag size:", formatBagFileSize(dirBytes)));
        outLines.emplace_back(bagInfoLabeledLine("Storage id:", storageId));
        outLines.emplace_back(bagInfoLabeledLine("Duration:", formatBagDurationSec(durationNs)));
        outLines.emplace_back(bagInfoLabeledLine("Start:", formatBagTimePoint(startNs)));
        outLines.emplace_back(
            bagInfoLabeledLine("End:", formatBagTimePoint(startNs + durationNs)));
        outLines.emplace_back(bagInfoLabeledLine("Messages:", std::to_string(messages)));
        // Topic information：每主题一行（首行跟标签、续行缩进 19 空格）；元信息不含
        // serialization_format 字段——录制链路单格式 CDR，写死（中性技术标识同
        // storage_identifier: mcap，不改元信息结构保旧 bag 兼容）
        bool firstTopic = true;
        for (const auto& topic : topics)
        {
            const auto& meta = topic.at("topic_metadata");
            std::ostringstream line;
            line << "Topic: " << meta.at("name").get<std::string>()
                 << " | Type: " << meta.at("type").get<std::string>()
                 << " | Count: " << topic.at("message_count").get<uint64_t>()
                 << " | Serialization Format: cdr";
            if (firstTopic)
            {
                outLines.emplace_back(bagInfoLabeledLine("Topic information:", line.str()));
                firstTopic = false;
            }
            else
            {
                outLines.emplace_back(
                    std::string(static_cast<size_t>(kInfoLabelWidth), ' ') + line.str());
            }
        }
        if (firstTopic)
        {
            outLines.emplace_back(bagInfoLabeledLine("Topic information:", ""));
        }
    }
    catch (const nlohmann::json::exception& e)
    {
        outLines.clear();
        return fail(std::string("parse metadata.json failed: ") + e.what());
    }
    return true;
}

// 从 bag 目录内 mcap 分片重建 metadata.json（实现见头注释）。纯文件操作：不触任何 DDS
// 实体与节点状态，静态调用；统计取自 mcap summary（Statistics 记录含 per-channel 条数
// 与全局起止时间），无 summary/损坏时 readSummary 回退线性扫描重建（覆盖中断录制的 bag）。
bool FastDDSBagNode::bagReindex(const std::string& bagDir, std::string* error)
{
    auto fail = [&error](const std::string& reason) {
        if (error != nullptr)
        {
            *error = reason;
        }
        return false;
    };

    const std::filesystem::path dirPath(bagDir);
    std::error_code ec;
    if (!std::filesystem::exists(dirPath, ec))
    {
        return fail("bag path [" + bagDir + "] does not exist");
    }
    if (!std::filesystem::is_directory(dirPath, ec))
    {
        return fail("must specify a bag directory");
    }

    // 收集 <前缀>_<编号>.mcap 数据文件（其余条目跳过——限定存储格式，避免误开无关文件）；
    // 目录无任何条目与有文件无匹配分列报错（对齐参考实现两分支）
    std::vector<std::pair<uint64_t, std::string>> bagFiles;  // (编号, 文件名)
    std::size_t entryCount = 0;
    for (const auto& entry : std::filesystem::directory_iterator(dirPath, ec))
    {
        ++entryCount;
        uint64_t fileNumber = 0;
        if (entry.is_regular_file(ec) &&
            parseBagFileNumber(entry.path().filename().string(), fileNumber))
        {
            bagFiles.emplace_back(fileNumber, entry.path().filename().string());
        }
    }
    if (entryCount == 0)
    {
        return fail("empty directory");
    }
    if (bagFiles.empty())
    {
        return fail("no bag files found for reindexing");
    }
    // 编号升序（bag_10 排 bag_2 之后）：分片序列与录制侧滚动次序一致（非字典序）
    std::sort(bagFiles.begin(), bagFiles.end(),
        [](const auto& a, const auto& b) { return a.first < b.first; });

    // 逐分片读 mcap 统计：per-channel 条数经 channels() 映射主题名累加（std::map 迭代
    // 升序即主题名升序，对齐参考实现输出序）；起止时间取全局 min/max，无消息分片不参与
    std::map<std::string, uint64_t> topicCounts;
    std::map<std::string, std::string> topicTypes;  // 主题名 → 类型名（来自 Channel.metadata）
    uint64_t startNs = 0;
    uint64_t endNs = 0;
    bool hasMessage = false;
    for (const auto& bagFile : bagFiles)
    {
        const std::string& fileName = bagFile.second;
        mcap::McapReader reader;
        const mcap::Status openStatus = reader.open((dirPath / fileName).string());
        if (!openStatus.ok())
        {
            return fail("open " + fileName + " failed: " + openStatus.message);
        }
        const auto& stats = reader.statistics();
        if (!stats.has_value())
        {
            // open 时的 summary 自动解析缺失/失败（中断录制等）：回退线性扫描重建统计
            const mcap::Status scanStatus =
                reader.readSummary(mcap::ReadSummaryMethod::AllowFallbackScan);
            if (!scanStatus.ok() || !reader.statistics().has_value())
            {
                return fail("read summary of " + fileName + " failed: " + scanStatus.message);
            }
        }
        const auto& fileStats = reader.statistics().value();
        const auto channels = reader.channels();
        for (const auto& channelCount : fileStats.channelMessageCounts)
        {
            const auto channelIt = channels.find(channelCount.first);
            if (channelIt == channels.end())
            {
                return fail("channel " + std::to_string(channelCount.first) + " not found in " +
                            fileName);
            }
            const auto& topic = channelIt->second->topic;
            topicCounts[topic] += channelCount.second;
            // 类型名恢复：录制侧经 Channel.metadata 落盘（{"type", 类型名}）；旧格式 bag
            // 无该元数据时不覆盖保持空串回退（同主题跨分片 channel 声明一致，覆盖等值）
            const auto typeIt = channelIt->second->metadata.find("type");
            if (typeIt != channelIt->second->metadata.end() && !typeIt->second.empty())
            {
                topicTypes[topic] = typeIt->second;
            }
        }
        if (fileStats.messageCount > 0)
        {
            if (!hasMessage || fileStats.messageStartTime < startNs)
            {
                startNs = fileStats.messageStartTime;
            }
            if (fileStats.messageEndTime > endNs)
            {
                endNs = fileStats.messageEndTime;
            }
            hasMessage = true;
        }
        reader.close();
    }

    // 全局跨度（end - start）：与录制侧 metadata 写出口径一致（参考实现为逐分片时长
    // 累加的近似——分片间隙不计入，不照抄）
    const uint64_t durationNs = (hasMessage && endNs > startNs) ? endNs - startNs : 0;

    // 组装与写侧同构的 metadata.json（无条件覆盖：reindex 语义即从数据文件重建元信息）；
    // type 取 Channel.metadata 恢复，旧格式 bag（无该元数据）回退空串——文件里没有就不虚构
    uint64_t totalMessages = 0;
    nlohmann::json topicsWithCount = nlohmann::json::array();
    for (const auto& topicCount : topicCounts)
    {
        totalMessages += topicCount.second;
        std::string topicType;
        if (const auto typeIt = topicTypes.find(topicCount.first); typeIt != topicTypes.end())
        {
            topicType = typeIt->second;
        }
        topicsWithCount.push_back(
            {{"topic_metadata", {{"name", topicCount.first}, {"type", topicType}}},
             {"message_count", topicCount.second}});
    }
    std::vector<std::string> filePaths;
    filePaths.reserve(bagFiles.size());
    for (const auto& bagFile : bagFiles)
    {
        filePaths.push_back(bagFile.second);
    }
    nlohmann::json info;
    info["version"] = kBagMetadataVersion;
    info["storage_identifier"] = "mcap";
    info["relative_file_paths"] = filePaths;
    info["starting_time"]["nanoseconds_since_epoch"] = startNs;
    info["starting_time"]["nanoseconds_since_epoch_format"] = formatEpochNs(startNs);
    info["duration"]["nanoseconds"] = durationNs;
    info["duration"]["nanoseconds_format"] = formatDurationNs(durationNs);
    info["message_count"] = totalMessages;
    info["topics_with_message_count"] = topicsWithCount;
    {
        std::ofstream meta(dirPath / "metadata.json");
        if (!meta)
        {
            return fail("write metadata.json failed");
        }
        meta << info.dump(4) << "\n";
    }
    return true;
}
