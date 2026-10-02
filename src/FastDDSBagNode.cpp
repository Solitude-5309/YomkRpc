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
#include <chrono>
#include <cstring>
#include <ctime>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <set>
#include <sstream>
#include <thread>
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

// 数据监听器共享的写盘上下文（record 局部对象所有，生命周期覆盖全部 listener）。
struct McapWriteCtx
{
    mcap::McapWriter* writer = nullptr;
    std::mutex* payloadMtx = nullptr;   // 串行多 reader 并发 write（McapWriter 非线程安全）
    mcap::ChannelId channelId = 0;
    std::atomic<uint64_t>* firstNs = nullptr;  // 全局首条消息时间戳（metadata starting_time，0=未录到）
    std::atomic<uint64_t>* lastNs = nullptr;   // 全局末条消息时间戳（metadata duration）
};

mcap::Timestamp steadyToSystemNs()
{
    return mcap::Timestamp(std::chrono::duration_cast<std::chrono::nanoseconds>(
                               std::chrono::system_clock::now().time_since_epoch())
                               .count());
}
}  // namespace

// 数据监听器：take_next_sample 取透传 Blob → mcap write（同步直写，录制发生在 DDS 接收线程）。
class FastDDSBagNode::BagSubListener : public DataReaderListener
{
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
            {
                std::lock_guard<std::mutex> lock(*ctx_.payloadMtx);
                if (ctx_.writer->write(msg).ok())
                {
                    ++count_;
                    bytes_ += msg.dataSize;
                }
            }
            // 首末时间戳 CAS 维护（metadata 起始时间与时长）
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
    void* data_;          // create_data() 创建的 Blob 接收缓冲（BagSub 持有所有权）
    McapWriteCtx ctx_;
    uint64_t count_ = 0;  // 录制条数（payloadMtx_ 锁内累加，收尾时 reader 已删可直读）
    uint64_t bytes_ = 0;  // 录制字节数
    uint32_t sequence_ = 0;  // per-topic 递增序号
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
        uint32_t intervalMs)
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
    // 目录名精确到毫秒（同秒录制不重名）：strftime 无毫秒，取 epoch 毫秒低 3 位手拼补零；
    // 日期与时间段格式与 metadata 可读时间伴生键（_format）保持一致
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
    const std::string bagDirName = dirName.str();
    std::error_code fsError;
    if (!std::filesystem::create_directory(bagDirName, fsError))
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

    // ---- ③逐主题建订阅（类型注册按类型名共享；失败回滚已建订阅与 writer）
    std::map<std::string, TypeSupport> types;   // typeName → 已注册透传类型（同名主题共享）
    std::map<std::string, mcap::ChannelId> channels;  // topic → mcap channel
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
        // 首见主题注册 mcap Channel：schema_id=0 无 schema 通道，encoding 记 "cdr"
        // （原始 CDR 字节透传落盘的直接落点）
        auto channelIt = channels.find(topic);
        if (channelIt == channels.end())
        {
            mcap::Channel channel(topic, "cdr", 0);
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
        // 逐主题启动回执（裸 cout 对齐 FastDDSDebugNode 的 subscribed 提示惯例）：CLI 启动行
        // 只显示清单项数，通配展开后的实际录制清单由此逐条可见
        std::cout << "recording topic=" << topic << " type=" << typeName << std::endl;
    }

    // ---- ④录制循环：每 100ms 轮询停止标志（SIGINT 经 yomk::bagRecordStop 置位，
    // atomic store async-signal-safe）
    while (!yomk::g_bagRecordStop.load(std::memory_order_relaxed))
    {
        std::this_thread::sleep_for(std::chrono::milliseconds(kRecordPollMs));
    }

    // ---- ⑤停止序列：先删全部 reader 杜绝并发回调 → close 补写 summary 索引 → metadata → 统计
    for (auto& kv : subs_)
    {
        subscriber_->delete_datareader(kv.second.reader);
        kv.second.reader = nullptr;
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
    info["relative_file_paths"] = nlohmann::json::array({"bag_0.mcap"});
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
