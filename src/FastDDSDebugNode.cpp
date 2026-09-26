#include "FastDDSDebugNode.h"

#include <algorithm>
#include <chrono>
#include <ctime>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <stdexcept>
#include <thread>
#include <tuple>
#include <utility>

#include <fastdds/dds/core/status/StatusMask.hpp>
#include <fastdds/dds/core/status/SubscriptionMatchedStatus.hpp>
#include <fastdds/dds/domain/DomainParticipantFactory.hpp>
#include <fastdds/dds/domain/DomainParticipantListener.hpp>
#include <fastdds/dds/subscriber/DataReaderListener.hpp>
#include <fastdds/dds/subscriber/SampleInfo.hpp>
#include <fastdds/dds/xtypes/dynamic_types/DynamicData.hpp>
#include <fastdds/dds/xtypes/dynamic_types/DynamicPubSubType.hpp>
#include <fastdds/dds/xtypes/dynamic_types/DynamicType.hpp>
#include <fastdds/dds/xtypes/dynamic_types/DynamicTypeBuilderFactory.hpp>
#include <fastdds/dds/xtypes/type_representation/TypeObject.hpp>
#include <fastdds/dds/xtypes/utils.hpp>
#include <fastdds/rtps/reader/ReaderDiscoveryStatus.hpp>
#include <fastdds/rtps/writer/WriterDiscoveryStatus.hpp>

using namespace eprosima::fastdds::dds;
// using-directive 不注入兄弟命名空间：文件作用域内以别名保持 rtps:: 前缀与 FastDDS 头内写法一致
namespace rtps = eprosima::fastdds::rtps;

namespace
{
// 本地时间戳 hh:mm:ss.uuuuuu（调试输出头部用）
std::string nowStamp()
{
    using Clock = std::chrono::system_clock;
    constexpr int kMicroDigits = 6;              // 微秒固定位数
    constexpr long long kMicrosPerSec = 1000000; // 每秒微秒数
    const auto now = Clock::now();
    std::time_t sec = Clock::to_time_t(now);
    const auto us = std::chrono::duration_cast<std::chrono::microseconds>(
        now.time_since_epoch()).count() % kMicrosPerSec;
    std::tm local {};
    localtime_r(&sec, &local);
    std::ostringstream os;
    os << std::put_time(&local, "%H:%M:%S") << '.'
       << std::setw(kMicroDigits) << std::setfill('0') << us;
    return os.str();
}

// ---- topic info verbose 的端点详情辅助（EndpointDetail.qosLines 的行来源） ----
// 展示名对齐 ros2 topic info -v 的输出习惯（去 _QOS 后缀），枚举拼写对齐 FastDDS 3.6.1
// QosPolicies.hpp；Duration 判 Infinite 用官方 Time_t::is_infinite（时长结构 seconds/nanosec）

// Duration_t → "Infinite" 或 "<秒>.<纳秒> s"（dds::Time_t 公开成员 seconds/nanosec，
// Infinite 判定用官方 Time_t::is_infinite）
std::string formatDuration(const Duration_t& duration)
{
    if (Time_t::is_infinite(duration))
    {
        return "Infinite";
    }
    std::ostringstream os;
    os << duration.seconds;
    if (duration.nanosec != 0)
    {
        // 纳秒部分固定位数补零
        constexpr int kNanosDigits = 9;              // 纳秒位数（10^-9 秒）
        os << '.' << std::setw(kNanosDigits) << std::setfill('0') << duration.nanosec;
    }
    os << " s";
    return os.str();
}

// 端点 GUID → FastDDS 原生格式（ostringstream << GUID_t，“12字节前缀|4字节实体ID”点分十六进制）
std::string guidString(const rtps::GUID_t& guid)
{
    std::ostringstream os;
    os << guid;
    return os.str();
}

// 端点归属参与者名（GUID 前缀比对，RTPS 规范保证端点 GUID 前缀 == 所属参与者 GUID 前缀）；
// 调用方须已持 seenMtx_（快照锁内调用），未发现对应参与者返回空串
std::string participantNameOf(
        const std::map<rtps::GuidPrefix_t, std::string>& participants, const rtps::GUID_t& guid)
{
    const auto it = participants.find(guid.guidPrefix);
    return (it != participants.end()) ? it->second : std::string();
}

const char* reliabilityKindName(ReliabilityQosPolicyKind kind)
{
    switch (kind)
    {
        case RELIABLE_RELIABILITY_QOS:
            return "RELIABLE";
        case BEST_EFFORT_RELIABILITY_QOS:
            return "BEST_EFFORT";
        default:
            return "UNKNOWN";
    }
}

const char* durabilityKindName(DurabilityQosPolicyKind kind)
{
    switch (kind)
    {
        case TRANSIENT_LOCAL_DURABILITY_QOS:
            return "TRANSIENT_LOCAL";
        case TRANSIENT_DURABILITY_QOS:
            return "TRANSIENT";
        case PERSISTENT_DURABILITY_QOS:
            return "PERSISTENT";
        case VOLATILE_DURABILITY_QOS:
            return "VOLATILE";
        default:
            return "UNKNOWN";
    }
}

const char* livelinessKindName(LivelinessQosPolicyKind kind)
{
    switch (kind)
    {
        case MANUAL_BY_PARTICIPANT_LIVELINESS_QOS:
            return "MANUAL_BY_PARTICIPANT";
        case MANUAL_BY_TOPIC_LIVELINESS_QOS:
            return "MANUAL_BY_TOPIC";
        case AUTOMATIC_LIVELINESS_QOS:
            return "AUTOMATIC";
        default:
            return "UNKNOWN";
    }
}

const char* ownershipKindName(OwnershipQosPolicyKind kind)
{
    switch (kind)
    {
        case EXCLUSIVE_OWNERSHIP_QOS:
            return "EXCLUSIVE";
        case SHARED_OWNERSHIP_QOS:
            return "SHARED";
        default:
            return "UNKNOWN";
    }
}

const char* destinationOrderKindName(DestinationOrderQosPolicyKind kind)
{
    switch (kind)
    {
        case BY_SOURCE_TIMESTAMP_DESTINATIONORDER_QOS:
            return "BY_SOURCE_TIMESTAMP";
        case BY_RECEPTION_TIMESTAMP_DESTINATIONORDER_QOS:
            return "BY_RECEPTION_TIMESTAMP";
        default:
            return "UNKNOWN";
    }
}

// Partition 名单 → "[a, b]"（空名单 "[]"；Partition_t 内部为长度前缀 + C 字符串）
std::string partitionNames(const PartitionQosPolicy& partition)
{
    std::string out = "[";
    bool first = true;
    for (auto it = partition.begin(); it != partition.end(); ++it)
    {
        if (!first)
        {
            out += ", ";
        }
        out.append(it->name(), it->size());
        first = false;
    }
    out += "]";
    return out;
}

// 扩展组（fastcdr::optional）：发现数据携带（has_value）才追加行
void appendHistoryLine(std::vector<std::string>& lines, const HistoryQosPolicy& history)
{
    std::string line = "  History: ";
    if (history.kind == KEEP_ALL_HISTORY_QOS)
    {
        line += "KEEP_ALL";
    }
    else
    {
        line += "KEEP_LAST (depth " + std::to_string(history.depth) + ")";
    }
    lines.push_back(std::move(line));
}

void appendResourceLimitsLine(std::vector<std::string>& lines,
        const ResourceLimitsQosPolicy& limits)
{
    lines.push_back("  Resource Limits: max_samples=" + std::to_string(limits.max_samples) +
            ", max_instances=" + std::to_string(limits.max_instances) +
            ", max_samples_per_instance=" + std::to_string(limits.max_samples_per_instance));
}

// writer 端点的 QoS profile 行集：核心组恒输出（11 行）+ 扩展组按发现数据携带情况追加
std::vector<std::string> writerQosLines(const rtps::PublicationBuiltinTopicData& writer)
{
    std::vector<std::string> lines;
    lines.emplace_back(std::string("  Reliability: ") +
            reliabilityKindName(writer.reliability.kind));
    lines.emplace_back(std::string("  Durability: ") + durabilityKindName(writer.durability.kind));
    lines.emplace_back("  Deadline: " + formatDuration(writer.deadline.period));
    lines.emplace_back("  Latency Budget: " + formatDuration(writer.latency_budget.duration));
    lines.emplace_back("  Lifespan: " + formatDuration(writer.lifespan.duration));
    lines.emplace_back(std::string("  Liveliness: ") + livelinessKindName(writer.liveliness.kind));
    lines.emplace_back(
        "  Liveliness lease duration: " + formatDuration(writer.liveliness.lease_duration));
    lines.emplace_back(std::string("  Ownership: ") + ownershipKindName(writer.ownership.kind));
    lines.emplace_back("  Ownership Strength: " + std::to_string(writer.ownership_strength.value));
    lines.emplace_back(std::string("  Destination Order: ") +
            destinationOrderKindName(writer.destination_order.kind));
    lines.emplace_back("  Partition: " + partitionNames(writer.partition));
    if (writer.history.has_value())
    {
        appendHistoryLine(lines, *writer.history);
    }
    if (writer.resource_limits.has_value())
    {
        appendResourceLimitsLine(lines, *writer.resource_limits);
    }
    if (writer.publish_mode.has_value())
    {
        lines.emplace_back(std::string("  Publish Mode: ") +
                (writer.publish_mode->kind == ASYNCHRONOUS_PUBLISH_MODE ?
                "ASYNCHRONOUS" : "SYNCHRONOUS"));
    }
    if (writer.transport_priority.has_value())
    {
        lines.emplace_back(
            "  Transport Priority: " + std::to_string(writer.transport_priority->value));
    }
    if (writer.writer_data_lifecycle.has_value())
    {
        lines.emplace_back(
            std::string("  Writer Data Lifecycle: autodispose_unregistered_instances=") +
            (writer.writer_data_lifecycle->autodispose_unregistered_instances ? "true" : "false"));
    }
    return lines;
}

// reader 端点的 QoS profile 行集：核心组恒输出（11 行，无 Ownership Strength、多 Time Based
// Filter）+ 扩展组按发现数据携带情况追加
std::vector<std::string> readerQosLines(const rtps::SubscriptionBuiltinTopicData& reader)
{
    std::vector<std::string> lines;
    lines.emplace_back(std::string("  Reliability: ") +
            reliabilityKindName(reader.reliability.kind));
    lines.emplace_back(std::string("  Durability: ") + durabilityKindName(reader.durability.kind));
    lines.emplace_back("  Deadline: " + formatDuration(reader.deadline.period));
    lines.emplace_back("  Latency Budget: " + formatDuration(reader.latency_budget.duration));
    lines.emplace_back("  Lifespan: " + formatDuration(reader.lifespan.duration));
    lines.emplace_back(std::string("  Liveliness: ") + livelinessKindName(reader.liveliness.kind));
    lines.emplace_back(
        "  Liveliness lease duration: " + formatDuration(reader.liveliness.lease_duration));
    lines.emplace_back(std::string("  Ownership: ") + ownershipKindName(reader.ownership.kind));
    lines.emplace_back(
        "  Time Based Filter: " + formatDuration(reader.time_based_filter.minimum_separation));
    lines.emplace_back(std::string("  Destination Order: ") +
            destinationOrderKindName(reader.destination_order.kind));
    lines.emplace_back("  Partition: " + partitionNames(reader.partition));
    if (reader.history.has_value())
    {
        appendHistoryLine(lines, *reader.history);
    }
    if (reader.resource_limits.has_value())
    {
        appendResourceLimitsLine(lines, *reader.resource_limits);
    }
    return lines;
}

// 单端点详情拼装：归属节点名 + FastDDS 原生 GUID + QoS profile 行集
FastDDSDebugNode::EndpointDetail makeWriterDetail(
        const rtps::PublicationBuiltinTopicData& writer, const std::string& nodeName)
{
    FastDDSDebugNode::EndpointDetail detail;
    detail.nodeName = nodeName;
    detail.guid = guidString(writer.guid);
    detail.qosLines = writerQosLines(writer);
    return detail;
}

FastDDSDebugNode::EndpointDetail makeReaderDetail(
        const rtps::SubscriptionBuiltinTopicData& reader, const std::string& nodeName)
{
    FastDDSDebugNode::EndpointDetail detail;
    detail.nodeName = nodeName;
    detail.guid = guidString(reader.guid);
    detail.qosLines = readerQosLines(reader);
    return detail;
}
}  // namespace

class FastDDSDebugNode::DebugParticipantListener : public DomainParticipantListener
{
public:
    explicit DebugParticipantListener(FastDDSDebugNode& node) : node_(node) {}

    // 仅处理 DISCOVERED_WRITER（QoS 变更/移除忽略）；回调内仅缓存发现信息，不建订阅。
    void on_data_writer_discovery(
            DomainParticipant* /*participant*/,
            rtps::WriterDiscoveryStatus reason,
            const PublicationBuiltinTopicData& info,
            bool& should_be_ignored) override
    {
        should_be_ignored = false;
        if (reason == rtps::WriterDiscoveryStatus::DISCOVERED_WRITER)
        {
            node_.onWriterDiscovered(std::string(info.topic_name.to_string()), info);
        }
    }

    // 仅处理 DISCOVERED_READER（QoS 变更/移除忽略）；回调内仅缓存发现信息，
    // 供 listTopics 列出仅有订阅者的主题（不参与订阅建立链路）。
    void on_data_reader_discovery(
            DomainParticipant* /*participant*/,
            rtps::ReaderDiscoveryStatus reason,
            const SubscriptionBuiltinTopicData& info,
            bool& should_be_ignored) override
    {
        should_be_ignored = false;
        if (reason == rtps::ReaderDiscoveryStatus::DISCOVERED_READER)
        {
            node_.onReaderDiscovered(std::string(info.topic_name.to_string()), info);
        }
    }

    // 仅处理 DISCOVERED_PARTICIPANT（QoS 变更/移除忽略）；回调内仅缓存 GUID 与名称，
    // 供 nodeList 列出域内命名参与者（不参与订阅建立链路）。
    void on_participant_discovery(
            DomainParticipant* /*participant*/,
            rtps::ParticipantDiscoveryStatus reason,
            const ParticipantBuiltinTopicData& info,
            bool& should_be_ignored) override
    {
        should_be_ignored = false;
        if (reason == rtps::ParticipantDiscoveryStatus::DISCOVERED_PARTICIPANT)
        {
            node_.onParticipantDiscovered(info);
        }
    }

private:
    FastDDSDebugNode& node_;
};

// reader 专属数据监听器：无共享状态（topic/类型名/样本缓冲/输出 sink 均为实例私有）。
class FastDDSDebugNode::DebugSubListener : public DataReaderListener
{
public:
    DebugSubListener(std::string topicName, std::string typeName, TopicDataType* topicType,
                     OutputSink sink)
        : topic_(std::move(topicName))
        , typeName_(std::move(typeName))
        , topicType_(topicType)
        , sink_(std::move(sink))
        // DynamicPubSubType::create_data() 返回堆上 DynamicData::_ref_type（shared_ptr），
        // take_next_sample 经 deserialize 直接反序列化进该缓冲
        , sample_(createSample(topicType))
    {
    }

    ~DebugSubListener()
    {
        if (topicType_ != nullptr && sample_ != nullptr)
        {
            topicType_->delete_data(sample_);
        }
    }

    // 匹配状态反馈（调试工具核心信息：确认 reader/writer 是否真正匹配）
    void on_subscription_matched(DataReader* /*reader*/,
                                 const SubscriptionMatchedStatus& info) override
    {
        if (info.current_count_change != 0)
        {
            std::cout << "[FastDDSDebugNode] topic=" << topic_
                      << " matched writers=" << info.current_count << std::endl;
        }
    }

    // 循环 take 直到无数据；样本为 DynamicData，经内置 json_serialize 结构化（JSON）输出。
    void on_data_available(DataReader* reader) override
    {
        SampleInfo info;
        while (RETCODE_OK == reader->take_next_sample(sample_, &info))
        {
            if (!info.valid_data)
            {
                continue;
            }
            std::ostringstream os;
            os << "\n[" << nowStamp() << "] topic=" << topic_ << " type=" << typeName_
               << " seq=" << ++seq_ << "\n";
            // json_serialize 直接声明于 eprosima::fastdds::dds（3.6.1 无 xtypes:: 内层）
            if (RETCODE_OK != json_serialize(*sample_, DynamicDataJsonFormat::EPROSIMA, os))
            {
                os << "<json_serialize failed>";
            }
            os << "\n";
            deliver(os.str());
        }
    }

private:
    // 样本缓冲分配：失败即抛（构造失败语义），由 onWriterDiscovered 的回滚路径承接
    static DynamicData::_ref_type* createSample(TopicDataType* topicType)
    {
        auto* sample = static_cast<DynamicData::_ref_type*>(topicType->create_data());
        if (sample == nullptr)
        {
            throw std::runtime_error("FastDDSDebugNode: create dynamic sample failed");
        }
        return sample;
    }

    // 输出交付：未设置 sink 时默认 stdout
    void deliver(const std::string& text)
    {
        if (sink_)
        {
            sink_(text);
        }
        else
        {
            std::cout << text << std::flush;
        }
    }

    std::string topic_;      // 调试主题名（输出头部）
    std::string typeName_;   // 远端类型名（输出头部）
    TopicDataType* topicType_;        // 仅供 create/delete_data 样本缓冲，不持有所有权
    OutputSink sink_;                 // 建订阅时按值捕获的输出目的地
    DynamicData::_ref_type* sample_;  // 堆上 shared_ptr 样本缓冲，析构经 delete_data 释放
    uint64_t seq_ = 0;                // 交付序号
};

FastDDSDebugNode::FastDDSDebugNode()
    : listener_(std::make_unique<DebugParticipantListener>(*this))
{
}

FastDDSDebugNode::~FastDDSDebugNode()
{
    // 先停工作线程（不再触碰 subs_/participant_），再按 DataReader → Topic → Subscriber →
    // Participant 顺序清理
    {
        std::lock_guard<std::mutex> lock(mtx_);
        workerRunning_ = false;
    }
    cv_.notify_all();
    if (worker_.joinable())
    {
        worker_.join();
    }

    if (participant_ == nullptr)
    {
        return;
    }

    if (subscriber_ != nullptr)
    {
        // 先逐个停掉 reader（停回调），再删主题；监听器与样本缓冲随 subs_/listener_ 成员析构释放
        for (auto& kv : subs_)
        {
            if (kv.second.reader != nullptr)
            {
                subscriber_->delete_datareader(kv.second.reader);
            }
        }
        for (auto& kv : subs_)
        {
            if (kv.second.topic != nullptr)
            {
                participant_->delete_topic(kv.second.topic);
            }
        }
        participant_->delete_subscriber(subscriber_);
    }

    DomainParticipantFactory::get_instance()->delete_participant(participant_);
}

bool FastDDSDebugNode::setDomainId(uint32_t domainId)
{
    std::lock_guard<std::mutex> lock(mtx_);
    if (participant_ != nullptr)
    {
        return false;
    }

    // listener 仅服务发现回调（RTPS 层直接派发，不受 mask 控制）；mask 用 none() 对齐
    // FastDDSNode——实测 all() 会使本节点上的动态 reader 匹配成功但数据不交付。
    // 调试节点固定名：入域即被同域 nodeList 列出（发现层面可辨识），创建后不可改
    DomainParticipantQos qos = PARTICIPANT_QOS_DEFAULT;
    qos.name(std::string("yomkrpc-debug"));
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

    // 工作线程承担全部订阅建立（发现回调内同步建 reader 会导致匹配成功但数据不交付）
    workerRunning_ = true;
    try
    {
        worker_ = std::thread(&FastDDSDebugNode::workerLoop, this);
    }
    catch (...)
    {
        workerRunning_ = false;
        participant_->delete_subscriber(subscriber_);
        subscriber_ = nullptr;
        DomainParticipantFactory::get_instance()->delete_participant(participant_);
        participant_ = nullptr;
        return false;
    }
    return true;
}

bool FastDDSDebugNode::subscribeTopic(const std::string& topicName)
{
    std::lock_guard<std::mutex> lock(mtx_);
    if (participant_ == nullptr || subscriber_ == nullptr || topicName.empty() ||
            pending_.count(topicName) > 0 || subs_.count(topicName) > 0)
    {
        return false;
    }
    pending_.insert(topicName);
    // 订阅建立由工作线程完成（含 writer 先于登记、TypeObject 稍后就绪等时序），此处仅登记并唤醒
    cv_.notify_all();
    return true;
}

void FastDDSDebugNode::setOutputSink(OutputSink sink)
{
    std::lock_guard<std::mutex> lock(mtx_);
    sink_ = std::move(sink);
}

bool FastDDSDebugNode::listTopics(std::vector<std::pair<std::string, std::string>>& topics,
        uint32_t stableRounds, uint32_t intervalMs)
{
    if (stableRounds == 0)
    {
        stableRounds = kDefaultStableRounds;
    }
    if (intervalMs == 0)
    {
        intervalMs = kDefaultIntervalMs;
    }
    {
        std::lock_guard<std::mutex> lock(mtx_);
        if (participant_ == nullptr)
        {
            return false;  // 未入域
        }
    }
    return waitForTopicsStable(topics, stableRounds, intervalMs);
}

bool FastDDSDebugNode::topicInfo(const std::string& topicName, std::string& typeName,
        size_t& publisherCount, size_t& subscriptionCount,
        uint32_t stableRounds, uint32_t intervalMs,
        std::vector<EndpointDetail>* publishers, std::vector<EndpointDetail>* subscribers)
{
    if (stableRounds == 0)
    {
        stableRounds = kDefaultStableRounds;
    }
    if (intervalMs == 0)
    {
        intervalMs = kDefaultIntervalMs;
    }
    {
        std::lock_guard<std::mutex> lock(mtx_);
        if (participant_ == nullptr)
        {
            return false;  // 未入域
        }
    }
    return waitForTopicInfoStable(
        topicName, typeName, publisherCount, subscriptionCount, stableRounds, intervalMs,
        publishers, subscribers);
}

bool FastDDSDebugNode::nodeList(std::vector<std::string>& names,
        uint32_t stableRounds, uint32_t intervalMs)
{
    if (stableRounds == 0)
    {
        stableRounds = kDefaultStableRounds;
    }
    if (intervalMs == 0)
    {
        intervalMs = kDefaultIntervalMs;
    }
    {
        std::lock_guard<std::mutex> lock(mtx_);
        if (participant_ == nullptr)
        {
            return false;  // 未入域
        }
    }
    return waitForNodesStable(names, stableRounds, intervalMs);
}

bool FastDDSDebugNode::nodeInfo(const std::string& nodeName,
        std::vector<std::pair<std::string, std::string>>& publishers,
        std::vector<std::pair<std::string, std::string>>& subscribers,
        uint32_t stableRounds, uint32_t intervalMs)
{
    if (stableRounds == 0)
    {
        stableRounds = kDefaultStableRounds;
    }
    if (intervalMs == 0)
    {
        intervalMs = kDefaultIntervalMs;
    }
    {
        std::lock_guard<std::mutex> lock(mtx_);
        if (participant_ == nullptr)
        {
            return false;  // 未入域
        }
    }
    return waitForNodeInfoStable(nodeName, publishers, subscribers, stableRounds, intervalMs);
}

bool FastDDSDebugNode::waitForTopicsStable(std::vector<std::pair<std::string, std::string>>& topics,
        uint32_t stableRounds, uint32_t intervalMs)
{
    // 快照：锁内合并拷贝 seen_（writer）与 seenReaders_（reader——仅有订阅者的主题同样
    // 列出），类型名取各主题列表首项，writer 优先、reader 补缺，末尾按主题名排序
    //（两 map 各自有序但合并后交错）。只拿 seenMtx_ 叶子锁（不持锁睡眠，发现事件线程可并行写入）
    auto snapshot = [this]()
    {
        std::lock_guard<std::mutex> lock(seenMtx_);
        std::vector<std::pair<std::string, std::string>> items;
        items.reserve(seen_.size() + seenReaders_.size());
        for (const auto& kv : seen_)
        {
            items.emplace_back(kv.first, kv.second.front().type_name.to_string());
        }
        for (const auto& kv : seenReaders_)
        {
            if (seen_.count(kv.first) == 0)
            {
                items.emplace_back(kv.first, kv.second.front().type_name.to_string());
            }
        }
        std::sort(items.begin(), items.end());
        return items;
    };
    std::vector<std::pair<std::string, std::string>> prev = snapshot();
    uint32_t unchanged = 1;
    while (unchanged < stableRounds)
    {
        std::this_thread::sleep_for(std::chrono::milliseconds(intervalMs));
        std::vector<std::pair<std::string, std::string>> cur = snapshot();
        if (cur == prev)
        {
            ++unchanged;
        }
        else
        {
            prev = std::move(cur);  // 又有新发现，重置不变计数
            unchanged = 1;
        }
    }
    topics = std::move(prev);
    return true;
}

bool FastDDSDebugNode::waitForTopicInfoStable(const std::string& topicName, std::string& typeName,
        size_t& publisherCount, size_t& subscriptionCount,
        uint32_t stableRounds, uint32_t intervalMs,
        std::vector<EndpointDetail>* publishers, std::vector<EndpointDetail>* subscribers)
{
    // 快照：锁内构建目标主题详情（found 标志 / 类型名 / 发布者数 / 订阅者数 / verbose 端点
    // 详情列表——仅调用方传入对应指针时构建）；类型名 writer 优先、reader 补缺；只拿 seenMtx_
    // 叶子锁（不持锁睡眠，发现事件线程可并行写入）。端点详情纳入快照相等性：等待期间远端
    // 端点新增（含 QoS 重发）会使快照变化并重置不变计数
    auto snapshot = [this, &topicName, publishers, subscribers]()
    {
        std::lock_guard<std::mutex> lock(seenMtx_);
        auto w = seen_.find(topicName);
        auto r = seenReaders_.find(topicName);
        if (w == seen_.end() && r == seenReaders_.end())
        {
            return std::make_tuple(false, std::string(), size_t{0}, size_t{0},
                std::vector<EndpointDetail>(), std::vector<EndpointDetail>());
        }
        // 类型名 writer 优先、reader 补缺（前述早退保证至少一端命中；else-if 守卫消除
        // cppcheck 对 r 迭代器有效性的路径分析盲区）
        std::string name;
        if (w != seen_.end())
        {
            name = w->second.front().type_name.to_string();
        }
        else if (r != seenReaders_.end())
        {
            name = r->second.front().type_name.to_string();
        }
        const size_t pubCount = (w != seen_.end()) ? w->second.size() : size_t{0};
        const size_t subCount = (r != seenReaders_.end()) ? r->second.size() : size_t{0};
        std::vector<EndpointDetail> pubDetails;
        if (publishers != nullptr && w != seen_.end())
        {
            pubDetails.reserve(w->second.size());
            for (const auto& writer : w->second)
            {
                pubDetails.push_back(makeWriterDetail(writer,
                    participantNameOf(seenParticipants_, writer.guid)));
            }
        }
        std::vector<EndpointDetail> subDetails;
        if (subscribers != nullptr && r != seenReaders_.end())
        {
            subDetails.reserve(r->second.size());
            for (const auto& reader : r->second)
            {
                subDetails.push_back(makeReaderDetail(reader,
                    participantNameOf(seenParticipants_, reader.guid)));
            }
        }
        return std::make_tuple(true, name, pubCount, subCount, std::move(pubDetails),
            std::move(subDetails));
    };
    auto prev = snapshot();
    uint32_t unchanged = 1;
    while (unchanged < stableRounds)
    {
        std::this_thread::sleep_for(std::chrono::milliseconds(intervalMs));
        auto cur = snapshot();
        if (cur == prev)
        {
            ++unchanged;
        }
        else
        {
            prev = std::move(cur);  // 端点变化，重置不变计数
            unchanged = 1;
        }
    }
    const bool found = std::get<0>(prev);
    if (found)
    {
        // 快照 tuple 元素序号（0-4 在既有 get 调用中直用；详情列表序号命名以免 magic number）
        constexpr size_t kSubDetailsIdx = 5;  // 订阅者端点详情列表的元素序号
        typeName = std::get<1>(prev);
        publisherCount = std::get<2>(prev);
        subscriptionCount = std::get<3>(prev);
        if (publishers != nullptr)
        {
            *publishers = std::move(std::get<4>(prev));
        }
        if (subscribers != nullptr)
        {
            *subscribers = std::move(std::get<kSubDetailsIdx>(prev));
        }
    }
    return found;
}

bool FastDDSDebugNode::waitForNodesStable(std::vector<std::string>& names,
        uint32_t stableRounds, uint32_t intervalMs)
{
    // 快照：锁内拷贝参与者名称列表（仅有效命名参与者：名称非空且非 "/"——"/" 是 ROS2 参与
    // 者的默认占位名，rmw_fastrtps 把参与者名统一置为根 enclave "/"（rcl_init 兜底），节点名
    // 走另一通道，无辨识价值——跳过），末尾按名称排序。同名多参与者各自一行（缓存以 GUID 前缀
    // 为 key，每参与者一项）。只拿 seenMtx_ 叶子锁（不持锁睡眠，发现事件线程可并行写入）
    auto snapshot = [this]()
    {
        std::lock_guard<std::mutex> lock(seenMtx_);
        std::vector<std::string> items;
        items.reserve(seenParticipants_.size());
        for (const auto& kv : seenParticipants_)
        {
            if (!kv.second.empty() && kv.second != "/")
            {
                items.push_back(kv.second);
            }
        }
        std::sort(items.begin(), items.end());
        return items;
    };
    std::vector<std::string> prev = snapshot();
    uint32_t unchanged = 1;
    while (unchanged < stableRounds)
    {
        std::this_thread::sleep_for(std::chrono::milliseconds(intervalMs));
        std::vector<std::string> cur = snapshot();
        if (cur == prev)
        {
            ++unchanged;
        }
        else
        {
            prev = std::move(cur);  // 又有新参与者入域，重置不变计数
            unchanged = 1;
        }
    }
    names = std::move(prev);
    return true;
}

bool FastDDSDebugNode::waitForNodeInfoStable(const std::string& nodeName,
        std::vector<std::pair<std::string, std::string>>& publishers,
        std::vector<std::pair<std::string, std::string>>& subscribers,
        uint32_t stableRounds, uint32_t intervalMs)
{
    // 快照：锁内先收集目标名称参与者的 GUID 前缀集合（同名多参与者均归属——匹配源为 RTPS
    // 规范保证的"端点 GUID 前缀 == 所属参与者 GUID 前缀"，不依赖 participant_guid 字段），
    // 再扫 writer/reader 缓存：前缀命中 → 每主题一条 (topic, 类型名)（同主题去重，取首命中
    // 端点的类型名），两列表末尾按主题名排序。只拿 seenMtx_ 叶子锁（不持锁睡眠，发现事件
    // 线程可并行写入）
    auto snapshot = [this, &nodeName]()
    {
        std::lock_guard<std::mutex> lock(seenMtx_);
        std::set<rtps::GuidPrefix_t> prefixes;
        for (const auto& kv : seenParticipants_)
        {
            if (kv.second == nodeName)
            {
                prefixes.insert(kv.first);
            }
        }
        std::vector<std::pair<std::string, std::string>> pubs;
        std::vector<std::pair<std::string, std::string>> subs;
        for (const auto& kv : seen_)
        {
            for (const auto& w : kv.second)
            {
                if (prefixes.count(w.guid.guidPrefix) > 0)
                {
                    pubs.emplace_back(kv.first, w.type_name.to_string());
                    break;  // 同主题去重：任一归属 writer 命中即计一条
                }
            }
        }
        for (const auto& kv : seenReaders_)
        {
            for (const auto& r : kv.second)
            {
                if (prefixes.count(r.guid.guidPrefix) > 0)
                {
                    subs.emplace_back(kv.first, r.type_name.to_string());
                    break;  // 同主题去重：任一归属 reader 命中即计一条
                }
            }
        }
        std::sort(pubs.begin(), pubs.end());
        std::sort(subs.begin(), subs.end());
        return std::make_tuple(!prefixes.empty(), std::move(pubs), std::move(subs));
    };
    auto prev = snapshot();
    uint32_t unchanged = 1;
    while (unchanged < stableRounds)
    {
        std::this_thread::sleep_for(std::chrono::milliseconds(intervalMs));
        auto cur = snapshot();
        if (cur == prev)
        {
            ++unchanged;
        }
        else
        {
            prev = std::move(cur);  // 新发现使快照变化，重置不变计数
            unchanged = 1;
        }
    }
    if (!std::get<0>(prev))
    {
        return false;  // 收敛时仍无此名参与者
    }
    publishers = std::move(std::get<1>(prev));
    subscribers = std::move(std::get<2>(prev));
    return true;
}

// 发现线程回调入口：新见 writer 追加进 seen_ 列表并唤醒工作线程；回调内不建订阅。
// 仅拿 seenMtx_ 叶子锁：本回调在 Fast DDS EDP 锁临界区内被调用，拿 mtx_ 会与工作线程
// （持 mtx_ 建 reader 内部等 PDP/EDP 锁）锁序倒置死锁（详见 seenMtx_ 成员注释）。
bool FastDDSDebugNode::onWriterDiscovered(const std::string& topicName,
        const rtps::PublicationBuiltinTopicData& info)
{
    {
        std::lock_guard<std::mutex> lock(seenMtx_);
        seen_[topicName].push_back(info);
    }
    cv_.notify_all();
    return true;
}

// 发现线程回调入口：新见 reader 追加进 seenReaders_ 列表；不唤醒工作线程（订阅建立仅由
// writer 驱动，reader 缓存供 listTopics 列出仅有订阅者的主题与 topicInfo 统计订阅者数）。
// 仅拿 seenMtx_ 叶子锁（本回调在 Fast DDS PDP 大锁临界区内被调用，锁序倒置防护同上）。
bool FastDDSDebugNode::onReaderDiscovered(const std::string& topicName,
        const rtps::SubscriptionBuiltinTopicData& info)
{
    std::lock_guard<std::mutex> lock(seenMtx_);
    seenReaders_[topicName].push_back(info);
    return true;
}

// 发现线程回调入口：新见 participant 缓存 GUID 前缀与名称（前缀仅作幂等去重与 nodeInfo
// 归属匹配，emplace 重复发现不覆盖）；不唤醒工作线程（订阅建立链路不依赖参与者发现）。
// 仅拿 seenMtx_ 叶子锁（本回调在 Fast DDS PDP 大锁临界区内被调用，锁序倒置防护同上）。
void FastDDSDebugNode::onParticipantDiscovered(const rtps::ParticipantBuiltinTopicData& info)
{
    std::lock_guard<std::mutex> lock(seenMtx_);
    seenParticipants_.emplace(info.guid.guidPrefix,
            std::string(info.participant_name.to_string()));
}

// 官方文档 15.16 "Remote type discovery and endpoint matching" 接收端形态：
// TypeInformation → TypeObject → DynamicType → DynamicPubSubType → Topic → DataReader。
bool FastDDSDebugNode::tryStartSubscription(const std::string& topicName,
        const rtps::PublicationBuiltinTopicData& info, bool typeNotReadyWarn)
{
    std::lock_guard<std::mutex> lock(mtx_);
    // 未登记 / 已建立订阅 → 忽略（同名 writer 重复发现由此去重）
    if (pending_.count(topicName) == 0 || subs_.count(topicName) > 0 ||
            participant_ == nullptr || subscriber_ == nullptr)
    {
        return false;
    }

    // 远端 TypeInformation → TypeObject：complete 优先、未设（TK_NONE）时回退 minimal，
    // 对齐 DynamicPubSubType 构造的类型标识选择逻辑
    const auto& ti = info.type_information.type_information;
    const auto& tid = (xtypes::TK_NONE != ti.complete().typeid_with_size().type_id()._d())
            ? ti.complete().typeid_with_size().type_id()
            : ti.minimal().typeid_with_size().type_id();
    xtypes::TypeObject type_object;
    if (RETCODE_OK != DomainParticipantFactory::get_instance()->type_object_registry().get_type_object(
                tid, type_object))
    {
        // TypeLookup 尚未取回远端 TypeObject：保留登记，等待轮询/后续重复发现事件重试
        if (typeNotReadyWarn)
        {
            std::cout << "[FastDDSDebugNode] warning: TypeObject not ready, keep pending. topic="
                      << topicName << " type=" << info.type_name.to_string() << std::endl;
        }
        return false;
    }

    // TypeObject → DynamicType → 注册 DynamicPubSubType
    auto dyn_type = DynamicTypeBuilderFactory::get_instance()->create_type_w_type_object(
                type_object)->build();
    if (dyn_type == nullptr)
    {
        std::cout << "[FastDDSDebugNode] warning: build DynamicType failed. topic=" << topicName
                  << " type=" << info.type_name.to_string() << std::endl;
        return false;
    }

    TypeSupport ts(new DynamicPubSubType(dyn_type));
    ts.register_type(participant_);

    Topic* topic = participant_->create_topic(topicName, ts.get_type_name(), TOPIC_QOS_DEFAULT);
    if (topic == nullptr)
    {
        std::cout << "[FastDDSDebugNode] warning: create_topic failed. topic=" << topicName
                  << " type=" << ts.get_type_name() << std::endl;
        return false;
    }

    // 登记订阅项；reader 建立前任何失败仅回滚本次新建资源，主题保持 pending 等待重试
    try
    {
        DebugSub& entry = subs_[topicName];
        entry.type = ts;
        entry.topicType = ts.get();
        entry.topic = topic;
        entry.listener = std::make_unique<DebugSubListener>(topicName, ts.get_type_name(),
                    entry.topicType, sink_);
        entry.reader = subscriber_->create_datareader(topic, DATAREADER_QOS_DEFAULT, entry.listener.get());
        if (entry.reader == nullptr)
        {
            std::cout << "[FastDDSDebugNode] warning: create_datareader failed. topic=" << topicName
                      << " type=" << ts.get_type_name() << std::endl;
            subs_.erase(topicName);
            participant_->delete_topic(topic);
            return false;
        }
    }
    catch (...)
    {
        subs_.erase(topicName);
        participant_->delete_topic(topic);
        return false;
    }

    pending_.erase(topicName);
    std::cout << "[FastDDSDebugNode] subscribed topic=" << topicName
              << " type=" << ts.get_type_name() << std::endl;
    return true;
}

// 订阅建立工作线程：轮询 pending_ × seen_ 交集建订阅；cv_ 有界超时兼顾"新发现/新登记即时响应"
// 与"TypeObject 稍后就绪的重试"。查交集时按锁序 mtx_ → seenMtx_ 嵌套。
void FastDDSDebugNode::workerLoop()
{
    constexpr auto kWorkerInterval = std::chrono::milliseconds(100);  // 空闲等待/重试节奏
    std::unique_lock<std::mutex> lock(mtx_);
    while (workerRunning_)
    {
        std::string todo;
        rtps::PublicationBuiltinTopicData info;
        {
            std::lock_guard<std::mutex> seenLock(seenMtx_);
            for (const auto& name : pending_)
            {
                auto it = seen_.find(name);
                if (it != seen_.end())
                {
                    todo = name;
                    info = it->second.front();  // 取任一同主题 writer 发现信息快照，解锁后使用
                    break;
                }
            }
        }
        if (!todo.empty())
        {
            lock.unlock();
            tryStartSubscription(todo, info, false);
            lock.lock();
        }
        if (workerRunning_)
        {
            cv_.wait_for(lock, kWorkerInterval);
        }
    }
}
