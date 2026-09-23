#include "FastDDSDebugNode.h"

#include <chrono>
#include <ctime>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <stdexcept>
#include <thread>
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
    // FastDDSNode——实测 all() 会使本节点上的动态 reader 匹配成功但数据不交付
    participant_ = DomainParticipantFactory::get_instance()->create_participant(
        domainId, PARTICIPANT_QOS_DEFAULT, listener_.get(), StatusMask::none());
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

bool FastDDSDebugNode::listTopics(std::vector<std::pair<std::string, std::string>>& topics)
{
    std::lock_guard<std::mutex> lock(mtx_);
    if (participant_ == nullptr)
    {
        return false;
    }
    // seen_ 为 std::map，遍历天然按主题名有序
    for (const auto& kv : seen_)
    {
        topics.emplace_back(kv.first, kv.second.type_name.to_string());
    }
    return true;
}

// 发现线程回调入口：首见 writer 记入缓存并唤醒工作线程；回调内不建订阅。
bool FastDDSDebugNode::onWriterDiscovered(const std::string& topicName,
        const rtps::PublicationBuiltinTopicData& info)
{
    {
        std::lock_guard<std::mutex> lock(mtx_);
        if (!seen_.emplace(topicName, info).second)
        {
            return false;
        }
    }
    cv_.notify_all();
    return true;
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
// 与"TypeObject 稍后就绪的重试"。
void FastDDSDebugNode::workerLoop()
{
    constexpr auto kWorkerInterval = std::chrono::milliseconds(100);  // 空闲等待/重试节奏
    std::unique_lock<std::mutex> lock(mtx_);
    while (workerRunning_)
    {
        std::string todo;
        rtps::PublicationBuiltinTopicData info;
        for (const auto& name : pending_)
        {
            auto it = seen_.find(name);
            if (it != seen_.end())
            {
                todo = name;
                info = it->second;  // 快照，解锁后使用
                break;
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
