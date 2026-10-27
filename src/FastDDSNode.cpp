#include "FastDDSNode.h"

#include <fastdds/dds/core/LoanableSequence.hpp>
#include <fastdds/dds/core/status/StatusMask.hpp>
#include <fastdds/dds/domain/DomainParticipantFactory.hpp>
#include <fastdds/dds/subscriber/DataReaderListener.hpp>
#include <fastdds/dds/subscriber/SampleInfo.hpp>

using namespace eprosima::fastdds::dds;

namespace
{
// 纳秒每秒（Duration_t 秒/纳秒分解用）
constexpr int64_t kNsPerSec = 1000000000LL;
// ROS2 底座工厂的 history depth（rmw_qos_profile_default/sensor_data 规范值）
constexpr int32_t kRos2DefaultDepth = 10;
constexpr int32_t kRos2SensorDataDepth = 5;

// registerSubTopic 异常安全守卫：栈展开时释放已建资源，防止泄漏。
struct SubResGuard
{
    eprosima::fastdds::dds::Subscriber* subscriber = nullptr;
    eprosima::fastdds::dds::DataReader* reader = nullptr;
    eprosima::fastdds::dds::TopicDataType* type = nullptr;
    void* data = nullptr;
    ~SubResGuard()
    {
        if (subscriber != nullptr && reader != nullptr)
        {
            subscriber->delete_datareader(reader);
        }
        if (type != nullptr && data != nullptr)
        {
            type->delete_data(data);
        }
    }
};

// 纳秒转 Duration_t（sec, nsec 分解；调用方保证 ns >= 0）。
Duration_t toDuration(int64_t ns)
{
    return Duration_t(static_cast<int32_t>(ns / kNsPerSec), static_cast<uint32_t>(ns % kNsPerSec));
}

// DDSQosProfile 翻译到 Fast DDS 端点 QoS：起点为调用方已拷贝的 *_QOS_DEFAULT 底座，
// 非哨兵项（枚举非 Unset / 数值 >= 0）逐项覆盖，哨兵项保持底座默认（现状行为）；
// 枚举到 policy 的映射在此显式完成（不做数值直译，公共头枚举与 Fast DDS 内部枚举解耦）。
void toWriterQos(const DDSQosProfile& p, DataWriterQos& qos)
{
    if (p.historyKind != DDSHistoryKind::Unset)
    {
        qos.history().kind =
            (p.historyKind == DDSHistoryKind::KEEP_LAST) ? KEEP_LAST_HISTORY_QOS : KEEP_ALL_HISTORY_QOS;
    }
    // depth 仅 KEEP_LAST 下有意义；KEEP_ALL 时忽略（对齐计划裁定）
    if (p.depth >= 0 && p.historyKind == DDSHistoryKind::KEEP_LAST)
    {
        qos.history().depth = p.depth;
    }
    if (p.reliability != DDSReliability::Unset)
    {
        qos.reliability().kind =
            (p.reliability == DDSReliability::RELIABLE) ? RELIABLE_RELIABILITY_QOS : BEST_EFFORT_RELIABILITY_QOS;
    }
    if (p.durability != DDSDurability::Unset)
    {
        qos.durability().kind =
            (p.durability == DDSDurability::TRANSIENT_LOCAL) ? TRANSIENT_LOCAL_DURABILITY_QOS : VOLATILE_DURABILITY_QOS;
    }
    if (p.liveliness != DDSLiveliness::Unset)
    {
        qos.liveliness().kind =
            (p.liveliness == DDSLiveliness::AUTOMATIC) ? AUTOMATIC_LIVELINESS_QOS : MANUAL_BY_TOPIC_LIVELINESS_QOS;
    }
    if (p.deadlineNs >= 0)
    {
        qos.deadline().period = toDuration(p.deadlineNs);
    }
    if (p.lifespanNs >= 0)
    {
        qos.lifespan().duration = toDuration(p.lifespanNs);
    }
    if (p.livelinessLeaseNs >= 0)
    {
        qos.liveliness().lease_duration = toDuration(p.livelinessLeaseNs);
    }
}

// reader 版：无 lifespan（writer 侧策略，reader 翻译时忽略），其余同构。
void toReaderQos(const DDSQosProfile& p, DataReaderQos& qos)
{
    if (p.historyKind != DDSHistoryKind::Unset)
    {
        qos.history().kind =
            (p.historyKind == DDSHistoryKind::KEEP_LAST) ? KEEP_LAST_HISTORY_QOS : KEEP_ALL_HISTORY_QOS;
    }
    if (p.depth >= 0 && p.historyKind == DDSHistoryKind::KEEP_LAST)
    {
        qos.history().depth = p.depth;
    }
    if (p.reliability != DDSReliability::Unset)
    {
        qos.reliability().kind =
            (p.reliability == DDSReliability::RELIABLE) ? RELIABLE_RELIABILITY_QOS : BEST_EFFORT_RELIABILITY_QOS;
    }
    if (p.durability != DDSDurability::Unset)
    {
        qos.durability().kind =
            (p.durability == DDSDurability::TRANSIENT_LOCAL) ? TRANSIENT_LOCAL_DURABILITY_QOS : VOLATILE_DURABILITY_QOS;
    }
    if (p.liveliness != DDSLiveliness::Unset)
    {
        qos.liveliness().kind =
            (p.liveliness == DDSLiveliness::AUTOMATIC) ? AUTOMATIC_LIVELINESS_QOS : MANUAL_BY_TOPIC_LIVELINESS_QOS;
    }
    if (p.deadlineNs >= 0)
    {
        qos.deadline().period = toDuration(p.deadlineNs);
    }
    if (p.livelinessLeaseNs >= 0)
    {
        qos.liveliness().lease_duration = toDuration(p.livelinessLeaseNs);
    }
}
}  // namespace

// ===== DDSQosProfile ROS2 底座工厂（8 项全非哨兵 = 等价整包替换为 ROS2 底座） =====
// 分端点裁定依据 requested≤offered 匹配方向（见公共头注释）；时间三项保持 -1（底座 ∞，
// 与 ROS2 默认 ∞ 等值）。
DDSQosProfile DDSQosProfile::ros2DefaultPublisher()
{
    DDSQosProfile p;
    p.historyKind = DDSHistoryKind::KEEP_LAST;
    p.depth = kRos2DefaultDepth;
    p.reliability = DDSReliability::RELIABLE;
    p.durability = DDSDurability::VOLATILE;
    p.liveliness = DDSLiveliness::AUTOMATIC;
    return p;
}

DDSQosProfile DDSQosProfile::ros2DefaultSubscriber()
{
    DDSQosProfile p;
    p.historyKind = DDSHistoryKind::KEEP_LAST;
    p.depth = kRos2DefaultDepth;
    p.reliability = DDSReliability::BEST_EFFORT;
    p.durability = DDSDurability::VOLATILE;
    p.liveliness = DDSLiveliness::AUTOMATIC;
    return p;
}

DDSQosProfile DDSQosProfile::ros2SensorData()
{
    DDSQosProfile p;
    p.historyKind = DDSHistoryKind::KEEP_LAST;
    p.depth = kRos2SensorDataDepth;
    p.reliability = DDSReliability::BEST_EFFORT;
    p.durability = DDSDurability::VOLATILE;
    p.liveliness = DDSLiveliness::AUTOMATIC;
    return p;
}

class FastDDSNode::SubListener : public DataReaderListener
{
public:
    SubListener(void* data, DataCallback cb) : data_(data), callback_(std::move(cb)) {}

    // 数据交付两条路径，对齐性不同：
    //   loan 零拷贝（loanSupported_）：指针指向接收缓冲 CDR body = base+4，仅 4 字节对齐——
    //     plain 且 alignof>4 的类型（double/int64）须 memcpy 后再读，严格对齐 ARM 直接解引用触发 SIGBUS；
    //   legacy 反序列化（legacyTake）：指针按 alignof(T) 对齐，全类型全架构可直接解引用。
    // 类型选型与安全消费指引见 README。
    void on_data_available(DataReader* reader) override
    {
        if (loanSupported_)
        {
            while (true)
            {
                LoanableSequence<void*> dataSeq;  // max_len=0 → 请求借出
                SampleInfoSeq infoSeq;
                ReturnCode_t ret = reader->take(dataSeq, infoSeq, LENGTH_UNLIMITED);
                if (ret == RETCODE_OK)
                {
                    for (LoanableCollection::size_type i = 0; i < infoSeq.length(); ++i)
                    {
                        if (infoSeq[i].valid_data && infoSeq[i].instance_state == ALIVE_INSTANCE_STATE && callback_)
                        {
                            // buffer()[i] 是 LoanableCollection 唯一元素访问方式（无 operator[]/span），FastDDS 官方惯用法；
                            // i 受 infoSeq.length() 界定 → 指针算术安全，抑制 clang-tidy 误报。
                            callback_(dataSeq.buffer()[i]);  // NOLINT(cppcoreguidelines-pro-bounds-pointer-arithmetic)
                        }
                    }
                    reader->return_loan(dataSeq, infoSeq);  // 复位为 max_len=0，可继续 take 剩余样本
                    continue;
                }
                if (ret != RETCODE_NO_DATA)
                {
                    // 罕见保险：take 返回非预期错误码时永久回退传统反序列化路径（通用 take 正常不会走到这里）
                    loanSupported_ = false;
                    legacyTake(reader);
                }
                break;
            }
        }
        else
        {
            legacyTake(reader);
        }
    }

private:
    // 回退路径：反序列化进对齐的 data_ 后回调。
    void legacyTake(DataReader* reader)
    {
        SampleInfo info;
        while (RETCODE_OK == reader->take_next_sample(data_, &info))
        {
            if (info.valid_data && info.instance_state == ALIVE_INSTANCE_STATE && callback_)
            {
                callback_(data_);
            }
        }
    }

    void* data_;
    DataCallback callback_;
    bool loanSupported_ = true;
};

FastDDSNode::FastDDSNode() = default;

FastDDSNode::~FastDDSNode()
{
    if (participant_ == nullptr)
    {
        return;
    }

    if (subscriber_ != nullptr)
    {
        for (auto& kv : subTopics_)
        {
            if (kv.second.reader != nullptr)
            {
                subscriber_->delete_datareader(kv.second.reader);
            }
            if (kv.second.topicType != nullptr && kv.second.data != nullptr)
            {
                kv.second.topicType->delete_data(kv.second.data);
            }
        }
        participant_->delete_subscriber(subscriber_);
    }

    if (publisher_ != nullptr)
    {
        for (auto& kv : pubTopics_)
        {
            if (kv.second.writer != nullptr)
            {
                publisher_->delete_datawriter(kv.second.writer);
            }
        }
        participant_->delete_publisher(publisher_);
    }

    for (auto& kv : topics_)
    {
        participant_->delete_topic(kv.second);
    }

    DomainParticipantFactory::get_instance()->delete_participant(participant_);
}

bool FastDDSNode::setDomainId(uint32_t domainId, const std::string& nodeName)
{
    std::lock_guard<std::mutex> lock(mtx_);
    if (participant_ != nullptr)
    {
        return false;
    }

    // nodeName 非空时下沉为参与者名称：SPDP 发现数据原生携带 participant_name，同域对端经
    // 发现机制可读（创建时固化不可改）；string_255 定长 256 字节，超长自动截断
    DomainParticipantQos qos = PARTICIPANT_QOS_DEFAULT;
    if (!nodeName.empty())
    {
        qos.name(nodeName);
    }
    participant_ = DomainParticipantFactory::get_instance()->create_participant(
        domainId, qos, nullptr, StatusMask::none());
    if (participant_ == nullptr)
    {
        return false;
    }

    publisher_ = participant_->create_publisher(PUBLISHER_QOS_DEFAULT, nullptr, StatusMask::none());
    subscriber_ = participant_->create_subscriber(SUBSCRIBER_QOS_DEFAULT, nullptr, StatusMask::none());
    return (publisher_ != nullptr) && (subscriber_ != nullptr);
}

Topic* FastDDSNode::getOrCreateTopic(const std::string& topicName, const std::string& typeName)
{
    auto it = topics_.find(topicName);
    if (it != topics_.end())
    {
        // 主题已存在：类型名必须一致才能复用
        if (it->second->get_type_name() != typeName)
        {
            return nullptr;
        }
        return it->second;
    }

    Topic* topic = participant_->create_topic(topicName, typeName, TOPIC_QOS_DEFAULT);
    if (topic != nullptr)
    {
        try
        {
            topics_[topicName] = topic;
        }
        catch (...)
        {
            participant_->delete_topic(topic);
            throw;
        }
    }
    return topic;
}

// type 所有权无条件接管（失败路径亦 delete）；writer 创建失败回滚本次新建的主题。
bool FastDDSNode::registerPubTopic(const std::string& topicName, void* type, const DDSQosProfile& qos)
{
    std::lock_guard<std::mutex> lock(mtx_);
    if (participant_ == nullptr || publisher_ == nullptr || type == nullptr || pubTopics_.count(topicName) > 0)
    {
        delete static_cast<TopicDataType*>(type);
        return false;
    }

    TypeSupport ts(static_cast<TopicDataType*>(type));
    ts.register_type(participant_);

    PubInfo info;
    info.type = ts;
    bool topicExisted = topics_.count(topicName) > 0;
    Topic* topic = getOrCreateTopic(topicName, ts.get_type_name());
    if (topic == nullptr)
    {
        return false;
    }

    // 端点 QoS：默认底座 + DDSQosProfile 非哨兵项逐项覆盖（全哨兵时翻译结果恒等现状）
    DataWriterQos wqos = DATAWRITER_QOS_DEFAULT;
    toWriterQos(qos, wqos);
    info.writer = publisher_->create_datawriter(topic, wqos);
    if (info.writer == nullptr)
    {
        // 本次新创建的主题需回滚
        if (!topicExisted)
        {
            participant_->delete_topic(topic);
            topics_.erase(topicName);
        }
        return false;
    }

    try
    {
        pubTopics_[topicName] = std::move(info);
    }
    catch (...)
    {
        publisher_->delete_datawriter(info.writer);
        if (!topicExisted)
        {
            participant_->delete_topic(topic);
            topics_.erase(topicName);
        }
        throw;
    }
    return true;
}

// type 所有权无条件接管（含失败/异常路径，经 SubResGuard 与主题回滚释放）。
bool FastDDSNode::registerSubTopic(const std::string& topicName, void* type, DataCallback callback,
    const DDSQosProfile& qos)
{
    std::lock_guard<std::mutex> lock(mtx_);
    if (participant_ == nullptr || subscriber_ == nullptr || type == nullptr || subTopics_.count(topicName) > 0)
    {
        delete static_cast<TopicDataType*>(type);
        return false;
    }

    auto* topicType = static_cast<TopicDataType*>(type);
    TypeSupport ts(topicType);
    ts.register_type(participant_);

    bool topicExisted = topics_.count(topicName) > 0;
    Topic* topic = getOrCreateTopic(topicName, ts.get_type_name());
    if (topic == nullptr)
    {
        return false;
    }

    void* data = topicType->create_data();
    if (data == nullptr)
    {
        if (!topicExisted)
        {
            participant_->delete_topic(topic);
            topics_.erase(topicName);
        }
        return false;
    }

    SubResGuard guard;
    guard.type = topicType;
    guard.data = data;
    guard.subscriber = subscriber_;

    SubInfo info;
    info.type = ts;
    info.topicType = topicType;
    info.data = data;
    info.listener = std::make_unique<SubListener>(data, std::move(callback));

    // 端点 QoS：默认底座 + DDSQosProfile 非哨兵项逐项覆盖（全哨兵时翻译结果恒等现状）
    DataReaderQos rqos = DATAREADER_QOS_DEFAULT;
    toReaderQos(qos, rqos);
    info.reader = subscriber_->create_datareader(topic, rqos, info.listener.get());
    if (info.reader == nullptr)
    {
        if (!topicExisted)
        {
            participant_->delete_topic(topic);
            topics_.erase(topicName);
        }
        return false;
    }
    guard.reader = info.reader;

    try
    {
        subTopics_[topicName] = std::move(info);
    }
    catch (...)
    {
        if (!topicExisted)
        {
            participant_->delete_topic(topic);
            topics_.erase(topicName);
        }
        throw;
    }
    guard.reader = nullptr;
    guard.data = nullptr;
    return true;
}

bool FastDDSNode::publish(const std::string& topicName, const void* data)
{
    std::lock_guard<std::mutex> lock(mtx_);
    auto it = pubTopics_.find(topicName);
    if (it == pubTopics_.end() || it->second.writer == nullptr || data == nullptr)
    {
        return false;
    }
    return it->second.writer->write(data) == RETCODE_OK;
}

bool FastDDSNode::loan(const std::string& topicName, void*& sample)
{
    std::lock_guard<std::mutex> lock(mtx_);
    sample = nullptr;
    auto it = pubTopics_.find(topicName);
    if (it == pubTopics_.end() || it->second.writer == nullptr)
    {
        return false;
    }
    return it->second.writer->loan_sample(sample) == RETCODE_OK;
}

bool FastDDSNode::discardLoan(const std::string& topicName, void*& sample)
{
    std::lock_guard<std::mutex> lock(mtx_);
    auto it = pubTopics_.find(topicName);
    if (it == pubTopics_.end() || it->second.writer == nullptr || sample == nullptr)
    {
        return false;
    }
    return it->second.writer->discard_loan(sample) == RETCODE_OK;
}
