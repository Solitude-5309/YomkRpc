#include "FastDDSNode.h"

#include <fastdds/dds/core/LoanableSequence.hpp>
#include <fastdds/dds/core/status/StatusMask.hpp>
#include <fastdds/dds/domain/DomainParticipantFactory.hpp>
#include <fastdds/dds/subscriber/DataReaderListener.hpp>
#include <fastdds/dds/subscriber/SampleInfo.hpp>

using namespace eprosima::fastdds::dds;

class FastDDSNode::SubListener : public DataReaderListener
{
public:
    SubListener(void *data, DataCallback cb)
        : data_(data), callback_(std::move(cb))
    {
    }

    void on_data_available(DataReader *reader) override
    {
        // 优先走 reader loan 路径（零拷贝）：任意类型均可用，回调指针仅回调期间有效
        if (loanSupported_)
        {
            while (true)
            {
                LoanableSequence<void *> dataSeq; // max_len=0 → 借出
                SampleInfoSeq infoSeq;
                ReturnCode_t ret = reader->take(dataSeq, infoSeq, LENGTH_UNLIMITED);
                if (ret == RETCODE_OK)
                {
                    for (LoanableCollection::size_type i = 0; i < infoSeq.length(); ++i)
                    {
                        if (infoSeq[i].valid_data &&
                            infoSeq[i].instance_state == ALIVE_INSTANCE_STATE && callback_)
                        {
                            callback_(dataSeq.buffer()[i]);
                        }
                    }
                    reader->return_loan(dataSeq, infoSeq);
                    continue;
                }
                if (ret != RETCODE_NO_DATA)
                {
                    // loan 路径不可用（如类型不支持），置标志永久回退传统路径
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
    // 传统路径：反序列化进内部缓冲后回调
    void legacyTake(DataReader *reader)
    {
        SampleInfo info;
        while (RETCODE_OK == reader->take_next_sample(data_, &info))
        {
            if (info.instance_state == ALIVE_INSTANCE_STATE && info.valid_data && callback_)
            {
                callback_(data_);
            }
        }
    }

private:
    void *data_;
    DataCallback callback_;
    bool loanSupported_ = true;
};

FastDDSNode::FastDDSNode() = default;

FastDDSNode::~FastDDSNode()
{
    // 清理顺序：DataReader/DataWriter → Topic → Publisher/Subscriber → Participant
    if (participant_ == nullptr)
    {
        return;
    }

    if (subscriber_ != nullptr)
    {
        for (auto &kv : subTopics_)
        {
            if (kv.second.reader != nullptr)
            {
                subscriber_->delete_datareader(kv.second.reader);
            }
            // 释放内部 create_data() 创建的数据缓冲
            if (kv.second.topicType != nullptr && kv.second.data != nullptr)
            {
                kv.second.topicType->delete_data(kv.second.data);
            }
        }
        participant_->delete_subscriber(subscriber_);
    }

    if (publisher_ != nullptr)
    {
        for (auto &kv : pubTopics_)
        {
            if (kv.second.writer != nullptr)
            {
                publisher_->delete_datawriter(kv.second.writer);
            }
        }
        participant_->delete_publisher(publisher_);
    }

    // 清理统一维护的主题
    for (auto &kv : topics_)
    {
        participant_->delete_topic(kv.second);
    }

    DomainParticipantFactory::get_instance()->delete_participant(participant_);
}

bool FastDDSNode::setDomainId(uint32_t domainId)
{
    std::lock_guard<std::mutex> lock(mtx_);
    if (participant_ != nullptr)
    {
        return false;
    }

    participant_ = DomainParticipantFactory::get_instance()->create_participant(
        domainId, PARTICIPANT_QOS_DEFAULT, nullptr, StatusMask::none());
    if (participant_ == nullptr)
    {
        return false;
    }

    publisher_ = participant_->create_publisher(PUBLISHER_QOS_DEFAULT, nullptr, StatusMask::none());
    subscriber_ = participant_->create_subscriber(SUBSCRIBER_QOS_DEFAULT, nullptr, StatusMask::none());
    return (publisher_ != nullptr) && (subscriber_ != nullptr);
}

Topic *FastDDSNode::getOrCreateTopic(
    const std::string &topicName,
    const std::string &typeName)
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

    Topic *topic = participant_->create_topic(topicName, typeName, TOPIC_QOS_DEFAULT);
    if (topic != nullptr)
    {
        topics_[topicName] = topic;
    }
    return topic;
}

bool FastDDSNode::registerPubTopic(const std::string &topicName, void *type)
{
    std::lock_guard<std::mutex> lock(mtx_);
    if (participant_ == nullptr || publisher_ == nullptr || type == nullptr || pubTopics_.count(topicName) > 0)
    {
        return false;
    }

    // 转换为 TopicDataType*，TypeSupport 接管所有权
    TypeSupport ts(static_cast<TopicDataType *>(type));
    // 类型可能已被订阅侧注册，重复注册的错误可忽略
    ts.register_type(participant_);

    PubInfo info;
    info.type = ts;
    bool topicExisted = topics_.count(topicName) > 0;
    Topic *topic = getOrCreateTopic(topicName, ts.get_type_name());
    if (topic == nullptr)
    {
        return false;
    }

    info.writer = publisher_->create_datawriter(topic, DATAWRITER_QOS_DEFAULT);
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

    pubTopics_[topicName] = std::move(info);
    return true;
}

bool FastDDSNode::registerSubTopic(const std::string &topicName, void *type,
                                   DataCallback callback)
{
    std::lock_guard<std::mutex> lock(mtx_);
    if (participant_ == nullptr || subscriber_ == nullptr || type == nullptr || subTopics_.count(topicName) > 0)
    {
        return false;
    }

    // 转换为 TopicDataType*，TypeSupport 接管所有权
    auto *topicType = static_cast<TopicDataType *>(type);
    TypeSupport ts(topicType);
    // 类型可能已被发布侧注册，重复注册的错误可忽略
    ts.register_type(participant_);

    // 先获取或创建 Topic，确保资源有效后再创建数据缓冲（避免失败路径泄漏）
    bool topicExisted = topics_.count(topicName) > 0;
    Topic *topic = getOrCreateTopic(topicName, ts.get_type_name());
    if (topic == nullptr)
    {
        return false;
    }

    // 由 type 内部创建数据缓冲，无需用户传入
    void *data = topicType->create_data();

    SubInfo info;
    info.type = ts;
    info.topicType = topicType;
    info.data = data;
    info.listener = std::make_unique<SubListener>(data, std::move(callback));

    info.reader = subscriber_->create_datareader(topic, DATAREADER_QOS_DEFAULT, info.listener.get());
    if (info.reader == nullptr)
    {
        // 清理已创建的数据缓冲
        topicType->delete_data(data);
        // 本次新创建的主题需回滚
        if (!topicExisted)
        {
            participant_->delete_topic(topic);
            topics_.erase(topicName);
        }
        return false;
    }

    subTopics_[topicName] = std::move(info);
    return true;
}

bool FastDDSNode::publish(const std::string &topicName, const void *data)
{
    std::lock_guard<std::mutex> lock(mtx_);
    auto it = pubTopics_.find(topicName);
    if (it == pubTopics_.end() || it->second.writer == nullptr || data == nullptr)
    {
        return false;
    }
    return it->second.writer->write(const_cast<void *>(data)) == RETCODE_OK;
}

bool FastDDSNode::loan(const std::string &topicName, void *&sample)
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

bool FastDDSNode::discardLoan(const std::string &topicName, void *&sample)
{
    std::lock_guard<std::mutex> lock(mtx_);
    auto it = pubTopics_.find(topicName);
    if (it == pubTopics_.end() || it->second.writer == nullptr || sample == nullptr)
    {
        return false;
    }
    return it->second.writer->discard_loan(sample) == RETCODE_OK;
}
