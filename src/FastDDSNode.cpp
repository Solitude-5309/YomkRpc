#include "FastDDSNode.h"

#include <fastdds/dds/core/LoanableSequence.hpp>
#include <fastdds/dds/core/status/StatusMask.hpp>
#include <fastdds/dds/domain/DomainParticipantFactory.hpp>
#include <fastdds/dds/subscriber/DataReaderListener.hpp>
#include <fastdds/dds/subscriber/SampleInfo.hpp>

using namespace eprosima::fastdds::dds;

namespace
{
    // registerSubTopic 异常安全守卫，发生异常时，由本守卫释放，防止内存泄漏
    struct SubResGuard
    {
        eprosima::fastdds::dds::Subscriber *subscriber = nullptr;
        eprosima::fastdds::dds::DataReader *reader = nullptr;
        eprosima::fastdds::dds::TopicDataType *type = nullptr;
        void *data = nullptr;
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
} // namespace

class FastDDSNode::SubListener : public DataReaderListener
{
public:
    SubListener(void *data, DataCallback cb)
        : data_(data), callback_(std::move(cb))
    {
    }

    // 数据交付有两条路径，交付给 callback_ 的 const void* 对齐性不同（消费方须据此安全读值）：
    //   1) loan 零拷贝（主路径，loanSupported_）：reader->take 借出，callback 收到 dataSeq.buffer()[i]，
    //      指向接收缓冲的 CDR body = base+4（representation_header_size=4，仅 4 字节对齐）。FastDDS 按
    //      loanability = is_plain && data-sharing 自动决定 loan-vs-copy；is_plain 是 loanability 闸门而非
    //      alignof 闸门，故对 alignof>4 的 plain 类型不会退化为对齐拷贝，仍交付 base+4：
    //        - plain 且 alignof≤4（bool/byte/char/int8-32/uint/float 及定长数组=点云/图像）：base+4 天然对齐，
    //          消费方可直接 static_cast<const T*> 解引用（全架构含严格对齐 ARM 安全），是零拷贝大数组推荐表示；
    //        - plain 且 alignof>4（double/int64/uint64，如 MFloat64/MInt64）：base+4 仅 4 字节对齐，消费方须
    //          memcpy 到对齐局部再读；直接类型化解引用在严格对齐 ARM 触发 SIGBUS（x86-64 良性、UBSan 报 misaligned）。
    //   2) legacy 对齐反序列化（回退路径，见 legacyTake）：take_next_sample 反序列化进 create_data()=new T() 的
    //      data_ 缓冲，按 alignof(T) 对齐 → 交付指针恒对齐，全类型全架构可直接解引用。
    void on_data_available(DataReader *reader) override
    {
        if (loanSupported_)
        {
            while (true)
            {
                LoanableSequence<void *> dataSeq; // max_len=0 → 请求借出
                SampleInfoSeq infoSeq;
                ReturnCode_t ret = reader->take(dataSeq, infoSeq, LENGTH_UNLIMITED);
                if (ret == RETCODE_OK)
                {
                    for (LoanableCollection::size_type i = 0; i < infoSeq.length(); ++i)
                    {
                        if (infoSeq[i].valid_data &&
                            infoSeq[i].instance_state == ALIVE_INSTANCE_STATE && callback_)
                        {
                            // buffer()[i] 是 LoanableCollection 唯一元素访问方式（无 operator[]/span），FastDDS 官方惯用法；
                            // i 受 infoSeq.length() 界定 → 指针算术安全，抑制 clang-tidy 误报。
                            callback_(dataSeq.buffer()[i]); // NOLINT(cppcoreguidelines-pro-bounds-pointer-arithmetic)
                        }
                    }
                    reader->return_loan(dataSeq, infoSeq); // 复位为 max_len=0，可继续 take 剩余样本
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
    // 传统路径：反序列化进对齐的 data_ 后回调（take 不可用时的保险回退）。
    // data_ 由 create_data()=`new T()` 分配、按 alignof(T) 对齐 → 交付指针恒对齐，全类型全架构安全。
    void legacyTake(DataReader *reader)
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

// 惰性创建 participant/publisher/subscriber（一次性）：已创建（participant_!=nullptr）或 participant 创建失败返回 false。
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

// 注册发布主题：无条件接管 type 所有权（失败路径亦 delete）；创建 writer 并登记到 pubTopics_。
// 节点未就绪 / type==nullptr / 主题名已存在 / 类型名不符 → 返回 false；writer 创建失败回滚本次新建的主题。
bool FastDDSNode::registerPubTopic(const std::string &topicName, void *type)
{
    std::lock_guard<std::mutex> lock(mtx_);
    if (participant_ == nullptr || publisher_ == nullptr || type == nullptr || pubTopics_.count(topicName) > 0)
    {
        delete static_cast<TopicDataType *>(type);
        return false;
    }

    TypeSupport ts(static_cast<TopicDataType *>(type));
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

// 注册订阅主题：无条件接管 type 所有权，create_data() 建对齐接收缓冲 data，装 SubListener 后创建 reader。
// 失败/异常路径经 SubResGuard 与主题回滚释放已建资源，防止 reader/data/type 泄漏。
bool FastDDSNode::registerSubTopic(const std::string &topicName, void *type,
                                   DataCallback callback)
{
    std::lock_guard<std::mutex> lock(mtx_);
    if (participant_ == nullptr || subscriber_ == nullptr || type == nullptr || subTopics_.count(topicName) > 0)
    {
        delete static_cast<TopicDataType *>(type);
        return false;
    }

    auto *topicType = static_cast<TopicDataType *>(type);
    TypeSupport ts(topicType);
    ts.register_type(participant_);

    bool topicExisted = topics_.count(topicName) > 0;
    Topic *topic = getOrCreateTopic(topicName, ts.get_type_name());
    if (topic == nullptr)
    {
        return false;
    }

    void *data = topicType->create_data();
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

    info.reader = subscriber_->create_datareader(topic, DATAREADER_QOS_DEFAULT, info.listener.get());
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

// 同步写入一条消息：data 为借用（write 内部完成序列化/拷贝），调用返回后 caller 即可释放；
// 主题未注册发布 / writer 为空 / data==nullptr 返回 false。
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

// 从 writer 池借出一个待发样本（仅 plain 类型支持）：成功返回 true 且 sample 指向池内缓冲，填值后经 publish 免序列化发送；
// 非 plain / 池耗尽返回 false 且 sample=nullptr（回退普通发布）。借出指针在 write/discard 后被中间件收回，不可再访问。
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

// 归还未发布的借出样本（loan 借出但不 publish 时须调用，否则池泄漏）；writer 不存在或 sample==nullptr 返回 false。
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
