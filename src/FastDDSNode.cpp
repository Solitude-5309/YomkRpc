#include "FastDDSNode.h"

#include <fastdds/dds/core/LoanableSequence.hpp>
#include <fastdds/dds/core/status/StatusMask.hpp>
#include <fastdds/dds/domain/DomainParticipantFactory.hpp>
#include <fastdds/dds/subscriber/DataReaderListener.hpp>
#include <fastdds/dds/subscriber/SampleInfo.hpp>

using namespace eprosima::fastdds::dds;

namespace
{
    // F1：registerSubTopic 异常安全守卫。data(create_data) 与 reader(create_datareader) 在所有权
    // 成功移交 subTopics_ 前，若后续任一步骤抛异常(make_unique / map 节点分配 bad_alloc)或走失败
    // return，均由本守卫释放，消除原实现仅在 reader==nullptr 分支手动 delete_data、而 make_unique
    // 抛出时 data 泄漏的缺陷。析构顺序与 ~FastDDSNode 一致：先 delete_datareader 后 delete_data。
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

    void on_data_available(DataReader *reader) override
    {
        // 通用借出机制（reader 侧零拷贝）：以 max_len=0 的 LoanableSequence 调 take()，交付方式由 FastDDS 内部按
        // “可借出性”(is_plain && data-sharing，非按 alignof) 自动决定——
        //   · plain 且 data-sharing 生效：零拷贝借出，dataSeq.buffer()[i] 指向接收缓冲 CDR body(base+4)；
        //   · 非 plain / 未启用 data-sharing：内部反序列化进 create_data() 分配的对齐实例（等价 take_next_sample 拷贝）。
        // 两情形应用代码一致：take → 回调交付 → return_loan；return_loan 对“未借出”集合是无害 no-op（DataReader.hpp doc L270-273）。
        //
        // 对齐契约（关键，务必阅读）：借出指针为 base+4（4=RTPS representation_header_size）。
        //   ✓ 可安全直接 deref 的 plain 类型 = alignof≤4：bool / byte(octet) / char / int8 / uint8 / int16 / uint16 /
        //     int32 / uint32 / float，及其定长数组（如 float pts[N]（点云）、octet pixels[N]（图像））——base+4 天然对齐，
        //     全架构（含严格对齐 ARM）安全，是零拷贝大数组负载的推荐表示。
        //   ✗ alignof>4 的 plain 标量 = double / int64 / uint64（如 MFloat64/MInt64）：借出指针仅 4 字节对齐，消费方须
        //     memcpy 到对齐局部再读（与 YOMKRPC_LOAN 发布侧同一契约）；x86-64 良性，严格对齐 ARM 直接 deref 会 SIGBUS。
        //   建议：大数组/点云/图像负载优先选用 alignof≤4 的 plain 类型（float/byte），从源头规避该边界，勿用定长 double/int64。
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
        // MC10 异常安全：topics_ 节点分配若抛 bad_alloc，已建 topic 未入表 → ~FastDDSNode 不会 delete_topic，
        // 且残留于 participant 会阻断后续同名 create_topic（强保证破坏）。catch 内 delete_topic 回滚后重抛。
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

bool FastDDSNode::registerPubTopic(const std::string &topicName, void *type)
{
    std::lock_guard<std::mutex> lock(mtx_);
    if (participant_ == nullptr || publisher_ == nullptr || type == nullptr || pubTopics_.count(topicName) > 0)
    {
        // P1 所有权契约：本方法无条件接管 caller 传入的 type；守卫拒绝（重复注册/节点未就绪）时
        // 尚未构造 TypeSupport，须在此释放，否则 caller new 出的 TopicDataType 泄漏（与 F1 同类缺陷）。
        // TopicDataType 有虚析构，经基类指针 delete 安全；delete nullptr 亦安全（type==null 分支无副作用）。
        delete static_cast<TopicDataType *>(type);
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

    // MC10 异常安全：pubTopics_ 节点分配若抛 bad_alloc，已建 writer 未入表 → ~FastDDSNode 不会
    // delete_datawriter（orphan writer 滞留至 participant 销毁）；本次新建 topic 亦须回滚（与上方
    // writer==nullptr 分支对称），提供强异常保证。catch 内 delete_datawriter + 回滚新建 topic 后重抛。
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

bool FastDDSNode::registerSubTopic(const std::string &topicName, void *type,
                                   DataCallback callback)
{
    std::lock_guard<std::mutex> lock(mtx_);
    if (participant_ == nullptr || subscriber_ == nullptr || type == nullptr || subTopics_.count(topicName) > 0)
    {
        // P1 所有权契约：同 registerPubTopic，守卫拒绝时释放 caller 的 type，避免失败路径泄漏。
        delete static_cast<TopicDataType *>(type);
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
    if (data == nullptr)
    {
        // 防御性校验（罕见 OOM）：create_data 失败则回滚本次新建主题后返回，
        // 避免后续 legacyTake 以 nullptr 缓冲调用 take_next_sample
        if (!topicExisted)
        {
            participant_->delete_topic(topic);
            topics_.erase(topicName);
        }
        return false;
    }

    // F1：data/reader 所有权成功移交 subTopics_ 前的异常安全守卫，覆盖 make_unique 或
    // map 节点分配抛 bad_alloc 时的资源释放（消除原 create_data 裸指针异常路径泄漏缺陷）
    SubResGuard guard;
    // MC9 cppcheck 抑制：guard 成员登记后正常路径从不读取（所有权成功入 map），仅在异常/失败路径
    // （make_unique 抛 bad_alloc、create_datareader 失败 return）由 ~SubResGuard 读取并清理；cppcheck
    // 只走正常路径数据流、未建模 EH 析构读，故误报“未使用”。
    // cppcheck-suppress unreadVariable
    guard.type = topicType;
    guard.data = data;
    // cppcheck-suppress unreadVariable
    guard.subscriber = subscriber_;

    SubInfo info;
    info.type = ts;
    info.topicType = topicType;
    info.data = data;
    info.listener = std::make_unique<SubListener>(data, std::move(callback));

    info.reader = subscriber_->create_datareader(topic, DATAREADER_QOS_DEFAULT, info.listener.get());
    if (info.reader == nullptr)
    {
        // data 由 guard 释放（不再手动 delete_data，避免双重释放）
        // 本次新创建的主题需回滚
        if (!topicExisted)
        {
            participant_->delete_topic(topic);
            topics_.erase(topicName);
        }
        return false;
    }
    guard.reader = info.reader; // reader 纳入守卫，覆盖 map 分配抛出时 reader 泄漏

    // #2 强保证对称（对齐 registerPubTopic）：subTopics_ 节点分配抛 bad_alloc 时 info 未被 move、
    // reader+data 由已武装的 SubResGuard 在栈展开清理（无泄漏）；但本次 getOrCreateTopic 新建并入
    // topics_ 的 topic 不在守卫覆盖内 → catch 内仅回滚 topic 后重抛，升级基本保证为强保证，与 pub 侧一致
    // （残留 topic 会阻断后续同名 create_topic，故必须显式回滚，不能仅依赖 ~FastDDSNode 兜底）。
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
    guard.reader = nullptr; // 所有权已入 map，解除守卫（后续由 ~FastDDSNode 释放）
    // cppcheck-suppress redundantAssignment
    guard.data = nullptr; // 解除 data 守卫：正常路径覆盖登记值，异常/失败路径该值由 ~SubResGuard 读并 delete_data
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
    // MC9 clang-tidy 抑制：FastDDS DataWriter::write(void*) 签名要求非 const 指针，但 write 仅序列化
    // 读取、不修改 sample；publish(const void* data) 承诺不改 data，故此处 const_cast 去 const 桥接安全。
    return it->second.writer->write(const_cast<void *>(data)) == RETCODE_OK; // NOLINT(cppcoreguidelines-pro-type-const-cast)
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
    // 严格对齐架构告诫：loan_sample 返回待发 CDR payload 的 body 指针(base+4，4=RTPS 规范表示头)。
    // plain 且 alignof>4 的类型(MFloat64/MInt64)该指针仅 4 字节对齐，调用方直接类型化写入在严格对齐
    // 架构(ARM)上触发 SIGBUS(x86-64 良性)。此为 FastDDS 零拷贝 loan 固有特性(doc 未承诺对齐)，
    // YomkRpc 透传该指针、无法在不牺牲零拷贝发布下对齐它；跨严格对齐平台由调用方按对齐安全方式写入。
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
