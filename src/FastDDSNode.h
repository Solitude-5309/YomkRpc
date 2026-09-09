#ifndef FASTDDSNODE_H
#define FASTDDSNODE_H

#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <string>

#include <fastdds/dds/domain/DomainParticipant.hpp>
#include <fastdds/dds/publisher/DataWriter.hpp>
#include <fastdds/dds/publisher/Publisher.hpp>
#include <fastdds/dds/subscriber/DataReader.hpp>
#include <fastdds/dds/subscriber/Subscriber.hpp>
#include <fastdds/dds/topic/Topic.hpp>
#include <fastdds/dds/topic/TypeSupport.hpp>

// 单个 DDS 节点的封装：持有一个 DomainParticipant 及其 publisher/subscriber，管理本节点下的
// 发布主题（pubTopics_）与订阅主题（subTopics_）。全部公开方法内部以 mtx_ 串行化；由 YomkRpcService
// 持有（每个 nodeName 一个实例），析构时按 DataReader/Writer → Topic → Publisher/Subscriber → Participant 顺序清理。
class FastDDSNode
{
    class SubListener;                                      // DataReaderListener 实现：收到数据时交付用户回调（详见 .cpp 的两条交付路径与对齐契约）
    using DataCallback = std::function<void(const void *)>; // 交付消息实例指针，仅回调期间有效
    // 发布主题登记项。
    struct PubInfo
    {
        eprosima::fastdds::dds::TypeSupport type;             // 已注册类型（TypeSupport 以 shared_ptr 持有 TopicDataType 所有权）
        eprosima::fastdds::dds::DataWriter *writer = nullptr;
    };
    // 订阅主题登记项。
    struct SubInfo
    {
        eprosima::fastdds::dds::TypeSupport type;                   // 已注册类型（同 PubInfo.type）
        eprosima::fastdds::dds::DataReader *reader = nullptr;
        eprosima::fastdds::dds::TopicDataType *topicType = nullptr; // 指向 type 所辖同一对象，仅供 delete_data(data)，不单独持有所有权
        void *data = nullptr;                                       // 内部 create_data() 创建的对齐接收缓冲，析构时 delete_data() 释放
        std::unique_ptr<SubListener> listener;                      // reader 的数据监听器，持有 data 与用户回调
    };

public:
    FastDDSNode();
    ~FastDDSNode();
    FastDDSNode(const FastDDSNode &) = delete;
    FastDDSNode &operator=(const FastDDSNode &) = delete;

public:
    // 设置 DDS 域并惰性创建 participant/publisher/subscriber；仅可成功一次，已创建或创建失败返回 false。
    bool setDomainId(uint32_t domainId);
    // 注册订阅主题：接管 type 所有权，收到消息时经 callback 交付；主题名冲突/类型名不符/资源创建失败返回 false。
    bool registerSubTopic(const std::string &topicName,
                          void *type,
                          DataCallback callback);
    // 注册发布主题：接管 type 所有权并创建 writer；主题名冲突/类型名不符/writer 创建失败返回 false。
    bool registerPubTopic(const std::string &topicName,
                          void *type);
    // 同步发布一条消息（data 借用，write 内部完成序列化/拷贝）；主题未注册发布/data==nullptr/write 失败返回 false。
    bool publish(const std::string &topicName, const void *data);
    // 借出 writer 池内样本（仅 plain 类型支持）：成功返回 true 且 sample 指向池内缓冲，填值后 publish 免序列化；
    // 失败（非 plain/池耗尽）返回 false 且 sample=nullptr，回退普通发布路径。借出指针 write/discard 后失效。
    bool loan(const std::string &topicName, void *&sample);
    // 归还未发布的借出样本（loan 借出但不 publish 时须调用，否则池泄漏）；writer 不存在或 sample==nullptr 返回 false。
    bool discardLoan(const std::string &topicName, void *&sample);

private:
    // 复用或新建主题：已存在则校验类型名一致（不符返回 nullptr），否则 create_topic 并登记到 topics_。
    eprosima::fastdds::dds::Topic *getOrCreateTopic(const std::string &topicName,
                                                    const std::string &typeName);

private:
    eprosima::fastdds::dds::DomainParticipant *participant_ = nullptr;
    eprosima::fastdds::dds::Publisher *publisher_ = nullptr;        // 发布者，拥有全部 writer
    eprosima::fastdds::dds::Subscriber *subscriber_ = nullptr;      // 订阅者，拥有全部 reader
    std::map<std::string, eprosima::fastdds::dds::Topic *> topics_; // topicName → Topic，发布/订阅共享
    std::map<std::string, PubInfo> pubTopics_;
    std::map<std::string, SubInfo> subTopics_;
    std::mutex mtx_; // 串行化本节点全部公开方法
};

#endif // FASTDDSNODE_H
