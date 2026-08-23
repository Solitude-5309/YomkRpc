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

class FastDDSNode
{
    class SubListener;
    using DataCallback = std::function<void(const void *)>;
    struct PubInfo
    {
        eprosima::fastdds::dds::TypeSupport type;
        eprosima::fastdds::dds::DataWriter *writer = nullptr;
    };
    struct SubInfo
    {
        eprosima::fastdds::dds::TypeSupport type;
        eprosima::fastdds::dds::DataReader *reader = nullptr;
        eprosima::fastdds::dds::TopicDataType *topicType = nullptr;
        void *data = nullptr; // 内部 create_data() 创建，析构时 delete_data() 释放
        std::unique_ptr<SubListener> listener;
    };

public:
    FastDDSNode();
    ~FastDDSNode();
    FastDDSNode(const FastDDSNode &) = delete;
    FastDDSNode &operator=(const FastDDSNode &) = delete;

public:
    bool setDomainId(uint32_t domainId);
    bool registerSubTopic(const std::string &topicName,
                          void *type,
                          DataCallback callback);
    bool registerPubTopic(const std::string &topicName,
                          void *type);
    bool publish(const std::string &topicName, const void *data);
    // 借出 writer 池内样本（仅 plain 类型支持）：填值后 publish 免序列化；
    // 失败（非 plain/池耗尽）返回 false 且 sample=nullptr，回退普通发布路径
    bool loan(const std::string &topicName, void *&sample);
    // 放弃未发布的借出样本
    bool discardLoan(const std::string &topicName, void *&sample);

private:
    eprosima::fastdds::dds::Topic *getOrCreateTopic(const std::string &topicName,
                                                    const std::string &typeName);

private:
    eprosima::fastdds::dds::DomainParticipant *participant_ = nullptr;
    eprosima::fastdds::dds::Publisher *publisher_ = nullptr;
    eprosima::fastdds::dds::Subscriber *subscriber_ = nullptr;
    std::map<std::string, eprosima::fastdds::dds::Topic *> topics_;
    std::map<std::string, PubInfo> pubTopics_;
    std::map<std::string, SubInfo> subTopics_;
    std::mutex mtx_;
};

#endif // FASTDDSNODE_H
