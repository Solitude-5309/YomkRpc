#ifndef FASTDDSNODE_H
#define FASTDDSNODE_H

#include "YomkRpcService.h"  // DDSQosProfile（端点 QoS 描述，公共头零 fastdds 依赖；平铺引号形式命中 include/，安装树经 YomkRpc/ 前缀提供同一头）
#include "FastDDSXmlHelper.h"  // ParticipantQosSpec（XML 配置通道 + serverLocator 直配 spec；缺省实参需完整类型，自身引公共头与 fastdds）

#include <cstdint>
#include <fastdds/dds/domain/DomainParticipant.hpp>
#include <fastdds/dds/publisher/DataWriter.hpp>
#include <fastdds/dds/publisher/Publisher.hpp>
#include <fastdds/dds/subscriber/DataReader.hpp>
#include <fastdds/dds/subscriber/Subscriber.hpp>
#include <fastdds/dds/topic/Topic.hpp>
#include <fastdds/dds/topic/TypeSupport.hpp>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <string>

// 单个 DDS 节点的封装：持有一个 DomainParticipant 及其 publisher/subscriber，管理本节点下的
// 发布主题（pubTopics_）与订阅主题（subTopics_）。全部公开方法内部以 mtx_ 串行化；由 YomkRpcService
// 持有（每个 nodeName 一个实例），析构时按 DataReader/Writer → Topic → Publisher/Subscriber → Participant 顺序清理。
class FastDDSNode
{
    class SubListener;  // DataReaderListener 实现：收到数据时交付用户回调（详见 .cpp 的两条交付路径与对齐契约）
    using DataCallback = std::function<void(const void*)>;  // 交付消息实例指针，仅回调期间有效
    // 发布主题登记项。
    struct PubInfo
    {
        eprosima::fastdds::dds::TypeSupport type;  // 已注册类型（TypeSupport 以 shared_ptr 持有 TopicDataType 所有权）
        eprosima::fastdds::dds::DataWriter* writer = nullptr;
    };
    // 订阅主题登记项。
    struct SubInfo
    {
        eprosima::fastdds::dds::TypeSupport type;  // 已注册类型（同 PubInfo.type）
        eprosima::fastdds::dds::DataReader* reader = nullptr;
        eprosima::fastdds::dds::TopicDataType* topicType =
            nullptr;           // 指向 type 所辖同一对象，仅供 delete_data(data)，不单独持有所有权
        void* data = nullptr;  // 内部 create_data() 创建的对齐接收缓冲，析构时 delete_data() 释放
        std::unique_ptr<SubListener> listener;  // reader 的数据监听器，持有 data 与用户回调
    };

public:
    FastDDSNode();
    ~FastDDSNode();
    FastDDSNode(const FastDDSNode&) = delete;
    FastDDSNode& operator=(const FastDDSNode&) = delete;

public:
    // 设置 DDS 域并惰性创建 participant/publisher/subscriber；仅可成功一次，已创建或创建失败返回 false。
    // nodeName 非空时下沉为参与者名称（DomainParticipantQos::name，随 SPDP 发现传播给同域对端，
    // 创建时固化不可改；string_255 定长 256 字节，超长自动截断），空名不置名（participant 保持
    // Fast DDS 默认名 RTPSParticipant；默认参，兼容既有调用）。
    // spec：participant 级配置（XML 配置通道 + 发现服务器直配，缺省两字段全空=现状行为零改动）。
    // xml 非空按三级提取链叠加（按节点名 profile → is_default_profile 默认档 → 落空保持
    // DEFAULT 底座）；serverSpec 非空强制 CLIENT + 直配 server 列表（覆盖 XML
    // discoveryServersList，节点名在提取后最终覆盖 XML name）。serverSpec 非法返回 false 并经
    // err 回填原因（可空指针，缺省不回填）。
    bool setDomainId(uint32_t domainId, const std::string& nodeName = "",
        const yomkrpc_xml::ParticipantQosSpec& spec = yomkrpc_xml::ParticipantQosSpec{},
        std::string* err = nullptr);
    // 注册订阅主题：接管 type 所有权，收到消息时经 callback 交付；主题名冲突/类型名不符/资源创建失败返回 false。
    // qos：端点 QoS 描述（缺省全哨兵=Fast DDS 端点默认底座，现状行为）；非哨兵项逐项覆盖
    // （如 reliability=BEST_EFFORT、depth；语义与 rclcpp::QoS "底座+逐项覆盖"同构）。
    bool registerSubTopic(const std::string& topicName, void* type, DataCallback callback,
        const DDSQosProfile& qos = DDSQosProfile{});
    // 注册发布主题：接管 type 所有权并创建 writer；主题名冲突/类型名不符/writer 创建失败返回 false。
    // qos：同 registerSubTopic；缺省全哨兵=现状行为，非哨兵项逐项覆盖。
    bool registerPubTopic(const std::string& topicName, void* type,
        const DDSQosProfile& qos = DDSQosProfile{});
    // 同步发布一条消息（data 借用，write 内部完成序列化/拷贝）；主题未注册发布/data==nullptr/write 失败返回 false。
    bool publish(const std::string& topicName, const void* data);
    // 借出 writer 池内样本（仅 plain 类型支持）：成功返回 true 且 sample 指向池内缓冲，填值后 publish 免序列化；
    // 失败（非 plain/池耗尽）返回 false 且 sample=nullptr，回退普通发布路径。借出指针 write/discard 后失效。
    bool loan(const std::string& topicName, void*& sample);
    // 归还未发布的借出样本（loan 借出但不 publish 时须调用，否则池泄漏）；writer 不存在或 sample==nullptr 返回 false。
    bool discardLoan(const std::string& topicName, void*& sample);

private:
    // 复用或新建主题：已存在则校验类型名一致（不符返回 nullptr），否则 create_topic 并登记到 topics_。
    eprosima::fastdds::dds::Topic* getOrCreateTopic(const std::string& topicName, const std::string& typeName);

private:
    eprosima::fastdds::dds::DomainParticipant* participant_ = nullptr;
    eprosima::fastdds::dds::Publisher* publisher_ = nullptr;        // 发布者，拥有全部 writer
    eprosima::fastdds::dds::Subscriber* subscriber_ = nullptr;      // 订阅者，拥有全部 reader
    std::map<std::string, eprosima::fastdds::dds::Topic*> topics_;  // topicName → Topic，发布/订阅共享
    std::map<std::string, PubInfo> pubTopics_;
    std::map<std::string, SubInfo> subTopics_;
    std::mutex mtx_;  // 串行化本节点全部公开方法
};

#endif  // FASTDDSNODE_H
