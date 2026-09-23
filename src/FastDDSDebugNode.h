#ifndef FASTDDSDEBUGNODE_H
#define FASTDDSDEBUGNODE_H

#include <condition_variable>
#include <cstdint>
#include <fastdds/dds/builtin/topic/PublicationBuiltinTopicData.hpp>
#include <fastdds/dds/domain/DomainParticipant.hpp>
#include <fastdds/dds/subscriber/DataReader.hpp>
#include <fastdds/dds/subscriber/Subscriber.hpp>
#include <fastdds/dds/topic/Topic.hpp>
#include <fastdds/dds/topic/TypeSupport.hpp>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <set>
#include <string>
#include <thread>
#include <utility>
#include <vector>

// 类型无关的调试节点：加入域后按主题名登记订阅意图，经 DDS 发现机制收到远端 DataWriter 携带的
// XTypes TypeInformation → 从全局 TypeObjectRegistry 取回远端 TypeObject → 生成 DynamicType 并
// 自动建立订阅者；收到消息经内置 json_serialize 结构化（JSON）输出，全程不依赖任何 IDL 生成类型。
// 线程模型：发现回调仅缓存发现信息并唤醒工作线程，DataReader 的创建统一在工作线程完成
// （实测发现回调线程内同步建 reader 会导致匹配成功但数据不交付）；公开方法以 mtx_ 串行化；
// 析构按停工作线程 → DataReader → Topic → Subscriber → Participant 顺序清理。
class FastDDSDebugNode
{
    class DebugParticipantListener;  // 参与者监听器：on_data_writer_discovery → onWriterDiscovered
    class DebugSubListener;          // 数据监听器：take 动态样本 → json_serialize 结构化输出
    // 订阅登记项。
    struct DebugSub
    {
        eprosima::fastdds::dds::TypeSupport type;  // 持有 DynamicPubSubType（shared_ptr 所有权）
        eprosima::fastdds::dds::TopicDataType* topicType =
            nullptr;  // 指向 type 所辖同一对象，仅供 listener create/delete_data，不单独持有所有权
        eprosima::fastdds::dds::Topic* topic = nullptr;  // 订阅主题，析构时 delete_topic
        eprosima::fastdds::dds::DataReader* reader = nullptr;
        std::unique_ptr<DebugSubListener> listener;  // reader 的数据监听器，持有样本缓冲与输出 sink
    };

public:
    FastDDSDebugNode();
    ~FastDDSDebugNode();
    FastDDSDebugNode(const FastDDSDebugNode&) = delete;
    FastDDSDebugNode& operator=(const FastDDSDebugNode&) = delete;

public:
    // 输出回调：每条消息交付一次格式化文本；未设置时默认输出到 stdout
    using OutputSink = std::function<void(const std::string&)>;
    // 设置 DDS 域并创建 participant（挂发现监听）与 subscriber；仅可成功一次，失败/重复调用返回 false。
    bool setDomainId(uint32_t domainId);
    // 登记待调试主题：发现匹配的远端 DataWriter 后由工作线程自动解析类型并建立订阅者；
    // 未 setDomainId / 重复登记返回 false；类型解析暂不可用时保持登记，工作线程持续重试。
    bool subscribeTopic(const std::string& topicName);
    // 设置输出目的地；须在订阅实际建立前调用（工作线程建订阅时按值捕获），用于测试注入。
    void setOutputSink(OutputSink sink);
    // 列出已发现的远端主题与数据类型名（基于发现缓存：本节点入域时 EDP 全量重放既有 writer
    // 发现信息，域内既有 participant 上新增 writer 经实时发现事件追加，缓存单调累积）。
    // 纯同步查询，发现重放是异步的——入域后立即查询可能得到空列表，等待窗口由调用方负责；
    // 未 setDomainId 返回 false，入域后列表可为空（返回 true）。
    bool listTopics(std::vector<std::pair<std::string, std::string>>& topics);

private:
    // 发现线程回调入口（DebugParticipantListener 转发）：首见 writer 记入 seen_ 缓存后唤醒
    // 工作线程（发现事件不重发，缓存供登记晚于发现时回放）；回调内不建订阅。
    bool onWriterDiscovered(const std::string& topicName,
                            const eprosima::fastdds::rtps::PublicationBuiltinTopicData& info);
    // 建订阅链路（自加锁，仅由工作线程调用）：TypeInformation → TypeObject → DynamicType →
    // Topic → DataReader，任一步失败放弃本次订阅；typeNotReadyWarn 控制 TypeObject 未就绪
    // 告警（工作线程重试时静默）。
    bool tryStartSubscription(const std::string& topicName,
                              const eprosima::fastdds::rtps::PublicationBuiltinTopicData& info,
                              bool typeNotReadyWarn = true);
    // 订阅建立工作线程主体：轮询 pending_ × seen_ 交集并尝试建订阅；等待由 cv_ 有界超时驱动。
    void workerLoop();

private:
    eprosima::fastdds::dds::DomainParticipant* participant_ = nullptr;
    eprosima::fastdds::dds::Subscriber* subscriber_ = nullptr;  // 订阅者，拥有全部 reader
    std::unique_ptr<DebugParticipantListener> listener_;        // participant 的发现监听器
    OutputSink sink_;  // 输出目的地，默认空 → stdout
    std::set<std::string> pending_;         // 已登记、待发现的主题（含解析失败等待重试者）
    // 已发现 writer 缓存（topic → 首见发现信息）：发现事件不重发，供登记晚于发现时回放
    std::map<std::string, eprosima::fastdds::rtps::PublicationBuiltinTopicData> seen_;
    std::map<std::string, DebugSub> subs_;  // 已建立订阅
    std::mutex mtx_;  // 串行化公开方法、发现回调与工作线程
    std::condition_variable cv_;  // 发现/登记变化时唤醒工作线程
    std::thread worker_;          // 订阅建立工作线程（setDomainId 成功后启动）
    bool workerRunning_ = false;  // 工作线程生命周期标志（mtx_ 保护）
};

#endif  // FASTDDSDEBUGNODE_H
