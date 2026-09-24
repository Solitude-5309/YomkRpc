#ifndef FASTDDSDEBUGNODE_H
#define FASTDDSDEBUGNODE_H

#include <condition_variable>
#include <cstdint>
#include <fastdds/dds/builtin/topic/PublicationBuiltinTopicData.hpp>
#include <fastdds/dds/builtin/topic/SubscriptionBuiltinTopicData.hpp>
#include <fastdds/rtps/builtin/data/ParticipantBuiltinTopicData.hpp>
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
    class DebugParticipantListener;  // 参与者监听器：writer/reader 发现回调 → onWriterDiscovered/onReaderDiscovered
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
    // listTopics 收敛查询默认参数：连续 5 次快照集合不变即收敛（2 次窗口过短，域内 participant
    // 间歇上线时易误判提前收敛漏主题）、200ms 快照轮询间隔（默认窗口约 1s）
    static constexpr uint32_t kDefaultStableRounds = 5;
    static constexpr uint32_t kDefaultIntervalMs = 200;
    // 设置 DDS 域并创建 participant（挂发现监听）与 subscriber；仅可成功一次，失败/重复调用返回 false。
    bool setDomainId(uint32_t domainId);
    // 登记待调试主题：发现匹配的远端 DataWriter 后由工作线程自动解析类型并建立订阅者；
    // 未 setDomainId / 重复登记返回 false；类型解析暂不可用时保持登记，工作线程持续重试。
    bool subscribeTopic(const std::string& topicName);
    // 设置输出目的地；须在订阅实际建立前调用（工作线程建订阅时按值捕获），用于测试注入。
    void setOutputSink(OutputSink sink);
    // 列出已发现的远端主题与数据类型名（自适应收敛查询，基于发现缓存：远端 DataWriter 与
    // DataReader 均记录——仅有订阅者而无发布者的主题同样列出；本节点入域时 EDP 全量重放既有
    // 端点发现信息，域内既有 participant 上新增端点经实时发现事件追加，缓存单调累积）：
    // 轮询缓存快照，连续 stableRounds 次集合不变即认为发现收敛，以最终快照填充
    // topics 返回 true（未 setDomainId 返回 false，入域后列表可为空）。stableRounds=1 即单次
    // 快照（免等待的快速查询语义）；等待期间远端新增端点会使快照变化并把不变计数重置为 1。
    // stableRounds/intervalMs 传 0 时钳制为默认 5 次/200ms；阻塞调用（最长约
    // stableRounds*intervalMs），等待中 Ctrl+C 依赖进程信号处理。集合不变判定基于有序对比较
    // （快照合并后按主题名排序）。调用方无需再自设发现等待窗口。
    bool listTopics(std::vector<std::pair<std::string, std::string>>& topics,
            uint32_t stableRounds = kDefaultStableRounds,
            uint32_t intervalMs = kDefaultIntervalMs);
    // 查询单主题发现详情（独立收敛查询，与 listTopics 互不影响）：先检查入域状态，再轮询
    // 该主题的详情快照（found 标志 + 类型名 + writer/reader 端点计数），连续 stableRounds
    // 次不变即认为收敛返回。命中（任一 writer 或 reader 已发现）返回 true 并填充 typeName
    // （原始 DDS 类型名，不做任何转换）、publisherCount/subscriptionCount（远端端点实例数）；
    // 收敛时仍未见该主题返回 false（未 setDomainId 亦 false）。0 值钳制默认 5 次/200ms；
    // 最长阻塞约 stableRounds*intervalMs；等待期间远端新增端点使快照变化并重置计数。
    bool topicInfo(const std::string& topicName, std::string& typeName,
            size_t& publisherCount, size_t& subscriptionCount,
            uint32_t stableRounds = kDefaultStableRounds,
            uint32_t intervalMs = kDefaultIntervalMs);
    // 列出域内已发现的命名参与者（独立收敛查询，与 listTopics/topicInfo 互不影响）：先检查
    // 入域状态，再轮询参与者名称快照，连续 stableRounds 次不变即认为收敛返回。仅列出
    // participant_name 非空且非 "/" 的参与者（空名跳过；"/" 是 ROS2 参与者的默认占位名——
    // rmw_fastrtps 把参与者名统一置为根 enclave "/"（rcl_init 兜底），节点名走另一通道，无
    // 辨识价值——同样跳过），不输出 GUID 串；同名多参与者各自一行；发现缓存不含自身（自身
    // 不在发现回调中）。未 setDomainId 返回 false；域内无有效命名参与者时返回 true 且
    // names 为空。0 值钳制默认 5 次/200ms；最长阻塞约 stableRounds*intervalMs；等待期间
    // 新参与者入域会使快照变化并重置计数。
    bool nodeList(std::vector<std::string>& names,
            uint32_t stableRounds = kDefaultStableRounds,
            uint32_t intervalMs = kDefaultIntervalMs);

private:
    // listTopics 的收敛轮询辅助（私有实现细节）：假定调用方已完成入域检查，仅承担快照轮询
    // 循环——每次快照锁内合并拷贝 seen_ 与 seenReaders_（writer 优先、reader 补缺，按主题名
    // 排序），连续 stableRounds 次相同即以最终快照填充 topics 返回 true；等待期间远端新增
    // writer/reader 使快照变化并重置计数。
    bool waitForTopicsStable(std::vector<std::pair<std::string, std::string>>& topics,
            uint32_t stableRounds, uint32_t intervalMs);
    // topicInfo 的收敛轮询辅助（独立于 waitForTopicsStable）：快照为单个目标主题的详情四元组
    // （found 标志 + 类型名 + writer 端点计数 + reader 端点计数），连续 stableRounds 次不变即
    // 收敛；caller 已完成入域检查。命中的最终快照填充输出并返回 true，未发现返回 false。
    bool waitForTopicInfoStable(const std::string& topicName, std::string& typeName,
            size_t& publisherCount, size_t& subscriptionCount,
            uint32_t stableRounds, uint32_t intervalMs);
    // nodeList 的收敛轮询辅助（独立于 waitForTopicsStable/waitForTopicInfoStable）：快照为
    // 域内有效命名参与者名称列表（名称非空且非 "/" 者——空名与 ROS2 占位名 "/" 跳过——按名称
    // 排序），连续 stableRounds 次不变即收敛；caller 已完成入域检查。最终快照填充输出并返回 true。
    bool waitForNodesStable(std::vector<std::string>& names,
            uint32_t stableRounds, uint32_t intervalMs);
    // 发现线程回调入口（DebugParticipantListener 转发）：新见 writer 追加进 seen_ 列表后唤醒
    // 工作线程（发现事件对同一端点不重发，缓存供登记晚于发现时回放）；回调内不建订阅。回调运行于
    // Fast DDS 发现锁临界区内，仅拿 seenMtx_ 叶子锁（锁序倒置死锁防护见成员注释）。
    bool onWriterDiscovered(const std::string& topicName,
                            const eprosima::fastdds::rtps::PublicationBuiltinTopicData& info);
    // 发现线程回调入口（DebugParticipantListener 转发）：新见 reader 追加进 seenReaders_ 列表
    // （不唤醒工作线程——订阅建立仅由 writer 驱动，reader 缓存供 listTopics 列出仅有订阅者
    // 的主题与 topicInfo 统计订阅者数）。回调运行于 Fast DDS PDP 锁临界区内，仅拿 seenMtx_ 叶子锁。
    bool onReaderDiscovered(const std::string& topicName,
                            const eprosima::fastdds::rtps::SubscriptionBuiltinTopicData& info);
    // 发现线程回调入口（DebugParticipantListener 转发）：新见 participant 缓存 GUID 串与
    // 名称（幂等去重；不唤醒工作线程——订阅建立链路不依赖参与者发现）。回调运行于 Fast DDS
    // PDP 锁临界区内，仅拿 seenMtx_ 叶子锁（锁序倒置防护同上）。
    void onParticipantDiscovered(const eprosima::fastdds::rtps::ParticipantBuiltinTopicData& info);
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
    // 已发现 writer 缓存（topic → 发现信息列表，同主题每个远端 DataWriter 各占一项）：发现
    // 事件不重发，供登记晚于发现时回放；列表长度即该主题远端发布者数（topicInfo 计数源，
    // listTopics 快照取列表首项类型名）
    std::map<std::string, std::vector<eprosima::fastdds::rtps::PublicationBuiltinTopicData>> seen_;
    // 已发现 reader 缓存（topic → 发现信息列表）：与 seen_ 并列，供 listTopics 列出仅有
    // 订阅者而无发布者的主题、供 topicInfo 统计订阅者数（订阅建立链路不依赖此缓存）
    std::map<std::string, std::vector<eprosima::fastdds::rtps::SubscriptionBuiltinTopicData>> seenReaders_;
    // 已发现参与者缓存（GUID 串 → participant_name）：与 seen_/seenReaders_ 并列，供
    // nodeList 列出域内命名参与者（GUID 串仅作幂等去重 key，不对外输出）
    std::map<std::string, std::string> seenParticipants_;
    std::map<std::string, DebugSub> subs_;  // 已建立订阅
    std::mutex mtx_;  // 串行化公开方法与工作线程（participant_/pending_/subs_/workerRunning_）
    // 发现缓存专用叶子锁（保护 seen_/seenReaders_/seenParticipants_）：发现回调在 Fast DDS 持有 PDP/EDP 内部
    // 锁的临界区内被调用，只允许拿此锁——若拿 mtx_ 会与工作线程（tryStartSubscription 持
    // mtx_ 调 create_topic/create_datareader，内部等 PDP 锁）形成锁序倒置死锁（实测 AB-BA）。
    // 锁序固定：mtx_ → seenMtx_，seenMtx_ 永不反向嵌套。
    std::mutex seenMtx_;
    std::condition_variable cv_;  // 发现/登记变化时唤醒工作线程
    std::thread worker_;          // 订阅建立工作线程（setDomainId 成功后启动）
    bool workerRunning_ = false;  // 工作线程生命周期标志（mtx_ 保护）
};

#endif  // FASTDDSDEBUGNODE_H
