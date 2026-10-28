/**
 * @file TestYomkRpcSetNaming.cpp
 * @brief 闭环 6 用例 1：set_name 类型名直改匹配矩阵（真实 DDS 端到端）
 *
 * 范围：机制层取证（主计划 qos-alias-interop 闭环 6：participant 类型表 set_name 跟随新名
 *       vs TypeObjectRegistry tid 恒定）的运行期实测。三段：
 *   S1 对称改名正验证：pub/sub 两端 MStringPubSubType 均先 set_name("std_msgs::msg::dds_::String_")
 *      再经 /register_pub_topic//register_sub_topic 注册，端到端收流 ≥1 条（改名即通）；debug
 *      节点 topicInfo 读回 Type == 新名（新名随发现数据传播的读回实证）。
 *   S2 不对称改名矩阵（实测固化）：pub 端 set_name 新名、sub 端保持原名 YomkRpc::MString，
 *      同主题对跑 → 收流 >0——两端 Fast DDS 3.x 均携带 TypeInformation，EDP 按 tid/TypeObject
 *      兼容性匹配而 tid 与名字无关（“tid 恒定”在匹配层的直接体现），set_name 改名在
 *      Fast DDS 内部对跑不隔离；与 ROS2 互通仍走名字匹配（ROS2 端不参与 XTypes，见
 *      README 5.6 与 run_ros2_interop.sh 真机实测）。
 *   S3 typename 探针（闭环 6 用例 4a 并入）：Log Info + SetCategoryFilter(全类别正则) +
 *      dup2 fd 级 stdout/stderr 捕获（设施思路同 TestFastDDSDebugNode），对跑窗口抓库日志；
 *      断言从宽——S1 的 topicInfo 新名读回已闭环主问题，TypeLookup 请求名/typename 取值以
 *      [OBSERVE] 行固化输出，唯一未定点的结论按实测写回主计划存档。
 *
 * 关键不变式（沿用 TestYomkRpcTopic）：每个 eOk 创建的节点必须在 main 返回前经 /delete_node
 *   显式删除（析构 reader 停止回调，测试局部捕获状态全程存活，无悬垂）。
 *
 * 风格：纯 main() + CHECK 宏 + 失败计数（零第三方依赖），返回非 0 表示存在失败用例。
 */

#include "TestCheck.h"
#include "FastDDSDebugNode.h"   // debug 节点直测（topicInfo 类型名读回）
#include "YomkRpcService.h"     // 服务 + DDSTopic/DDSSubRequest/DDSPublish + String + YOMK_* 宏

#include <YomkRpcMsg/YomkRpcMsg.hpp>            // YomkRpc::MString 数据类
#include <YomkRpcMsg/YomkRpcMsgPubSubTypes.hpp> // MStringPubSubType（set_name 主体）

#include <fastdds/dds/log/Log.hpp> // S3 探针：SetVerbosity/SetCategoryFilter

#include <fcntl.h>
#include <sys/select.h>
#include <unistd.h> // S3 探针：dup2 fd 级捕获（读端非阻塞 + select 排空线程）

#include <atomic>
#include <chrono>
#include <cstdint>
#include <iostream>
#include <regex>
#include <string>
#include <thread>
#include <vector>

namespace
{
    // 有效域（FastDDS 约 0-232），与既有测试 200/201/202/203/210/211/212 错开
    constexpr uint32_t TEST_DOMAIN = 205;
    // ROS2 约定类型名（std_msgs/msg/String 的 DDS 字面量形态）
    constexpr const char *kRosTypeName = "std_msgs::msg::dds_::String_";
    constexpr const char *SN_TOPIC = "/chatter_naming";     // S1 对称段/S3 探针段共用主题
    constexpr const char *SN_TOPIC_MISMATCH = "/chatter_mismatch"; // S2 不对称段

    // 构造 /create_node 请求包
    YomkPkgPtr mkNode(uint32_t domainId, const std::string &nodeName)
    {
        return YomkMkPtr(DDSNode, DDSNode{nodeName, domainId});
    }

    // 构造 /register_pub_topic 请求包（type 所有权移交服务端）
    YomkPkgPtr mkPub(const std::string &nodeName, const std::string &topicName,
                     YomkRpc::MStringPubSubType *type)
    {
        return YomkMkPtr(DDSTopic, DDSTopic{nodeName, topicName, type});
    }

    // 构造 /register_sub_topic 请求包
    YomkPkgPtr mkSub(const std::string &nodeName, const std::string &topicName,
                     YomkRpc::MStringPubSubType *type,
                     std::function<void(const void *)> cb)
    {
        return YomkMkPtr(DDSSubRequest, DDSSubRequest{nodeName, topicName, type, cb});
    }

    // 构造 /publish 请求包（负载第三参为非 const 指针，与 DDSPublish 契约一致）
    YomkPkgPtr mkPubMsg(const std::string &nodeName, const std::string &topicName,
                        YomkRpc::MString *msg)
    {
        return YomkMkPtr(DDSPublish, DDSPublish{nodeName, topicName, msg});
    }

    // S3 探针：stdout/stderr fd 级捕获（设施思路同 TestFastDDSDebugNode，双流各一管道）。
    // start 即启动独立 drain 线程持续排空管道（stop 恢复 fd 后写端不再写入但自身未关闭，
    // 阻塞 read 会永挂——实测根因：Info 日志洪窗后管道残量读完即空，read 阻塞于未关闭写端；
    // drain 线程同时防窗口内日志 > 64KB 管道缓冲时写端阻塞死锁）。
    class StdCapture
    {
    public:
        StdCapture()
        {
            ::pipe(outPipe_);
            ::pipe(errPipe_);
            // 读端非阻塞：select 只保证首次 read 有数据，循环 read 需非阻塞防末次空读阻塞
            for (int fd : {outPipe_[0], errPipe_[0]})
            {
                const int flags = ::fcntl(fd, F_GETFL, 0);
                ::fcntl(fd, F_SETFL, flags | O_NONBLOCK);
            }
            savedOut_ = ::dup(STDOUT_FILENO);
            savedErr_ = ::dup(STDERR_FILENO);
        }

        ~StdCapture()
        {
            stop();
            ::close(outPipe_[0]);
            ::close(outPipe_[1]);
            ::close(errPipe_[0]);
            ::close(errPipe_[1]);
            ::close(savedOut_);
            ::close(savedErr_);
        }

        void start()
        {
            std::fflush(stdout);
            std::fflush(stderr);
            ::dup2(outPipe_[1], STDOUT_FILENO);
            ::dup2(errPipe_[1], STDERR_FILENO);
            draining_.store(true);
            drainThread_ = std::thread([this]() { drainLoop(); });
        }

        void stop()
        {
            if (!draining_.exchange(false))
            {
                return;
            }
            std::fflush(stdout);
            std::fflush(stderr);
            ::dup2(savedOut_, STDOUT_FILENO);
            ::dup2(savedErr_, STDERR_FILENO);
            if (drainThread_.joinable())
            {
                drainThread_.join();
            }
        }

        const std::string &out() const
        {
            return outText_;
        }

        const std::string &err() const
        {
            return errText_;
        }

    private:
        // 读单次可读数据（读端已置非阻塞，无数据立即返回）
        void drainOnce(int fd, std::string &into)
        {
            char buf[4096];
            ssize_t n = 0;
            while ((n = ::read(fd, buf, sizeof(buf))) > 0)
            {
                into.append(buf, static_cast<std::size_t>(n));
            }
        }

        // 周期 select + 非阻塞读；停标志置位后最后排干一次（覆盖线程退出前残余）
        void drainLoop()
        {
            const int maxFd = (outPipe_[0] > errPipe_[0] ? outPipe_[0] : errPipe_[0]) + 1;
            while (draining_.load())
            {
                fd_set fds;
                FD_ZERO(&fds);
                FD_SET(outPipe_[0], &fds);
                FD_SET(errPipe_[0], &fds);
                timeval tv{0, 100 * 1000};
                if (::select(maxFd, &fds, nullptr, nullptr, &tv) > 0)
                {
                    if (FD_ISSET(outPipe_[0], &fds)) { drainOnce(outPipe_[0], outText_); }
                    if (FD_ISSET(errPipe_[0], &fds)) { drainOnce(errPipe_[0], errText_); }
                }
            }
            drainOnce(outPipe_[0], outText_);
            drainOnce(errPipe_[0], errText_);
        }

        int outPipe_[2] = {-1, -1};
        int errPipe_[2] = {-1, -1};
        int savedOut_ = -1;
        int savedErr_ = -1;
        std::atomic<bool> draining_{false};
        std::thread drainThread_;
        std::string outText_;
        std::string errText_;
    };

    // S1：对称改名正验证（pub/sub 同 set_name 新名 → 收流 ≥1 + topicInfo 读回新名）
    void testSymmetricRename(YomkRpcService *svc)
    {
        CHECK(svc->invoke("/create_node", mkNode(TEST_DOMAIN, "setname_pub")).m_status ==
                  YomkResponse::eOk,
              "S1 创建 setname_pub(domain 205) → eOk");
        CHECK(svc->invoke("/create_node", mkNode(TEST_DOMAIN, "setname_sub")).m_status ==
                  YomkResponse::eOk,
              "S1 创建 setname_sub(domain 205) → eOk");

        // pub 端：类型对象 new 后、移交前 set_name（宏实参位内联 new 无法先改名）
        auto *pubType = new YomkRpc::MStringPubSubType();
        pubType->set_name(kRosTypeName);
        CHECK(svc->invoke("/register_pub_topic", mkPub("setname_pub", SN_TOPIC, pubType))
                      .m_status == YomkResponse::eOk,
              "S1 pub 端 set_name(新名) + register_pub_topic → eOk");

        std::atomic<int> received{0};
        std::string lastRecv;
        std::mutex mtx;
        auto *subType = new YomkRpc::MStringPubSubType();
        subType->set_name(kRosTypeName);
        CHECK(svc->invoke("/register_sub_topic",
                          mkSub("setname_sub", SN_TOPIC, subType, [&](const void *data) {
                              auto *m = static_cast<const YomkRpc::MString *>(data);
                              std::lock_guard<std::mutex> lk(mtx);
                              lastRecv = m->data();
                              received++;
                          }))
                      .m_status == YomkResponse::eOk,
              "S1 sub 端同名 set_name + register_sub_topic → eOk");

        std::this_thread::sleep_for(std::chrono::seconds(1)); // 等待 discovery
        constexpr int kPubCount = 5;
        for (int i = 0; i < kPubCount; ++i)
        {
            YomkRpc::MString msg;
            msg.data("setname-" + std::to_string(i));
            CHECK(svc->invoke("/publish", mkPubMsg("setname_pub", SN_TOPIC, &msg)).m_status ==
                      YomkResponse::eOk,
                  "S1 publish 一条 → eOk");
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
        std::this_thread::sleep_for(std::chrono::seconds(1)); // 等待投递

        int r = received.load();
        CHECK(r > 0 && r <= kPubCount, "S1 对称改名端到端收流 (0,5]（set_name 改名即通）");
        std::cout << "[OBSERVE] S1 symmetric received=" << r << "/" << kPubCount;
        {
            std::lock_guard<std::mutex> lk(mtx);
            std::cout << " last=\"" << lastRecv << "\"";
        }
        std::cout << std::endl;

        // debug 节点 topicInfo 读回：新名随发现数据传播（直测节点层，独立 dbg 实例同域）
        {
            FastDDSDebugNode dbg;
            CHECK(dbg.setDomainId(TEST_DOMAIN), "S1 debug 节点 setDomainId(205) → true");
            std::string typeName;
            size_t pubCount = 0;
            size_t subCount = 0;
            CHECK(dbg.topicInfo(SN_TOPIC, typeName, pubCount, subCount, 5, 200),
                  "S1 topicInfo(chatter_naming) 收敛命中 → true");
            CHECK(typeName == kRosTypeName,
                  "S1 topicInfo 读回 Type == std_msgs::msg::dds_::String_（发现名即新名）");
        }

        CHECK(svc->invoke("/delete_node", YomkMkPtr(String, "setname_pub")).m_status ==
                  YomkResponse::eOk,
              "S1 删除 setname_pub → eOk（退出前清理不变式）");
        CHECK(svc->invoke("/delete_node", YomkMkPtr(String, "setname_sub")).m_status ==
                  YomkResponse::eOk,
              "S1 删除 setname_sub → eOk（退出前清理不变式）");
    }

    // S2：不对称隔离负验证（pub 新名 / sub 原名 → EDP 类型名不匹配 → 收 0）
    void testAsymmetricIsolation(YomkRpcService *svc)
    {
        CHECK(svc->invoke("/create_node", mkNode(TEST_DOMAIN, "mismatch_pub")).m_status ==
                  YomkResponse::eOk,
              "S2 创建 mismatch_pub(domain 205) → eOk");
        CHECK(svc->invoke("/create_node", mkNode(TEST_DOMAIN, "mismatch_sub")).m_status ==
                  YomkResponse::eOk,
              "S2 创建 mismatch_sub(domain 205) → eOk");

        auto *pubType = new YomkRpc::MStringPubSubType();
        pubType->set_name(kRosTypeName); // 仅 pub 端改名
        CHECK(svc->invoke("/register_pub_topic", mkPub("mismatch_pub", SN_TOPIC_MISMATCH, pubType))
                      .m_status == YomkResponse::eOk,
              "S2 pub 端 set_name(新名) + register_pub_topic → eOk");

        std::atomic<int> received{0};
        std::string lastRecv;
        std::mutex mtx;
        auto *subType = new YomkRpc::MStringPubSubType(); // sub 端保持原名
        CHECK(svc->invoke("/register_sub_topic",
                          mkSub("mismatch_sub", SN_TOPIC_MISMATCH, subType,
                                [&](const void *data) {
                                    auto *m = static_cast<const YomkRpc::MString *>(data);
                                    std::lock_guard<std::mutex> lk(mtx);
                                    lastRecv = m->data();
                                    received++;
                                }))
                      .m_status == YomkResponse::eOk,
              "S2 sub 端保持原名 + register_sub_topic → eOk");

        std::this_thread::sleep_for(std::chrono::seconds(1));
        for (int i = 0; i < 5; ++i)
        {
            YomkRpc::MString msg;
            msg.data("mismatch-" + std::to_string(i));
            CHECK(svc->invoke("/publish", mkPubMsg("mismatch_pub", SN_TOPIC_MISMATCH, &msg))
                          .m_status == YomkResponse::eOk,
                  "S2 publish 一条 → eOk");
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
        std::this_thread::sleep_for(std::chrono::seconds(2)); // 观察窗

        // 实测固化（原计划预判收 0 被推翻）：两端 Fast DDS 3.x 均携带 TypeInformation，EDP
        // 走 tid/TypeObject 兼容性匹配而 tid 与名字无关（闭环 6 机制取证“tid 恒定”在匹配
        // 层的直接体现）→ set_name 改名在 Fast DDS 内部对跑不隔离；与 ROS2 互通仍走名字
        // 匹配（ROS2 端不参与 XTypes 类型发现，退化为 type_name 字符串比较，见 README 5.6
        // 与 run_ros2_interop.sh 真机实测）
        int r = received.load();
        CHECK(r > 0,
              "S2 不对称改名同主题收 >0（Fast DDS 3.x EDP 按 tid/TypeObject 兼容匹配，名字无关实测固化）");
        std::cout << "[OBSERVE] S2 asymmetric received=" << r << "/5";
        {
            std::lock_guard<std::mutex> lk(mtx);
            std::cout << " last=\"" << lastRecv << "\"";
        }
        std::cout << std::endl;

        CHECK(svc->invoke("/delete_node", YomkMkPtr(String, "mismatch_pub")).m_status ==
                  YomkResponse::eOk,
              "S2 删除 mismatch_pub → eOk（退出前清理不变式）");
        CHECK(svc->invoke("/delete_node", YomkMkPtr(String, "mismatch_sub")).m_status ==
                  YomkResponse::eOk,
              "S2 删除 mismatch_sub → eOk（退出前清理不变式）");
    }

    // S3：typename 探针（用例 4a 并入）——库日志观察窗，断言从宽
    void testTypenameProbe(YomkRpcService *svc)
    {
        // Info 级 + 全类别正则（SetCategoryFilter 为白名单语义，"匹配所有类别"等价不过滤；
        // 类别名以实际输出为准，[OBSERVE] 固化后按需收窄）
        eprosima::fastdds::dds::Log::SetVerbosity(eprosima::fastdds::dds::Log::Kind::Info);
        eprosima::fastdds::dds::Log::SetCategoryFilter(std::regex(".*"));

        StdCapture capture;
        capture.start();

        // 对跑窗口：对称改名 pair 全链路（注册/建订/发布/收流期间的库日志全部入捕获）
        CHECK(svc->invoke("/create_node", mkNode(TEST_DOMAIN, "probe_pub")).m_status ==
                  YomkResponse::eOk,
              "S3 创建 probe_pub(domain 205) → eOk");
        CHECK(svc->invoke("/create_node", mkNode(TEST_DOMAIN, "probe_sub")).m_status ==
                  YomkResponse::eOk,
              "S3 创建 probe_sub(domain 205) → eOk");

        auto *pubType = new YomkRpc::MStringPubSubType();
        pubType->set_name(kRosTypeName);
        CHECK(svc->invoke("/register_pub_topic", mkPub("probe_pub", "/chatter_probe", pubType))
                      .m_status == YomkResponse::eOk,
              "S3 probe pub 注册 → eOk");

        std::atomic<int> received{0};
        auto *subType = new YomkRpc::MStringPubSubType();
        subType->set_name(kRosTypeName);
        CHECK(svc->invoke("/register_sub_topic",
                          mkSub("probe_sub", "/chatter_probe", subType,
                                [&](const void *) { received++; }))
                      .m_status == YomkResponse::eOk,
              "S3 probe sub 注册 → eOk");

        std::this_thread::sleep_for(std::chrono::seconds(1));
        for (int i = 0; i < 3; ++i)
        {
            YomkRpc::MString msg;
            msg.data("probe-" + std::to_string(i));
            svc->invoke("/publish", mkPubMsg("probe_pub", "/chatter_probe", &msg));
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
        std::this_thread::sleep_for(std::chrono::seconds(1));
        capture.stop();

        // 设施自检断言（从宽）：对跑窗口收流 ≥1 即探针设施工作正常
        CHECK(received.load() > 0, "S3 探针对跑窗口收流 ≥1（设施自检）");

        // [OBSERVE] 固化：捕获规模 + 含 type 关键词的日志行（前 15 行、每行截 160 字符）
        const std::string &outText = capture.out();
        const std::string &errText = capture.err();
        std::cout << "[OBSERVE] S3 capture stdout=" << outText.size()
                  << "B stderr=" << errText.size() << "B" << std::endl;
        const std::string &hay = errText.size() >= outText.size() ? errText : outText;
        std::string lower = hay;
        for (char &c : lower)
        {
            if (c >= 'A' && c <= 'Z') { c = static_cast<char>(c - 'A' + 'a'); }
        }
        int shown = 0;
        std::size_t pos = 0;
        while (shown < 15 && (pos = lower.find("type", pos)) != std::string::npos)
        {
            const std::size_t lineStart = hay.rfind('\n', pos);
            const std::size_t begin = (lineStart == std::string::npos) ? 0 : lineStart + 1;
            const std::size_t lineEnd = hay.find('\n', begin);
            std::string line = hay.substr(begin,
                (lineEnd == std::string::npos ? hay.size() : lineEnd) - begin);
            if (line.size() > 160) { line = line.substr(0, 160) + "..."; }
            std::cout << "[OBSERVE] S3 log: " << line << std::endl;
            pos = (lineEnd == std::string::npos) ? hay.size() : lineEnd + 1;
            ++shown;
        }
        if (shown == 0)
        {
            std::cout << "[OBSERVE] S3 no type-keyword log lines in capture window" << std::endl;
        }

        // 恢复全局日志态（Warning 为 FastDDS 默认级别），避免污染后续用例输出
        eprosima::fastdds::dds::Log::UnsetCategoryFilter();
        eprosima::fastdds::dds::Log::SetVerbosity(eprosima::fastdds::dds::Log::Kind::Warning);

        CHECK(svc->invoke("/delete_node", YomkMkPtr(String, "probe_pub")).m_status ==
                  YomkResponse::eOk,
              "S3 删除 probe_pub → eOk（退出前清理不变式）");
        CHECK(svc->invoke("/delete_node", YomkMkPtr(String, "probe_sub")).m_status ==
                  YomkResponse::eOk,
              "S3 删除 probe_sub → eOk（退出前清理不变式）");
    }
} // namespace

int main()
{
    YOMK_INIT();

    // 注册被测服务：所有权移交框架（shared_ptr 持有），init() 内部调用后 weak_from_this() 方有效
    auto *svc = new YomkRpcService(YOMK_SERVER_P);
    CHECK(YOMK_ADD_SERVICE(svc) == 0, "YomkRpcService 注册成功（所有权移交框架，init() 已内部调用）");

    testSymmetricRename(svc);
    testAsymmetricIsolation(svc);
    testTypenameProbe(svc);

    return testReport("TestYomkRpcSetNaming");
}
