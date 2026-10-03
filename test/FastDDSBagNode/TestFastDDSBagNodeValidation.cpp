/**
 * @file TestFastDDSBagNodeValidation.cpp
 * @brief FastDDSBagNode 节点层启动校验测试（直测节点，不经服务层）
 *
 * 范围：record 的启动校验阶段全分支——未入域守卫、重复入域拒绝、输入清单快速校验
 *       （空清单/空名/重复）、发现收敛后的无端点主题整体报错（error 文本逐主题列出）、
 *       混合清单（有端点 + 无端点）整体报错，及校验失败不建 bag 目录不落任何文件。
 *       录制收尾落盘（mcap 内容断言）归 TestFastDDSBagNodeRecord。
 * 覆盖：
 *   V1 未入域 record → false "bag node not created"；
 *   V2 setDomainId(201) 成功 → 重复 setDomainId 拒绝（仅成功一次）；
 *   V3 清单快速校验（收敛等待之前）：空清单 → "no topics given"、含空名 →
 *      "empty topic name in topic list"、重复主题 → "duplicate topic [..] in topic list"、
 *      多通配（≥2 个 '*'）→ "仅支持单个通配"、outputDir 已存在 →
 *      "bag directory [..] already exists"（不落盘）、-b 分片上限过小（999 与下限前边界
 *      1023）→ "too small, minimum split file size is 1024"（不等待不落盘）；
 *   V4 无端点主题（域 201 无 rt/bag_val_miss 端点，慢路径等满收敛窗）→ false +
 *      error 含 "主题 [rt/bag_val_miss] 既无发布者也无订阅者" 与域号、等待时长；bagDir() 为空；
 *   V5 混合清单 {"rt/bag_val_hit"（远端 MString 发布者在线）, "rt/bag_val_miss2"} → 整体报错
 *      （error 含 miss 主题、不含 hit 主题误报），不建 bag 目录；失败后节点未定格可重试；
 *      精确命中 + 模式空命中 {"rt/bag_val_hit", "rt/bag_val_nope_*"} → 整体报错，
 *      error 含「模式 [...] 未匹配到任何主题」且不误报 hit。
 * 校验失败不落盘断言：全程 bagDir() 为空，无 bag_* 目录产生（工作目录无新增 bag_ 前缀目录）。
 *
 * 域号 201：与 TestYomkRpcBagServiceLifecycle(200)/TestFastDDSBagNodeRecord(202) 错开，
 * ctest 串行执行互不残留。
 *
 * 风格：纯 main() + CHECK 宏 + 失败计数（零第三方依赖），返回非 0 表示存在失败用例。
 */

#include "TestCheck.h"
#include "FastDDSBagNode.h" // 被测节点（直测，不经服务层）

#include <YomkRpcMsg/YomkRpcMsgPubSubTypes.hpp> // MStringPubSubType（V5 远端发布端制造"有端点"主题）

#include <fastdds/dds/domain/DomainParticipant.hpp>
#include <fastdds/dds/domain/DomainParticipantFactory.hpp>
#include <fastdds/dds/publisher/DataWriter.hpp>
#include <fastdds/dds/publisher/Publisher.hpp>
#include <fastdds/dds/topic/Topic.hpp>
#include <fastdds/dds/topic/TypeSupport.hpp>

#include <chrono>
#include <cstdint>
#include <filesystem>
#include <string>
#include <thread>
#include <vector>

namespace
{
    constexpr uint32_t TEST_DOMAIN = 201; // 独立域，避开其他测试用例
    constexpr uint64_t kTooSmallSplit = 999;  // -b 分片上限过小（低于下限 1024）
    constexpr uint64_t kBoundarySplit = 1023; // 分片下限前一边界值（仍拒绝）

    // 工作目录下已存在的 bag_ 前缀目录集合（用例前快照，用例后比对零新增）
    std::vector<std::string> snapshotBagDirs()
    {
        std::vector<std::string> dirs;
        std::error_code ec;
        for (const auto &entry : std::filesystem::directory_iterator(".", ec))
        {
            if (entry.is_directory(ec) && entry.path().filename().string().rfind("bag_", 0) == 0)
            {
                dirs.push_back(entry.path().filename().string());
            }
        }
        return dirs;
    }

    bool bagDirCreated(const std::vector<std::string> &before)
    {
        return snapshotBagDirs().size() != before.size();
    }
} // namespace

int main()
{
    const auto bagDirsBefore = snapshotBagDirs();

    // ---- V1：未入域 record → false "bag node not created" ----
    {
        FastDDSBagNode node;
        std::vector<FastDDSBagNode::BagTopicStat> stats;
        std::string error;
        const bool ok = node.record({"rt/bag_val_any"}, stats, &error);
        CHECK(!ok, "V1 未入域 record → false");
        CHECK(error.find("bag node not created") != std::string::npos,
              "V1 error 含 bag node not created");
        CHECK(node.bagDir().empty(), "V1 失败后 bagDir() 为空（未建 bag）");
    }

    // ---- V2：入域成功 + 重复入域拒绝 ----
    FastDDSBagNode node;
    CHECK(node.setDomainId(TEST_DOMAIN), "V2 setDomainId(201) 首次成功（真实创建 participant）");
    CHECK(!node.setDomainId(TEST_DOMAIN), "V2 重复 setDomainId → false（仅成功一次）");
    CHECK(!node.setDomainId(0), "V2 换域重复 setDomainId → false");

    // ---- V3：清单快速校验（先于发现收敛等待，瞬间返回） ----
    {
        std::vector<FastDDSBagNode::BagTopicStat> stats;
        std::string error;
        CHECK(!node.record({}, stats, &error), "V3 空清单 → false");
        CHECK(error.find("no topics given") != std::string::npos, "V3 空清单 → no topics given");

        CHECK(!node.record({"t_ok", ""}, stats, &error), "V3 含空名 → false");
        CHECK(error.find("empty topic name in topic list") != std::string::npos,
              "V3 含空名 → empty topic name in topic list");

        CHECK(!node.record({"t_dup", "t_dup"}, stats, &error), "V3 重复主题 → false");
        CHECK(error.find("duplicate topic [t_dup] in topic list") != std::string::npos,
              "V3 重复主题 → duplicate topic [t_dup] in topic list");

        CHECK(!node.record({"a*b*c"}, stats, &error), "V3 多通配（≥2 个 '*'）→ false");
        CHECK(error.find("仅支持单个通配") != std::string::npos,
              "V3 多通配 → 仅支持单个通配");

        // outputDir 已存在：fail-fast 于发现校验前（纯输入错误，不等待不落盘）
        // 用例目录名避开 bag_ 前缀，防泄漏快照断言（bagDirCreated）误报
        std::error_code mkEc;
        std::filesystem::create_directory("taken_dir", mkEc);
        CHECK(!mkEc, "V3 前置：taken_dir 创建成功");
        CHECK(!node.record({"rt/bag_val_noend"}, stats, &error, 0, 0, "taken_dir"),
              "V3 outputDir 已存在 → false");
        CHECK(error.find("bag directory [taken_dir] already exists") != std::string::npos,
              "V3 outputDir 已存在 → already exists 文案");
        std::error_code emptyEc;
        CHECK(std::filesystem::is_empty("taken_dir", emptyEc),
              "V3 outputDir 已存在拒绝后目录内无新文件（不落盘）");
        std::filesystem::remove("taken_dir", mkEc);

        // -b 分片上限过小：fail-fast 于发现校验前（纯输入错误，不等待不落盘；下限 1024）
        CHECK(!node.record({"rt/bag_val_small"}, stats, &error, 0, 0, "", kTooSmallSplit) &&
                  error.find("too small") != std::string::npos,
              "V3 maxBagSize=999 → false + too small 文案");
        CHECK(!node.record({"rt/bag_val_small"}, stats, &error, 0, 0, "", kBoundarySplit) &&
                  error.find("minimum split file size is 1024") != std::string::npos,
              "V3 maxBagSize=1023 边界 → false + 下限 1024 文案");
    }

    // ---- V4：无端点主题（慢路径等满收敛窗）→ 整体报错 + error 文本契约 ----
    {
        std::vector<FastDDSBagNode::BagTopicStat> stats;
        std::string error;
        const bool ok = node.record({"rt/bag_val_miss"}, stats, &error);
        CHECK(!ok, "V4 无端点主题 record → false");
        CHECK(error.find("主题 [rt/bag_val_miss] 既无发布者也无订阅者") != std::string::npos,
              "V4 error 含「主题 [...] 既无发布者也无订阅者」逐主题文本");
        CHECK(error.find("域 " + std::to_string(TEST_DOMAIN)) != std::string::npos,
              "V4 error 含域号（域 201）");
        CHECK(error.find("已等待") != std::string::npos && error.find("ms") != std::string::npos,
              "V4 error 含等待时长（已等待 M ms）");
        CHECK(stats.empty(), "V4 校验失败 stats 不回填（保持空）");
        CHECK(node.bagDir().empty(), "V4 校验失败 bagDir() 为空（未建 bag）");
        CHECK(!bagDirCreated(bagDirsBefore), "V4 校验失败无 bag_ 目录产生");
    }

    // ---- V5：混合清单 → 整体报错（hit 有远端发布者、miss 无端点） ----
    {
        namespace dds = eprosima::fastdds::dds;  // 块内别名：直连 FastDDS API 搭建远端发布端

        // 远端发布端：域 201 建 MString 发布者（仅端点在线制造"有发布者"主题，不发数据）
        auto *peerParticipant = dds::DomainParticipantFactory::get_instance()->create_participant(
            TEST_DOMAIN, dds::PARTICIPANT_QOS_DEFAULT);
        CHECK(peerParticipant != nullptr, "V5 远端发布端 participant 创建成功");
        dds::TypeSupport ts(new YomkRpc::MStringPubSubType());
        ts.register_type(peerParticipant);
        auto *pub = peerParticipant->create_publisher(dds::PUBLISHER_QOS_DEFAULT);
        auto *topic =
            peerParticipant->create_topic("rt/bag_val_hit", ts.get_type_name(), dds::TOPIC_QOS_DEFAULT);
        auto *writer =
            (pub != nullptr && topic != nullptr) ? pub->create_datawriter(topic, dds::DATAWRITER_QOS_DEFAULT)
                                                 : nullptr;
        CHECK(writer != nullptr, "V5 远端 DataWriter 创建成功（rt/bag_val_hit，仅建端点不发布数据）");

        // 混合清单整体报错：miss 无端点即整体拒绝（error 只列 miss，hit 不因有端点而通过）
        std::vector<FastDDSBagNode::BagTopicStat> stats;
        std::string error;
        const bool ok = node.record({"rt/bag_val_hit", "rt/bag_val_miss2"}, stats, &error);
        CHECK(!ok, "V5 混合清单 → false（一个无端点即整体报错）");
        CHECK(error.find("rt/bag_val_miss2") != std::string::npos,
              "V5 error 含 miss 主题（rt/bag_val_miss2）");
        CHECK(error.find("主题 [rt/bag_val_hit]") == std::string::npos,
              "V5 error 不误报有端点的 hit 主题");
        CHECK(node.bagDir().empty(), "V5 混合清单拒绝不建 bag（bagDir() 为空）");
        CHECK(!bagDirCreated(bagDirsBefore), "V5 混合清单拒绝无 bag_ 目录产生");

        // 失败后节点未定格可重试：同节点再次 record 仍能走到校验（非 record already finished）
        std::string error2;
        const bool ok2 = node.record({"rt/bag_val_miss3"}, stats, &error2);
        CHECK(!ok2, "V5 失败后重试仍走校验 → false");
        CHECK(error2.find("record already finished") == std::string::npos,
              "V5 校验失败不定格（重试报错非 record already finished）");
        CHECK(error2.find("rt/bag_val_miss3") != std::string::npos, "V5 重试 error 含新清单主题");

        // 精确命中 + 模式空命中 → 整体报错：error 含模式空命中文案、不误报有端点的 hit
        std::string error3;
        const bool ok3 = node.record({"rt/bag_val_hit", "rt/bag_val_nope_*"}, stats, &error3);
        CHECK(!ok3, "V5 精确命中 + 模式空命中 → false（整体报错）");
        CHECK(error3.find("模式 [rt/bag_val_nope_*] 未匹配到任何主题") != std::string::npos,
              "V5 error 含「模式 [...] 未匹配到任何主题」逐项文本");
        CHECK(error3.find("主题 [rt/bag_val_hit]") == std::string::npos,
              "V5 error 不误报有端点的 hit 主题");
        CHECK(node.bagDir().empty(), "V5 模式空命中拒绝不建 bag（bagDir() 为空）");
        CHECK(!bagDirCreated(bagDirsBefore), "V5 模式空命中拒绝无 bag_ 目录产生");

        // 远端发布端清理（先于 bag 节点析构亦可，二者独立参与者）
        if (peerParticipant != nullptr)
        {
            dds::DomainParticipantFactory::get_instance()->delete_participant(peerParticipant);
        }
    }

    CHECK(!bagDirCreated(bagDirsBefore), "全程无 bag_ 目录产生（校验失败不落盘）");

    return testReport("TestFastDDSBagNodeValidation");
}
