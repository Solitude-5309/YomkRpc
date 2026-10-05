/**
 * @file TestFastDDSBagNodeInfo.cpp
 * @brief FastDDSBagNode 节点层 bag info 测试（直测静态函数，DDS-free）
 *
 * 范围：bagInfoText 对手写 metadata.json 临时 bag 目录的全行输出断言（标签 19 列、
 *       多主题/多分片续行缩进对齐、大小/时间格式化边界）与三类失败路径契约。
 *       经 record 真实落盘的产物断言归 TestFastDDSBagNodeRecord；服务层回执链路归
 *       TestYomkRpcBagServiceContract。
 * 覆盖：
 *   I1 单主题全行断言（首行空行、七段标签行逐行精确匹配、Bag size=目录两文件字节和）；
 *   I2 多主题两行（续行 19 空格缩进对齐）；
 *   I3 多分片 Files 续行缩进；
 *   I4 Bag size 边界（1023→"1023 B"、1024→"1.0 KiB"、2048→"2.0 KiB"）；
 *   I5 时间格式（TZ=UTC 固定时区：epoch 0 的 %e 空格填充日 + 9 位纳秒全零）；
 *   I6 路径不存在 → false "bag path [..] does not exist"；
 *   I7 目录存在无 metadata.json → false "could not find metadata.json in bag directory [..]"；
 *   I8 JSON 损坏 → false "parse metadata.json failed: ..." 且 outLines 清空；
 *   I9 空 topics → "Topic information: " 空内容行（对齐参考实现防御行为）。
 *
 * 用例目录避开 bag_ 前缀（防泄漏快照误报惯例），结束统一清理。
 *
 * 风格：纯 main() + CHECK 宏 + 失败计数（零第三方依赖），返回非 0 表示存在失败用例。
 */

#include "TestCheck.h"
#include "FastDDSBagNode.h" // 被测静态函数 bagInfoText（DDS-free 直测）

#include <cstdint>
#include <cstdlib> // setenv
#include <ctime>   // tzset
#include <filesystem>
#include <fstream>
#include <string>
#include <vector>

namespace
{
    constexpr int kLabelWidth = 19; // info 标签列宽（"Topic information: " 宽度）

    // 标签行期望值拼装（与节点侧 bagInfoLabeledLine 同构：标签补齐 19 列 + 内容）
    std::string labeled(const std::string &label, const std::string &content)
    {
        return label + std::string(kLabelWidth - label.size(), ' ') + content;
    }

    // 造临时 bag 目录：手写 metadata.json（字段与节点写侧产物同构）+ 定长假数据文件
    // （字节数参与 Bag size 目录递归累加）；dataFileBytes=0 不造数据文件
    bool makeBagDir(const std::string &dir, const std::string &metadataJson,
                    std::uintmax_t dataFileBytes)
    {
        std::error_code ec;
        std::filesystem::remove_all(dir, ec);
        if (!std::filesystem::create_directory(dir, ec))
        {
            return false;
        }
        std::ofstream meta(dir + "/metadata.json", std::ios::binary);
        if (!meta)
        {
            return false;
        }
        meta << metadataJson;
        meta.close();
        if (!meta)
        {
            return false;
        }
        if (dataFileBytes == 0)
        {
            return true;
        }
        std::ofstream data(dir + "/bag_0.mcap", std::ios::binary);
        if (!data)
        {
            return false;
        }
        const std::string chunk(1024, 'x');
        for (std::uintmax_t left = dataFileBytes; left > 0;)
        {
            const std::uintmax_t n = left < chunk.size() ? left : chunk.size();
            data.write(chunk.data(), static_cast<std::streamsize>(n));
            if (!data)
            {
                return false;
            }
            left -= n;
        }
        data.close();
        return static_cast<bool>(data);
    }

    void cleanupDir(const std::string &dir)
    {
        std::error_code ec;
        std::filesystem::remove_all(dir, ec);
    }
} // namespace

int main()
{
    // 固定时区：Start/End 行人类可读段断言不随执行环境时区漂移
    setenv("TZ", "UTC", 1);
    tzset();

    // 单主题 metadata（含写侧 _format 伴生键，字段同构 dump(4) 产物；伴生键读侧不消费）
    const std::string metaSingle = R"({
    "version": 1,
    "storage_identifier": "mcap",
    "relative_file_paths": [
        "bag_0.mcap"
    ],
    "starting_time": {
        "nanoseconds_since_epoch": 1570799349123456789,
        "nanoseconds_since_epoch_format": "2019-10-11_06-09-09_123-456-789"
    },
    "duration": {
        "nanoseconds": 12000000000,
        "nanoseconds_format": "00-00-12_000-000-000"
    },
    "message_count": 13,
    "topics_with_message_count": [
        {
            "topic_metadata": {
                "name": "/rt/a",
                "type": "YomkRpc::MString"
            },
            "message_count": 5
        }
    ]
}
)";

    // ---- I1：单主题全行断言（含首行空行、19 列标签） ----
    {
        const std::string dir = "info_i1";
        CHECK(makeBagDir(dir, metaSingle, 0), "I1 前置：临时 bag 目录创建成功");
        std::vector<std::string> lines;
        std::string error;
        CHECK(FastDDSBagNode::bagInfoText(dir, lines, &error), "I1 bagInfoText → true");
        CHECK(error.empty(), "I1 成功路径 error 为空");
        CHECK(lines.size() == 9, "I1 行数 = 9（空行 + 七段标签 + 单主题行）");
        if (lines.size() == 9)
        {
            CHECK(lines[0].empty(), "I1 首行为空行");
            CHECK(lines[1] == labeled("Files:", "bag_0.mcap"), "I1 Files 行");
            // Bag size = 目录递归总大小（仅 metadata.json 一个文件，< 1 KiB 计 B 零小数）
            const auto metaBytes = static_cast<std::uintmax_t>(metaSingle.size());
            CHECK(lines[2] == labeled("Bag size:", std::to_string(metaBytes) + " B"),
                  "I1 Bag size 行（metadata.json 自身字节计内）");
            CHECK(lines[3] == labeled("Storage id:", "mcap"), "I1 Storage id 行");
            CHECK(lines[4] == labeled("Duration:", "12.000000000s"), "I1 Duration 行");
            CHECK(lines[5] == labeled("Start:",
                      "Oct 11 2019 13:09:09.123456789 (1570799349.123456789)"),
                  "I1 Start 行（UTC 人类可读 + epoch 秒.9 位纳秒）");
            CHECK(lines[6] == labeled("End:",
                      "Oct 11 2019 13:09:21.123456789 (1570799361.123456789)"),
                  "I1 End 行（= Start + duration）");
            CHECK(lines[7] == labeled("Messages:", "13"), "I1 Messages 行");
            CHECK(lines[8] == labeled("Topic information:",
                      "Topic: /rt/a | Type: YomkRpc::MString | Count: 5 | Serialization Format: cdr"),
                  "I1 Topic information 行");
        }
        cleanupDir(dir);
    }

    // ---- I2：多主题两行（续行 19 空格缩进对齐） ----
    {
        const std::string meta = R"({
    "version": 1,
    "storage_identifier": "mcap",
    "relative_file_paths": ["bag_0.mcap"],
    "starting_time": {"nanoseconds_since_epoch": 1570799349123456789},
    "duration": {"nanoseconds": 12000000000},
    "message_count": 13,
    "topics_with_message_count": [
        {"topic_metadata": {"name": "/rt/a", "type": "YomkRpc::MString"}, "message_count": 5},
        {"topic_metadata": {"name": "/rt/b", "type": "YomkRpc::MString"}, "message_count": 8}
    ]
}
)";
        const std::string dir = "info_i2";
        CHECK(makeBagDir(dir, meta, 0), "I2 前置：两主题 bag 目录创建成功");
        std::vector<std::string> lines;
        CHECK(FastDDSBagNode::bagInfoText(dir, lines, nullptr), "I2 bagInfoText → true");
        CHECK(lines.size() == 10, "I2 行数 = 10（九段 + 第二主题续行）");
        if (lines.size() == 10)
        {
            const std::string rowA =
                "Topic: /rt/a | Type: YomkRpc::MString | Count: 5 | Serialization Format: cdr";
            const std::string rowB =
                "Topic: /rt/b | Type: YomkRpc::MString | Count: 8 | Serialization Format: cdr";
            CHECK(lines[8] == labeled("Topic information:", rowA), "I2 首主题行跟标签");
            CHECK(lines[9] == std::string(kLabelWidth, ' ') + rowB, "I2 续行 19 空格缩进对齐");
        }
        cleanupDir(dir);
    }

    // ---- I3：多分片 Files 续行缩进 ----
    {
        const std::string meta = R"({
    "version": 1,
    "storage_identifier": "mcap",
    "relative_file_paths": ["bag_0.mcap", "bag_1.mcap", "bag_2.mcap"],
    "starting_time": {"nanoseconds_since_epoch": 1570799349123456789},
    "duration": {"nanoseconds": 12000000000},
    "message_count": 13,
    "topics_with_message_count": [
        {"topic_metadata": {"name": "/rt/a", "type": "YomkRpc::MString"}, "message_count": 13}
    ]
}
)";
        const std::string dir = "info_i3";
        CHECK(makeBagDir(dir, meta, 0), "I3 前置：三分片 bag 目录创建成功");
        std::vector<std::string> lines;
        CHECK(FastDDSBagNode::bagInfoText(dir, lines, nullptr), "I3 bagInfoText → true");
        CHECK(lines.size() == 11, "I3 行数 = 11（九段 + 两个分片续行）");
        if (lines.size() == 11)
        {
            CHECK(lines[1] == labeled("Files:", "bag_0.mcap"), "I3 首分片行跟标签");
            CHECK(lines[2] == std::string(kLabelWidth, ' ') + "bag_1.mcap", "I3 分片续行 1 缩进");
            CHECK(lines[3] == std::string(kLabelWidth, ' ') + "bag_2.mcap", "I3 分片续行 2 缩进");
        }
        cleanupDir(dir);
    }

    // ---- I4：Bag size 边界（1023→B、1024→KiB、2048→2.0 KiB） ----
    {
        // metadata 固定一份，数据文件补差值使目录总和精确命中换算边界
        const std::string meta = R"({"version":1,"storage_identifier":"mcap",
"relative_file_paths":["bag_0.mcap"],"starting_time":{"nanoseconds_since_epoch":0},
"duration":{"nanoseconds":0},"message_count":0,"topics_with_message_count":[]})";
        const auto metaBytes = static_cast<std::uintmax_t>(meta.size());
        CHECK(metaBytes < 1023, "I4 前置：metadata.json 小于 1023 字节（差值数据可凑边界）");
        const std::string dir = "info_i4";
        std::vector<std::string> lines;

        CHECK(makeBagDir(dir, meta, 1023 - metaBytes), "I4 前置：1023 边界目录创建成功");
        CHECK(FastDDSBagNode::bagInfoText(dir, lines, nullptr), "I4 总 1023 字节 → true");
        CHECK(lines.size() == 9 && lines[2] == labeled("Bag size:", "1023 B"),
              "I4 总 1023 字节 → \"1023 B\"");
        cleanupDir(dir);

        CHECK(makeBagDir(dir, meta, 1024 - metaBytes), "I4 前置：1024 边界目录创建成功");
        CHECK(FastDDSBagNode::bagInfoText(dir, lines, nullptr), "I4 总 1024 字节 → true");
        CHECK(lines.size() == 9 && lines[2] == labeled("Bag size:", "1.0 KiB"),
              "I4 总 1024 字节 → \"1.0 KiB\"");
        cleanupDir(dir);

        CHECK(makeBagDir(dir, meta, 2048 - metaBytes), "I4 前置：2048 目录创建成功");
        CHECK(FastDDSBagNode::bagInfoText(dir, lines, nullptr), "I4 总 2048 字节 → true");
        CHECK(lines.size() == 9 && lines[2] == labeled("Bag size:", "2.0 KiB"),
              "I4 总 2048 字节 → \"2.0 KiB\"");
        cleanupDir(dir);
    }

    // ---- I5：时间格式（TZ=UTC 固定时区：epoch 0 的 %e 空格填充 + 全零纳秒） ----
    {
        const std::string meta = R"({
    "version": 1,
    "storage_identifier": "mcap",
    "relative_file_paths": [],
    "starting_time": {"nanoseconds_since_epoch": 0},
    "duration": {"nanoseconds": 0},
    "message_count": 0,
    "topics_with_message_count": []
}
)";
        const std::string dir = "info_i5";
        CHECK(makeBagDir(dir, meta, 0), "I5 前置：epoch 0 bag 目录创建成功");
        std::vector<std::string> lines;
        CHECK(FastDDSBagNode::bagInfoText(dir, lines, nullptr), "I5 bagInfoText → true");
        CHECK(lines.size() == 9, "I5 行数 = 9");
        if (lines.size() == 9)
        {
            CHECK(lines[5] == labeled("Start:",
                      "Jan  1 1970 00:00:00.000000000 (0.000000000)"),
                  "I5 epoch 0 → %e 空格填充日（\"Jan  1\"双空格）+ 9 位纳秒全零");
            CHECK(lines[6] == labeled("End:",
                      "Jan  1 1970 00:00:00.000000000 (0.000000000)"),
                  "I5 End = Start + 0");
        }
        cleanupDir(dir);
    }

    // ---- I6/I7/I8：失败路径（false + error 文案契约 + outLines 清空） ----
    {
        std::vector<std::string> lines;
        std::string error;

        CHECK(!FastDDSBagNode::bagInfoText("info_no_such_dir", lines, &error),
              "I6 路径不存在 → false");
        CHECK(error == "bag path [info_no_such_dir] does not exist", "I6 error 精确文案");
        CHECK(lines.empty(), "I6 失败后 outLines 为空");

        const std::string dir = "info_i7";
        std::error_code ec;
        std::filesystem::remove_all(dir, ec);
        CHECK(std::filesystem::create_directory(dir, ec), "I7 前置：无 metadata 的空目录创建成功");
        CHECK(!FastDDSBagNode::bagInfoText(dir, lines, &error), "I7 无 metadata.json → false");
        CHECK(error == "could not find metadata.json in bag directory [" + dir + "]",
              "I7 error 精确文案");
        cleanupDir(dir);

        const std::string dir8 = "info_i8";
        CHECK(makeBagDir(dir8, "{ not valid json", 0), "I8 前置：损坏 metadata 目录创建成功");
        CHECK(!FastDDSBagNode::bagInfoText(dir8, lines, &error), "I8 JSON 损坏 → false");
        CHECK(error.rfind("parse metadata.json failed: ", 0) == 0,
              "I8 error 前缀 parse metadata.json failed:");
        CHECK(lines.empty(), "I8 失败后 outLines 清空");
        cleanupDir(dir8);
    }

    // ---- I9：空 topics → Topic information 空内容行（对齐参考实现防御行为） ----
    {
        const std::string meta = R"({
    "version": 1,
    "storage_identifier": "mcap",
    "relative_file_paths": ["bag_0.mcap"],
    "starting_time": {"nanoseconds_since_epoch": 0},
    "duration": {"nanoseconds": 0},
    "message_count": 0,
    "topics_with_message_count": []
}
)";
        const std::string dir = "info_i9";
        CHECK(makeBagDir(dir, meta, 0), "I9 前置：空主题清单 bag 目录创建成功");
        std::vector<std::string> lines;
        CHECK(FastDDSBagNode::bagInfoText(dir, lines, nullptr), "I9 bagInfoText → true");
        CHECK(lines.size() == 9, "I9 行数 = 9（Topic information 空内容行仍输出）");
        if (lines.size() == 9)
        {
            CHECK(lines[8] == labeled("Topic information:", ""), "I9 空主题 → 标签行空内容");
        }
        cleanupDir(dir);
    }

    return testReport("TestFastDDSBagNodeInfo");
}
