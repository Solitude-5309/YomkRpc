/**
 * @file TestFastDDSBagNodePlay.cpp
 * @brief FastDDSBagNode 节点层回放测试（真实 DDS 端到端：构造 bag → play → 透传订阅端对账）
 *
 * 范围：play 全链路——预扫 metadata.json（type/qos 解析与非法输入拒播）→ 读线程逐条预读 →
 *       播放线程按录制时间轴经透传 writer 发布 → 订阅端精确收数对账。bag 由 mcap writer +
 *       手写 metadata.json 构造（makeBag，写侧同款落盘形态：每分片自含 Channel 声明、
 *       metadata 键集同 record 组装），不依赖 record 产物。暂停/恢复（bagPlayPause/Resume
 *       的播放侧联动）与 10 项 play 选项归后续轮；服务层端点契约归
 *       TestYomkRpcBagServiceContract。
 * 覆盖：
 *   P5 预扫输入校验（失败快速返回不建任何 DDS 实体，节点可继续复用）：目录不存在 /
 *      路径是文件 / 缺 metadata.json / metadata 非 JSON / topics 空清单 / 有元信息无
 *      分片 / qos 串非法枚举名 / 多逗号 → play false 且 error 明确；
 *   P2 旧格式 bag 拒播（type 空串 → error 命中 "has no type info (legacy bag)"，有意
 *      偏离参考实现告警跳过的既有裁定）；
 *   P1 双主题基础回放对账：双主题各 30 条 @200ms（6s 时间轴）→ qos 快照
 *      RELIABLE,TRANSIENT_LOCAL 恢复发布 → 预先在场的 TRANSIENT_LOCAL 订阅端（writer
 *      history depth 1000 支持匹配建立后 latched 补发）精确收 30+30；stats 契约（预扫
 *      清单序 / type 透传 / count 精确 / bytes == count×5）；
 *   P3a QoS 快照恢复正向（reliability）：RELIABLE,TRANSIENT_LOCAL bag → BEST_EFFORT
 *      订阅端（request ≤ offered 匹配）与 RELIABLE 订阅端均可 latched 补发精确收满；
 *   P3b QoS 快照恢复负向：BEST_EFFORT,VOLATILE bag → RELIABLE 订阅端收 0（request
 *      RELIABLE > offered BEST_EFFORT，匹配不建立；短回放窗先于匹配完成，非匹配期丢样本
 *      即为收 0 的唯一可能路径）；
 *   P3c QoS 快照恢复正向（durability）：BEST_EFFORT,TRANSIENT_LOCAL bag → BEST_EFFORT
 *      订阅端 latched 补发精确收满（若 durability 误恢复 VOLATILE 则匹配建立前的样本
 *      全部丢失，16 条无法凑满——以收数反向钉死 durability 快照恢复）；
 *   P4 Ctrl+C 停止：播放中途 yomk::bagPlayStop 置位（模拟 CLI SIGINT onSignal 路径）→
 *      play 提前返回 true、stats 部分计数（1.5s @100ms 双主题远小于 80 条总量）、订阅端
 *      收数 ≤ 播出数（writer 已删，无补发窗口）；
 *   P6 多分片回放守恒：双分片 bag（bag_0/bag_1 各 10 条，时间轴连续）→ 跨分片升序游标
 *      回放 → 订阅端精确收 20、stats 守恒。
 *   P7 回放速率倍率（对齐 ros2 --rate 语义）：P7.1 rate=2.0 全程（span 1.6s → 播放
 *      耗时 ≉0.8s 窗口 0.6-1.2s）；P7.2 rate=0.5 全程（≈3.2s 窗口 2.6-4.2s）；P7.3
 *      运行时动态调率（rate=1.0 启动，第 2 条播放时刻 bagPlaySetRate(2.0)——换率重锚保
 *      已播 bag 进度连续，总耗时 ≉1.0s 窗口 0.8-1.4s）；P7.4 调率接口语义（SetRate(0)/
 *      (-1) 拒绝 false 且 GetRate 不变、SetRate(2.0) 生效、bagPlayReset 复位 1.0）；
 *      P7.5 rate=0/-1 启动（play false + invalid playback rate + stats 清空）。
 *      节奏断言口径：play 长驻阻塞线程总耗时（start→done，轮询粒度 5ms）——订阅端
 *      TRANSIENT_LOCAL latched 补发到达时刻随匹配建立漂移不可作间隔证据，消息真实发出
 *      以 stats 精确计数钉死。
 *   P8 主题过滤（--topics，对齐 convert topics 语义 + ros2 --topics 缺省语义）：P8.1
 *      单主题过滤（stats 仅入选行、订阅端 p8a 收 20 / p8b 零收）；P8.2 双主题 + 重复
 *      项去重（stats 升序各 20）；P8.3 通配展开（*p8a 单主题 / /rt/p8* 双主题）；P8.4
 *      精确项不存在（false + not found in bag + stats 清空）；P8.5 模式未命中
 *      （matched no bag topic）与双 '*'（record 惯例文案）。
 *   P9 暂停态启动（--start-paused，库层形态：bagPlayPause 预置 + 播放挂起；对齐
 *      record 同名选项先例与 ros2 --start-paused 语义）：P9.1 暂停窗订阅端零增量 +
 *      恢复全量补播 + 总耗时 ≈ 暂停窗 + 全局 span（暂停期不泄入时间轴，重锚语义）
 *      ——全局 span 1950ms（40 条 @50ms 交错），病态泄入表现为恢复后瞬间追发
 *      （总耗时塌缩到暂停窗内）；P9.2 暂停态与 --topics 过滤叠加（恢复后仅入选
 *      主题补播，被过滤主题双条件下仍零收）。
 *   P10 循环回放（-l/--loop，对齐 ros2 语义 "starts back at the beginning on reaching
 *      the end and plays indefinitely"——到尾回开头无限播放）：P10.1 循环两轮不倾泻
 *      （2.5s 窗收数 [21,35]——第二轮按录制时间轴节拍推进，轮边界不重锚的倾泻形态
 *      2.5s 内 40 条全发越上界；总耗时 ≥1900ms 钉住第一轮完整节拍）；P10.2 与
 *      --topics 叠加（回卷轮过滤仍生效）；P10.3 与暂停态启动叠加（暂停窗零增量
 *      语义 loop 下保持，跨暂停累计两轮起步）。
 *   P11 起始偏移（--start-offset，对齐 ros2 语义 "Start the playback player this many
 *      seconds into the bag file"——从 bag 开始第 SEC 秒起播，读侧跳过前段消息不入队
 *      不计统计不告警，偏移基准=过滤前全场首条 logTime）：P11.1 单轮跳段节拍（kP8Bag
 *      offset=1.0s 自然播毕，双主题各精确 10 条——播段 t≥1000/t≥1050 的后段 10 条；
 *      总耗时 ≈ 播段 span 950ms，跳过失效倾泻形态越下界）；P11.2 offset 超全场时长
 *      （零消息播出、播毕成功返回、快速返回）；P11.3 loop×offset 叠加（专用 kP11Bag
 *      每轮仅末条可播，5s 窗收数 ≥15 钉住"每轮回卷保持偏移"——对齐 ros2 do-while
 *      每轮 seek 回偏移点；跳过段读侧不被播放节拍反压、回卷全速空转，收数随读侧
 *      速度浮动故不设上界；回卷从零的错误形态每轮 6 条被 span 2500ms 节拍反压、
 *      5s 收 ≤12 条越下界）；P11.4 负值启动
 *      （false + invalid start offset + stats 清空，对齐 P7.5 rate 校验用例形态）。
 *   P12 开播延迟（-d/--delay，对齐 ros2 语义 "Sleep before play. Negative durations
 *      invalid. Will delay at the beginning of each loop."——播放线程在每轮轮边界
 *      （首轮首条与每轮回卷轮）重锚前先睡 SEC 秒再播出——读播解耦下读侧全速预读入队，
 *      延迟须落播放侧才能挡住每轮播出起点；sleep 期间发布实体已建为订阅端留匹配窗口；
 *      节点层只拒负值，0 为合法缺省不延迟）：P12.1 单轮延迟（kP8Bag delay=1.0s——启动后 500ms 窗双
 *      订阅端零增量钉住 delay 窗零发出（未生效形态已发 ~5 条即拒）；waitForCount
 *      双主题各 20 收满；自然播毕耗时含 delay 窗 [2600,4200]ms，未生效形态
 *      ≈2.15-2.65s 越下界）；P12.2 loop×delay 每轮开头生效（delay=0.5s 2.7s 窗收数
 *      [15,20]——首轮 0.5s 窗+span 1.95s+开销 ≈2.45-2.95s 播毕、第二轮回卷后轮边界
 *      再睡 0.5s 首条 = 播毕+0.5 ≥2.95s 必不在 2.7s 窗内——上界 20 零竞态误报钉住
 *      "每轮开头延迟"（3.0s 窗在空载时序下第二轮首条 2.95-3.0s 撞窗边界竞态实测
 *      21 越上界，缩 2.7s 消除）；delay 仅首轮生效形态第二轮 ≈2.45s 即播 2.7s 窗
 *      收 ≥21 越上界）；P12.3 负值启动（false +
 *      invalid delay + stats 清空，对齐 P11.4 形态）。
 *   P13 退出确认等待（--wait-for-all-acked，对齐 ros2 语义 "退出前等全部 RELIABLE 订阅者
 *      确认收齐（0=永等）"——播毕或停止后、销毁发布实体前逐 RELIABLE writer 分片等 ACK，
 *      BEST_EFFORT 跳过；超时/中断仅告警不改变返回值）：P13.2 无订阅端永等不挂死
 *      （kP13Bag waitAckedMs=0，无 matched RELIABLE reader → wait 立即通过——空集全确认，
 *      ok=true 正常播毕；先于 P13.1 执行——P13.1 的 reader 在场会使"无订阅端"失真）；
 *      P13.1 显式生效+播毕收满（双 RELIABLE 订阅端在线 waitAckedMs=2000，双主题各 20
 *      收满正常播毕——进程内 ACK 快，wait 不 hang 不误报）；P13.3 BEST_EFFORT 跳过
 *      （复用 kP3cBag，writer 侧 BEST_EFFORT 不参与 ackNack 直接跳过，waitAckedMs=100
 *      快速播毕）；P13.4 超时不失败（waitAckedMs=1 极小片——进程内 ACK 毫秒级可能过
 *      可能超时，两种结果都 ok=true，超时告警不改返回值不断言告警文本）。
 *   P14 主题重映射（--remap old:=new ...，对齐 ros2 发布实体命名层精确匹配语义）：P14.1
 *      单主题重映射（kP8Bag /rt/p8a:=/rt/remap_a——新名 reader 精确收 20、旧名 reader
 *      零增量（数据迁新名无 matched）、未 remap 的 p8b 照常 20；stats 显示 remap 后
 *      新名）；P14.2 多规则全 remap（双规则双主题全迁新名各收 20，stats 两行全新名，
 *      旧名 reader 全零增量）；P14.3 与 --topics 叠加（过滤按旧名先过滤后 remap——
 *      ra3 收 20、p8b 被过滤零发，stats 单行新名）；P14.4 非法格式+重复 old 拒播
 *      （缺 ':=' → invalid remap rule、old 重复 → duplicate remap rule，均 false +
 *      stats 清空，对齐 P12.3 形态）；P14.5 old 不在 bag 静默无效（规则不命中即无
 *      效果——正常播毕、原名照常 +20，对齐 ros2 rcl remap 语义）。
 *   P15 预读队列深度（--read-ahead-queue-size，对齐 ros2 语义 "size of message queue
 *      rosbag tries to hold in memory to help deterministic playback"——读侧 PlayQueue
 *      背压边界，大→占内存但防播放延迟；writer QoS 独立常量不联动）：P15.1 小深度
 *      守恒（queue=1 读播交替极端形态，双 RELIABLE,TL reader 各精确 +20，stats 双行
 *      topic/count/bytes 全精确）；P15.2 大深度守恒（queue=10000 ≥整场 40 零背压，
 *      各精确 +20，stats 全精确）；P15.3 零值拒播（0 容量队列 push 背压恒真死锁必拦
 *      → false + invalid read-ahead queue size + stats 清空，对齐 P12.3 形态）。
 *   P16 QoS 覆盖文件（--qos-profile-overrides-path，对齐 ros2 同名选项语义，本仓
 *      YAML 换 JSON：顶层键=bag 原主题名，值={reliability, durability} 枚举名可独立
 *      给——文件命中 > bag 内快照 > 旧 bag 回退三档优先级最高档）：P16.1 reliability
 *      覆盖生效（p8a 覆盖 BEST_EFFORT+VOLATILE——RELIABLE,TL reader 匹配不建立收 0、
 *      未覆盖 p8b 照常 20，反向钉死覆盖生效）；P16.2 durability 覆盖+未知键忽略+
 *      不在清单静默（p8b 覆盖 VOLATILE——TL reader 匹配不建立收 0；p8a 恒等覆盖+
 *      未知字段键忽略+absent 主题键静默无效——照常 20）；P16.3 错误路径三连（非法
 *      枚举 → topic invalid qos string、文件不存在 → open failed、坏 JSON → parse
 *      failed，均 false + stats 清空，对齐 P12.3 形态）。
 *   P17 set_name 类型 bag 回放（闭环 6）：metadata type 字段为新名 → 回放端按发现名
 *      重建透传 writer → 订阅端同名透传 reader 收流成功（类型名在录制-回放-订阅全链透传）。
 *
 * 域号 203：与 Lifecycle(200)/Validation(201)/Record(202) 错开，ctest 串行执行互不残留。
 * bag 目录为固定命名（play_p5_* / play_test_*），每用例收尾 remove_all 自清理（不落
 * 时间戳目录，无运行后残留）。
 *
 * 风格：纯 main() + CHECK 宏 + 失败计数（零第三方依赖），返回非 0 表示存在失败用例。
 */

#include "TestCheck.h"
#include "FastDDSBagNode.h"     // 被测节点（直测，不经服务层）
#include "YomkRpcBagService.h"  // yomk::bagPlayStop/bagPlayReset（停止标志读写端）

#include <mcap/writer.hpp>      // makeBag 造分片（MCAP_IMPLEMENTATION 单译元在被测库内）
#include <nlohmann/json.hpp>    // makeBag 手写 metadata.json（record 组装同款键集）

#include <fastdds/dds/domain/DomainParticipant.hpp>
#include <fastdds/dds/domain/DomainParticipantFactory.hpp>
#include <fastdds/dds/subscriber/DataReader.hpp>
#include <fastdds/dds/subscriber/DataReaderListener.hpp>
#include <fastdds/dds/subscriber/Subscriber.hpp>
#include <fastdds/dds/subscriber/SampleInfo.hpp>
#include <fastdds/dds/topic/Topic.hpp>
#include <fastdds/dds/topic/TopicDataType.hpp>
#include <fastdds/dds/topic/TypeSupport.hpp>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstring> // memcpy（PlayPassthroughType::serialize）
#include <filesystem>
#include <fstream>
#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <vector>

namespace
{
    // 匿名空间别名（与 main 内局部别名同路径，目标实体符号完整性经探测程序验证）：遮蔽
    // 全局 using-directive 注入的前向声明子集实体（缺 DataReader/Topic/SampleInfo/
    // RETCODE_OK/QoS 常量）——helper 内 dds:: 于匿名空间层单候选解析，main 内另有同路径
    // 局部别名（Record 先例同款），两处同路径同目标实体、类型互通
    namespace dds = eprosima::fastdds::dds;

    constexpr uint32_t TEST_DOMAIN = 203;    // 独立域，避开其他测试用例
    // bag 构造基准时间戳（固定值不依赖墙钟——回放时间轴只消费相对差值）
    constexpr uint64_t kBaseLogTimeNs = 1700000000000000000ULL;
    constexpr int kWaitTimeoutMs = 12000;    // 订阅端收数等待上限（SPDP+EDP 1~3s + 回放窗）
    constexpr int kJoinTimeoutMs = 10000;    // play 线程 join 前完成标志轮询上限
    constexpr std::size_t kPayloadSize = 5;  // 每条消息字节数（对齐 Convert 先例；bytes 断言基数）

    // 回放 bag 目录常量（固定命名 + 用例收尾 remove_all 自清理，无时间戳目录残留）
    const std::string kP1Bag = "play_test_p1";
    const std::string kP3aBag = "play_test_p3a";
    const std::string kP3bBag = "play_test_p3b";
    const std::string kP3cBag = "play_test_p3c";
    const std::string kP4Bag = "play_test_p4";
    const std::string kP6Bag = "play_test_p6";
    const std::string kP7Bag = "play_test_p7";
    const std::string kP8Bag = "play_test_p8";
    const std::string kP11Bag = "play_test_p11";
    const std::string kP13Bag = "play_test_p13";
    const std::string kP17Bag = "play_test_p17";
    const std::string kP5Missing = "play_p5_miss";      // 不创建（目录不存在）
    const std::string kP5IsFile = "play_p5_isfile";     // 路径是文件
    const std::string kP5NoMeta = "play_p5_nometa";     // 缺 metadata.json
    const std::string kP5BadJson = "play_p5_badjson";   // metadata 非 JSON
    const std::string kP5NoTopics = "play_p5_notopics"; // topics 空清单
    const std::string kP5NoShard = "play_p5_noshard";   // 有元信息无分片
    const std::string kP5BadEnum = "play_p5_badenum";   // qos 非法枚举名
    const std::string kP5MultiComma = "play_p5_multicomma"; // qos 多逗号
    const std::string kP5Legacy = "play_p5_legacy";     // 旧格式（type 空串，P2）

    // bag 主题构造单条目（type 空串 = 旧格式；qos 空串 = metadata 不写 qos 键）
    struct BagTopicSpec
    {
        std::string topic;
        std::string type;
        std::string qos;
        std::size_t messageCount;
    };

    // 造多主题多分片 bag（mcap 分片 + 手写 metadata.json，写侧同款 open/Channel/addChannel/
    // write/close，对齐 record 落盘形态）：
    //   分片：bag_0.mcap..bag_{S-1}.mcap 各自 open(None 压缩)，每分片对全部主题 addChannel
    //   （分片自含 Channel 声明，同 record）；
    //   时间轴：全局消息序按主题轮转交错（模拟并发录制到达序），logTime = 基准 + 全局序 ×
    //   stepNs（全局单调递增，回放游标跨主题升序归并）；消息按全局序连续切分到分片（前段
    //   bag_0 后段 bag_1，模拟 -b 切分的连续时间轴）；
    //   metadata：record 组装同款键集（starting_time/duration 的 _format 伴生键占位——
    //   play 预扫只读数值键）
    bool makeBag(const std::string &dir, const std::vector<BagTopicSpec> &topics,
                 std::uint64_t stepNs, std::size_t shardCount = 1)
    {
        std::error_code ec;
        std::filesystem::create_directories(dir, ec);
        std::uint64_t total = 0;
        for (const auto &t : topics)
        {
            total += t.messageCount;
        }
        if (total == 0)
        {
            return false;
        }

        // 分片 writer 与各主题 channel（channel.id 分片内独立——分片自含声明同 record）
        const std::string payload(kPayloadSize, 'x'); // 任意非空透传载荷（字节内容不参与断言）
        std::vector<std::vector<mcap::ChannelId>> channelIds(shardCount,
            std::vector<mcap::ChannelId>(topics.size(), 0));
        std::vector<std::unique_ptr<mcap::McapWriter>> writers;
        for (std::size_t s = 0; s < shardCount; ++s)
        {
            auto writer = std::make_unique<mcap::McapWriter>();
            mcap::McapWriterOptions options("");
            options.compression = mcap::Compression::None;
            if (!writer->open(dir + "/bag_" + std::to_string(s) + ".mcap", options).ok())
            {
                return false;
            }
            for (std::size_t j = 0; j < topics.size(); ++j)
            {
                mcap::KeyValueMap metadata;
                if (!topics[j].type.empty())
                {
                    metadata.emplace("type", topics[j].type);
                }
                if (!topics[j].qos.empty())
                {
                    metadata.emplace("qos", topics[j].qos);
                }
                mcap::Channel channel(topics[j].topic, "cdr", 0, metadata);
                writer->addChannel(channel);
                channelIds[s][j] = channel.id;
            }
            writers.push_back(std::move(writer));
        }

        // 全局消息序轮转交错写入（第 r 轮依次写各主题一条，主题耗尽跳过），分片按全局序
        // 连续切分（k×S/total）
        std::uint64_t k = 0;
        for (std::uint64_t round = 0; k < total; ++round)
        {
            for (std::size_t j = 0; j < topics.size() && k < total; ++j)
            {
                if (round >= topics[j].messageCount)
                {
                    continue; // 该主题已写完
                }
                const std::size_t shard = static_cast<std::size_t>(k * shardCount / total);
                mcap::Message msg;
                msg.channelId = channelIds[shard][j];
                msg.sequence = static_cast<std::uint32_t>(round);
                msg.logTime = kBaseLogTimeNs + k * stepNs;
                msg.publishTime = msg.logTime;
                // uint8_t 与 std::byte 同为单字节原始存储，别名转换安全（mcap 接口要求数据指针）
                msg.data = reinterpret_cast<const std::byte *>(payload.data()); // NOLINT(cppcoreguidelines-pro-type-reinterpret-cast)
                msg.dataSize = payload.size();
                if (!writers[shard]->write(msg).ok())
                {
                    return false;
                }
                ++k;
            }
        }
        for (auto &writer : writers)
        {
            writer->close();
        }

        // metadata.json：record 组装同款键集（record 收尾全键，_format 伴生键占位——play
        // 预扫只读数值键）
        std::vector<std::string> shardNames;
        for (std::size_t s = 0; s < shardCount; ++s)
        {
            shardNames.emplace_back("bag_" + std::to_string(s) + ".mcap");
        }
        const std::uint64_t spanNs = (total > 1) ? (total - 1) * stepNs : 0;
        nlohmann::json topicsWithCount = nlohmann::json::array();
        for (const auto &t : topics)
        {
            nlohmann::json meta = {{"name", t.topic}, {"type", t.type}};
            if (!t.qos.empty())
            {
                meta["qos"] = t.qos;
            }
            topicsWithCount.push_back({{"topic_metadata", meta}, {"message_count", t.messageCount}});
        }
        nlohmann::json info;
        info["version"] = 1;
        info["storage_identifier"] = "mcap";
        info["relative_file_paths"] = shardNames;
        info["starting_time"] = {{"nanoseconds_since_epoch", kBaseLogTimeNs},
                                 {"nanoseconds_since_epoch_format", "manual-made"}};
        info["duration"] = {{"nanoseconds", spanNs}, {"nanoseconds_format", "manual-made"}};
        info["message_count"] = total;
        info["topics_with_message_count"] = topicsWithCount;
        std::ofstream metaFile(dir + "/metadata.json");
        metaFile << info.dump(4) << "\n";
        return metaFile.good();
    }

    // 透传接收类型（照抄被测库 PassthroughPubSubType 形态：不解析数据，deserialize 原样
    // 字节拷贝；类型名与 bag 内 type 一致使 EDP 按名匹配——DDS 匹配只比主题名 + 类型名
    // 字符串 + QoS 兼容）。测试 TU 独立定义（匿名命名空间，与库内同名类型无符号冲突）。
    struct PlayBlob
    {
        std::string bytes;
    };

    class PlayPassthroughType : public dds::TopicDataType
    {
    public:
        explicit PlayPassthroughType(const std::string &typeName)
        {
            set_name(typeName);
            // 上限仅约束本端 serialize 的预判（订阅端不发布不触达）；接收侧按实际
            // payload.length 拷贝不受限。64KB 覆盖常规消息
            max_serialized_type_size = 64 * 1024;
            is_compute_key_provided = false;
        }
        ~PlayPassthroughType() override = default;

        bool serialize(const void *const data, eprosima::fastdds::rtps::SerializedPayload_t &payload,
                       dds::DataRepresentationId_t data_representation) override
        {
            static_cast<void>(data_representation);
            const auto *blob = static_cast<const PlayBlob *>(data);
            if (payload.max_size < blob->bytes.size())
            {
                return false;
            }
            std::memcpy(payload.data, blob->bytes.data(), blob->bytes.size());
            payload.length = static_cast<uint32_t>(blob->bytes.size());
            return true;
        }
        bool deserialize(eprosima::fastdds::rtps::SerializedPayload_t &payload, void *data) override
        {
            auto *blob = static_cast<PlayBlob *>(data);
            // 全量透传（含 encapsulation header）：与 play 发布端写入口径对称
            blob->bytes.assign(payload.data,
                    payload.data + payload.length);  // NOLINT(cppcoreguidelines-pro-bounds-pointer-arithmetic)
            return true;
        }
        std::uint32_t calculate_serialized_size(const void *const data,
                                                dds::DataRepresentationId_t data_representation) override
        {
            static_cast<void>(data);
            static_cast<void>(data_representation);
            return max_serialized_type_size;
        }
        bool compute_key(eprosima::fastdds::rtps::SerializedPayload_t &payload,
                         eprosima::fastdds::rtps::InstanceHandle_t &ihandle, bool force_md5) override
        {
            static_cast<void>(payload);
            static_cast<void>(ihandle);
            static_cast<void>(force_md5);
            return true;  // 无键类型：不做实例归属
        }
        bool compute_key(const void *const data, eprosima::fastdds::rtps::InstanceHandle_t &ihandle,
                         bool force_md5) override
        {
            static_cast<void>(data);
            static_cast<void>(ihandle);
            static_cast<void>(force_md5);
            return true;
        }
        void *create_data() override
        {
            return new PlayBlob();
        }
        void delete_data(void *data) override
        {
            delete static_cast<PlayBlob *>(data);
        }
    };

    // 订阅端计数 listener：take 全部样本只计数（透传语义不解码内容——对账维度为条数）。
    // 其余回调沿用基类默认空实现（FastDDS 3.x 各回调均非纯虚）
    class PlayCountListener : public dds::DataReaderListener
    {
    public:
        std::atomic<std::uint64_t> count{0};

        void on_data_available(dds::DataReader *reader) override
        {
            if (reader == nullptr)
            {
                return;
            }
            PlayBlob blob;
            dds::SampleInfo info;
            while (reader->take_next_sample(&blob, &info) == dds::RETCODE_OK)
            {
                if (info.valid_data)
                {
                    ++count;
                }
            }
        }
    };

    // 订阅端 topic 一次性搭建（透传类型注册幂等 + create_topic；每主题名只调用一次，
    // 供多 reader 共用）
    dds::Topic *makePlayTopic(dds::DomainParticipant *participant, const std::string &topicName,
                              const std::string &typeName)
    {
        dds::TypeSupport type(new PlayPassthroughType(typeName));
        type.register_type(participant);
        return participant->create_topic(topicName, type.get_type_name(), dds::TOPIC_QOS_DEFAULT);
    }

    // 订阅端 reader 搭建（QoS 入参指定 reliability/durability 口径；history depth 与
    // resource limits 显式 1000 对齐 play writer 侧口径——防 FastDDS 默认
    // max_samples_per_instance=100 触发 QoS 一致性拒绝，play 实现同款防御）
    dds::DataReader *makePlayReader(dds::Subscriber *subscriber, dds::Topic *topic,
                                    PlayCountListener *listener,
                                    dds::ReliabilityQosPolicyKind reliability,
                                    dds::DurabilityQosPolicyKind durability)
    {
        dds::DataReaderQos rqos;
        rqos.reliability().kind = reliability;
        rqos.durability().kind = durability;
        rqos.history().depth = 1000;
        rqos.resource_limits().max_samples_per_instance = 1000;
        rqos.resource_limits().max_samples = 1000;
        return subscriber->create_datareader(topic, rqos, listener);
    }

    // play 长驻阻塞线程启动（主线程负责等待收数/停止置位；ok/done 为线程产物，join 后读；
    // playbackRate 缺省 1.0、topics 缺省空（不过滤）、loop 缺省 false、startOffsetSec
    // 缺省 0.0、delaySec 缺省 0.0、waitAckedMs 缺省 -1、readAheadQueueSize 缺省 1000、
    // qosOverridesPath 缺省空（不加载覆盖文件恒等）透传节点层入口——P7 速率/P8 过滤/
    // P10 循环/P11 偏移/P12 延迟/P13 退出确认等待/P14 重映射/P15 队列深度/P16 QoS
    // 覆盖用例经尾参传入；topics/topicRemaps/qosOverridesPath 按值捕获：调用点
    // {"..."} 初始化列表临时在语句尾销毁，引用捕获悬垂）
    std::thread startPlayThread(FastDDSBagNode &node, const std::string &bagDir,
                                std::vector<FastDDSBagNode::BagTopicStat> &stats,
                                std::string &error, bool &ok, std::atomic<bool> &done,
                                double playbackRate = 1.0,
                                const std::vector<std::string> &topics = {},
                                bool loop = false, double startOffsetSec = 0.0,
                                double delaySec = 0.0, int64_t waitAckedMs = -1,
                                const std::vector<std::string> &topicRemaps = {},
                                size_t readAheadQueueSize = 1000,
                                const std::string &qosOverridesPath = {})
    {
        return std::thread([&node, &bagDir, &stats, &error, &ok, &done, playbackRate, topics,
                            loop, startOffsetSec, delaySec, waitAckedMs, topicRemaps,
                            readAheadQueueSize, qosOverridesPath]()
        {
            ok = node.play(bagDir, stats, &error, playbackRate, topics, loop, startOffsetSec,
                           delaySec, waitAckedMs, topicRemaps, readAheadQueueSize,
                           qosOverridesPath);
            done.store(true);
        });
    }

    // play 线程收尾（轮询完成标志后 join，超时兑底防悬挂——ctest 300s 总超时最终兑底）
    void joinPlay(std::thread &playThread, const std::atomic<bool> &done,
                  int timeoutMs = kJoinTimeoutMs)
    {
        for (int waited = 0; waited < timeoutMs && !done.load(); waited += 100)
        {
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
        playThread.join();
    }

    // play 线程耗时测量收尾（P7 节奏断言专用：轮询 done 首真后记完成时刻再 join，返回
    // 耗时毫秒——轮询粒度 5ms，测量误差 ≤5ms；joinPlay 的 100ms 粒度对秒级节奏断言
    // 不够细，须单独实现）
    double joinPlayElapsedMs(std::thread &playThread, const std::atomic<bool> &done,
                             const std::chrono::steady_clock::time_point &start,
                             int timeoutMs = kJoinTimeoutMs)
    {
        for (int waited = 0; waited < timeoutMs && !done.load(); waited += 5)
        {
            std::this_thread::sleep_for(std::chrono::milliseconds(5));
        }
        const auto doneTime = std::chrono::steady_clock::now();
        playThread.join();
        return std::chrono::duration<double, std::milli>(doneTime - start).count();
    }

    // 订阅端收数等待（每 50ms 轮询至 expect 条或超时；返回是否收满）
    bool waitForCount(const std::atomic<std::uint64_t> &counter, std::uint64_t expect,
                      int timeoutMs = kWaitTimeoutMs)
    {
        for (int waited = 0; waited < timeoutMs && counter.load() < expect; waited += 50)
        {
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
        }
        return counter.load() >= expect;
    }
} // namespace

int main()
{
    namespace fs = std::filesystem;
    // main 作用域别名（Record 先例同款）：遮蔽全局注入的 dds 命名空间名——某 FastDDS 头经
    // using-directive 注入的 dds 实体缺 QoS 常量（RELIABLE_RELIABILITY_QOS 等），真层
    // eprosima::fastdds::dds 符号完整（探测程序已证）
    namespace dds = eprosima::fastdds::dds;
    std::error_code ec;

    // 被测节点：域 203，全程单实例（play 预扫失败不建实体可重复调用；播完实体全清可重复回放）
    FastDDSBagNode node;
    const bool nodeCreated = node.setDomainId(TEST_DOMAIN);
    CHECK(nodeCreated, "setDomainId(203) 成功（回放节点域号独立）");
    if (!nodeCreated)
    {
        return testReport("TestFastDDSBagNodePlay");
    }

    // 订阅端：单 participant 贯穿全程（与回放节点同域对账；每用例独立 reader，主题名互异）
    auto *participant = dds::DomainParticipantFactory::get_instance()->create_participant(
        TEST_DOMAIN, dds::PARTICIPANT_QOS_DEFAULT);
    auto *subscriber =
        (participant != nullptr) ? participant->create_subscriber(dds::SUBSCRIBER_QOS_DEFAULT)
                                 : nullptr;
    CHECK(participant != nullptr && subscriber != nullptr, "订阅端 participant/subscriber 创建成功");

    // 收尾清理（提前 return 与正常路径共用：显式删除订阅端规避静态析构序问题 + bag 目录清单）
    const auto cleanup = [&]()
    {
        if (participant != nullptr)
        {
            dds::DomainParticipantFactory::get_instance()->delete_participant(participant);
        }
        for (const std::string &dir : {kP1Bag, kP3aBag, kP3bBag, kP3cBag, kP4Bag, kP6Bag,
                 kP7Bag, kP8Bag, kP11Bag, kP13Bag, kP17Bag, kP5NoMeta, kP5BadJson, kP5NoTopics,
                 kP5NoShard, kP5BadEnum, kP5MultiComma, kP5Legacy})
        {
            fs::remove_all(dir, ec);
        }
        fs::remove(kP5IsFile, ec);
    };
    if (participant == nullptr || subscriber == nullptr)
    {
        cleanup();
        return testReport("TestFastDDSBagNodePlay");
    }

    // ---- P5：预扫输入校验（fail 快速返回不建任何 DDS 实体，节点可继续复用） ----
    yomk::bagPlayReset();  // 复位残留停止/暂停标志（对齐 CLI runBagPlay 的发起前置）
    std::vector<FastDDSBagNode::BagTopicStat> stats;
    std::string error;

    // P5.1 目录不存在
    CHECK(!node.play(kP5Missing, stats, &error) &&
              error.find("bag path [" + kP5Missing + "] does not exist") != std::string::npos,
          "P5.1 目录不存在 → false + 路径错误文本");
    // P5.2 路径是文件（非目录）
    {
        std::ofstream touchFile(kP5IsFile);
        touchFile << "x";
    }
    CHECK(!node.play(kP5IsFile, stats, &error) &&
              error.find("must specify a bag directory") != std::string::npos,
          "P5.2 路径是文件 → false + 目录类型错误文本");
    // P5.3 缺 metadata.json（空目录）
    CHECK(fs::create_directory(kP5NoMeta, ec), "P5.3 前置：空目录创建成功");
    CHECK(!node.play(kP5NoMeta, stats, &error) &&
              error.find("could not find metadata.json in bag directory") != std::string::npos,
          "P5.3 缺 metadata.json → false + 元信息缺失错误文本");
    // P5.4 metadata 非 JSON
    CHECK(fs::create_directory(kP5BadJson, ec), "P5.4 前置：坏 JSON 目录创建成功");
    {
        std::ofstream out(kP5BadJson + "/metadata.json");
        out << "{ not json";
    }
    CHECK(!node.play(kP5BadJson, stats, &error) &&
              error.find("parse metadata.json failed") != std::string::npos,
          "P5.4 metadata 非 JSON → false + 解析失败错误文本");
    // P5.5 topics 空清单
    CHECK(fs::create_directory(kP5NoTopics, ec), "P5.5 前置：空清单目录创建成功");
    {
        nlohmann::json info = {{"storage_identifier", "mcap"}, {"message_count", 0},
            {"topics_with_message_count", nlohmann::json::array()}};
        std::ofstream out(kP5NoTopics + "/metadata.json");
        out << info.dump(4) << "\n";
    }
    CHECK(!node.play(kP5NoTopics, stats, &error) &&
              error.find("no topics found in metadata.json") != std::string::npos,
          "P5.5 topics 空清单 → false + 空清单错误文本");
    // P5.6 有元信息无分片（metadata 完整但目录无 bag_*.mcap）
    CHECK(fs::create_directory(kP5NoShard, ec), "P5.6 前置：无分片目录创建成功");
    {
        nlohmann::json info = {{"storage_identifier", "mcap"},
            {"relative_file_paths", nlohmann::json::array({"bag_0.mcap"})},
            {"starting_time", {{"nanoseconds_since_epoch", kBaseLogTimeNs}}},
            {"duration", {{"nanoseconds", 100}}},
            {"message_count", 2},
            {"topics_with_message_count", nlohmann::json::array({{
                 {"topic_metadata",
                     {{"name", "/rt/x"}, {"type", "YomkRpc::MString"},
                         {"qos", "RELIABLE,TRANSIENT_LOCAL"}}},
                 {"message_count", 2}}})}};
        std::ofstream out(kP5NoShard + "/metadata.json");
        out << info.dump(4) << "\n";
    }
    CHECK(!node.play(kP5NoShard, stats, &error) &&
              error.find("no bag files found for playing") != std::string::npos,
          "P5.6 有元信息无分片 → false + 无分片错误文本");
    // P5.7 qos 非法枚举名（reliability 段合法、durability 段拼写错误——注意 TRANSIENT/
    // PERSISTENT 是标准 DDS durability 合法档位，parseDurabilityKindName 全档支持）
    CHECK(makeBag(kP5BadEnum,
              {{"/rt/x", "YomkRpc::MString", "RELIABLE,TRANSIENT_LOCALL", 2}}, 1000000000ULL),
          "P5.7 前置：非法枚举名 bag 构造成功");
    CHECK(!node.play(kP5BadEnum, stats, &error) &&
              error.find("invalid qos string [RELIABLE,TRANSIENT_LOCALL]") != std::string::npos,
          "P5.7 qos 非法枚举名 → false + invalid qos string（含原串）");
    // P5.8 qos 多逗号（三段串整体拒绝）
    CHECK(makeBag(kP5MultiComma,
              {{"/rt/x", "YomkRpc::MString", "RELIABLE,TRANSIENT_LOCAL,X", 2}}, 1000000000ULL),
          "P5.8 前置：多逗号 qos bag 构造成功");
    CHECK(!node.play(kP5MultiComma, stats, &error) &&
              error.find("invalid qos string") != std::string::npos,
          "P5.8 qos 多逗号 → false + invalid qos string");

    // ---- P2：旧格式 bag 拒播（type 空串，有意偏离参考实现告警跳过的既有裁定） ----
    CHECK(makeBag(kP5Legacy, {{"/rt/x", "", "", 2}}, 1000000000ULL),
          "P2 前置：旧格式 bag（type 空串）构造成功");
    CHECK(!node.play(kP5Legacy, stats, &error) &&
              error.find("has no type info (legacy bag), cannot replay") != std::string::npos,
          "P2 旧格式 bag → false + 拒播错误文本（error 命中 type）");

    // ---- P1：双主题基础回放对账（时间轴 6s；TRANSIENT_LOCAL 订阅端先建在场） ----
    CHECK(makeBag(kP1Bag,
              {{"/rt/play_a", "YomkRpc::MString", "RELIABLE,TRANSIENT_LOCAL", 30},
                  {"/rt/play_b", "YomkRpc::MString", "RELIABLE,TRANSIENT_LOCAL", 30}},
              200 * 1000000ULL),
          "P1 前置：双主题 bag 构造成功");
    PlayCountListener listenA;
    PlayCountListener listenB;
    auto *topicA = makePlayTopic(participant, "/rt/play_a", "YomkRpc::MString");
    auto *topicB = makePlayTopic(participant, "/rt/play_b", "YomkRpc::MString");
    auto *readerA = makePlayReader(subscriber, topicA, &listenA,
        dds::RELIABLE_RELIABILITY_QOS, dds::TRANSIENT_LOCAL_DURABILITY_QOS);
    auto *readerB = makePlayReader(subscriber, topicB, &listenB,
        dds::RELIABLE_RELIABILITY_QOS, dds::TRANSIENT_LOCAL_DURABILITY_QOS);
    CHECK(topicA != nullptr && topicB != nullptr && readerA != nullptr && readerB != nullptr,
          "P1 前置：双主题订阅端 reader 先建在场");

    yomk::bagPlayReset();
    std::vector<FastDDSBagNode::BagTopicStat> p1Stats;
    std::string p1Error;
    std::atomic<bool> p1Done{false};
    bool p1Ok = false;
    std::thread p1Thread = startPlayThread(node, kP1Bag, p1Stats, p1Error, p1Ok, p1Done);

    CHECK(waitForCount(listenA.count, 30) && waitForCount(listenB.count, 30),
          "P1 双订阅端精确收满（writer depth 1000 匹配后 latched 补发 + 实时回放）");
    joinPlay(p1Thread, p1Done);  // play 6s 时间轴自然结束
    CHECK(p1Ok, "P1 play 全程完成返回 true（不设定格，可重复调用）");
    if (!p1Ok)
    {
        std::cerr << "P1 play error: " << p1Error << std::endl;
    }
    CHECK(p1Stats.size() == 2, "P1 stats 双主题两项");
    if (p1Stats.size() == 2)
    {
        CHECK(p1Stats[0].topic == "/rt/play_a" && p1Stats[1].topic == "/rt/play_b",
              "P1 stats 预扫清单序（metadata 主题序）");
        CHECK(p1Stats[0].type == "YomkRpc::MString" && p1Stats[1].type == "YomkRpc::MString",
              "P1 stats.type 透传 bag 内类型名");
        CHECK(p1Stats[0].count == 30 && p1Stats[1].count == 30,
              "P1 stats 精确条数（时间轴全播）");
        CHECK(p1Stats[0].bytes == 30 * kPayloadSize && p1Stats[1].bytes == 30 * kPayloadSize,
              "P1 stats.bytes == count×5（透传字节数）");
    }

    // ---- P3a：QoS 快照恢复正向（reliability）——RELIABLE,TRANSIENT_LOCAL bag 双口径订阅端 ----
    CHECK(makeBag(kP3aBag,
              {{"/rt/play_qos_tl", "YomkRpc::MString", "RELIABLE,TRANSIENT_LOCAL", 20}},
              150 * 1000000ULL),
          "P3a 前置：RELIABLE,TRANSIENT_LOCAL bag 构造成功");
    PlayCountListener tlBest;
    PlayCountListener tlRel;
    auto *topicTl = makePlayTopic(participant, "/rt/play_qos_tl", "YomkRpc::MString");
    auto *readerTlBest = makePlayReader(subscriber, topicTl, &tlBest,
        dds::BEST_EFFORT_RELIABILITY_QOS, dds::TRANSIENT_LOCAL_DURABILITY_QOS);
    auto *readerTlRel = makePlayReader(subscriber, topicTl, &tlRel,
        dds::RELIABLE_RELIABILITY_QOS, dds::TRANSIENT_LOCAL_DURABILITY_QOS);
    CHECK(topicTl != nullptr && readerTlBest != nullptr && readerTlRel != nullptr,
          "P3a 前置：双口径 reader 先建在场");
    yomk::bagPlayReset();
    std::vector<FastDDSBagNode::BagTopicStat> p3aStats;
    std::string p3aError;
    std::atomic<bool> p3aDone{false};
    bool p3aOk = false;
    std::thread p3aThread = startPlayThread(node, kP3aBag, p3aStats, p3aError, p3aOk, p3aDone);
    CHECK(waitForCount(tlBest.count, 20),
          "P3a BEST_EFFORT 订阅端收满（request ≤ offered 匹配 + latched 补发）");
    CHECK(waitForCount(tlRel.count, 20), "P3a RELIABLE 订阅端收满");
    joinPlay(p3aThread, p3aDone);
    CHECK(p3aOk, "P3a play 全程完成返回 true");

    // ---- P3b：QoS 快照恢复负向——BEST_EFFORT,VOLATILE bag → RELIABLE 订阅端不匹配 ----
    CHECK(makeBag(kP3bBag,
              {{"/rt/play_qos_be", "YomkRpc::MString", "BEST_EFFORT,VOLATILE", 8}},
              20 * 1000000ULL),
          "P3b 前置：BEST_EFFORT,VOLATILE 短回放 bag 构造成功");
    PlayCountListener beRel;
    auto *topicBe = makePlayTopic(participant, "/rt/play_qos_be", "YomkRpc::MString");
    auto *readerBeRel = makePlayReader(subscriber, topicBe, &beRel,
        dds::RELIABLE_RELIABILITY_QOS, dds::TRANSIENT_LOCAL_DURABILITY_QOS);
    CHECK(topicBe != nullptr && readerBeRel != nullptr, "P3b 前置：RELIABLE 订阅端先建在场");
    yomk::bagPlayReset();
    std::vector<FastDDSBagNode::BagTopicStat> p3bStats;
    std::string p3bError;
    std::atomic<bool> p3bDone{false};
    bool p3bOk = false;
    std::thread p3bThread = startPlayThread(node, kP3bBag, p3bStats, p3bError, p3bOk, p3bDone);
    joinPlay(p3bThread, p3bDone);  // 160ms 短回放自然结束
    CHECK(p3bOk, "P3b play 短回放完成返回 true");
    std::this_thread::sleep_for(std::chrono::seconds(2));  // 否定窗口：若误匹配早已可见
    CHECK(beRel.count.load() == 0,
          "P3b RELIABLE 订阅端收 0（request RELIABLE > offered BEST_EFFORT 匹配不建立）");

    // ---- P3c：QoS 快照恢复正向（durability）——BEST_EFFORT,TRANSIENT_LOCAL bag →
    //      BEST_EFFORT 订阅端 latched 补发精确收满（若 durability 误恢复 VOLATILE 则
    //      匹配建立前的样本全部丢失，16 条无法凑满——以收数反向钉死快照恢复） ----
    CHECK(makeBag(kP3cBag,
              {{"/rt/play_qos_dtl", "YomkRpc::MString", "BEST_EFFORT,TRANSIENT_LOCAL", 16}},
              250 * 1000000ULL),
          "P3c 前置：BEST_EFFORT,TRANSIENT_LOCAL bag 构造成功");
    PlayCountListener dtlBest;
    auto *topicDtl = makePlayTopic(participant, "/rt/play_qos_dtl", "YomkRpc::MString");
    auto *readerDtl = makePlayReader(subscriber, topicDtl, &dtlBest,
        dds::BEST_EFFORT_RELIABILITY_QOS, dds::TRANSIENT_LOCAL_DURABILITY_QOS);
    CHECK(topicDtl != nullptr && readerDtl != nullptr, "P3c 前置：BEST_EFFORT 订阅端先建在场");
    yomk::bagPlayReset();
    std::vector<FastDDSBagNode::BagTopicStat> p3cStats;
    std::string p3cError;
    std::atomic<bool> p3cDone{false};
    bool p3cOk = false;
    std::thread p3cThread = startPlayThread(node, kP3cBag, p3cStats, p3cError, p3cOk, p3cDone);
    CHECK(waitForCount(dtlBest.count, 16), "P3c BEST_EFFORT 订阅端 latched 补发收满（durability 快照恢复）");
    joinPlay(p3cThread, p3cDone);
    CHECK(p3cOk, "P3c play 全程完成返回 true");

    // ---- P4：Ctrl+C 停止（播放中途 bagPlayStop 置位 → 提前返回 + 部分计数） ----
    CHECK(makeBag(kP4Bag,
              {{"/rt/play_stop_a", "YomkRpc::MString", "RELIABLE,TRANSIENT_LOCAL", 40},
                  {"/rt/play_stop_b", "YomkRpc::MString", "RELIABLE,TRANSIENT_LOCAL", 40}},
              100 * 1000000ULL),
          "P4 前置：双主题 8s 时间轴 bag 构造成功");
    PlayCountListener stopA;
    PlayCountListener stopB;
    auto *topicSa = makePlayTopic(participant, "/rt/play_stop_a", "YomkRpc::MString");
    auto *topicSb = makePlayTopic(participant, "/rt/play_stop_b", "YomkRpc::MString");
    auto *readerSa = makePlayReader(subscriber, topicSa, &stopA,
        dds::RELIABLE_RELIABILITY_QOS, dds::TRANSIENT_LOCAL_DURABILITY_QOS);
    auto *readerSb = makePlayReader(subscriber, topicSb, &stopB,
        dds::RELIABLE_RELIABILITY_QOS, dds::TRANSIENT_LOCAL_DURABILITY_QOS);
    CHECK(topicSa != nullptr && topicSb != nullptr && readerSa != nullptr && readerSb != nullptr,
          "P4 前置：双订阅端 reader 先建在场");
    yomk::bagPlayReset();
    std::vector<FastDDSBagNode::BagTopicStat> p4Stats;
    std::string p4Error;
    std::atomic<bool> p4Done{false};
    bool p4Ok = false;
    std::thread p4Thread = startPlayThread(node, kP4Bag, p4Stats, p4Error, p4Ok, p4Done);
    std::this_thread::sleep_for(std::chrono::milliseconds(1500));  // 播放中途
    yomk::bagPlayStop();  // 模拟 CLI SIGINT onSignal 的 async-signal-safe 置位
    joinPlay(p4Thread, p4Done);
    CHECK(p4Ok, "P4 bagPlayStop 置位后 play 提前返回 true");
    CHECK(p4Stats.size() == 2, "P4 stats 双主题两项");
    const std::uint64_t playedSum =
        (p4Stats.size() == 2) ? (p4Stats[0].count + p4Stats[1].count) : 0;
    CHECK(playedSum > 0 && playedSum < 80,
          "P4 stats 部分计数（1.5s @100ms 双主题远小于 80 条总量）");
    CHECK(stopA.count.load() + stopB.count.load() <= playedSum,
          "P4 订阅端收数 ≤ 播出数（writer 已删，无补发窗口）");

    // ---- P6：多分片回放守恒（bag_0/bag_1 各 10 条，时间轴连续跨分片衔接） ----
    CHECK(makeBag(kP6Bag,
              {{"/rt/play_shard", "YomkRpc::MString", "RELIABLE,TRANSIENT_LOCAL", 20}},
              200 * 1000000ULL, 2),
          "P6 前置：双分片 bag 构造成功");
    CHECK(fs::exists(kP6Bag + "/bag_0.mcap", ec) && fs::exists(kP6Bag + "/bag_1.mcap", ec),
          "P6 前置：双分片落盘");
    PlayCountListener shardListen;
    auto *topicShard = makePlayTopic(participant, "/rt/play_shard", "YomkRpc::MString");
    auto *readerShard = makePlayReader(subscriber, topicShard, &shardListen,
        dds::RELIABLE_RELIABILITY_QOS, dds::TRANSIENT_LOCAL_DURABILITY_QOS);
    CHECK(topicShard != nullptr && readerShard != nullptr, "P6 前置：订阅端 reader 先建在场");
    yomk::bagPlayReset();
    std::vector<FastDDSBagNode::BagTopicStat> p6Stats;
    std::string p6Error;
    std::atomic<bool> p6Done{false};
    bool p6Ok = false;
    std::thread p6Thread = startPlayThread(node, kP6Bag, p6Stats, p6Error, p6Ok, p6Done);
    CHECK(waitForCount(shardListen.count, 20), "P6 跨分片回放订阅端精确收 20（总数守恒）");
    joinPlay(p6Thread, p6Done);
    CHECK(p6Ok, "P6 play 全程完成返回 true");
    CHECK(p6Stats.size() == 1 && p6Stats[0].count == 20 && p6Stats[0].bytes == 20 * kPayloadSize,
          "P6 stats 守恒（跨分片计数与字节对账）");

    // ---- P7：回放速率倍率（对齐 ros2 --rate：倍率缩放消息间调度间隔 + 运行时动态调率） ----
    CHECK(makeBag(kP7Bag,
              {{"/rt/play_rate", "YomkRpc::MString", "RELIABLE,TRANSIENT_LOCAL", 5}},
              400 * 1000000ULL),
          "P7 前置：单主题 5 条 @400ms bag 构造成功（时间轴 span 1.6s）");

    // P7.1 rate=2.0 全程快放：span 1.6s → 播放 ≉0.8s（窗口 0.6-1.2s）
    yomk::bagPlayReset();
    std::vector<FastDDSBagNode::BagTopicStat> p71Stats;
    std::string p71Error;
    std::atomic<bool> p71Done{false};
    bool p71Ok = false;
    const auto p71Start = std::chrono::steady_clock::now();
    std::thread p71Thread =
        startPlayThread(node, kP7Bag, p71Stats, p71Error, p71Ok, p71Done, 2.0);
    const double p71ElapsedMs = joinPlayElapsedMs(p71Thread, p71Done, p71Start);
    CHECK(p71Ok, "P7.1 play 全程完成返回 true");
    CHECK(p71Stats.size() == 1 && p71Stats[0].count == 5,
          "P7.1 stats 计数 5（时间轴全播，消息真实发出）");
    CHECK(p71ElapsedMs >= 600.0 && p71ElapsedMs <= 1200.0,
          "P7.1 rate=2.0 播放耗时 ≉0.8s（窗口 0.6-1.2s，实测 " +
              std::to_string(static_cast<int>(p71ElapsedMs)) + "ms）");

    // P7.2 rate=0.5 全程慢放：span 1.6s → 播放 ≉3.2s（窗口 2.6-4.2s）
    yomk::bagPlayReset();
    std::vector<FastDDSBagNode::BagTopicStat> p72Stats;
    std::string p72Error;
    std::atomic<bool> p72Done{false};
    bool p72Ok = false;
    const auto p72Start = std::chrono::steady_clock::now();
    std::thread p72Thread =
        startPlayThread(node, kP7Bag, p72Stats, p72Error, p72Ok, p72Done, 0.5);
    const double p72ElapsedMs = joinPlayElapsedMs(p72Thread, p72Done, p72Start);
    CHECK(p72Ok, "P7.2 play 全程完成返回 true");
    CHECK(p72Stats.size() == 1 && p72Stats[0].count == 5,
          "P7.2 stats 计数 5（时间轴全播）");
    CHECK(p72ElapsedMs >= 2600.0 && p72ElapsedMs <= 4200.0,
          "P7.2 rate=0.5 播放耗时 ≉3.2s（窗口 2.6-4.2s，实测 " +
              std::to_string(static_cast<int>(p72ElapsedMs)) + "ms）");

    // P7.3 运行时动态调率：rate=1.0 启动，400ms（第 2 条播放时刻 @1x，播放线程正处第
    //      2→3 条睡眠分片段）时 bagPlaySetRate(2.0)——换率重锚保已播 bag 进度连续
    //      （无跳发/重复，对齐 ros2 PlayerClock set_rate 进度语义）：剩余 1.2s bag 时间
    //      按 2x 播 → 总耗时 ≉1.0s（窗口 0.8-1.4s）
    yomk::bagPlayReset();
    std::vector<FastDDSBagNode::BagTopicStat> p73Stats;
    std::string p73Error;
    std::atomic<bool> p73Done{false};
    bool p73Ok = false;
    const auto p73Start = std::chrono::steady_clock::now();
    std::thread p73Thread =
        startPlayThread(node, kP7Bag, p73Stats, p73Error, p73Ok, p73Done);
    std::this_thread::sleep_for(std::chrono::milliseconds(400));
    CHECK(yomk::bagPlaySetRate(2.0),
          "P7.3 运行时 bagPlaySetRate(2.0) 返回 true（换率即时生效）");
    const double p73ElapsedMs = joinPlayElapsedMs(p73Thread, p73Done, p73Start);
    CHECK(p73Ok, "P7.3 play 全程完成返回 true");
    CHECK(p73Stats.size() == 1 && p73Stats[0].count == 5,
          "P7.3 stats 计数 5（换率进度连续：无跳发计数不缺、无重复计数不超）");
    CHECK(p73ElapsedMs >= 800.0 && p73ElapsedMs <= 1400.0,
          "P7.3 动态调率耗时 ≉1.0s（400ms@1x + 1.2s bag@2x，窗口 0.8-1.4s，实测 " +
              std::to_string(static_cast<int>(p73ElapsedMs)) + "ms）");

    // P7.4 调率接口语义（进程内 API 对齐 ros2 ~/set_rate、~/get_rate 服务）：P7.3 结束
    //      时原子为 2.0（SetRate 生效未复位）
    CHECK(!yomk::bagPlaySetRate(0.0) && yomk::bagPlayGetRate() == 2.0,
          "P7.4 SetRate(0) 拒绝返回 false 且 GetRate 保持 2.0");
    CHECK(!yomk::bagPlaySetRate(-1.0) && yomk::bagPlayGetRate() == 2.0,
          "P7.4 SetRate(-1) 拒绝返回 false 且 GetRate 保持 2.0");
    CHECK(yomk::bagPlaySetRate(2.0) && yomk::bagPlayGetRate() == 2.0,
          "P7.4 SetRate(2.0) 合法值生效（GetRate 读回一致）");
    yomk::bagPlayReset();
    CHECK(yomk::bagPlayGetRate() == 1.0,
          "P7.4 bagPlayReset 复位速率为 1.0（防上次会话残留倍率）");

    // P7.5 rate=0 与 rate=-1 启动：入口校验拒绝快速返回（不建任何 DDS 实体），stats 清空
    std::vector<FastDDSBagNode::BagTopicStat> p75Stats;
    std::string p75Error;
    p75Stats.emplace_back();  // 预置残留：验证失败路径 stats 清空契约（失败=未播放）
    CHECK(!node.play(kP7Bag, p75Stats, &p75Error, 0.0) &&
              p75Error.find("invalid playback rate") != std::string::npos,
          "P7.5 rate=0 启动 → false + invalid playback rate");
    CHECK(p75Stats.empty(), "P7.5 rate=0 stats 清空（失败即未播放）");
    CHECK(!node.play(kP7Bag, p75Stats, &p75Error, -1.0) &&
              p75Error.find("invalid playback rate") != std::string::npos,
          "P7.5 rate=-1 启动 → false + invalid playback rate");
    CHECK(p75Stats.empty(), "P7.5 rate=-1 stats 清空");

    // ---- P8：主题过滤（--topics，对齐 convert topics 语义 + ros2 --topics 缺省语义） ----
    CHECK(makeBag(kP8Bag,
              {{"/rt/p8a", "YomkRpc::MString", "RELIABLE,TRANSIENT_LOCAL", 20},
                  {"/rt/p8b", "YomkRpc::MString", "RELIABLE,TRANSIENT_LOCAL", 20}},
              50 * 1000000ULL),
          "P8 前置：双主题各 20 条 @50ms bag 构造成功（span 950ms）");

    // P8.1 单主题过滤：--topics 只含 /rt/p8a → stats 仅 p8a 行；双 reader 都在场对账：
    // 入选主题收满、被过滤主题零收（过滤在发布侧，被过滤主题无任何发布实体）
    auto *topicP8a = makePlayTopic(participant, "/rt/p8a", "YomkRpc::MString");
    auto *topicP8b = makePlayTopic(participant, "/rt/p8b", "YomkRpc::MString");
    PlayCountListener p8aListen;
    PlayCountListener p8bListen;
    auto *readerP8a = makePlayReader(subscriber, topicP8a, &p8aListen,
        dds::RELIABLE_RELIABILITY_QOS, dds::TRANSIENT_LOCAL_DURABILITY_QOS);
    auto *readerP8b = makePlayReader(subscriber, topicP8b, &p8bListen,
        dds::RELIABLE_RELIABILITY_QOS, dds::TRANSIENT_LOCAL_DURABILITY_QOS);
    CHECK(topicP8a != nullptr && topicP8b != nullptr && readerP8a != nullptr &&
              readerP8b != nullptr,
          "P8 前置：双主题订阅端 reader 先建在场（对账过滤面）");
    yomk::bagPlayReset();
    std::vector<FastDDSBagNode::BagTopicStat> p81Stats;
    std::string p81Error;
    std::atomic<bool> p81Done{false};
    bool p81Ok = false;
    std::thread p81Thread = startPlayThread(node, kP8Bag, p81Stats, p81Error, p81Ok,
        p81Done, 1.0, {"/rt/p8a"});
    CHECK(waitForCount(p8aListen.count, 20), "P8.1 入选主题 p8a 订阅端精确收 20");
    joinPlay(p81Thread, p81Done);
    CHECK(p81Ok, "P8.1 play 全程完成返回 true");
    CHECK(p81Stats.size() == 1 && p81Stats[0].topic == "/rt/p8a" &&
              p81Stats[0].count == 20 && p81Stats[0].bytes == 20 * kPayloadSize,
          "P8.1 stats 仅含入选主题 p8a（count/bytes 精确）");
    CHECK(p8bListen.count.load() == 0, "P8.1 被过滤主题 p8b 订阅端零收（过滤在发布侧）");

    // P8.2 双主题过滤 + 重复项 + 乱序清单：{"p8b","p8a","p8a"} → 入选集合去重，
    // stats 仍预扫清单序（p8a/p8b 各 20），订阅端各收满
    yomk::bagPlayReset();
    std::vector<FastDDSBagNode::BagTopicStat> p82Stats;
    std::string p82Error;
    std::atomic<bool> p82Done{false};
    bool p82Ok = false;
    std::thread p82Thread = startPlayThread(node, kP8Bag, p82Stats, p82Error, p82Ok,
        p82Done, 1.0, {"/rt/p8b", "/rt/p8a", "/rt/p8a"});
    CHECK(waitForCount(p8aListen.count, 40), "P8.2 p8a 订阅端累计收满 40（两轮回放）");
    joinPlay(p82Thread, p82Done);
    CHECK(p82Ok, "P8.2 play 全程完成返回 true");
    CHECK(p82Stats.size() == 2 && p82Stats[0].topic == "/rt/p8a" &&
              p82Stats[1].topic == "/rt/p8b" && p82Stats[0].count == 20 &&
              p82Stats[1].count == 20,
          "P8.2 stats 两行升序各 20（重复项去重、清单序不随 --topics 乱序）");
    CHECK(p8bListen.count.load() == 20, "P8.2 p8b 订阅端精确收 20（第二轮回放）");

    // P8.3 通配展开（同步 play，stats 为权威对账——writer 播毕即删无 latched 补发）：
    // {"*p8a"} 前后缀夹逼仅命中 p8a；{"/rt/p8*"} 前缀通配双主题全命中
    yomk::bagPlayReset();
    std::vector<FastDDSBagNode::BagTopicStat> p83Stats;
    std::string p83Error;
    std::atomic<bool> p83Done{false};
    bool p83Ok = false;
    std::thread p83Thread = startPlayThread(node, kP8Bag, p83Stats, p83Error, p83Ok,
        p83Done, 1.0, {"*p8a"});
    joinPlay(p83Thread, p83Done);
    CHECK(p83Ok && p83Stats.size() == 1 && p83Stats[0].topic == "/rt/p8a" &&
              p83Stats[0].count == 20,
          "P8.3 前缀通配 *p8a 仅命中单主题（stats 1 行 count 20）");
    yomk::bagPlayReset();
    std::vector<FastDDSBagNode::BagTopicStat> p83bStats;
    std::string p83bError;
    std::atomic<bool> p83bDone{false};
    bool p83bOk = false;
    std::thread p83bThread = startPlayThread(node, kP8Bag, p83bStats, p83bError, p83bOk,
        p83bDone, 1.0, {"/rt/p8*"});
    joinPlay(p83bThread, p83bDone);
    CHECK(p83bOk && p83bStats.size() == 2 && p83bStats[0].count == 20 &&
              p83bStats[1].count == 20,
          "P8.3 通配 /rt/p8* 命中双主题（stats 两行各 20）");

    // P8.4 精确项不存在：清单对账 fail 快速返回（不建任何 DDS 实体），stats 清空
    std::vector<FastDDSBagNode::BagTopicStat> p84Stats;
    std::string p84Error;
    p84Stats.emplace_back();  // 预置残留：验证失败路径 stats 清空契约
    CHECK(!node.play(kP8Bag, p84Stats, &p84Error, 1.0, {"/rt/nope"}) &&
              p84Error.find("not found in bag") != std::string::npos,
          "P8.4 精确项不在 bag 清单 → false + not found in bag");
    CHECK(p84Stats.empty(), "P8.4 stats 清空（失败即未播放）");

    // P8.5 模式项未命中与双 '*'：对齐 convert/record 惯例文案
    std::vector<FastDDSBagNode::BagTopicStat> p85Stats;
    std::string p85Error;
    CHECK(!node.play(kP8Bag, p85Stats, &p85Error, 1.0, {"/rt/nope*"}) &&
              p85Error.find("matched no bag topic") != std::string::npos,
          "P8.5 通配未命中 → false + matched no bag topic");
    CHECK(!node.play(kP8Bag, p85Stats, &p85Error, 1.0, {"a*b*c"}) &&
              p85Error.find("'*' 出现多次") != std::string::npos,
          "P8.5 双 '*' → false + 出现多次（record 惯例）");

    // ---- P9：--start-paused 暂停态启动（库层形态，对齐 record 先例与 ros2 语义） ----
    // P9.1 暂停窗零发出 + 恢复全量补播 + 暂停期不泄入时间轴：预置 bagPlayPause → 后台
    // play → 700ms 窗订阅端零增量（不依赖 P8 累计基数，只断言窗口内不动）→ resume →
    // 补播收满；总耗时 ≈ 700 + 1950（全局 span）+开销，泄入病态会塌缩到暂停窗内
    yomk::bagPlayReset();
    yomk::bagPlayPause();  // 预置暂停意图（先于 play；节点层全程只 load 该原子）
    std::vector<FastDDSBagNode::BagTopicStat> p91Stats;
    std::string p91Error;
    std::atomic<bool> p91Done{false};
    bool p91Ok = false;
    const auto p91aBase = p8aListen.count.load();
    const auto p91bBase = p8bListen.count.load();
    const auto p91Start = std::chrono::steady_clock::now();
    std::thread p91Thread = startPlayThread(node, kP8Bag, p91Stats, p91Error, p91Ok,
        p91Done);
    std::this_thread::sleep_for(std::chrono::milliseconds(700));
    CHECK(p8aListen.count.load() == p91aBase && p8bListen.count.load() == p91bBase,
          "P9.1 暂停窗 700ms 双订阅端零增量（播放挂起零发出）");
    yomk::bagPlayResume();
    CHECK(waitForCount(p8aListen.count, p91aBase + 20),
          "P9.1 恢复后 p8a 补播收满（+20）");
    const double p91ElapsedMs = joinPlayElapsedMs(p91Thread, p91Done, p91Start);
    CHECK(p91Ok, "P9.1 play 全程完成返回 true");
    CHECK(p91Stats.size() == 2 && p91Stats[0].count == 20 && p91Stats[1].count == 20,
          "P9.1 stats 双主题各 20（恢复后全量补播）");
    CHECK(p91ElapsedMs >= 2200.0 && p91ElapsedMs <= 3400.0,
          "P9.1 总耗时 ≈ 暂停窗 700ms + span 1950ms（暂停期不泄入时间轴：泄入则恢复后"
          "瞬间追发塌缩到暂停窗内，二次计入则超出 3350ms）");

    // P9.2 暂停态与 --topics 过滤叠加：预置暂停 + topics 仅 p8b → 暂停窗双端零增量，
    // 恢复后 p8b 补播 +20、p8a 仍零增量（stats 仅 p8b 行）
    yomk::bagPlayReset();
    yomk::bagPlayPause();
    std::vector<FastDDSBagNode::BagTopicStat> p92Stats;
    std::string p92Error;
    std::atomic<bool> p92Done{false};
    bool p92Ok = false;
    const auto p92aBase = p8aListen.count.load();
    const auto p92bBase = p8bListen.count.load();
    std::thread p92Thread = startPlayThread(node, kP8Bag, p92Stats, p92Error, p92Ok,
        p92Done, 1.0, {"/rt/p8b"});
    std::this_thread::sleep_for(std::chrono::milliseconds(700));
    CHECK(p8aListen.count.load() == p92aBase && p8bListen.count.load() == p92bBase,
          "P9.2 暂停窗双订阅端零增量（过滤叠加暂停态）");
    yomk::bagPlayResume();
    CHECK(waitForCount(p8bListen.count, p92bBase + 20),
          "P9.2 恢复后 p8b 补播收满（+20）");
    joinPlay(p92Thread, p92Done);
    CHECK(p92Ok, "P9.2 play 全程完成返回 true");
    CHECK(p92Stats.size() == 1 && p92Stats[0].topic == "/rt/p8b" &&
              p92Stats[0].count == 20,
          "P9.2 stats 仅 p8b 行（暂停态与 --topics 叠加正确）");
    CHECK(p8aListen.count.load() == p92aBase,
          "P9.2 p8a 全程零收（被过滤 + 暂停态双条件）");

    // ---- P10：-l/--loop 循环回放（库层形态，对齐 ros2 语义：到尾回开头无限播放） ----
    // P10.1 循环两轮不倾泻：后台 play(loop=true) → 2.5s 窗（第一轮 span 1950ms 完 +
    // 第二轮 ~550ms）→ bagPlayStop → 收数 [21,35]：下界 20+1 钉住轮边界重锚后第二轮
    // 首条立即播出，上界 20+15 钉住第二轮按录制时间轴节拍推进（回卷后不重锚的倾泻
    // 形态会把第二轮 20 条瞬间全发，2.5s 内收满 40 越上界）；总耗时 ≥1900ms 钉住
    // 第一轮完整节拍
    yomk::bagPlayReset();
    std::vector<FastDDSBagNode::BagTopicStat> p101Stats;
    std::string p101Error;
    std::atomic<bool> p101Done{false};
    bool p101Ok = false;
    const auto p101aBase = p8aListen.count.load();
    const auto p101bBase = p8bListen.count.load();
    const auto p101Start = std::chrono::steady_clock::now();
    std::thread p101Thread = startPlayThread(node, kP8Bag, p101Stats, p101Error, p101Ok,
        p101Done, 1.0, {}, true);
    std::this_thread::sleep_for(std::chrono::milliseconds(2500));
    yomk::bagPlayStop();
    const double p101ElapsedMs = joinPlayElapsedMs(p101Thread, p101Done, p101Start);
    CHECK(p101Ok, "P10.1 stop 路径 play 返回 true");
    const auto p101aGot = p8aListen.count.load() - p101aBase;
    const auto p101bGot = p8bListen.count.load() - p101bBase;
    CHECK(p101aGot >= 21 && p101aGot <= 35,
          "P10.1 p8a 两轮收数 [21,35]（第一轮 20 + 第二轮 1~15 按节拍推进，倾泻形态"
          " 2.5s 内 40 条全发越上界）");
    CHECK(p101bGot >= 21 && p101bGot <= 35,
          "P10.1 p8b 两轮收数 [21,35]（双主题同款）");
    CHECK(p101ElapsedMs >= 1900.0,
          "P10.1 总耗时 ≥ 1900ms（第一轮完整节拍 1950ms span）");
    CHECK(p101Stats.size() == 2 && p101Stats[0].count >= 21 && p101Stats[1].count >= 21,
          "P10.1 stats 双主题各 ≥21（第二轮起步累计）");

    // P10.2 loop 与 --topics 叠加：仅 p8a 入选 + loop → 回卷轮过滤仍生效（p8a 两轮
    // 累计 ≥21、p8b 全程零收）
    yomk::bagPlayReset();
    std::vector<FastDDSBagNode::BagTopicStat> p102Stats;
    std::string p102Error;
    std::atomic<bool> p102Done{false};
    bool p102Ok = false;
    const auto p102aBase = p8aListen.count.load();
    const auto p102bBase = p8bListen.count.load();
    std::thread p102Thread = startPlayThread(node, kP8Bag, p102Stats, p102Error, p102Ok,
        p102Done, 1.0, {"/rt/p8a"}, true);
    std::this_thread::sleep_for(std::chrono::milliseconds(2500));
    yomk::bagPlayStop();
    joinPlay(p102Thread, p102Done);
    CHECK(p102Ok, "P10.2 stop 路径 play 返回 true");
    CHECK(p8aListen.count.load() - p102aBase >= 21,
          "P10.2 p8a 两轮累计 ≥21（loop 回卷轮起步）");
    CHECK(p8bListen.count.load() == p102bBase,
          "P10.2 p8b 全程零收（回卷轮过滤仍生效）");
    CHECK(p102Stats.size() == 1 && p102Stats[0].topic == "/rt/p8a" &&
              p102Stats[0].count >= 21,
          "P10.2 stats 仅 p8a 行且 ≥21");

    // P10.3 loop 与暂停态启动叠加：预置 bagPlayPause → play(loop=true) → 700ms 暂停窗
    // 零增量（P9 语义 loop 下保持）→ resume → 2.4s（第一轮剩余 ~1250ms + 调度开销余量
    // ——P9.1 实测开销可达数百 ms，窗口过短第二轮首条来不及播出 → 第二轮 ~800ms 节拍
    // 推进）→ stop → 收数 [21,35]（下界 20+1 钉跨暂停两轮起步，上界防倾泻同 P10.1 口径）
    yomk::bagPlayReset();
    yomk::bagPlayPause();
    std::vector<FastDDSBagNode::BagTopicStat> p103Stats;
    std::string p103Error;
    std::atomic<bool> p103Done{false};
    bool p103Ok = false;
    const auto p103aBase = p8aListen.count.load();
    const auto p103bBase = p8bListen.count.load();
    std::thread p103Thread = startPlayThread(node, kP8Bag, p103Stats, p103Error, p103Ok,
        p103Done, 1.0, {}, true);
    std::this_thread::sleep_for(std::chrono::milliseconds(700));
    CHECK(p8aListen.count.load() == p103aBase && p8bListen.count.load() == p103bBase,
          "P10.3 暂停窗 700ms 双订阅端零增量（P9 语义 loop 下保持）");
    yomk::bagPlayResume();
    std::this_thread::sleep_for(std::chrono::milliseconds(2400));
    yomk::bagPlayStop();
    joinPlay(p103Thread, p103Done);
    CHECK(p103Ok, "P10.3 stop 路径 play 返回 true");
    const auto p103aGot = p8aListen.count.load() - p103aBase;
    CHECK(p103aGot >= 21 && p103aGot <= 35,
          "P10.3 p8a 跨暂停累计 [21,35]（第一轮播毕 + 第二轮起步按节拍推进）");
    CHECK(p103Stats.size() == 2 && p103Stats[0].count >= 21,
          "P10.3 stats 双主题累计 ≥21");

    // ---- P11：--start-offset 起始偏移（库层形态，对齐 ros2 语义 "Start the playback
    // player this many seconds into the bag file"：从 bag 开始第 SEC 秒起播，读侧跳过
    // 前段消息不入队不计统计不告警；偏移基准=过滤前全场首条 logTime） ----
    // P11.1 单轮跳段节拍：kP8Bag（全局交错 t=0..1950ms）offset=1.0s 自然播毕 → p8a
    // 精确播 t≥1000ms 的 10 条、p8b 精确播 t≥1050ms 的 10 条；总耗时 ≈ 播段 span
    // 950ms（首播条立即播出至末条按差值调度），跳过失效的倾泻形态（40 条全发）计数
    // 与耗时双双越界
    yomk::bagPlayReset();
    std::vector<FastDDSBagNode::BagTopicStat> p111Stats;
    std::string p111Error;
    std::atomic<bool> p111Done{false};
    bool p111Ok = false;
    const auto p111aBase = p8aListen.count.load();
    const auto p111bBase = p8bListen.count.load();
    const auto p111Start = std::chrono::steady_clock::now();
    std::thread p111Thread = startPlayThread(node, kP8Bag, p111Stats, p111Error, p111Ok,
        p111Done, 1.0, {}, false, 1.0);
    const double p111ElapsedMs = joinPlayElapsedMs(p111Thread, p111Done, p111Start);
    CHECK(p111Ok, "P11.1 offset=1.0s 自然播毕返回 true");
    CHECK(p8aListen.count.load() - p111aBase == 10,
          "P11.1 p8a 精确 10 条（t≥1000ms 后段，前 10 条读侧跳过不入队）");
    CHECK(p8bListen.count.load() - p111bBase == 10,
          "P11.1 p8b 精确 10 条（t≥1050ms 后段，跳过语义与时间轴对账一致）");
    CHECK(p111Stats.size() == 2 && p111Stats[0].count == 10 && p111Stats[1].count == 10,
          "P11.1 stats 双主题各 10（跳过段不计入收尾统计）");
    CHECK(p111ElapsedMs >= 800.0 && p111ElapsedMs <= 1900.0,
          "P11.1 总耗时 ≈ 播段 span 950ms + 开销（跳过段零等待；倾泻形态 <500ms 越下界）");

    // P11.2 offset 超全场时长：offset=5.0s > span 1950ms → 零消息播出、正常播毕成功
    // 返回（对齐 ros2 超界语义：不报错；stats 行仍按预扫清单回填、计数 0）
    yomk::bagPlayReset();
    std::vector<FastDDSBagNode::BagTopicStat> p112Stats;
    std::string p112Error;
    std::atomic<bool> p112Done{false};
    bool p112Ok = false;
    const auto p112aBase = p8aListen.count.load();
    const auto p112bBase = p8bListen.count.load();
    const auto p112Start = std::chrono::steady_clock::now();
    std::thread p112Thread = startPlayThread(node, kP8Bag, p112Stats, p112Error, p112Ok,
        p112Done, 1.0, {}, false, 5.0);
    const double p112ElapsedMs = joinPlayElapsedMs(p112Thread, p112Done, p112Start);
    CHECK(p112Ok, "P11.2 offset=5.0s 超全场 → 播毕成功返回 true（零消息不报错）");
    CHECK(p8aListen.count.load() == p112aBase && p8bListen.count.load() == p112bBase,
          "P11.2 双订阅端零收（全场消息均早于偏移点被跳过）");
    CHECK(p112Stats.size() == 2 && p112Stats[0].count == 0 && p112Stats[1].count == 0,
          "P11.2 stats 双主题行计 0（预扫清单回填、零播出=零计数）");
    CHECK(p112ElapsedMs < 700.0,
          "P11.2 总耗时 <700ms（零消息快速返回，无逐条等待）");

    // P11.3 loop×offset 每轮保持偏移：专用 kP11Bag（单主题 6 条 @500ms，t=0..2500ms
    // span 2500ms）offset=2.25s → 每轮仅末条 t=2500ms 可播（播后立即 EOS 回卷）。
    // 收数推演（首跑实测校准）：正确形态每轮 1 条且跳过段读侧不被播放节拍反压 →
    // 回卷全速空转（亚毫秒级/轮，首跑 5s 窗实测 ≈9.5 万条，随读侧速度浮动）——只设
    // 下界钉语义不设上界；回卷从零的错误形态每轮 6 条被 span 2500ms 节拍反压 →
    // 5s 恰 2 轮收 ≤12 条，远越下界 15
    CHECK(makeBag(kP11Bag,
              {{"/rt/p11", "YomkRpc::MString", "RELIABLE,TRANSIENT_LOCAL", 6}},
              500 * 1000000ULL),
          "P11.3 前置：单主题 6 条 @500ms bag 构造成功（span 2500ms）");
    auto *topicP11 = makePlayTopic(participant, "/rt/p11", "YomkRpc::MString");
    PlayCountListener p11Listen;
    auto *readerP11 = makePlayReader(subscriber, topicP11, &p11Listen,
        dds::RELIABLE_RELIABILITY_QOS, dds::TRANSIENT_LOCAL_DURABILITY_QOS);
    CHECK(topicP11 != nullptr && readerP11 != nullptr,
          "P11.3 前置：单主题 reader 先建在场");
    yomk::bagPlayReset();
    std::vector<FastDDSBagNode::BagTopicStat> p113Stats;
    std::string p113Error;
    std::atomic<bool> p113Done{false};
    bool p113Ok = false;
    const auto p113Base = p11Listen.count.load();
    std::thread p113Thread = startPlayThread(node, kP11Bag, p113Stats, p113Error, p113Ok,
        p113Done, 1.0, {}, true, 2.25);
    std::this_thread::sleep_for(std::chrono::milliseconds(5000));
    yomk::bagPlayStop();
    joinPlay(p113Thread, p113Done);
    CHECK(p113Ok, "P11.3 stop 路径 play 返回 true");
    const auto p113Got = p11Listen.count.load() - p113Base;
    CHECK(p113Got >= 15,
          "P11.3 loop×offset 收数 ≥15（每轮回卷保持偏移、每轮仅末条播出——跳过段读侧"
          "空转收数随读侧速度浮动不设上界；回卷从零形态被节拍反压 5s ≤12 条越下界，"
          "实测 " + std::to_string(p113Got) + " 条）");
    CHECK(p113Stats.size() == 1 && p113Stats[0].count >= 15,
          "P11.3 stats 单主题累计 ≥15（与订阅端收数同口径）");

    // P11.4 负值启动：入口校验拒绝快速返回（不建任何 DDS 实体），stats 清空——错误
    // 文案对齐 P7.5 rate 校验双落点先例（invalid start offset）
    std::vector<FastDDSBagNode::BagTopicStat> p114Stats;
    std::string p114Error;
    p114Stats.emplace_back();  // 预置残留：验证失败路径 stats 清空契约（失败=未播放）
    CHECK(!node.play(kP11Bag, p114Stats, &p114Error, 1.0, {}, false, -0.5) &&
              p114Error.find("invalid start offset") != std::string::npos,
          "P11.4 offset=-0.5 启动 → false + invalid start offset");
    CHECK(p114Stats.empty(), "P11.4 stats 清空（失败即未播放）");

    // ---- P12：-d/--delay 开播延迟（库层形态，对齐 ros2 语义 "Sleep before play.
    // Negative durations invalid. Will delay at the beginning of each loop."：读线程
    // 每轮循环体开头先睡 SEC 秒再读播；节点层只拒负值，0 为合法缺省不延迟） ----
    // P12.1 单轮延迟零发出+播毕：kP8Bag delay=1.0s——启动后 500ms 窗双订阅端零增量
    //（delay 窗零发出；未生效形态 500ms 已发 ~5 条即拒）；waitForCount 双主题各 20
    // 收满；自然播毕耗时 = delay 窗 1000 + span 1950 + 开销（未生效形态 ≈2.15-2.65s
    // 越下界 2600，第二重区分）
    yomk::bagPlayReset();
    std::vector<FastDDSBagNode::BagTopicStat> p121Stats;
    std::string p121Error;
    std::atomic<bool> p121Done{false};
    bool p121Ok = false;
    const auto p121aBase = p8aListen.count.load();
    const auto p121bBase = p8bListen.count.load();
    const auto p121Start = std::chrono::steady_clock::now();
    std::thread p121Thread = startPlayThread(node, kP8Bag, p121Stats, p121Error, p121Ok,
        p121Done, 1.0, {}, false, 0.0, 1.0);
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    CHECK(p8aListen.count.load() == p121aBase && p8bListen.count.load() == p121bBase,
          "P12.1 delay 窗 500ms 双订阅端零增量（delay 窗零发出；未生效形态已发 ~5 条）");
    CHECK(waitForCount(p8aListen.count, p121aBase + 20) &&
              waitForCount(p8bListen.count, p121bBase + 20),
          "P12.1 双订阅端收满各 20（delay 窗后首条立即播、全场按时间轴播毕）");
    const double p121ElapsedMs = joinPlayElapsedMs(p121Thread, p121Done, p121Start);
    CHECK(p121Ok, "P12.1 delay=1.0s 自然播毕返回 true");
    CHECK(p121Stats.size() == 2 && p121Stats[0].count == 20 && p121Stats[1].count == 20,
          "P12.1 stats 双主题各 20（延迟不改变收尾统计口径）");
    CHECK(p121ElapsedMs >= 2600.0 && p121ElapsedMs <= 4200.0,
          "P12.1 耗时 ∈ [2600,4200]ms（delay 窗 1000 + span 1950 + 开销；未生效形态 "
          "≈2.15-2.65s 越下界）");

    // P12.2 loop×delay 每轮开头生效：delay=0.5s 2.7s 窗——首轮 0.5s 窗+span 1950ms+开销
    // ≈2.45-2.95s 播毕全收；第二轮回卷重建游标后轮开头再睡 0.5s → 第二轮首条 = 播毕+0.5
    // ≥2.95s 必不在 2.7s 窗内，2.7s 窗收 20（3.0s 窗在空载时序下第二轮首条 2.95-3.0s
    // 撞窗边界竞态实测 21 越上界，缩 2.7s 消除）。上界 20 钉住"每轮开头延迟"——delay
    // 仅首轮生效形态第二轮 ≈2.45s 即播、2.7s 窗内再收数条（检出即 ≥21）越上界；下界
    // 15 防首轮匹配/调度开销吞条误伤（开销 ≤1.5s 时首轮收窗 ≥700ms 收 ≥15）
    yomk::bagPlayReset();
    std::vector<FastDDSBagNode::BagTopicStat> p122Stats;
    std::string p122Error;
    std::atomic<bool> p122Done{false};
    bool p122Ok = false;
    const auto p122aBase = p8aListen.count.load();
    const auto p122bBase = p8bListen.count.load();
    std::thread p122Thread = startPlayThread(node, kP8Bag, p122Stats, p122Error, p122Ok,
        p122Done, 1.0, {}, true, 0.0, 0.5);
    std::this_thread::sleep_for(std::chrono::milliseconds(2700));
    yomk::bagPlayStop();
    joinPlay(p122Thread, p122Done);
    CHECK(p122Ok, "P12.2 stop 路径 play 返回 true");
    const auto p122aGot = p8aListen.count.load() - p122aBase;
    CHECK(p122aGot >= 15 && p122aGot <= 20,
          "P12.2 loop×delay 收数 [15,20]（首轮播毕全收 + 第二轮轮开头再睡未开始——"
          "上界 20 钉住每轮开头延迟，仅首轮生效形态 2.7s 窗收 ≥21 越上界，实测 " +
          std::to_string(p122aGot) + " 条）");
    CHECK(p122Stats.size() == 2 && p122Stats[0].count >= 15 && p122Stats[0].count <= 20,
          "P12.2 stats 双主题 [15,20]（与订阅端收数同口径）");
    CHECK(p8bListen.count.load() - p122bBase >= 15,
          "P12.2 p8b 同口径 ≥15（双主题时间轴一致）");

    // P12.3 负值启动：入口校验拒绝快速返回（不建任何 DDS 实体），stats 清空——错误
    // 文案对齐 P11.4 先例（invalid delay；节点层只拒负值，0 合法缺省）
    std::vector<FastDDSBagNode::BagTopicStat> p123Stats;
    std::string p123Error;
    p123Stats.emplace_back();  // 预置残留：验证失败路径 stats 清空契约（失败=未播放）
    CHECK(!node.play(kP8Bag, p123Stats, &p123Error, 1.0, {}, false, 0.0, -0.5) &&
              p123Error.find("invalid delay") != std::string::npos,
          "P12.3 delay=-0.5 启动 → false + invalid delay");
    CHECK(p123Stats.empty(), "P12.3 stats 清空（失败即未播放）");

    // ---- P13：--wait-for-all-acked 退出确认等待（库层形态，对齐 ros2 语义"退出前等
    // 全部 RELIABLE 订阅者确认收齐（0=永等）"：播毕或停止后、销毁发布实体前逐
    // RELIABLE writer 分片等 ACK，BEST_EFFORT 跳过，超时/中断仅告警不改变返回值） ----
    // P13 用例共用夹具：kP13Bag 双主题各 20 条 @50ms（RELIABLE,TRANSIENT_LOCAL 快照
    // 同 P12 惯例；span 1950ms 同 kP8Bag 形态）
    CHECK(makeBag(kP13Bag,
              {{"/rt/p13a", "YomkRpc::MString", "RELIABLE,TRANSIENT_LOCAL", 20},
                  {"/rt/p13b", "YomkRpc::MString", "RELIABLE,TRANSIENT_LOCAL", 20}},
              50 * 1000000ULL),
          "P13 前置：双主题各 20 条 @50ms RELIABLE bag 构造成功");

    // P13.2 无订阅端永等不挂死（先于 P13.1 执行——P13.1 的 reader 在场会使"无订阅端"
    // 失真）：waitAckedMs=0 永等，无 matched RELIABLE reader → wait 立即通过（空集
    // 全确认），ok=true 正常播毕不挂死
    yomk::bagPlayReset();
    std::vector<FastDDSBagNode::BagTopicStat> p132Stats;
    std::string p132Error;
    std::atomic<bool> p132Done{false};
    bool p132Ok = false;
    std::thread p132Thread = startPlayThread(node, kP13Bag, p132Stats, p132Error, p132Ok,
        p132Done, 1.0, {}, false, 0.0, 0.0, 0);
    joinPlay(p132Thread, p132Done);
    CHECK(p132Ok, "P13.2 waitAckedMs=0 无订阅端 → wait 立即通过正常播毕返回 true（不挂死）");
    CHECK(p132Stats.size() == 2 && p132Stats[0].count == 20 && p132Stats[1].count == 20,
          "P13.2 stats 双主题各 20（确认等待不改变收尾统计口径）");

    // P13.1 显式生效+播毕收满：双 RELIABLE 订阅端在线 → waitAckedMs=2000 收尾逐 writer
    // 等 ACK（进程内 RELIABLE reader ACK 快，wait 立即通过——验证不 hang 不误报）
    auto *topicP13a = makePlayTopic(participant, "/rt/p13a", "YomkRpc::MString");
    auto *topicP13b = makePlayTopic(participant, "/rt/p13b", "YomkRpc::MString");
    PlayCountListener p13aListen;
    PlayCountListener p13bListen;
    auto *readerP13a = makePlayReader(subscriber, topicP13a, &p13aListen,
        dds::RELIABLE_RELIABILITY_QOS, dds::TRANSIENT_LOCAL_DURABILITY_QOS);
    auto *readerP13b = makePlayReader(subscriber, topicP13b, &p13bListen,
        dds::RELIABLE_RELIABILITY_QOS, dds::TRANSIENT_LOCAL_DURABILITY_QOS);
    CHECK(topicP13a != nullptr && topicP13b != nullptr && readerP13a != nullptr &&
              readerP13b != nullptr,
          "P13.1 前置：双 RELIABLE 订阅端 reader 先建在场");
    yomk::bagPlayReset();
    std::vector<FastDDSBagNode::BagTopicStat> p131Stats;
    std::string p131Error;
    std::atomic<bool> p131Done{false};
    bool p131Ok = false;
    std::thread p131Thread = startPlayThread(node, kP13Bag, p131Stats, p131Error, p131Ok,
        p131Done, 1.0, {}, false, 0.0, 0.0, 2000);
    CHECK(waitForCount(p13aListen.count, 20) && waitForCount(p13bListen.count, 20),
          "P13.1 双订阅端收满各 20（确认等待不改变播放流程）");
    joinPlay(p131Thread, p131Done);
    CHECK(p131Ok, "P13.1 waitAckedMs=2000 自然播毕返回 true（wait 不 hang）");
    CHECK(p131Stats.size() == 2 && p131Stats[0].count == 20 && p131Stats[1].count == 20,
          "P13.1 stats 双主题各 20（确认级收尾统计精确）");

    // P13.3 BEST_EFFORT 跳过：复用 kP3cBag（BEST_EFFORT,TRANSIENT_LOCAL 快照——writer
    // 侧 BEST_EFFORT 不参与 ackNack）→ waitAckedMs=100 收尾跳过等待快速播毕
    yomk::bagPlayReset();
    std::vector<FastDDSBagNode::BagTopicStat> p133Stats;
    std::string p133Error;
    std::atomic<bool> p133Done{false};
    bool p133Ok = false;
    std::thread p133Thread = startPlayThread(node, kP3cBag, p133Stats, p133Error, p133Ok,
        p133Done, 1.0, {}, false, 0.0, 0.0, 100);
    joinPlay(p133Thread, p133Done);
    CHECK(p133Ok, "P13.3 waitAckedMs=100 BEST_EFFORT bag 播毕返回 true（等待路径跳过）");

    // P13.4 超时不失败：kP13Bag waitAckedMs=1 极小片——进程内 ACK 毫秒级可能过可能
    // 超时，两种结果都 ok=true（超时告警不改返回值；不断言告警文本）
    yomk::bagPlayReset();
    std::vector<FastDDSBagNode::BagTopicStat> p134Stats;
    std::string p134Error;
    std::atomic<bool> p134Done{false};
    bool p134Ok = false;
    std::thread p134Thread = startPlayThread(node, kP13Bag, p134Stats, p134Error, p134Ok,
        p134Done, 1.0, {}, false, 0.0, 0.0, 1);
    joinPlay(p134Thread, p134Done);
    CHECK(p134Ok, "P13.4 waitAckedMs=1 极小片 → ok=true（超时/通过两态都不改返回值）");

    // P14.1 单主题重映射：/rt/p8a:=/rt/remap_a → 数据发新名（发布实体真实名）；新名
    // reader 精确收 20、旧名 reader 零增量（数据已迁新名无 matched）、未 remap 的 p8b
    // 照常；stats 显示 remap 后新名（预扫清单序）
    auto *topicRma = makePlayTopic(participant, "/rt/remap_a", "YomkRpc::MString");
    PlayCountListener rmaListen;
    auto *readerRma = makePlayReader(subscriber, topicRma, &rmaListen,
        dds::RELIABLE_RELIABILITY_QOS, dds::TRANSIENT_LOCAL_DURABILITY_QOS);
    CHECK(topicRma != nullptr && readerRma != nullptr,
          "P14.1 前置：新名 /rt/remap_a reader 先建在场（RELIABLE,TL）");
    const auto p8aBase141 = p8aListen.count.load();
    yomk::bagPlayReset();
    std::vector<FastDDSBagNode::BagTopicStat> p141Stats;
    std::string p141Error;
    std::atomic<bool> p141Done{false};
    bool p141Ok = false;
    std::thread p141Thread = startPlayThread(node, kP8Bag, p141Stats, p141Error, p141Ok,
        p141Done, 1.0, {}, false, 0.0, 0.0, -1, {"/rt/p8a:=/rt/remap_a"});
    CHECK(waitForCount(rmaListen.count, 20), "P14.1 新名 /rt/remap_a 订阅端精确收 20");
    joinPlay(p141Thread, p141Done);
    CHECK(p141Ok, "P14.1 play 全程完成返回 true");
    CHECK(p141Stats.size() == 2 && p141Stats[0].topic == "/rt/remap_a" &&
              p141Stats[0].count == 20 && p141Stats[1].topic == "/rt/p8b" &&
              p141Stats[1].count == 20,
          "P14.1 stats 两行：remap 后新名 + 未 remap 原名（预扫清单序、count 精确）");
    CHECK(p8aListen.count.load() == p8aBase141,
          "P14.1 旧名 /rt/p8a reader 零增量（数据已迁新名，旧名无 matched writer）");

    // P14.2 多规则全 remap：双规则双主题全迁新名（/rt/ra2 /rt/rb2）各收 20；旧名双
    // reader 全零增量；stats 两行全新名
    auto *topicRa2 = makePlayTopic(participant, "/rt/ra2", "YomkRpc::MString");
    auto *topicRb2 = makePlayTopic(participant, "/rt/rb2", "YomkRpc::MString");
    PlayCountListener ra2Listen;
    PlayCountListener rb2Listen;
    auto *readerRa2 = makePlayReader(subscriber, topicRa2, &ra2Listen,
        dds::RELIABLE_RELIABILITY_QOS, dds::TRANSIENT_LOCAL_DURABILITY_QOS);
    auto *readerRb2 = makePlayReader(subscriber, topicRb2, &rb2Listen,
        dds::RELIABLE_RELIABILITY_QOS, dds::TRANSIENT_LOCAL_DURABILITY_QOS);
    CHECK(topicRa2 != nullptr && topicRb2 != nullptr && readerRa2 != nullptr &&
              readerRb2 != nullptr,
          "P14.2 前置：双新名 reader 先建在场（RELIABLE,TL）");
    const auto p8aBase142 = p8aListen.count.load();
    const auto p8bBase142 = p8bListen.count.load();
    yomk::bagPlayReset();
    std::vector<FastDDSBagNode::BagTopicStat> p142Stats;
    std::string p142Error;
    std::atomic<bool> p142Done{false};
    bool p142Ok = false;
    std::thread p142Thread = startPlayThread(node, kP8Bag, p142Stats, p142Error, p142Ok,
        p142Done, 1.0, {}, false, 0.0, 0.0, -1,
        {"/rt/p8a:=/rt/ra2", "/rt/p8b:=/rt/rb2"});
    CHECK(waitForCount(ra2Listen.count, 20) && waitForCount(rb2Listen.count, 20),
          "P14.2 双新名订阅端各精确收 20");
    joinPlay(p142Thread, p142Done);
    CHECK(p142Ok, "P14.2 play 全程完成返回 true");
    CHECK(p142Stats.size() == 2 && p142Stats[0].topic == "/rt/ra2" &&
              p142Stats[0].count == 20 && p142Stats[1].topic == "/rt/rb2" &&
              p142Stats[1].count == 20,
          "P14.2 stats 两行全新名（多规则逐主题映射生效）");
    CHECK(p8aListen.count.load() == p8aBase142 && p8bListen.count.load() == p8bBase142,
          "P14.2 旧名双 reader 全零增量（双主题数据全迁新名）");

    // P14.3 与 --topics 叠加（过滤按旧名先过滤后 remap）：只回放 p8a 并 remap →
    // ra3 收 20、p8b 被过滤零发；stats 单行 remap 后新名
    auto *topicRa3 = makePlayTopic(participant, "/rt/ra3", "YomkRpc::MString");
    PlayCountListener ra3Listen;
    auto *readerRa3 = makePlayReader(subscriber, topicRa3, &ra3Listen,
        dds::RELIABLE_RELIABILITY_QOS, dds::TRANSIENT_LOCAL_DURABILITY_QOS);
    CHECK(topicRa3 != nullptr && readerRa3 != nullptr,
          "P14.3 前置：新名 /rt/ra3 reader 先建在场（RELIABLE,TL）");
    const auto p8bBase143 = p8bListen.count.load();
    yomk::bagPlayReset();
    std::vector<FastDDSBagNode::BagTopicStat> p143Stats;
    std::string p143Error;
    std::atomic<bool> p143Done{false};
    bool p143Ok = false;
    std::thread p143Thread = startPlayThread(node, kP8Bag, p143Stats, p143Error, p143Ok,
        p143Done, 1.0, {"/rt/p8a"}, false, 0.0, 0.0, -1, {"/rt/p8a:=/rt/ra3"});
    CHECK(waitForCount(ra3Listen.count, 20), "P14.3 新名 /rt/ra3 订阅端精确收 20");
    joinPlay(p143Thread, p143Done);
    CHECK(p143Ok && p143Stats.size() == 1 && p143Stats[0].topic == "/rt/ra3" &&
              p143Stats[0].count == 20,
          "P14.3 stats 单行新名（--topics 过滤按旧名生效，入选集合只含 p8a）");
    CHECK(p8bListen.count.load() == p8bBase143, "P14.3 p8b 被过滤零增量（过滤先于 remap）");

    // P14.4 非法格式+重复 old 拒播：节点层校验失败快速返回（不建任何 DDS 实体），
    // stats 清空——CLI 双层校验的节点层兜底（对齐 P12.3 同步快速失败形态）
    std::vector<FastDDSBagNode::BagTopicStat> p144Stats;
    std::string p144Error;
    p144Stats.emplace_back();  // 预置残留：验证失败路径 stats 清空契约
    CHECK(!node.play(kP8Bag, p144Stats, &p144Error, 1.0, {}, false, 0.0, 0.0, -1,
              {"/rt/p8a:/rt/bad"}) &&
              p144Error.find("invalid remap rule") != std::string::npos &&
              p144Stats.empty(),
          "P14.4 缺 ':=' 格式非法 → false + invalid remap rule + stats 清空");
    CHECK(!node.play(kP8Bag, p144Stats, &p144Error, 1.0, {}, false, 0.0, 0.0, -1,
              {"/rt/a:=/rt/b", "/rt/a:=/rt/c"}) &&
              p144Error.find("duplicate remap rule") != std::string::npos,
          "P14.4 重复 old 规则 → false + duplicate remap rule");

    // P14.5 old 不在 bag 静默无效：规则不命中即无效果（对齐 ros2 rcl remap 语义，
    // 无匹配发生即无效果不报错）——正常播毕，数据照常发原名
    const auto p8aBase145 = p8aListen.count.load();
    const auto p8bBase145 = p8bListen.count.load();
    yomk::bagPlayReset();
    std::vector<FastDDSBagNode::BagTopicStat> p145Stats;
    std::string p145Error;
    std::atomic<bool> p145Done{false};
    bool p145Ok = false;
    std::thread p145Thread = startPlayThread(node, kP8Bag, p145Stats, p145Error, p145Ok,
        p145Done, 1.0, {}, false, 0.0, 0.0, -1, {"/rt/nonexistent:=/rt/whatever"});
    joinPlay(p145Thread, p145Done);
    CHECK(p145Ok, "P14.5 未命中规则不影响播放 → ok=true 正常播毕");
    CHECK(p145Stats.size() == 2 && p145Stats[0].topic == "/rt/p8a" &&
              p145Stats[0].count == 20 && p145Stats[1].topic == "/rt/p8b" &&
              p145Stats[1].count == 20,
          "P14.5 stats 原名照常（静默无效不改清单序与统计口径）");
    CHECK(p8aListen.count.load() == p8aBase145 + 20 &&
              p8bListen.count.load() == p8bBase145 + 20,
          "P14.5 双 reader 各 +20（数据照常发原名）");

    // ---- P15：--read-ahead-queue-size 预读队列深度（库层形态，对齐 ros2 语义 "size of
    // message queue rosbag tries to hold in memory to help deterministic playback"：
    // 读侧 PlayQueue 背压边界，大→占内存但防播放延迟；writer QoS 独立常量不联动） ----
    // P15.1 小深度守恒：queue=1 读播交替极端形态（每入队 1 条即被取出），双 RELIABLE,TL
    // reader 各精确 +20；stats 双行 topic/count/bytes 全精确（深度不影响守恒）
    const auto p8aBase151 = p8aListen.count.load();
    const auto p8bBase151 = p8bListen.count.load();
    yomk::bagPlayReset();
    std::vector<FastDDSBagNode::BagTopicStat> p151Stats;
    std::string p151Error;
    std::atomic<bool> p151Done{false};
    bool p151Ok = false;
    std::thread p151Thread = startPlayThread(node, kP8Bag, p151Stats, p151Error, p151Ok,
        p151Done, 1.0, {}, false, 0.0, 0.0, -1, {}, 1);
    joinPlay(p151Thread, p151Done);
    CHECK(p151Ok, "P15.1 queue=1 小深度 → ok=true 正常播毕");
    CHECK(p151Stats.size() == 2 && p151Stats[0].topic == "/rt/p8a" &&
              p151Stats[0].count == 20 && p151Stats[0].bytes == 20 * kPayloadSize &&
              p151Stats[1].topic == "/rt/p8b" && p151Stats[1].count == 20 &&
              p151Stats[1].bytes == 20 * kPayloadSize,
          "P15.1 stats 双行 20 条×5B 全精确（小深度零丢失零重复）");
    CHECK(p8aListen.count.load() == p8aBase151 + 20 &&
              p8bListen.count.load() == p8bBase151 + 20,
          "P15.1 双 RELIABLE,TL reader 各精确 +20（读播交替极端形态守恒）");

    // P15.2 大深度守恒：queue=10000 ≥整场 40 条（零背压形态，读侧全速入队），各精确
    // +20；stats 全精确——深度上界不改变守恒
    const auto p8aBase152 = p8aListen.count.load();
    const auto p8bBase152 = p8bListen.count.load();
    yomk::bagPlayReset();
    std::vector<FastDDSBagNode::BagTopicStat> p152Stats;
    std::string p152Error;
    std::atomic<bool> p152Done{false};
    bool p152Ok = false;
    std::thread p152Thread = startPlayThread(node, kP8Bag, p152Stats, p152Error, p152Ok,
        p152Done, 1.0, {}, false, 0.0, 0.0, -1, {}, 10000);
    joinPlay(p152Thread, p152Done);
    CHECK(p152Ok, "P15.2 queue=10000 大深度 → ok=true 正常播毕");
    CHECK(p152Stats.size() == 2 && p152Stats[0].count == 20 && p152Stats[1].count == 20,
          "P15.2 stats 双行各 20 精确（零背压形态守恒）");
    CHECK(p8aListen.count.load() == p8aBase152 + 20 &&
              p8bListen.count.load() == p8bBase152 + 20,
          "P15.2 双 reader 各精确 +20（大深度零背压守恒）");

    // P15.3 零值拒播：0 容量队列 push 背压恒真死锁必拦（入口校验快速返回不建任何
    // DDS 实体），stats 清空——CLI 三段校验的节点层兑底（对齐 P12.3 同步快速失败形态）
    std::vector<FastDDSBagNode::BagTopicStat> p153Stats;
    std::string p153Error;
    p153Stats.emplace_back();  // 预置残留：验证失败路径 stats 清空契约（失败=未播放）
    CHECK(!node.play(kP8Bag, p153Stats, &p153Error, 1.0, {}, false, 0.0, 0.0, -1, {}, 0) &&
              p153Error.find("invalid read-ahead queue size") != std::string::npos,
          "P15.3 queue=0 启动 → false + invalid read-ahead queue size");
    CHECK(p153Stats.empty(), "P15.3 stats 清空（失败即未播放）");

    // ---- P16：--qos-profile-overrides-path QoS 覆盖文件（对齐 ros2 同名选项语义，本仓
    // YAML 换 JSON：顶层键=bag 原主题名，值={reliability, durability} 枚举名可独立给
    // ——文件命中 > bag 内快照 > 旧 bag 回退三档优先级最高档；未知字段键忽略、非法枚
    // 举值报错、不在 bag 清单的主题键静默无效）----
    // P16.1 reliability 覆盖生效（对齐 P3b 反向钉死形态）：p8a 覆盖 BEST_EFFORT+
    // VOLATILE → offer 降级致 RELIABLE,TL reader 匹配不建立收 0；未覆盖 p8b 照常收
    // 满——覆盖只作用命中主题（复用 P8 双 reader 零新 bag）
    {
        const std::string qosFile1 = "play_p16_qos1.json";
        std::ofstream out(qosFile1);
        out << R"({"/rt/p8a": {"reliability": "BEST_EFFORT", "durability": "VOLATILE"}})";
        out.close();
        const auto p8aBase161 = p8aListen.count.load();
        const auto p8bBase161 = p8bListen.count.load();
        yomk::bagPlayReset();
        std::vector<FastDDSBagNode::BagTopicStat> p161Stats;
        std::string p161Error;
        std::atomic<bool> p161Done{false};
        bool p161Ok = false;
        std::thread p161Thread = startPlayThread(node, kP8Bag, p161Stats, p161Error,
            p161Ok, p161Done, 1.0, {}, false, 0.0, 0.0, -1, {}, 1000, qosFile1);
        CHECK(waitForCount(p8bListen.count, p8bBase161 + 20),
              "P16.1 未覆盖 p8b RELIABLE,TL reader 收满 +20（覆盖只作用命中主题）");
        joinPlay(p161Thread, p161Done);
        CHECK(p161Ok, "P16.1 覆盖文件加载生效 → ok=true 正常播毕");
        CHECK(p161Stats.size() == 2 && p161Stats[0].count == 20 && p161Stats[1].count == 20,
              "P16.1 stats 两行各 20（覆盖不改变统计口径）");
        std::this_thread::sleep_for(std::chrono::seconds(2));  // 否定窗口（对齐 P3b）
        CHECK(p8aListen.count.load() == p8aBase161,
              "P16.1 p8a RELIABLE,TL reader 收 0（offer 降 BEST_EFFORT,VL 匹配不建立——覆盖生效实锤）");
        std::filesystem::remove(qosFile1);
    }

    // P16.2 durability 覆盖+未知键忽略+不在清单静默：p8b 覆盖 durability=VOLATILE →
    // offer 降级致 TL reader 匹配不建立收 0；p8a 恒等覆盖（reliability=RELIABLE）+
    // 未知字段键 history 忽略 + absent 主题键静默无效 → 照常收满
    {
        const std::string qosFile2 = "play_p16_qos2.json";
        std::ofstream out(qosFile2);
        out << R"({"/rt/p8b": {"durability": "VOLATILE"},)"
               R"( "/rt/p8a": {"history": "KEEP_LAST", "reliability": "RELIABLE"},)"
               R"( "/rt/absent": {"reliability": "BEST_EFFORT"}})";
        out.close();
        const auto p8aBase162 = p8aListen.count.load();
        const auto p8bBase162 = p8bListen.count.load();
        yomk::bagPlayReset();
        std::vector<FastDDSBagNode::BagTopicStat> p162Stats;
        std::string p162Error;
        std::atomic<bool> p162Done{false};
        bool p162Ok = false;
        std::thread p162Thread = startPlayThread(node, kP8Bag, p162Stats, p162Error,
            p162Ok, p162Done, 1.0, {}, false, 0.0, 0.0, -1, {}, 1000, qosFile2);
        CHECK(waitForCount(p8aListen.count, p8aBase162 + 20),
              "P16.2 p8a 恒等覆盖+未知键忽略+不在清单静默 → RELIABLE,TL reader 收满 +20");
        joinPlay(p162Thread, p162Done);
        CHECK(p162Ok, "P16.2 覆盖文件加载生效 → ok=true 正常播毕");
        CHECK(p162Stats.size() == 2 && p162Stats[0].count == 20 && p162Stats[1].count == 20,
              "P16.2 stats 两行各 20（覆盖不改变统计口径）");
        std::this_thread::sleep_for(std::chrono::seconds(2));  // 否定窗口（对齐 P3b）
        CHECK(p8bListen.count.load() == p8bBase162,
              "P16.2 p8b RELIABLE,TL reader 收 0（durability 覆盖 VOLATILE 降级匹配不建立）");
        std::filesystem::remove(qosFile2);
    }

    // P16.3 错误路径三连（同步直调，对齐 P12.3/P15.3 快速失败形态）：文件级校验在
    // 入口段完成（先于 bag 预扫触达），失败即拒播 + stats 清空
    std::vector<FastDDSBagNode::BagTopicStat> p163Stats;
    std::string p163Error;
    p163Stats.emplace_back();  // 预置残留：验证失败路径 stats 清空契约
    {
        const std::string qosBadEnum = "play_p16_badenum.json";
        std::ofstream out(qosBadEnum);
        out << R"({"/rt/p8a": {"reliability": "RELIABL"}})";
        out.close();
        CHECK(!node.play(kP8Bag, p163Stats, &p163Error, 1.0, {}, false, 0.0, 0.0, -1,
                  {}, 1000, qosBadEnum) &&
                  p163Error.find("topic [/rt/p8a] invalid qos string [RELIABL]") !=
                      std::string::npos &&
                  p163Stats.empty(),
              "P16.3a 非法枚举值 → false + topic invalid qos string + stats 清空");
        std::filesystem::remove(qosBadEnum);
    }
    {
        const std::string qosMissing = "play_p16_missing.json";
        CHECK(!node.play(kP8Bag, p163Stats, &p163Error, 1.0, {}, false, 0.0, 0.0, -1,
                  {}, 1000, qosMissing) &&
                  p163Error.find(
                      "open qos overrides file [play_p16_missing.json] failed") !=
                      std::string::npos,
              "P16.3b 文件不存在 → false + open qos overrides file failed");
    }
    {
        const std::string qosBadJson = "play_p16_badjson.json";
        std::ofstream out(qosBadJson);
        out << "{not valid json";
        out.close();
        CHECK(!node.play(kP8Bag, p163Stats, &p163Error, 1.0, {}, false, 0.0, 0.0, -1,
                  {}, 1000, qosBadJson) &&
                  p163Error.find(
                      "parse qos overrides file [play_p16_badjson.json] failed:") !=
                      std::string::npos,
              "P16.3c 坏 JSON → false + parse qos overrides file failed");
        std::filesystem::remove(qosBadJson);
    }
    CHECK(p163Stats.empty(), "P16.3 stats 清空（失败即未播放）");

    // ---- P17：set_name 类型 bag 回放（闭环 6）：metadata type 字段为新名 → 回放端按发现名
    //      重建透传 writer → 订阅端同名透传 reader 收流成功（类型名全链透传实证） ----
    CHECK(makeBag(kP17Bag,
              {{"/rt/play_setname", "std_msgs::msg::dds_::String_", "RELIABLE,TRANSIENT_LOCAL", 20}},
              200 * 1000000ULL),
          "P17 前置：set_name 新名短回放 bag 构造成功");
    PlayCountListener snRecv;
    auto *topicSn = makePlayTopic(participant, "/rt/play_setname",
        "std_msgs::msg::dds_::String_");
    auto *readerSn = makePlayReader(subscriber, topicSn, &snRecv,
        dds::RELIABLE_RELIABILITY_QOS, dds::TRANSIENT_LOCAL_DURABILITY_QOS);
    CHECK(topicSn != nullptr && readerSn != nullptr, "P17 前置：透传 reader 先建在场");
    yomk::bagPlayReset();
    std::vector<FastDDSBagNode::BagTopicStat> p17Stats;
    std::string p17Error;
    std::atomic<bool> p17Done{false};
    bool p17Ok = false;
    std::thread p17Thread = startPlayThread(node, kP17Bag, p17Stats, p17Error, p17Ok, p17Done);
    CHECK(waitForCount(snRecv.count, 20),
          "P17 透传订阅端收满（bag type 新名 → 回放/订阅端按名匹配全通）");
    joinPlay(p17Thread, p17Done);
    CHECK(p17Ok, "P17 play 全程完成返回 true");
    CHECK(p17Stats.size() == 1 && p17Stats[0].count == 20, "P17 stats 单主题 20 条");

    cleanup();
    return testReport("TestFastDDSBagNodePlay");
}
