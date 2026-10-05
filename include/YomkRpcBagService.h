#pragma once
// YomkRpcBagService 对外头：定义 YOMKRPC_BAG_* 宏（见 YomkRpcAPI.h）所打包的请求结构
// （DDSBagNode/DDSBagRecord/DDSBagInfo/DDSBagReindex），以及服务类 YomkRpcBagService 的声明。
// 录制能力由内部的
// FastDDSBagNode 提供（类型无关：透传原始 CDR 字节直写 mcap，schema_id=0 无 schema 通道，
// 零消息类型依赖）。
#include <YomkServer/YomkAPI.h>

#include <atomic>
#include <memory>
#include <mutex>
#include <string>
#include <vector>
class FastDDSBagNode;
using namespace yomk;

class YomkRpcBagService : public YomkService
{
public:
    YomkRpcBagService(YomkServer* server);
    virtual ~YomkRpcBagService();
    virtual int init() override;

private:
    YomkResponse getVersion(YomkPkgPtr pkg);
    YomkResponse createNode(YomkPkgPtr pkg);
    YomkResponse bagRecord(YomkPkgPtr pkg);
    YomkResponse deleteNode(YomkPkgPtr pkg);
    YomkResponse bagInfo(YomkPkgPtr pkg);
    YomkResponse bagReindex(YomkPkgPtr pkg);

private:
    // 单 bag 节点：一个进程至多一个实例，重复 create 须先 delete；服务析构时自动销毁节点
    // 并按节点内部顺序清理全部 DDS 实体（推荐进程退出前经 /delete_node 显式清理）。
    std::unique_ptr<FastDDSBagNode> node_;
    std::mutex mtx_;  // 串行化 node_ 的增删查改与节点入口调用
};

// bag record 录制停止标志：录制循环（FastDDSBagNode::record 内部，每 100ms 轮询）只读，
// SIGINT 处理函数（CLI onSignal）经 bagRecordStop 写——atomic 无锁 store 保证
// async-signal-safe（不可经 YOMK_REQUEST 停止：请求链路含锁与内存分配，handler 内禁用）。
// 调用方在发起录制前应先 bagRecordReset 复位，避免上次会话残留置位导致秒退；
// bag 服务 handler 录制前亦兜底复位 stop（只复位 stop 不动 paused——暂停是调用方启动意图）。
//
// bag record 暂停标志（--start-paused）：暂停态由 CLI --start-paused 或库用户
// bagRecordPause 置位，回调写路径（BagSubListener）每条消息检查——置位期间照常 take
// 排空（防恢复后旧数据涌入）但丢弃不写，bagRecordResume 后开始写入。订阅与落盘路径
// 不受影响；不经 DDSBagRecord 协议传递（CLI 与 bag 服务同进程，全局标志直读）。
namespace yomk
{
inline std::atomic<bool> g_bagRecordStop{false};
inline std::atomic<bool> g_bagRecordPaused{false};
inline void bagRecordStop()
{
    g_bagRecordStop.store(true);
}
inline void bagRecordPause()
{
    g_bagRecordPaused.store(true);
}
inline void bagRecordResume()
{
    g_bagRecordPaused.store(false);
}
inline void bagRecordReset()
{
    g_bagRecordStop.store(false);
    g_bagRecordPaused.store(false);  // 双复位：防上次会话残留暂停态（表现为何都不录）
}
}  // namespace yomk

// create_node 请求负载。
struct DDSBagNode
{
    uint32_t domainId;  // DDS 域号，合法范围 [0,232]
};

// bag_record 请求负载。
struct DDSBagRecord
{
    std::vector<std::string> topics;  // 待录制主题清单（至少 1 个，清单内重复/空名拒绝）
    std::string outputDir;            // bag 目录名/路径（相对/绝对均可，父目录自动创建，
                                      // 已存在报错）；空 = 缺省时间戳名 bag_<YYYY-MM-DD_HH-MM-SS_mmm>
    uint64_t maxBagSize = 0;          // 单分片最大字节数；0=不分片（尾部缺省字段，旧调用方零改动）
    uint64_t maxBagDurationSec = 0;   // 单分片最大时长秒；0=不分片（尾部缺省字段，同用先到先分）
    uint64_t maxCacheSize = 0;        // 写缓存双缓冲字节数；0=直写（尾部缺省字段，CLI 默认传 100MiB）
};

// bag_info 请求负载。
struct DDSBagInfo
{
    std::string bagDir;  // bag 目录名/路径（相对/绝对均可，须含 metadata.json）
};

// bag_reindex 请求负载。
struct DDSBagReindex
{
    std::string bagDir;  // bag 目录名/路径（相对/绝对均可；metadata.json 缺失/损坏均可重建）
};

// clang-format off
// YomkMsg 是 YomkServer 第三方宏，cppcheck 未 --library 配置识别（unknownMacro 属工具配置需求，非自有源码缺陷）
// cppcheck-suppress unknownMacro
YomkMsg(DDSBagNode, DDSBagNode, msg)
YomkMsg(DDSBagRecord, DDSBagRecord, msg)
YomkMsg(DDSBagInfo, DDSBagInfo, msg)
YomkMsg(DDSBagReindex, DDSBagReindex, msg)
