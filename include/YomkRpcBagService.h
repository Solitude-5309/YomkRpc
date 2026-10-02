#pragma once
// YomkRpcBagService 对外头：定义 YOMKRPC_BAG_* 宏（见 YomkRpcAPI.h）所打包的请求结构
// （DDSBagNode/DDSBagRecord），以及服务类 YomkRpcBagService 的声明。录制能力由内部的
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

private:
    // 单 bag 节点：一个进程至多一个实例，重复 create 须先 delete；服务析构时自动销毁节点
    // 并按节点内部顺序清理全部 DDS 实体（推荐进程退出前经 /delete_node 显式清理）。
    std::unique_ptr<FastDDSBagNode> node_;
    std::mutex mtx_;  // 串行化 node_ 的增删查改与节点入口调用
};

// bag record 录制停止标志：录制循环（FastDDSBagNode::record 内部，每 100ms 轮询）只读，
// SIGINT 处理函数（CLI onSignal）经 bagRecordStop 写——atomic 无锁 store 保证
// async-signal-safe（不可经 YOMK_REQUEST 停止：请求链路含锁与内存分配，handler 内禁用）。
// 调用方在发起录制前应先 bagRecordReset 复位，避免上次会话残留置位导致秒退。
namespace yomk
{
inline std::atomic<bool> g_bagRecordStop{false};
inline void bagRecordStop()
{
    g_bagRecordStop.store(true);
}
inline void bagRecordReset()
{
    g_bagRecordStop.store(false);
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
};

// clang-format off
// YomkMsg 是 YomkServer 第三方宏，cppcheck 未 --library 配置识别（unknownMacro 属工具配置需求，非自有源码缺陷）
// cppcheck-suppress unknownMacro
YomkMsg(DDSBagNode, DDSBagNode, msg)
YomkMsg(DDSBagRecord, DDSBagRecord, msg)
