#pragma once
// Fast DDS participant 级 QoS 合成辅助（XML 配置通道三级提取链 + 发现服务器 serverLocator
// 直配）：三个节点类（FastDDSNode/FastDDSDebugNode/FastDDSBagNode）的 setDomainId 合成链共用。
// 纯头文件 inline 实现，无对应源文件；仅内部使用，不出现在公共头（公共头零 fastdds 依赖）。

#include <fastdds/dds/core/detail/DDSReturnCode.hpp>
#include <fastdds/dds/domain/DomainParticipantFactory.hpp>

#include <fastdds/rtps/attributes/RTPSParticipantAttributes.hpp>
#include <fastdds/rtps/common/Locator.hpp>
#include <fastdds/rtps/transport/TCPv4TransportDescriptor.hpp>  // TCP 直配项的 participant 侧传输注入
#include <fastdds/utils/IPLocator.hpp>

#include <cstdint>
#include <cstdlib>
#include <string>
#include <vector>

#include "YomkRpcService.h"  // DDSServerInfo（平铺引号命中 include/；本头仅库内与测试树编译，src 头不安装）

namespace yomkrpc_xml
{

// participant 级配置来源：服务层从 YomkRpcService::xmlCache()（已成功加载的 XML raw 串）与
// 负载发现服务器直配聚合串组装后传入 setDomainId；各字段均可为空（=现状行为，零改动兼容）。
struct ParticipantQosSpec
{
    std::string xml;            // 已成功 load 的 XML profiles raw 串；空=未加载，跳过提取
    std::string udpServerSpec;  // UDP 发现服务器直配聚合串 "ip[:port];ip[:port]"；空=不直配
    std::string tcpServerSpec;  // TCP 发现服务器直配聚合串（格式同上，port 为 TCP 监听口）；空=不直配
};

// serverLocator port 合法上界（UDP 端口上限）
constexpr uint32_t kDdsPortMax = 65535;

// serverSpec 聚合串解析 → DDSServerInfo 列表：分号分隔（对齐 ROS_DISCOVERY_SERVER 官方格式）
// "ip[:port]" 项，port 缺省 11811，
// 合法范围 [1,65535]；空项/空 ip/非纯数字/越界 port 均为非法，err 回填具体项文本并返回 false。
inline bool parseServerLocator(const std::string& spec,
        std::vector<DDSServerInfo>& out, std::string& err)
{
    out.clear();
    err.clear();
    size_t begin = 0;
    while (true)
    {
        const size_t sep = spec.find(';', begin);
        const std::string item =
            spec.substr(begin, sep == std::string::npos ? std::string::npos : sep - begin);
        if (item.empty())
        {
            err = "empty server item in serverLocator [" + spec + "]";
            return false;
        }
        DDSServerInfo server;
        const size_t colon = item.find(':');
        server.address = colon == std::string::npos ? item : item.substr(0, colon);
        if (server.address.empty())
        {
            err = "empty address in serverLocator item [" + item + "]";
            return false;
        }
        if (colon != std::string::npos)
        {
            const std::string portText = item.substr(colon + 1);
            if (portText.empty() || portText.find_first_not_of("0123456789") != std::string::npos)
            {
                err = "invalid port in serverLocator item [" + item + "]";
                return false;
            }
            const unsigned long parsed = std::strtoul(portText.c_str(), nullptr, 10);
            if (parsed < 1 || parsed > kDdsPortMax)
            {
                err = "port out of range [1,65535] in serverLocator item [" + item + "]";
                return false;
            }
            server.port = static_cast<uint32_t>(parsed);
        }
        out.push_back(std::move(server));
        if (sep == std::string::npos)
        {
            break;
        }
        begin = sep + 1;
    }
    return true;
}

// 发现服务器直配叠加：CLIENT 协议 + m_DiscoveryServers 重建（覆盖 XML discoveryServersList，
// 双通道合并语义：直配 > XML 提取打底 > 默认）。3.6.1 client 连接 server 只需 ip:port +
// CLIENT 协议（server 响应自报 GUID）。UDP/TCP 两聚合串分别解析：UDP 项填默认 UDPv4 locator，
// TCP 项填 TCPv4 locator——participant 必须具备 TCPv4 传输才可达，builtin 默认集不含 TCP，
// 故补注入 TCPv4TransportDescriptor（3.6 transport().user_transports 与 builtin "alongside"
// 共存，UDP/SHM 数据通道不受影响；XML 提取已注入 TCPv4 描述符时跳过防重复）。任一 spec 非法时 err 回填
// 返回 false（不改动 qos）；双空串=不直配（幂等返回 true）。
inline bool applyDiscoveryServers(eprosima::fastdds::dds::DomainParticipantQos& qos,
        const std::string& udpServerSpec, const std::string& tcpServerSpec, std::string& err)
{
    std::vector<DDSServerInfo> udpServers;
    std::vector<DDSServerInfo> tcpServers;
    // 空 spec = 该通道不直配（跳过解析，非错误）；parseServerLocator 本身视空 spec 为非法
    if ((!udpServerSpec.empty() && !parseServerLocator(udpServerSpec, udpServers, err)) ||
        (!tcpServerSpec.empty() && !parseServerLocator(tcpServerSpec, tcpServers, err)))
    {
        return false;
    }
    if (udpServers.empty() && tcpServers.empty())
    {
        return true;
    }
    using eprosima::fastdds::rtps::DiscoveryProtocol;
    using eprosima::fastdds::rtps::IPLocator;
    using eprosima::fastdds::rtps::Locator;
    using eprosima::fastdds::rtps::TCPv4TransportDescriptor;
    qos.wire_protocol().builtin.discovery_config.discoveryProtocol = DiscoveryProtocol::CLIENT;
    auto& servers = qos.wire_protocol().builtin.discovery_config.m_DiscoveryServers;
    servers.clear();
    for (const auto& server : udpServers)
    {
        Locator locator;  // 默认 kind=LOCATOR_KIND_UDPv4
        IPLocator::setIPv4(locator, server.address);
        locator.port = server.port;
        servers.push_back(locator);
    }
    for (const auto& server : tcpServers)
    {
        Locator locator;
        locator.kind = LOCATOR_KIND_TCPv4;  // kind 是 Locator 公有成员，IPLocator 无 setKind
        IPLocator::setIPv4(locator, server.address);
        locator.port = server.port;
        servers.push_back(locator);
    }
    if (!tcpServers.empty())
    {
        bool hasTcpTransport = false;
        for (const auto& transport : qos.transport().user_transports)
        {
            // 描述符基类无 kind() 元数据，用 dynamic_pointer_cast 识别 TCPv4 描述符（含派生）
            if (std::dynamic_pointer_cast<TCPv4TransportDescriptor>(transport))
            {
                hasTcpTransport = true;
                break;
            }
        }
        if (!hasTcpTransport)
        {
            qos.transport().user_transports.push_back(std::make_shared<TCPv4TransportDescriptor>());
        }
    }
    return true;
}

// participant 三级提取链（qos 入参须已置 PARTICIPANT_QOS_DEFAULT）：
// ①按节点名 profile（profile_name 逐字全等无通配，配套约定 profile 名 = YomkRpc 节点名）
// → ②默认档（is_default_profile=true 兜底，factory 单值槽位语义，建议至多一个默认档）
// → ③两级全落空保持 DEFAULT 底座（回退现状，非错误）。节点名下沉由调用点既有逻辑在
// 提取后执行（XML 提取会整体填充 qos 含 name，节点名必须最终覆盖，不可被 XML 顶掉；
// xml 为空=未加载现状直接返回）。Debug/Bag 节点无用户名，按名 profile 无意义仅默认档可兜底。
inline void participantQosFromXml(eprosima::fastdds::dds::DomainParticipantQos& qos,
        const std::string& nodeName, const std::string& xml)
{
    if (xml.empty())
    {
        return;
    }
    auto* factory = eprosima::fastdds::dds::DomainParticipantFactory::get_instance();
    if (factory->get_participant_qos_from_xml(xml, qos, nodeName) !=
            eprosima::fastdds::dds::RETCODE_OK &&
        factory->get_default_participant_qos_from_xml(xml, qos) !=
            eprosima::fastdds::dds::RETCODE_OK)
    {
        qos = eprosima::fastdds::dds::PARTICIPANT_QOS_DEFAULT;
    }
}

}  // namespace yomkrpc_xml
