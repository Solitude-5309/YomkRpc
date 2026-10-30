/**
 * @file TestYomkRpcNodeLifecycle.cpp
 * @brief MC2：真实 DDS 节点生命周期 + 边界值测试
 *
 * 范围：MC1（DDS-free）刻意豁免的 /create_node 成功/重名路径（经 setDomainId 创建真实
 *       participant），以及 FastDDSNode 的真实 DDS 生命周期与 domainId/nodeName 边界行为。
 * 覆盖：
 *   Part A（服务层，经 invoke）：
 *     A-1 生命周期：创建/重名/同域共存/删除/重复删除；
 *     A-2 domainId 边界：有效域 0 与上界 232（eOk）、233 与 UINT32_MAX（越界 → createNode
 *         前置校验直接 eNo "out of valid range"，不再深入 setDomainId）；
 *     A-3 nodeName 边界：空串（O2 拒绝 → eNo "node name is empty"）、单字符/1024B 超长（eOk）。
 *   Part B（直接单测 FastDDSNode）：setDomainId 重复设置守卫（participant_!=nullptr → false）
 *     与非法域 UINT32_MAX（create_participant 返回 null → false）；O1 后服务层已短路非法域，
 *     故 setDomainId 的 participant==null 分支改由此直接单测覆盖。
 *   Part C（闭环 4 helper 白盒）：parseServerLocator 容错矩阵 / applyDiscoveryServers（UDP+TCP
 *     双通道，TCP 项自动补注入 TCPv4 传输）读回 CLIENT+locator 逐项（非法 spec 不改动 qos）/
 *     participantQosFromXml 三态（按名 profile → 默认档兑底 → 保持 DEFAULT 回退）与 xml 空串直返。
 *   Part D（闭环 4 直配端到端）：FastDDSNode 白盒带 spec 创建（server 不在场 CLIENT 模式
 *     仍创建成功）+ 非法 UDP/TCP serverLocator false + err 回显。
 *   Part E（闭环 4 双通道合并）：XML discoveryServersList 提取打底 → 直配重建覆盖
 *     （合并语义：直配 > XML 打底 > 默认）。
 *   Part F（闭环 4 连通性）：spawn 项目自带 discovery server 子进程（fastdds discovery CLI，
 *     setpgid 独立进程组 + 收尾组杀——CLI 是 python 包装，其内层 spawn 的 server 二进制
 *     不会被单杀 python 的 SIGTERM 波及，孤儿持 stdio 管道写端致上层读端永挂（实测根因），
 *     组杀整组全灭根治）；A/B 双节点均直配 CLIENT 且故意异域（201/202）——DS 模式 PDP/EDP
 *     全链路不校验 domain_id（PDPClientListener/PDPServerListener 源码注释："In Discovery
 *     Server we don't impose domain ids to be the same"；跨域丢弃仅存在于简单发现
 *     PDPSimple 的基类校验），互通即域号与 server 无关的实证；多播 PDP 被禁，发现必经 server。
 *
 * 关键不变式：每个 eOk 创建的节点必须在 main 返回前经 /delete_node 显式删除；Part B/D 的
 *   FastDDSNode 为栈对象、在 main 返回前离开作用域析构。以规避 YOMK 框架 atexit 服务析构
 *   晚于 FastDDS DomainParticipantFactory 单例销毁导致的静态析构顺序问题
 *   （参见 examples/ExampleYomkRpcPub.cpp：退出前显式销毁节点）。
 *
 * 风格：纯 main() + CHECK 宏 + 失败计数（零第三方依赖），返回非 0 表示存在失败用例。
 */

#include "TestCheck.h"
#include "YomkRpcService.h" // 服务/DDSNode/String/YOMK_* 宏
#include "FastDDSNode.h"    // Part B 直接单元测试 setDomainId
#include "FastDDSXmlHelper.h" // Part C-E 白盒：yomkrpc_xml 三件套（parseServerLocator/applyDiscoveryServers/participantQosFromXml）

#include <YomkRpcMsg/YomkRpcMsg.hpp>            // Part F 收发类型
#include <YomkRpcMsg/YomkRpcMsgPubSubTypes.hpp> // MStringPubSubType

#include <atomic>
#include <chrono>
#include <cstdint>
#include <iostream>
#include <signal.h> // Part F kill（POSIX）
#include <string>
#include <sys/wait.h> // Part F waitpid
#include <thread>
#include <unistd.h> // Part F fork/execl
#include <vector>

namespace
{
      // 有效域（FastDDS 约 0-232），避开默认域 0 的潜在网络干扰
      constexpr uint32_t TEST_DOMAIN = 200;

      // 构造 /create_node 请求包
      YomkPkgPtr mkNode(uint32_t domainId, const std::string &nodeName)
      {
            return YomkMkPtr(DDSNode, DDSNode{nodeName, domainId});
      }

      // Part A-1：生命周期——创建/重名/同域共存/删除/重复删除
      void testLifecycle(YomkRpcService *svc)
      {
            CHECK(svc->invoke("/create_node", mkNode(TEST_DOMAIN, "node_a")).m_status == YomkResponse::eOk,
                  "创建 node_a(domain 200) → eOk（真实创建 participant）");

            auto dup = svc->invoke("/create_node", mkNode(TEST_DOMAIN, "node_a"));
            CHECK(dup.m_status == YomkResponse::eNo && dup.m_msg.find("already exists") != std::string::npos,
                  "重名 node_a → eNo already exists（服务层前置校验，不新建 participant）");

            CHECK(svc->invoke("/create_node", mkNode(TEST_DOMAIN, "node_b")).m_status == YomkResponse::eOk,
                  "创建 node_b → eOk（与 node_a 同域共存，各自独立 participant）");

            CHECK(svc->invoke("/delete_node", YomkMkPtr(String, "node_a")).m_status == YomkResponse::eOk,
                  "删除 node_a → eOk（析构清理 participant）");

            auto del2 = svc->invoke("/delete_node", YomkMkPtr(String, "node_a"));
            CHECK(del2.m_status == YomkResponse::eNo && del2.m_msg.find("not exists") != std::string::npos,
                  "重复删除 node_a → eNo not exists");

            CHECK(svc->invoke("/delete_node", YomkMkPtr(String, "node_b")).m_status == YomkResponse::eOk,
                  "删除 node_b → eOk");
      }

      // Part A-2：domainId 边界——有效域 [0,232]（O1 前置校验）；越界 233/UINT32_MAX 直接 eNo
      void testDomainIdBoundary(YomkRpcService *svc)
      {
            CHECK(svc->invoke("/create_node", mkNode(0, "node_d0")).m_status == YomkResponse::eOk,
                  "domainId=0 → eOk（有效域下界）");
            CHECK(svc->invoke("/delete_node", YomkMkPtr(String, "node_d0")).m_status == YomkResponse::eOk,
                  "删除 node_d0 → eOk");

            CHECK(svc->invoke("/create_node", mkNode(232, "node_d232")).m_status == YomkResponse::eOk,
                  "domainId=232 → eOk（有效域上界，FastDDS 端口算术上限）");
            CHECK(svc->invoke("/delete_node", YomkMkPtr(String, "node_d232")).m_status == YomkResponse::eOk,
                  "删除 node_d232 → eOk");

            auto r233 = svc->invoke("/create_node", mkNode(233, "node_d233"));
            CHECK(r233.m_status == YomkResponse::eNo && r233.m_msg.find("out of valid range") != std::string::npos,
                  "domainId=233 → eNo out of valid range（O1 越界拒绝，不建 participant）");

            auto rmax = svc->invoke("/create_node", mkNode(UINT32_MAX, "node_dmax"));
            CHECK(rmax.m_status == YomkResponse::eNo && rmax.m_msg.find("out of valid range") != std::string::npos,
                  "domainId=UINT32_MAX → eNo out of valid range（O1 前置拦截，不再回绕深入 setDomainId）");
            std::cout << "[OBSERVE] domainId=UINT32_MAX status=" << rmax.m_status
                      << " msg=\"" << rmax.m_msg << "\"" << std::endl;
      }

      // Part A-3：nodeName 边界——空串（O2 拒绝）/单字符/超长（后两者创建后显式删除）
      void testNodeNameBoundary(YomkRpcService *svc)
      {
            auto rempty = svc->invoke("/create_node", mkNode(TEST_DOMAIN, ""));
            CHECK(rempty.m_status == YomkResponse::eNo && rempty.m_msg.find("node name is empty") != std::string::npos,
                  "nodeName 空串 → eNo node name is empty（O2 拒绝，不建节点/不建 participant）");

            CHECK(svc->invoke("/create_node", mkNode(TEST_DOMAIN, "x")).m_status == YomkResponse::eOk,
                  "nodeName 单字符 x → eOk");
            CHECK(svc->invoke("/delete_node", YomkMkPtr(String, "x")).m_status == YomkResponse::eOk,
                  "删除单字符节点 → eOk");

            const std::string longName(1024, 'N');
            CHECK(svc->invoke("/create_node", mkNode(TEST_DOMAIN, longName)).m_status == YomkResponse::eOk,
                  "nodeName 1024B 超长 → eOk（无长度校验）");
            CHECK(svc->invoke("/delete_node", YomkMkPtr(String, longName)).m_status == YomkResponse::eOk,
                  "删除超长名节点 → eOk");
      }

      // Part B：直接单测 FastDDSNode::setDomainId——重复设置守卫（服务层不可达）+ 非法域 participant==null 分支
      void testSetDomainIdGuard()
      {
            FastDDSNode node;
            CHECK(node.setDomainId(TEST_DOMAIN, "test-lifecycle-node"),
                  "Part B: 首次 setDomainId(200, \"test-lifecycle-node\") → true（创建 participant，test 前缀命名）");
            CHECK(!node.setDomainId(TEST_DOMAIN, "test-lifecycle-node"),
                  "Part B: 重复 setDomainId → false（participant_!=nullptr 守卫，服务层每次新建 FastDDSNode 故不可达）");
            // node 栈对象在此离开作用域析构，清理 participant（DomainParticipantFactory 仍存活）

            // O1 后服务层已短路非法域，setDomainId 的 participant==null 分支（create_participant 返回 null）改由此直接覆盖：
            // UINT32_MAX→(int32_t)-1 非法域，create_participant 返回 null，participant_ 保持 null、析构早退，不建 participant
            FastDDSNode badNode;
            CHECK(!badNode.setDomainId(UINT32_MAX, "test-lifecycle-bad"),
                  "Part B: setDomainId(UINT32_MAX) → false（非法域 create_participant 返回 null，覆盖 participant==null 分支）");
      }

      namespace dds = eprosima::fastdds::dds;
      namespace rtps = eprosima::fastdds::rtps;

      // 闭环 4 白盒用例 XML：与 TestYomkRpcServiceLifecycle 服务层用例同构的三 profile 合法文件
      // （按名 123 / 默认档 456 / 带 servers 789+192.168.1.1:7411）；NoDefault 版仅 xml_by_name
      // （回退态验证用），leaseDuration 为提取链最简可观察字段
      const char *kWhiteboxXml =
          "<?xml version=\"1.0\" encoding=\"UTF-8\" ?>\n"
          "<profiles xmlns=\"http://www.eprosima.com/XMLSchemas/fastRTPS_Profiles\">\n"
          "    <participant profile_name=\"xml_by_name\" is_default_profile=\"false\">\n"
          "        <rtps>\n"
          "            <builtin>\n"
          "                <discovery_config>\n"
          "                    <leaseDuration>\n"
          "                        <sec>123</sec>\n"
          "                    </leaseDuration>\n"
          "                </discovery_config>\n"
          "            </builtin>\n"
          "        </rtps>\n"
          "    </participant>\n"
          "    <participant profile_name=\"xml_default\" is_default_profile=\"true\">\n"
          "        <rtps>\n"
          "            <builtin>\n"
          "                <discovery_config>\n"
          "                    <leaseDuration>\n"
          "                        <sec>456</sec>\n"
          "                    </leaseDuration>\n"
          "                </discovery_config>\n"
          "            </builtin>\n"
          "        </rtps>\n"
          "    </participant>\n"
          "    <participant profile_name=\"xml_with_servers\" is_default_profile=\"false\">\n"
          "        <rtps>\n"
          "            <builtin>\n"
          "                <discovery_config>\n"
          "                    <leaseDuration>\n"
          "                        <sec>789</sec>\n"
          "                    </leaseDuration>\n"
          "                    <discoveryServersList>\n"
          "                        <locator>\n"
          "                            <udpv4>\n"
          "                                <address>192.168.1.1</address>\n"
          "                                <port>7411</port>\n"
          "                            </udpv4>\n"
          "                        </locator>\n"
          "                    </discoveryServersList>\n"
          "                </discovery_config>\n"
          "            </builtin>\n"
          "        </rtps>\n"
          "    </participant>\n"
          "</profiles>\n";

      const char *kWhiteboxXmlNoDefault =
          "<?xml version=\"1.0\" encoding=\"UTF-8\" ?>\n"
          "<profiles xmlns=\"http://www.eprosima.com/XMLSchemas/fastRTPS_Profiles\">\n"
          "    <participant profile_name=\"xml_by_name\" is_default_profile=\"false\">\n"
          "        <rtps>\n"
          "            <builtin>\n"
          "                <discovery_config>\n"
          "                    <leaseDuration>\n"
          "                        <sec>123</sec>\n"
          "                    </leaseDuration>\n"
          "                </discovery_config>\n"
          "            </builtin>\n"
          "        </rtps>\n"
          "    </participant>\n"
          "</profiles>\n";

      // 双默认档探针 XML：两个 is_default_profile="true"（leaseDuration 456/999）——
      // 验证默认档提取对同串多默认档的取值语义（实测：raw 串直提取首个默认档 456）
      const char *kWhiteboxXmlDualDefault =
          "<?xml version=\"1.0\" encoding=\"UTF-8\" ?>\n"
          "<profiles xmlns=\"http://www.eprosima.com/XMLSchemas/fastRTPS_Profiles\">\n"
          "    <participant profile_name=\"dual_default_first\" is_default_profile=\"true\">\n"
          "        <rtps>\n"
          "            <builtin>\n"
          "                <discovery_config>\n"
          "                    <leaseDuration>\n"
          "                        <sec>456</sec>\n"
          "                    </leaseDuration>\n"
          "                </discovery_config>\n"
          "            </builtin>\n"
          "        </rtps>\n"
          "    </participant>\n"
          "    <participant profile_name=\"dual_default_second\" is_default_profile=\"true\">\n"
          "        <rtps>\n"
          "            <builtin>\n"
          "                <discovery_config>\n"
          "                    <leaseDuration>\n"
          "                        <sec>999</sec>\n"
          "                    </leaseDuration>\n"
          "                </discovery_config>\n"
          "            </builtin>\n"
          "        </rtps>\n"
          "    </participant>\n"
          "</profiles>\n";

      // Part C：闭环 4 helper 白盒——三件套逐项（QoS 字段级读回）
      void testXmlHelperWhitebox()
      {
            // C-1 parseServerLocator 容错矩阵
            std::vector<DDSServerInfo> servers;
            std::string err;
            CHECK(yomkrpc_xml::parseServerLocator("127.0.0.1", servers, err) &&
                      servers.size() == 1 && servers[0].address == "127.0.0.1" && servers[0].port == 11811,
                  "Part C: 单项无 port → address 原样 + port 缺省 11811");
            CHECK(yomkrpc_xml::parseServerLocator("10.0.0.1:7400;192.168.1.1:65535", servers, err) &&
                      servers.size() == 2 && servers[0].port == 7400 && servers[1].port == 65535,
                  "Part C: 多项带 port → 逐项解析（分号分隔）");
            CHECK(!yomkrpc_xml::parseServerLocator("10.0.0.1;;10.0.0.2", servers, err) && !err.empty(),
                  "Part C: 空项 → false + err 非空");
            CHECK(!yomkrpc_xml::parseServerLocator(":7400", servers, err) && !err.empty(),
                  "Part C: 空 ip → false + err 非空");
            CHECK(!yomkrpc_xml::parseServerLocator("10.0.0.1:abc", servers, err) && !err.empty(),
                  "Part C: 非数字 port → false + err 非空");
            CHECK(!yomkrpc_xml::parseServerLocator("10.0.0.1:0", servers, err) && !err.empty(),
                  "Part C: port=0 下界外 → false + err 非空");
            CHECK(!yomkrpc_xml::parseServerLocator("10.0.0.1:65536", servers, err) && !err.empty(),
                  "Part C: port=65536 上界外 → false + err 非空");

            // C-2 applyDiscoveryServers 读回（合法 → CLIENT+locator 逐项；非法 → false 且不改动 qos）
            dds::DomainParticipantQos qos = dds::PARTICIPANT_QOS_DEFAULT;
            CHECK(yomkrpc_xml::applyDiscoveryServers(qos, "127.0.0.1:11811", "", err) && err.empty(),
                  "Part C: 合法 UDP spec → true");
            CHECK(qos.wire_protocol().builtin.discovery_config.discoveryProtocol ==
                      rtps::DiscoveryProtocol::CLIENT,
                  "Part C: 读回 discoveryProtocol == CLIENT（直配强制 client 角色）");
            CHECK(qos.wire_protocol().builtin.discovery_config.m_DiscoveryServers.size() == 1,
                  "Part C: 读回 m_DiscoveryServers size==1");
            const rtps::Locator &loc = *qos.wire_protocol().builtin.discovery_config.m_DiscoveryServers.begin();
            CHECK(rtps::IPLocator::toIPv4string(loc) == "127.0.0.1" && loc.port == 11811,
                  "Part C: 读回 locator 逐项 → ip 127.0.0.1 / port 11811");
            CHECK(!yomkrpc_xml::applyDiscoveryServers(qos, "10.0.0.1:xyz", "", err) && !err.empty(),
                  "Part C: 非法 spec → false + err 非空");
            CHECK(qos.wire_protocol().builtin.discovery_config.m_DiscoveryServers.size() == 1 &&
                      rtps::IPLocator::toIPv4string(
                          *qos.wire_protocol().builtin.discovery_config.m_DiscoveryServers.begin()) == "127.0.0.1",
                  "Part C: 非法 spec 不改动 qos（原 locator 保留）");

            // C-2b TCP 通道：TCPv4 locator 读回 + participant 侧自动补注入 TCPv4 传输
            //（builtin 默认集不含 TCP，不可达；注入与 builtin UDPv4/SHM alongside 共存）
            dds::DomainParticipantQos qosTcp = dds::PARTICIPANT_QOS_DEFAULT;
            CHECK(yomkrpc_xml::applyDiscoveryServers(qosTcp, "", "10.0.0.2:42100", err) && err.empty(),
                  "Part C: 合法 TCP spec → true");
            CHECK(qosTcp.wire_protocol().builtin.discovery_config.m_DiscoveryServers.size() == 1 &&
                      qosTcp.wire_protocol().builtin.discovery_config.m_DiscoveryServers.begin()->kind ==
                          LOCATOR_KIND_TCPv4 &&
                      rtps::IPLocator::toIPv4string(
                          *qosTcp.wire_protocol().builtin.discovery_config.m_DiscoveryServers.begin()) ==
                          "10.0.0.2" &&
                      qosTcp.wire_protocol().builtin.discovery_config.m_DiscoveryServers.begin()->port == 42100,
                  "Part C: TCP 通道读回 → TCPv4 locator 10.0.0.2:42100");
            CHECK(qosTcp.transport().user_transports.size() == 1 &&
                      std::dynamic_pointer_cast<rtps::TCPv4TransportDescriptor>(
                          qosTcp.transport().user_transports[0]) != nullptr,
                  "Part C: TCP 通道自动补注入 TCPv4TransportDescriptor");
            CHECK(yomkrpc_xml::applyDiscoveryServers(qosTcp, "10.0.0.3:11811", "10.0.0.4:42100", err) &&
                      qosTcp.transport().user_transports.size() == 1 &&
                      qosTcp.wire_protocol().builtin.discovery_config.m_DiscoveryServers.size() == 2,
                  "Part C: 二次叠加（UDP+TCP）→ 已有 TCPv4 描述符不重复注入，locator 双通道并存");

            // C-2c 多 server：双通道各多项 spec → locator 逐项按序入表，TCPv4 描述符仍只注入一次
            dds::DomainParticipantQos qosMulti = dds::PARTICIPANT_QOS_DEFAULT;
            CHECK(yomkrpc_xml::applyDiscoveryServers(qosMulti, "10.0.0.1:7400;192.168.1.1:7401",
                      "10.0.0.2:42100;10.0.0.5:42101", err) && err.empty(),
                  "Part C: 多 UDP+多 TCP spec → true");
            {
                const auto &multiServers =
                    qosMulti.wire_protocol().builtin.discovery_config.m_DiscoveryServers;
                CHECK(multiServers.size() == 4,
                      "Part C: 多 server 读回 m_DiscoveryServers size==4（UDP 先于 TCP 按序）");
                const char *expectIp[4] = {"10.0.0.1", "192.168.1.1", "10.0.0.2", "10.0.0.5"};
                uint32_t expectPort[4] = {7400, 7401, 42100, 42101};
                int32_t expectKind[4] = {LOCATOR_KIND_UDPv4, LOCATOR_KIND_UDPv4, LOCATOR_KIND_TCPv4,
                    LOCATOR_KIND_TCPv4};
                bool multiOk = multiServers.size() == 4;
                size_t idx = 0;
                for (const auto &locator : multiServers)
                {
                    multiOk = multiOk && idx < 4 &&
                              rtps::IPLocator::toIPv4string(locator) == expectIp[idx] &&
                              locator.port == expectPort[idx] && locator.kind == expectKind[idx];
                    ++idx;
                }
                multiOk = multiOk && idx == 4;
                CHECK(multiOk, "Part C: 多 server 逐项读回 ip/port/kind 按序匹配");
            }
            CHECK(qosMulti.transport().user_transports.size() == 1 &&
                      std::dynamic_pointer_cast<rtps::TCPv4TransportDescriptor>(
                          qosMulti.transport().user_transports[0]) != nullptr,
                  "Part C: 多 TCP server → TCPv4 描述符仍只注入一个");

            // C-3 participantQosFromXml 三态（leaseDuration 为最简可观察字段）
            dds::DomainParticipantQos byName = dds::PARTICIPANT_QOS_DEFAULT;
            yomkrpc_xml::participantQosFromXml(byName, "xml_by_name", kWhiteboxXml);
            CHECK(byName.wire_protocol().builtin.discovery_config.leaseDuration.seconds == 123,
                  "Part C: ①按名 profile 命中 → leaseDuration=123");

            dds::DomainParticipantQos byDefault = dds::PARTICIPANT_QOS_DEFAULT;
            yomkrpc_xml::participantQosFromXml(byDefault, "no_such_node", kWhiteboxXml);
            CHECK(byDefault.wire_protocol().builtin.discovery_config.leaseDuration.seconds == 456,
                  "Part C: ②按名落空 → is_default_profile 默认档兑底 leaseDuration=456");

            dds::DomainParticipantQos fallback = dds::PARTICIPANT_QOS_DEFAULT;
            const auto defLease = fallback.wire_protocol().builtin.discovery_config.leaseDuration.seconds;
            yomkrpc_xml::participantQosFromXml(fallback, "no_such_node", kWhiteboxXmlNoDefault);
            CHECK(fallback.wire_protocol().builtin.discovery_config.leaseDuration.seconds == defLease,
                  "Part C: ③两级全落空 → 保持 DEFAULT 底座（回退现状非错误）");

            dds::DomainParticipantQos emptyXml = dds::PARTICIPANT_QOS_DEFAULT;
            const auto defLease2 = emptyXml.wire_protocol().builtin.discovery_config.leaseDuration.seconds;
            yomkrpc_xml::participantQosFromXml(emptyXml, "xml_by_name", "");
            CHECK(emptyXml.wire_protocol().builtin.discovery_config.leaseDuration.seconds == defLease2,
                  "Part C: xml 空串 → 直接返回现状（未加载跳过提取）");

            // C-5 多默认档探针（计划闭环 4 验证项，实测固化）：同一 XML 串两个
            // is_default_profile="true" 时 get_default_participant_qos_from_xml（YomkRpc
            // 提取链实际走的 raw 串直提路径）取**首个**默认档（先档生效，与 factory 注册
            // 路径的后 set 覆盖语义不同——实测 [OBSERVE] lease=456 固化）
            dds::DomainParticipantQos dualDefault = dds::PARTICIPANT_QOS_DEFAULT;
            yomkrpc_xml::participantQosFromXml(dualDefault, "no_such_node", kWhiteboxXmlDualDefault);
            std::cout << "[OBSERVE] dual-default lease="
                      << dualDefault.wire_protocol().builtin.discovery_config.leaseDuration.seconds
                      << std::endl;
            CHECK(dualDefault.wire_protocol().builtin.discovery_config.leaseDuration.seconds == 456,
                  "Part C: 双默认档 → 先档生效（leaseDuration=456，raw 串直提取首个默认档，实测固化）");
      }

      // Part D：发现服务器直配端到端（真实 create_participant with CLIENT+servers；
      // server 不在场 CLIENT 模式仍创建成功——连接失败仅影响发现不影响创建）
      void testServerLocatorE2E()
      {
            std::string err;
            FastDDSNode node;
            CHECK(node.setDomainId(TEST_DOMAIN, "ds_direct_node",
                      yomkrpc_xml::ParticipantQosSpec{"", "127.0.0.1:11811", ""}, &err),
                  "Part D: 直配 setDomainId(200, CLIENT+UDP 127.0.0.1:11811, server 不在场) → true");
            CHECK(!node.setDomainId(TEST_DOMAIN, "ds_direct_node",
                      yomkrpc_xml::ParticipantQosSpec{"", "127.0.0.1:11811", ""}),
                  "Part D: 重复 setDomainId → false（既有 participant_!=nullptr 守卫）");

            FastDDSNode badNode;
            CHECK(!badNode.setDomainId(TEST_DOMAIN, "ds_bad_node",
                      yomkrpc_xml::ParticipantQosSpec{"", "127.0.0.1:99999", ""}, &err) &&
                      err.find("port out of range") != std::string::npos,
                  "Part D: 非法 UDP serverLocator(port 越界) → false + err 回显（不建 participant）");
            FastDDSNode badTcpNode;
            CHECK(!badTcpNode.setDomainId(TEST_DOMAIN, "ds_bad_tcp_node",
                      yomkrpc_xml::ParticipantQosSpec{"", "", "10.0.0.1:99999"}, &err) &&
                      err.find("port out of range") != std::string::npos,
                  "Part D: 非法 TCP serverLocator(port 越界) → false + err 回显（不建 participant）");
      }

      // Part E：双通道合并——XML discoveryServersList 提取打底 → 直配重建覆盖
      void testDualChannelMerge()
      {
            dds::DomainParticipantQos qos = dds::PARTICIPANT_QOS_DEFAULT;
            yomkrpc_xml::participantQosFromXml(qos, "xml_with_servers", kWhiteboxXml);
            const auto &servers = qos.wire_protocol().builtin.discovery_config.m_DiscoveryServers;
            CHECK(servers.size() == 1 && rtps::IPLocator::toIPv4string(*servers.begin()) == "192.168.1.1" &&
                      servers.begin()->port == 7411,
                  "Part E: XML discoveryServersList 提取打底 → 192.168.1.1:7411");

            std::string err;
            CHECK(yomkrpc_xml::applyDiscoveryServers(qos, "10.0.0.1:1234", "", err),
                  "Part E: 直配叠加 → true");
            CHECK(qos.wire_protocol().builtin.discovery_config.discoveryProtocol ==
                          rtps::DiscoveryProtocol::CLIENT &&
                      servers.size() == 1 && rtps::IPLocator::toIPv4string(*servers.begin()) == "10.0.0.1" &&
                      servers.begin()->port == 1234,
                  "Part E: 直配覆盖 XML → 列表重建为 10.0.0.1:1234 + CLIENT（合并语义：直配 > XML 打底）");
      }

      // Part F：发现服务器连通性（spawn fastdds discovery CLI 子进程 + A/B 双 client 直配互通收发）。
      // CLI 为 python 包装，内层 spawn server 二进制为孙进程——单杀 python 的 SIGTERM 杀不到
      // 孙进程，孤儿持 stdio 管道写端致 ctest/管道读者永挂（实测根因）；故子进程 setpgid
      // 独立进程组 + 收尾 kill(-pid) 组杀整组全灭。DS 模式 PDP/EDP 全链路不校验 domain_id
      // （PDPClientListener/PDPServerListener 源码注释 "In Discovery Server we don't impose
      // domain ids to be the same"；跨域丢弃仅存在于简单发现 PDPSimple）——A/B 故意异域
      // （201/202）直配同一 server，互通即域号与 server 无关的实证；m_DiscoveryServers
      // 非空禁用多播 PDP，双端互相发现必经 server（多播干扰隔离）
      void testDiscoveryServerConnectivity()
      {
            pid_t serverPid = fork();
            CHECK(serverPid >= 0, "Part F: fork discovery server 子进程");
            if (serverPid < 0)
            {
                  return;
            }
            if (serverPid == 0)
            {
                  // 子进程：setpgid 独立进程组（收尾组杀直达整组含 CLI 内层孙进程）后
                  // exec fastdds discovery CLI；setpgid/exec 失败 _exit 由存活探测捕获
                  if (setpgid(0, 0) != 0)
                  {
                        _exit(126);
                  }
                  execl(TEST_FASTDDS_DISCOVERY_BIN, "fastdds", "discovery", "-i", "0", "-l",
                        "127.0.0.1", "-p", "11811", static_cast<char *>(nullptr));
                  _exit(127);
            }

            // server 就绪探测：3s 窗内每 200ms 探存活——python CLI 启动需亚秒~1s，窗内退出
            //（exec 失败/端口被占秒退）即失败回显不静默；窗满存活即视为就绪
            for (int i = 0; i < 15 && kill(serverPid, 0) == 0; ++i)
            {
                  std::this_thread::sleep_for(std::chrono::milliseconds(200));
            }
            const bool serverAlive = kill(serverPid, 0) == 0;
            CHECK(serverAlive, "Part F: server 启动后存活（提前退出=exec 失败/端口被占，失败回显不静默）");
            if (!serverAlive)
            {
                  waitpid(serverPid, nullptr, 0);
                  return;
            }

            // A/B 双节点直配（白盒直调节点层；故意异域——域号不参与 DS 校验，见函数头注释）
            FastDDSNode nodeA, nodeB;
            const yomkrpc_xml::ParticipantQosSpec dsSpec{"", "127.0.0.1:11811", ""};
            CHECK(nodeA.setDomainId(201, "ds_conn_a", dsSpec), "Part F: 节点 A 直配 CLIENT（域 201）→ true");
            CHECK(nodeB.setDomainId(202, "ds_conn_b", dsSpec), "Part F: 节点 B 直配 CLIENT（域 202）→ true");

            std::atomic<int> recvCount{0};
            CHECK(nodeB.registerSubTopic("ds_conn_topic", new YomkRpc::MStringPubSubType(),
                      [&recvCount](const void *) { recvCount.fetch_add(1, std::memory_order_relaxed); }),
                  "Part F: B 登记 ds_conn_topic 订阅（收到即计数）");
            CHECK(nodeA.registerPubTopic("ds_conn_topic", new YomkRpc::MStringPubSubType()),
                  "Part F: A 登记 ds_conn_topic 发布");

            // 发布线程持续发（200ms 周期，覆盖 server 发现收敛窗）
            std::atomic<bool> stopPub{false};
            std::thread pubThread([&] {
                  YomkRpc::MString msg;
                  int seq = 0;
                  while (!stopPub.load(std::memory_order_relaxed))
                  {
                        msg.data("ds-conn-" + std::to_string(seq++));
                        (void)nodeA.publish("ds_conn_topic", &msg);
                        std::this_thread::sleep_for(std::chrono::milliseconds(200));
                  }
            });

            // 收流超时窗 6s（≥5s server 发现收敛）：B 收到 ≥1 条即实证经 server 发现互通
            bool received = false;
            for (int i = 0; i < 60 && !received; ++i)
            {
                  std::this_thread::sleep_for(std::chrono::milliseconds(100));
                  received = recvCount.load(std::memory_order_relaxed) > 0;
            }
            CHECK(received,
                  "Part F: B 在 6s 超时窗内收到 ≥1 条 A 消息（A/B 异域经 server 中继发现互通——域号与 server 无关的实证）");
            if (!received)
            {
                  std::cout << "[OBSERVE] 连通性超时：recvCount=" << recvCount.load() << std::endl;
            }

            // 收尾：停发布线程 → 节点栈对象出函数即析构 → 组杀 server 整组（CLI python +
            // 内层 server 二进制全灭，无孤儿残留）+ wait 防僵尸（stdio 管道无遗留写端，
            // ctest/管道读者不再永挂）
            stopPub.store(true);
            pubThread.join();
            kill(-serverPid, SIGTERM);
            waitpid(serverPid, nullptr, 0);
      }
} // namespace

int main()
{
      YOMK_INIT();

      auto *svc = new YomkRpcService(YOMK_SERVER_P);
      CHECK(YOMK_ADD_SERVICE(svc) == 0, "YomkRpcService 注册成功（所有权移交框架，init() 已内部调用）");

      testLifecycle(svc);
      testDomainIdBoundary(svc);
      testNodeNameBoundary(svc);
      testSetDomainIdGuard();
      testXmlHelperWhitebox();
      testServerLocatorE2E();
      testDualChannelMerge();
      testDiscoveryServerConnectivity();

      return testReport("TestYomkRpcNodeLifecycle");
}
