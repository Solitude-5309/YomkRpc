/**
 * @file TestYomkRpcServiceLifecycle.cpp
 * @brief 闭环 4：XML 配置通道 /load_xml 端点 + serverLocator 直配（服务层生命周期）
 *
 * 范围：/YomkRpcService/load_xml 端点五态（合法 / 路径不存在 / 空文件 / 格式非法 rc 回显 /
 *       重复 load 整体替换）+ xmlCache() 静态缓存白盒读回 + 时序语义（load 前 create_node
 *       现状零改动、load 后 create_node 合成链不阻断）+ create_node 非法 serverLocator
 *       容错回显（服务层 eNo 携带具体非法项）。
 * 职责边界：XML 提取链的 QoS 字段级读回（按名 profile / 默认档兜底 / 回退三态 +
 *       discovery_config 断言）、serverLocator 直配端到端、双通道合并、发现服务器连通性
 *       均落在 TestYomkRpcNodeLifecycle（节点层白盒；服务层无 QoS 读回接口）。
 *
 * XML fixture：/tmp 临时文件运行期写入（测试自包含，不依赖源树资源拷贝）；内容为最小
 *   participant profile（新风格 profiles 根元素），格式合法性以 load_XML_profiles_file
 *   实际校验兜底（合法用例 eOk 即实证）。
 *
 * 风格：纯 main() + CHECK 宏 + 失败计数（零第三方依赖），返回非 0 表示存在失败用例。
 */

#include "TestCheck.h"
#include "YomkRpcService.h" // 服务/DDSNode/DDSXmlLoad/String/YOMK_* 宏 + xmlCache() 静态访问器

#include <cstdint>
#include <fstream>
#include <iostream>
#include <string>

namespace
{
    constexpr uint32_t TEST_DOMAIN = 200;

    // 测试 XML profiles（最小 participant profile，新风格根元素）：leaseDuration 为
    // load_XML_profiles_file 可校验、提取链可观察的最简字段
    const char *kTestXml =
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
        "</profiles>\n";

    // 第二份合法 XML（重复 load 用例）：内容与前份不同，验证 xmlCache_ 整体替换语义
    const char *kTestXmlReloaded =
        "<?xml version=\"1.0\" encoding=\"UTF-8\" ?>\n"
        "<profiles xmlns=\"http://www.eprosima.com/XMLSchemas/fastRTPS_Profiles\">\n"
        "    <participant profile_name=\"xml_reloaded\" is_default_profile=\"false\">\n"
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

    // /tmp 写 fixture（运行期生成；返回 false 即用例内 CHECK 失败回显）
    bool writeTmpFile(const std::string &path, const std::string &content)
    {
        std::ofstream f(path, std::ios::binary | std::ios::trunc);
        if (!f)
        {
            return false;
        }
        f << content;
        return bool(f);
    }

    YomkPkgPtr mkLoad(const std::string &xmlPath)
    {
        return YomkMkPtr(DDSXmlLoad, DDSXmlLoad{xmlPath});
    }

    YomkPkgPtr mkNode(uint32_t domainId, const std::string &nodeName, const std::string &serverLocator = "")
    {
        return YomkMkPtr(DDSNode, DDSNode{nodeName, domainId, serverLocator});
    }
} // namespace

int main()
{
    YOMK_INIT();

    auto *svc = new YomkRpcService(YOMK_SERVER_P);
    CHECK(YOMK_ADD_SERVICE(svc) == 0, "YomkRpcService 注册成功（所有权移交框架，init() 已内部调用）");

    // ---- A：load 前时序（现状零改动）----
    CHECK(YomkRpcService::xmlCache().empty(), "进程内未 load → xmlCache() 为空串（未加载现状）");
    CHECK(svc->invoke("/create_node", mkNode(TEST_DOMAIN, "ts_before_node")).m_status == YomkResponse::eOk,
          "load 前 create_node → eOk（XML 未加载=提取链跳过，现状行为零改动）");

    // ---- B/C/D：load_xml 错误三态（均不落缓存）----
    auto notExist = svc->invoke("/load_xml", mkLoad("/tmp/yomkrpc_test_no_such_profiles.xml"));
    CHECK(notExist.m_status == YomkResponse::eNo && notExist.m_msg.find("not readable") != std::string::npos,
          "load_xml 路径不存在 → eNo not readable（文件读入前回显）");
    CHECK(YomkRpcService::xmlCache().empty(), "失败 load 不落缓存 → xmlCache() 仍空");

    const std::string emptyPath = "/tmp/yomkrpc_test_empty_profiles.xml";
    CHECK(writeTmpFile(emptyPath, ""), "写空 XML fixture → true");
    auto isEmpty = svc->invoke("/load_xml", mkLoad(emptyPath));
    CHECK(isEmpty.m_status == YomkResponse::eNo && isEmpty.m_msg.find("is empty") != std::string::npos,
          "load_xml 空文件 → eNo is empty（空 raw 串无提取价值，前置拦截）");

    const std::string badPath = "/tmp/yomkrpc_test_bad_profiles.xml";
    CHECK(writeTmpFile(badPath, "this is not xml at all"), "写非法 XML fixture → true");
    auto bad = svc->invoke("/load_xml", mkLoad(badPath));
    CHECK(bad.m_status == YomkResponse::eNo && bad.m_msg.find("failed, rc=") != std::string::npos,
          "load_xml 格式非法 → eNo failed, rc=<N>（load_XML_profiles_file 显式检查返回码，rc 整值回显）");
    std::cout << "[OBSERVE] 非法 XML msg=\"" << bad.m_msg << "\"" << std::endl;
    CHECK(YomkRpcService::xmlCache().empty(), "非法 XML 不落缓存 → xmlCache() 仍空");

    // ---- E：load_xml 合法（缓存落定）----
    const std::string validPath = "/tmp/yomkrpc_test_valid_profiles.xml";
    CHECK(writeTmpFile(validPath, kTestXml), "写合法 XML fixture → true");
    auto ok = svc->invoke("/load_xml", mkLoad(validPath));
    CHECK(ok.m_status == YomkResponse::eOk, "load_xml 合法文件 → eOk（load_XML_profiles_file 实际校验通过）");
    CHECK(!YomkRpcService::xmlCache().empty() &&
              YomkRpcService::xmlCache().find("xml_by_name") != std::string::npos &&
              YomkRpcService::xmlCache().find("xml_default") != std::string::npos,
          "成功 load → xmlCache() 为文件 raw 串（含两 profile 名，节点层三级提取链取用）");

    // ---- F：load 后时序（合成链不阻断）----
    CHECK(svc->invoke("/create_node", mkNode(TEST_DOMAIN, "ts_after_node")).m_status == YomkResponse::eOk,
          "load 后 create_node → eOk（participant 按三级提取链合成，QoS 字段级读回见 NodeLifecycle）");
    CHECK(svc->invoke("/delete_node", YomkMkPtr(String, "ts_before_node")).m_status == YomkResponse::eOk,
          "删除 ts_before_node → eOk");
    CHECK(svc->invoke("/delete_node", YomkMkPtr(String, "ts_after_node")).m_status == YomkResponse::eOk,
          "删除 ts_after_node → eOk（关键不变式：退出前清理）");

    // ---- G：重复 load 整体替换 ----
    const std::string reloadedPath = "/tmp/yomkrpc_test_reloaded_profiles.xml";
    CHECK(writeTmpFile(reloadedPath, kTestXmlReloaded), "写第二份合法 XML fixture → true");
    CHECK(svc->invoke("/load_xml", mkLoad(reloadedPath)).m_status == YomkResponse::eOk,
          "重复 load 第二份合法文件 → eOk（load_XML_profiles_file 注册表追加语义）");
    CHECK(!YomkRpcService::xmlCache().empty() &&
              YomkRpcService::xmlCache().find("xml_reloaded") != std::string::npos &&
              YomkRpcService::xmlCache().find("xml_by_name") == std::string::npos,
          "重复 load → xmlCache() 为最新成功文件整体替换（非追加拼接）");

    // ---- H：create_node 非法 serverLocator 容错回显 ----
    auto badPort = svc->invoke("/create_node", mkNode(TEST_DOMAIN, "srv_bad_node", "127.0.0.1:99999"));
    CHECK(badPort.m_status == YomkResponse::eNo && badPort.m_msg.find("port out of range") != std::string::npos,
          "serverLocator port=99999 越界 → eNo 回显 port out of range（setDomainId err 带出）");
    auto emptyItem = svc->invoke("/create_node", mkNode(TEST_DOMAIN, "srv_bad_node2", "10.0.0.1;;10.0.0.2"));
    CHECK(emptyItem.m_status == YomkResponse::eNo && emptyItem.m_msg.find("empty server item") != std::string::npos,
          "serverLocator 空项 → eNo 回显 empty server item");
    std::cout << "[OBSERVE] 越界 port msg=\"" << badPort.m_msg << "\"" << std::endl;

    return testReport("TestYomkRpcServiceLifecycle");
}
