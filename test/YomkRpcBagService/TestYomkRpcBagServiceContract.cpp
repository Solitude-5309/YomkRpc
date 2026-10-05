/**
 * @file TestYomkRpcBagServiceContract.cpp
 * @brief YomkRpcBagService 服务层契约测试（DDS-free，白盒经 invoke 直接分发）
 *
 * 范围：仅验证 bag 服务 5 端点在不触发任何 DDS 运行时（不创建 participant）前提下的
 *       输入校验与错误码契约；真实 DDS 生命周期（创建/录制/删除）归
 *       TestYomkRpcBagServiceLifecycle，节点层校验与落盘归 TestFastDDSBagNode。
 * 覆盖：
 *   T1 funcInfos 内省 5 端点齐全 + 未知端点 eNo；
 *   T2 /version 正常路径（eOk + 版本串契约 + 忽略 pkg）；
 *   T3 /create_node、/bag_record 解包双守卫（nullptr / 异类包 / 改名伪造）；
 *   T4 /delete_node 特殊契约：单节点模型无参载荷，handler (void)pkg 不走解包守卫，未建节点时
 *      无论何种载荷均 eNo "bag node not created"（与 /create_node 的解包守卫形态不同）；
 *   T5 /create_node domainId 越界前置拦截（233 / UINT32_MAX → eNo，不触 DDS）；
 *   T6 /bag_record topics 校验锁外先于节点检查：空清单 → eNo "no topics given"、
 *      含空名 → eNo "empty topic name in topic list"（均不触 DDS，与未建节点无关）；
 *      合法清单 + 未建节点 → eNo "bag node not created"（node_ 空检查先于触达节点层）；
 *   T7 /bag_info 纯文件读契约：解包双守卫；bagDir 空校验锁外前置（未建节点也报输入
 *      错误）；路径不存在报文件层错误（不依赖节点生命周期）；真实临时 bag 目录 →
 *      eOk + StringArray 首项空串（ros2 对齐空行）打包/解包保真与逐行内容透传。
 * DDS-free 保证：T5 的越界校验在锁外返回；T6 的 topics 校验在锁外、node_ 空检查在触达节点层前返回；
 *   T7 的 /bag_info 为纯文件读（静态函数直调，不触 node_ 与 DDS）；/version 不碰 DDS；本测试绝不传合法 DDSBagNode{0..232}（那会创建真实 participant）。
 *
 * 风格：纯 main() + CHECK 宏 + 失败计数（零第三方依赖），返回非 0 表示存在失败用例。
 */

#include "TestCheck.h"
#include "YomkRpcBagService.h" // 服务/DDSBagNode/DDSBagRecord/String/YOMK_* 宏

#include <cstdint>
#include <filesystem>
#include <fstream>
#include <string>

namespace
{
    // T2：/version 契约（不解包，忽略 pkg）
    void testVersion(YomkRpcBagService *svc)
    {
        auto resp = svc->invoke("/version");
        CHECK(resp.m_status == YomkResponse::eOk, "/version 返回 eOk");
        CHECK(resp.m_msg == "ok", "/version m_msg == ok");

        YomkUnPackPkg(resp.m_data, String, ver);
        CHECK(ver != nullptr, "/version m_data 可解包为 String");
        if (ver != nullptr)
        {
            CHECK(ver->d.rfind("YomkRpc v", 0) == 0, "/version 版本串前缀为 YomkRpc v");
        }

        auto respNull = svc->invoke("/version", nullptr);
        CHECK(respNull.m_status == YomkResponse::eOk, "/version 传 nullptr 仍 eOk（忽略 pkg）");
    }

    // T3：/create_node、/bag_record 的解包双守卫（nullptr / 异类包 / 改名伪造）
    void testUnpackGuards(YomkRpcBagService *svc)
    {
        auto checkGuards = [&](const char *ep, const char *expectName, YomkPkgPtr wrongPkg)
        {
            // (a) nullptr → 第一守卫 !pkg
            auto ra = svc->invoke(ep, nullptr);
            CHECK(ra.m_status == YomkResponse::eNo &&
                      ra.m_msg.find("pkg is null or pkg is not") != std::string::npos,
                  std::string(ep) + " : nullptr 包命中第一守卫 eNo");

            // (b) 异类包（保留自身名）→ 第一守卫 name()!=expectName
            auto rb = svc->invoke(ep, wrongPkg);
            CHECK(rb.m_status == YomkResponse::eNo &&
                      rb.m_msg.find("pkg is null or pkg is not") != std::string::npos,
                  std::string(ep) + " : 异类包命中第一守卫 eNo");

            // (c) 改名伪造：异类包改名为期望名 → 第一守卫通过、dynamic_pointer_cast 失败命中第二守卫
            wrongPkg->name(expectName);
            auto rc = svc->invoke(ep, wrongPkg);
            CHECK(rc.m_status == YomkResponse::eNo &&
                      rc.m_msg.find("dynamic_pointer_cast failed") != std::string::npos,
                  std::string(ep) + " : 改名伪造命中第二守卫(dynamic_pointer_cast 失败) eNo");
        };

        checkGuards("/create_node", "DDSBagNode", YomkMkPtr(String, "wrong"));
        checkGuards("/bag_record", "DDSBagRecord", YomkMkPtr(String, "wrong"));
        checkGuards("/bag_info", "DDSBagInfo", YomkMkPtr(String, "wrong"));
    }

    // T4：/delete_node 特殊契约——单节点模型无参载荷，(void)pkg 不走解包守卫
    void testNoPayloadEndpoints(YomkRpcBagService *svc)
    {
        // 未建节点：无论载荷形态，均在 node_ 空检查处早退（不触 DDS）
        auto rNull = svc->invoke("/delete_node", nullptr);
        CHECK(rNull.m_status == YomkResponse::eNo &&
                  rNull.m_msg.find("bag node not created") != std::string::npos,
              "/delete_node : nullptr 载荷 → eNo bag node not created（不走解包守卫）");

        auto rWrong = svc->invoke("/delete_node", YomkMkPtr(String, "wrong"));
        CHECK(rWrong.m_status == YomkResponse::eNo &&
                  rWrong.m_msg.find("bag node not created") != std::string::npos,
              "/delete_node : 异类载荷 → eNo bag node not created（载荷被忽略）");

        auto rNoArg = svc->invoke("/delete_node");
        CHECK(rNoArg.m_status == YomkResponse::eNo &&
                  rNoArg.m_msg.find("bag node not created") != std::string::npos,
              "/delete_node : 无载荷 → eNo bag node not created（对齐无参宏形态）");
    }

    // T5：/create_node domainId 越界前置拦截（锁外校验，不触 DDS）
    void testCreateDomainBoundary(YomkRpcBagService *svc)
    {
        auto r233 = svc->invoke("/create_node", YomkMkPtr(DDSBagNode, DDSBagNode{233}));
        CHECK(r233.m_status == YomkResponse::eNo &&
                  r233.m_msg.find("out of valid range") != std::string::npos,
              "domainId=233 → eNo out of valid range（前置拦截，不建 participant）");

        auto rmax = svc->invoke("/create_node", YomkMkPtr(DDSBagNode, DDSBagNode{UINT32_MAX}));
        CHECK(rmax.m_status == YomkResponse::eNo &&
                  rmax.m_msg.find("out of valid range") != std::string::npos,
              "domainId=UINT32_MAX → eNo out of valid range（前置拦截，不再回绕深入 setDomainId）");
    }

    // T6：/bag_record topics 校验锁外先于节点检查（空清单/空名不触 DDS，合法清单未建节点早退）
    void testBagRecordInputOrder(YomkRpcBagService *svc)
    {
        // 空清单：topics 校验先于 node_ 检查，报输入问题而非节点未建
        auto rEmpty = svc->invoke("/bag_record", YomkMkPtr(DDSBagRecord, DDSBagRecord{{}}));
        CHECK(rEmpty.m_status == YomkResponse::eNo &&
                  rEmpty.m_msg.find("no topics given") != std::string::npos,
              "/bag_record 空清单+未建节点 → eNo no topics given（topics 校验先于节点检查）");

        // 含空名：同上，报空名而非节点未建
        auto rBlank = svc->invoke("/bag_record", YomkMkPtr(DDSBagRecord, DDSBagRecord{{"t_ok", ""}}));
        CHECK(rBlank.m_status == YomkResponse::eNo &&
                  rBlank.m_msg.find("empty topic name in topic list") != std::string::npos,
              "/bag_record 含空名+未建节点 → eNo empty topic name in topic list");

        // 合法清单 + 未建节点：node_ 空检查先于触达节点层
        auto rValid = svc->invoke("/bag_record", YomkMkPtr(DDSBagRecord, DDSBagRecord{{"t_no_node"}}));
        CHECK(rValid.m_status == YomkResponse::eNo &&
                  rValid.m_msg.find("bag node not created") != std::string::npos,
              "/bag_record 合法包+未建节点 → eNo bag node not created（node_ 检查先于触达节点层）");
    }

    // T7：/bag_info 纯文件读契约（不依赖节点生命周期；StringArray 空串首项保真）
    void testBagInfoContract(YomkRpcBagService *svc)
    {
        // bagDir 空校验锁外前置：未建节点也报输入错误（对齐 bagRecord topics 校验先例）
        auto rEmpty = svc->invoke("/bag_info", YomkMkPtr(DDSBagInfo, DDSBagInfo{""}));
        CHECK(rEmpty.m_status == YomkResponse::eNo &&
                  rEmpty.m_msg.find("no bag dir given") != std::string::npos,
              "/bag_info 空目录+未建节点 → eNo no bag dir given（输入校验先于一切）");

        // 路径不存在：报文件层错误而非节点错误（纯文件读，未建节点亦可调）
        auto rMiss = svc->invoke("/bag_info", YomkMkPtr(DDSBagInfo, DDSBagInfo{"info_svc_miss"}));
        CHECK(rMiss.m_status == YomkResponse::eNo &&
                  rMiss.m_msg.find("bag path [info_svc_miss] does not exist") != std::string::npos,
              "/bag_info 路径不存在 → eNo 文件层错误（不依赖节点生命周期）");

        // 真实临时 bag 目录 → eOk + StringArray 逐行透传（含首项空串打包/解包保真）
        namespace fs = std::filesystem;
        const std::string dir = "info_svc_t7";
        std::error_code ec;
        fs::remove_all(dir, ec);
        CHECK(fs::create_directory(dir, ec), "T7 前置：临时 bag 目录创建成功");
        {
            std::ofstream meta(dir + "/metadata.json", std::ios::binary);
            CHECK(static_cast<bool>(meta), "T7 前置：metadata.json 创建成功");
            meta << R"({
    "version": 1,
    "storage_identifier": "mcap",
    "relative_file_paths": ["bag_0.mcap"],
    "starting_time": {"nanoseconds_since_epoch": 1570799349123456789},
    "duration": {"nanoseconds": 12000000000},
    "message_count": 13,
    "topics_with_message_count": [
        {"topic_metadata": {"name": "/rt/a", "type": "YomkRpc::MString"}, "message_count": 5}
    ]
}
)";
        }
        auto rOk = svc->invoke("/bag_info", YomkMkPtr(DDSBagInfo, DDSBagInfo{dir}));
        CHECK(rOk.m_status == YomkResponse::eOk && rOk.m_msg == "ok",
              "/bag_info 真实 bag 目录 → eOk ok");
        YomkUnPackPkg(rOk.m_data, StringArray, lines);
        CHECK(lines != nullptr && lines->d.size() == 9,
              "回执可解包为 9 行 StringArray（节点层单主题契约行数）");
        if (lines != nullptr && lines->d.size() == 9)
        {
            CHECK(lines->d[0].empty(), "StringArray 首项空串保真（ros2 对齐空行往返不丢）");
            CHECK(lines->d[1].rfind("Files:", 0) == 0 &&
                      lines->d[1].find("bag_0.mcap") != std::string::npos,
                  "Files 行透传节点层内容（标签 + 文件名）");
            CHECK(lines->d[3] == "Storage id:        mcap", "Storage id 行透传（19 列标签）");
            CHECK(lines->d[8].rfind("Topic information:", 0) == 0 &&
                      lines->d[8].find("/rt/a") != std::string::npos &&
                      lines->d[8].find("Serialization Format: cdr") != std::string::npos,
                  "Topic information 行透传（标签 + 主题 + 单格式标识）");
        }
        fs::remove_all(dir, ec);
    }
} // namespace

int main()
{
    YOMK_INIT();

    // 注册被测服务：所有权移交框架（shared_ptr 持有），init() 内部调用后 weak_from_this() 方有效
    auto *svc = new YomkRpcBagService(YOMK_SERVER_P);
    CHECK(YOMK_ADD_SERVICE(svc) == 0, "YomkRpcBagService 注册成功（所有权移交框架，init() 已内部调用）");

    // T1：内省——5 端点齐全
    auto infos = svc->funcInfos();
    CHECK(infos.size() == 5 && infos.count("/version") && infos.count("/create_node") &&
              infos.count("/bag_record") && infos.count("/delete_node") &&
              infos.count("/bag_info"),
          "funcInfos 内省 5 端点齐全");

    testVersion(svc);
    testUnpackGuards(svc);
    testNoPayloadEndpoints(svc);
    testCreateDomainBoundary(svc);
    testBagRecordInputOrder(svc);
    testBagInfoContract(svc);

    CHECK(svc->invoke("/no_such_endpoint").m_status == YomkResponse::eNo, "未知端点返回 eNo");

    return testReport("TestYomkRpcBagServiceContract");
}
