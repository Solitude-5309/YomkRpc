/**
 * @file TestHarnessSmoke.cpp
 * @brief MC0 测试基座自检（零 DDS、零框架 init）
 *
 * 目的：以最小代价证明测试基座端到端可用，不含任何业务断言（业务契约归 MC1+）：
 *  1. TestCheck.h 的 CHECK 宏计入通过、testReport 汇总并以退出码反映结果；
 *  2. 被测目标 yomkrpc_under_test 已从源码编译且可链接——引用 FastDDSNode 符号；
 *  3. FastDDSNode 默认构造后 participant_==nullptr，析构走早退分支，
 *     全程不触发任何 DDS 运行时（无需 YOMK_INIT、无 participant 创建），零泄漏。
 *
 * 风格：纯 main() + CHECK 宏 + 失败计数（零第三方依赖），返回非 0 表示存在失败用例。
 */

#include "TestCheck.h"
#include "FastDDSNode.h" // 证明链接到 yomkrpc_under_test（src/FastDDSNode.cpp）

int main()
{
    // 1. CHECK 框架自检：真条件计入通过，testReport 可汇总
    CHECK(1 + 1 == 2, "CHECK 框架：真条件计入通过、testReport 可汇总");

    // 2. 链接被测目标：FastDDSNode 默认构造 + 析构（participant_==nullptr → 析构早退，零 DDS）
    {
        FastDDSNode node;
        (void)node;
    }
    CHECK(true, "yomkrpc_under_test 链接成功，FastDDSNode 零 DDS 构造/析构安全");

    return testReport("TestHarnessSmoke");
}
