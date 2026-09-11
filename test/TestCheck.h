#pragma once
// 零第三方依赖的极简测试断言头（与 YomkServer/Test 套件风格一致）：
// 纯 main() + CHECK 宏 + 失败计数，进程返回非 0 表示存在失败用例。
// 每个测试可执行程序为单一 TU，inline 变量保证跨 TU 也无重复定义。
#include <iostream>
#include <string>

inline int g_checkTotal = 0;
inline int g_checkFailed = 0;

#define CHECK(cond, msg)                                                          \
    do                                                                            \
    {                                                                             \
        ++g_checkTotal;                                                           \
        if (!(cond))                                                              \
        {                                                                         \
            std::cout << "[FAIL] [line " << __LINE__ << "] " << msg << std::endl; \
            ++g_checkFailed;                                                      \
        }                                                                         \
        else                                                                      \
        {                                                                         \
            std::cout << "[ OK ] [line " << __LINE__ << "] " << msg << std::endl; \
        }                                                                         \
    } while (0)

// 汇总并返回进程退出码：有失败返回 1，全通过返回 0
inline int testReport(const std::string& suiteName)
{
    std::cout << "-------------------------------------------" << std::endl;
    if (g_checkFailed > 0)
    {
        std::cout << suiteName << " FAILED: " << g_checkFailed << "/" << g_checkTotal << " checks failed." << std::endl;
        return 1;
    }
    std::cout << suiteName << " all passed (" << g_checkTotal << " checks)." << std::endl;
    return 0;
}
