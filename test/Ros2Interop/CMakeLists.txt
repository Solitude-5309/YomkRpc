# =============================================================================
# 真机互通回归判定程序（run_ros2_interop.sh 驱动，ctest 树不含）
#
# 定位：examples/ExampleRos2InteropPub|Sub 为面向用户的教学示例（常驻运行形态）；判定职责归
# 本目录——本目录二进制与 run_ros2_interop.sh 构成真机回归设施（简单形态：Pub 1Hz 发 30 条
# 自退、Sub 订 _rev 收满 3 条退未收满返 1，两端不互通仅分别与 ROS2 CLI 对跑，判定口径见脚本头注释）。
#
# 为何不 add_test：需 /opt/ros/humble 环境 + ros2 CLI 对跑，CI/ctest 树无 ROS2 依赖
# （用户裁定：ctest 树不含真机互通），仅手动执行 test/run_ros2_interop.sh 验证。
#
# 用法：cmake --build test/build --target TestRos2InteropPub TestRos2InteropSub 后
#   test/run_ros2_interop.sh test/build/Ros2Interop/TestRos2InteropPub [TestRos2InteropSub]
# =============================================================================
add_executable(TestRos2InteropPub TestRos2InteropPub.cpp)
target_link_libraries(TestRos2InteropPub PRIVATE yomkrpc_under_test YomkRpcMsg)

add_executable(TestRos2InteropSub TestRos2InteropSub.cpp)
target_link_libraries(TestRos2InteropSub PRIVATE yomkrpc_under_test YomkRpcMsg)
