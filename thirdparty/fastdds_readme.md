# FastDDS 源码编译

## linux下 Fast-DDS-3.6.1 C++使用方法

### 编译示例

1. cd test/hello_world
2. mkdir build
3. cd build
4. cmake .. -DCMAKE_PREFIX_PATH=~/YomkServer/install [修改cmake版本要求为3.22]
5. cmake --build . --config Release

### 运行示例

#### 运行发布者

1. export LD_LIBRARY_PATH=~/YomkServer/install/lib:$LD_LIBRARY_PATH
2. ./hello_world publisher

#### 运行订阅者

1. export LD_LIBRARY_PATH=~/YomkServer/install/lib:$LD_LIBRARY_PATH
2. ./hello_world subscriber

### 使用Fast-DDS-Gen

1. cd Fast-DDS-Gen-4.3.0/scripts
2. 创建一个最简单的idl文件rpc.idl，内容如下

struct RPCString
{
    string s;
};

3. 创建一个gen目录用于存放生成文件
4. 生成C++代码：./fastddsgen -d gen rpc.idl 或 生成python代码：./fastddsgen -python -d gen rpc.idl
5. 生成支持loan的msg：./fastddsgen -python -d gen -de final rpc.idl 

### 编译python idl

1. 安装swig：sudo apt install swig
2. sudo apt install libpython3-dev
3. cd gen
4. mkdir build
5. cd build
6. cmake .. -DCMAKE_PREFIX_PATH=~/YomkServer/install
7. cmake --build . --config Release


