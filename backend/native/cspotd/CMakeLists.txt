cmake_minimum_required(VERSION 3.16)
project(cspotd C CXX)

# cspot 源码位置 (含 bell 子模块): 默认取本仓库旁的 SpotConnect 检出,
# 也可用 -DCSPOT_SOURCE_DIR=<path> 覆盖
set(CSPOT_SOURCE_DIR ""
    CACHE PATH "Path to cspot source (SpotConnect/common/cspot/cspot)")
if(NOT CSPOT_SOURCE_DIR)
    set(CSPOT_SOURCE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/../../../SpotConnect/common/cspot/cspot")
endif()

if(NOT EXISTS "${CSPOT_SOURCE_DIR}/CMakeLists.txt")
    message(FATAL_ERROR
        "cspot 源码未找到: ${CSPOT_SOURCE_DIR}\n"
        "请检出 SpotConnect (含 cspot/bell 子模块) 或指定 -DCSPOT_SOURCE_DIR=<path>")
endif()

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# bell 裁剪: 只留 cspot 需要的部分 (tremor 解码 / civetweb HTTP / 内置 mDNS)
set(BELL_DISABLE_CODECS ON CACHE BOOL "" FORCE)   # tremor 仍会编译 (cspot 直接依赖)
set(BELL_DISABLE_MQTT ON CACHE BOOL "" FORCE)
set(BELL_DISABLE_SINKS ON CACHE BOOL "" FORCE)
set(BELL_DISABLE_AVAHI ON CACHE BOOL "" FORCE)    # 用 bell 内置 mDNS (免 avahi-daemon)

add_subdirectory(${CSPOT_SOURCE_DIR} cspot EXCLUDE_FROM_ALL)

add_executable(cspotd cspotd.cpp)
target_link_libraries(cspotd PRIVATE cspot)
target_compile_options(cspotd PRIVATE -O2)
