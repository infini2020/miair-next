#!/usr/bin/env bash
# 编译 cspotd (Spotify Connect 原生守护进程)
# 依赖: cmake, g++(C++20), libmbedtls-dev, git (拉取 cspot/bell 源码)
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BUILD_DIR="${SCRIPT_DIR}/build"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../../.." && pwd)"

# cspot 源码: 优先环境变量, 其次仓库旁的 SpotConnect 检出
CSPOT_SOURCE_DIR="${CSPOT_SOURCE_DIR:-${REPO_ROOT}/SpotConnect/common/cspot/cspot}"

if [ ! -f "${CSPOT_SOURCE_DIR}/CMakeLists.txt" ]; then
    echo "错误: cspot 源码未找到 (${CSPOT_SOURCE_DIR})" >&2
    echo "请检出 SpotConnect: git clone https://github.com/infini2020/SpotConnect" >&2
    echo "并补齐子模块: git -C SpotConnect submodule update --init common/cspot" >&2
    echo "bell 与其子模块 (nlohmann_json, mdnssvc) 也需检出, 详见 backend/native/cspotd/README" >&2
    exit 1
fi

cmake -S "${SCRIPT_DIR}" -B "${BUILD_DIR}" \
    -DCSPOT_SOURCE_DIR="${CSPOT_SOURCE_DIR}" \
    -DCMAKE_BUILD_TYPE=Release
cmake --build "${BUILD_DIR}" -j"$(nproc)"

echo "cspotd 已编译: ${BUILD_DIR}/cspotd"
