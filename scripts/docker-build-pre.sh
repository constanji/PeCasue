#!/bin/bash
# Docker 镜像构建前的本地预构建脚本
# 执行此脚本后再运行 docker buildx build，确保镜像包含最新代码
#
# 用法: ./scripts/docker-build-pre.sh
# 或:   bash scripts/docker-build-pre.sh

set -e
cd "$(dirname "$0")/.."

echo "=== Docker 构建前预构建 ==="

# 1. agents-because（可选：有本地 dist 则 Docker 会复用，否则在 Docker 内构建）
#    若需最新 agents，先构建；否则可删除 dist 让 Docker 内构建
if [ -d "agents-because/dist" ] && [ -n "$(ls -A agents-because/dist 2>/dev/null)" ]; then
  echo "[1/3] agents-because: 发现本地 dist，将复用（若需最新请先删除 agents-because/dist）"
else
  echo "[1/3] agents-because: 无本地 dist，Docker 内将自动构建"
fi

# 2. packages/client（必须：Docker 直接 COPY，不构建）
echo "[2/3] 构建 packages/client..."
npm run build:client-package

# 3. client（必须：Docker 直接 COPY，不构建）
echo "[3/3] 构建 client..."
cd client && npm run build && cd ..

echo ""
echo "=== 预构建完成 ==="
echo "可执行: docker buildx build --builder builder-with-mirror --platform linux/amd64 --load -t because:latest -f Dockerfile.multi ."
