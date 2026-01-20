#!/bin/bash

echo "🛑 停止 BeCause 开发环境数据库服务..."
echo ""

# 停止 MongoDB 进程
echo "📊 停止 MongoDB..."
pkill -f "mongod --dbpath ./data-node" 2>/dev/null || echo "MongoDB 进程未找到"

# 停止 Docker 容器
echo "🐳 停止 Docker 容器..."
docker stop meilisearch-local vectordb-local 2>/dev/null || echo "部分容器未运行"

# 清理容器
echo "🧹 清理容器..."
docker rm meilisearch-local vectordb-local 2>/dev/null || echo "部分容器不存在"

echo ""
echo "✅ 所有数据库服务已停止！"

# 检查端口是否释放
echo ""
echo "🔍 检查端口释放状态..."
sleep 2

if lsof -i :27033 > /dev/null 2>&1; then
  echo "❌ 端口 27033 仍被占用"
else
  echo "✅ 端口 27033 已释放"
fi

if lsof -i :7700 > /dev/null 2>&1; then
  echo "❌ 端口 7700 仍被占用"
else
  echo "✅ 端口 7700 已释放"
fi

if lsof -i :5434 > /dev/null 2>&1; then
  echo "❌ 端口 5434 仍被占用"
else
  echo "✅ 端口 5434 已释放"
fi

echo ""
echo "🎉 服务停止完成！"
