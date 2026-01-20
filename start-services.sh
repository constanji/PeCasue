#!/bin/bash

echo "🚀 启动 BeCause 开发环境数据库服务..."
echo ""

# 创建日志目录
mkdir -p logs

# 启动 MongoDB
echo "📊 启动 MongoDB (端口 27033)..."
mongod --dbpath ./data-node --port 27033 --logpath ./logs/mongodb.log &
MONGODB_PID=$!
echo "✅ MongoDB PID: $MONGODB_PID"

# 等待MongoDB启动
sleep 3

# 启动 MeiliSearch
echo "🔍 启动 MeiliSearch (端口 7700)..."
docker run -d --name meilisearch-local \
  -p 7700:7700 \
  -e MEILI_MASTER_KEY=${MEILI_MASTER_KEY:-masterKey} \
  -v $(pwd)/meili_data_v1.12:/meili_data \
  getmeili/meilisearch:v1.12.3 > /dev/null 2>&1
echo "✅ MeiliSearch 容器已启动"

# 启动 VectorDB
echo "🧠 启动 VectorDB (端口 5434)..."
docker run -d --name vectordb-local \
  -p 5434:5432 \
  -e POSTGRES_DB=mydatabase \
  -e POSTGRES_USER=myuser \
  -e POSTGRES_PASSWORD=mypassword \
  -v becausechat_pgdata2:/var/lib/postgresql/data \
  pgvector/pgvector:0.8.0-pg15-trixie > /dev/null 2>&1
echo "✅ VectorDB 容器已启动"

echo ""
echo "⏳ 等待服务完全启动..."
sleep 5

echo ""
echo "🔍 检查服务状态..."

# 检查MongoDB
if nc -z localhost 27033 2>/dev/null; then
  echo "✅ MongoDB (27033): 运行中"
else
  echo "❌ MongoDB (27033): 未运行"
fi

# 检查MeiliSearch
if curl -s http://localhost:7700/health > /dev/null 2>&1; then
  echo "✅ MeiliSearch (7700): 运行中"
else
  echo "❌ MeiliSearch (7700): 未运行"
fi

# 检查VectorDB
if nc -z localhost 5434 2>/dev/null; then
  echo "✅ VectorDB (5434): 运行中"
else
  echo "❌ VectorDB (5434): 未运行"
fi

echo ""
echo "🎉 所有数据库服务启动完成！"
echo ""
echo "💡 接下来可以启动应用服务器："
echo "   npm run backend:dev    # 启动后端"
echo "   npm run frontend:dev   # 启动前端"
echo ""
echo "🔧 如需停止服务，请运行: ./stop-services.sh"
