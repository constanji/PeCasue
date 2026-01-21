#!/bin/bash

# 脚本名称: start-databases.sh
# 描述: 启动本地开发环境所需的数据库服务（MongoDB 和 VectorDB）
# 用法: ./start-databases.sh

# 颜色定义
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# 确保在项目根目录
SCRIPT_DIR=$(dirname "$(realpath "$0")")
cd "$SCRIPT_DIR" || exit 1

echo -e "${BLUE}🚀 启动 BeCause 数据库服务${NC}"
echo -e "${BLUE}================================${NC}\n"

# ==================== 数据安全保护 ====================
echo -e "${YELLOW}🛡️  数据安全保护${NC}"
echo -e "${YELLOW}----------------${NC}"

MONGODB_DATA_DIR="./data-node"
if [ -d "$MONGODB_DATA_DIR" ]; then
    MONGODB_DATA_SIZE=$(du -sh "$MONGODB_DATA_DIR" 2>/dev/null | awk '{print $1}')
    echo -e "${GREEN}✅ MongoDB数据目录: $MONGODB_DATA_DIR (大小: ${MONGODB_DATA_SIZE:-0M})${NC}"
else
    echo -e "${YELLOW}⚠️  MongoDB数据目录不存在，将创建${NC}"
    mkdir -p "$MONGODB_DATA_DIR"
fi

# 创建日志目录
mkdir -p logs

echo ""

# ==================== 检查并启动 MongoDB ====================
echo -e "${BLUE}📊 步骤1: 启动 MongoDB (端口 27033)${NC}"
echo -e "${BLUE}----------------${NC}"

# 检查MongoDB是否已在运行
MONGODB_PID=$(pgrep -f "mongod --dbpath ./data-node --port 27033")
if [ -n "$MONGODB_PID" ]; then
    echo -e "${YELLOW}⚠️  MongoDB已在运行 (PID: $MONGODB_PID)${NC}"
    
    # 验证连接
    if mongosh --port 27033 --eval "db.adminCommand('ping')" > /dev/null 2>&1; then
        echo -e "${GREEN}✅ MongoDB连接正常${NC}"
    else
        echo -e "${RED}❌ MongoDB进程存在但无法连接，正在重启...${NC}"
        kill "$MONGODB_PID" 2>/dev/null
        sleep 2
        MONGODB_PID=""
    fi
fi

# 如果MongoDB未运行，启动它
if [ -z "$MONGODB_PID" ]; then
    echo "正在启动MongoDB..."
    mongod --dbpath ./data-node --port 27033 --bind_ip_all --logpath ./logs/mongodb.log &
    MONGODB_PID=$!
    echo -e "${GREEN}✅ MongoDB已启动 (PID: $MONGODB_PID)${NC}"
    
    # 等待MongoDB启动
    echo "⏳ 等待MongoDB启动（5秒）..."
    sleep 5
    
    # 验证连接
    RETRY_COUNT=0
    MAX_RETRIES=6
    while [ $RETRY_COUNT -lt $MAX_RETRIES ]; do
        if mongosh --port 27033 --eval "db.adminCommand('ping')" > /dev/null 2>&1; then
            echo -e "${GREEN}✅ MongoDB连接成功${NC}"
            break
        else
            RETRY_COUNT=$((RETRY_COUNT + 1))
            if [ $RETRY_COUNT -lt $MAX_RETRIES ]; then
                echo "⏳ 等待MongoDB就绪... ($RETRY_COUNT/$MAX_RETRIES)"
                sleep 2
            else
                echo -e "${RED}❌ MongoDB启动失败，请检查日志: ./logs/mongodb.log${NC}"
                exit 1
            fi
        fi
    done
fi

# 检查端口监听
if lsof -i :27033 > /dev/null 2>&1; then
    echo -e "${GREEN}✅ MongoDB端口 27033 正在监听${NC}"
else
    echo -e "${RED}❌ MongoDB端口 27033 未监听${NC}"
fi

echo ""

# ==================== 检查并启动 VectorDB ====================
echo -e "${BLUE}🧠 步骤2: 启动 VectorDB (端口 5434)${NC}"
echo -e "${BLUE}----------------${NC}"

# 检查VectorDB容器是否已存在
VECTORDB_CONTAINER="vectordb-local"
if docker ps -a --format '{{.Names}}' | grep -q "^${VECTORDB_CONTAINER}$"; then
    if docker ps --format '{{.Names}}' | grep -q "^${VECTORDB_CONTAINER}$"; then
        echo -e "${YELLOW}⚠️  VectorDB容器已在运行${NC}"
        
        # 验证连接
        if docker exec "$VECTORDB_CONTAINER" pg_isready -U myuser -d mydatabase > /dev/null 2>&1; then
            echo -e "${GREEN}✅ VectorDB连接正常${NC}"
        else
            echo -e "${YELLOW}⚠️  VectorDB容器运行中但未就绪，等待中...${NC}"
            sleep 5
        fi
    else
        echo "正在启动现有VectorDB容器..."
        docker start "$VECTORDB_CONTAINER" > /dev/null 2>&1
        echo -e "${GREEN}✅ VectorDB容器已启动${NC}"
        sleep 3
    fi
else
    echo "正在创建并启动VectorDB容器..."
    
    # 检查数据卷是否存在
    if docker volume ls --format '{{.Name}}' | grep -q "^becausechat_pgdata2$"; then
        echo -e "${GREEN}✅ 发现现有数据卷: becausechat_pgdata2${NC}"
    else
        echo -e "${YELLOW}⚠️  数据卷不存在，将创建新数据卷${NC}"
    fi
    
    docker run -d --name "$VECTORDB_CONTAINER" \
      -p 5434:5432 \
      -e POSTGRES_DB=mydatabase \
      -e POSTGRES_USER=myuser \
      -e POSTGRES_PASSWORD=mypassword \
      -v becausechat_pgdata2:/var/lib/postgresql/data \
      pgvector/pgvector:0.8.0-pg15-trixie > /dev/null 2>&1
    
    if [ $? -eq 0 ]; then
        echo -e "${GREEN}✅ VectorDB容器已创建并启动${NC}"
    else
        echo -e "${RED}❌ VectorDB容器启动失败${NC}"
        exit 1
    fi
    
    # 等待VectorDB启动
    echo "⏳ 等待VectorDB启动（10秒）..."
    sleep 10
fi

# 验证VectorDB连接
RETRY_COUNT=0
MAX_RETRIES=10
while [ $RETRY_COUNT -lt $MAX_RETRIES ]; do
    if docker exec "$VECTORDB_CONTAINER" pg_isready -U myuser -d mydatabase > /dev/null 2>&1; then
        echo -e "${GREEN}✅ VectorDB连接成功${NC}"
        break
    else
        RETRY_COUNT=$((RETRY_COUNT + 1))
        if [ $RETRY_COUNT -lt $MAX_RETRIES ]; then
            echo "⏳ 等待VectorDB就绪... ($RETRY_COUNT/$MAX_RETRIES)"
            sleep 2
        else
            echo -e "${RED}❌ VectorDB启动失败，请检查日志: docker logs $VECTORDB_CONTAINER${NC}"
            exit 1
        fi
    fi
done

# 检查端口监听
if lsof -i :5434 > /dev/null 2>&1 || nc -z localhost 5434 2>/dev/null; then
    echo -e "${GREEN}✅ VectorDB端口 5434 正在监听${NC}"
else
    echo -e "${YELLOW}⚠️  VectorDB端口 5434 可能未监听（容器可能仍在启动中）${NC}"
fi

echo ""

# ==================== 最终状态检查 ====================
echo -e "${BLUE}🔍 最终状态检查${NC}"
echo -e "${BLUE}----------------${NC}"

# MongoDB状态
if mongosh --port 27033 --eval "db.adminCommand('ping')" > /dev/null 2>&1; then
    echo -e "${GREEN}✅ MongoDB: 运行中 (localhost:27033)${NC}"
else
    echo -e "${RED}❌ MongoDB: 未运行${NC}"
fi

# VectorDB状态
if docker exec "$VECTORDB_CONTAINER" pg_isready -U myuser -d mydatabase > /dev/null 2>&1; then
    echo -e "${GREEN}✅ VectorDB: 运行中 (localhost:5434)${NC}"
else
    echo -e "${RED}❌ VectorDB: 未运行${NC}"
fi

echo ""

# ==================== 连接信息 ====================
echo -e "${BLUE}📋 数据库连接信息${NC}"
echo -e "${BLUE}----------------${NC}"
echo "MongoDB:"
echo "  URI: mongodb://localhost:27033/BecauseAi"
echo "  数据目录: ./data-node"
echo ""
echo "VectorDB (PostgreSQL + pgvector):"
echo "  Host: localhost"
echo "  Port: 5434"
echo "  Database: mydatabase"
echo "  User: myuser"
echo "  Password: mypassword"
echo ""

# ==================== 完成提示 ====================
echo -e "${GREEN}🎉 数据库服务启动完成！${NC}"
echo ""
echo -e "${YELLOW}💡 接下来可以启动应用服务器：${NC}"
echo "   npm run backend:dev    # 启动后端开发服务器"
echo "   npm run frontend:dev   # 启动前端开发服务器"
echo ""
echo -e "${YELLOW}🔧 停止数据库服务：${NC}"
echo "   ./stop-databases.sh    # 停止所有数据库服务"
echo "   或手动停止:"
echo "   kill $MONGODB_PID                    # 停止MongoDB"
echo "   docker stop $VECTORDB_CONTAINER      # 停止VectorDB"
echo ""

