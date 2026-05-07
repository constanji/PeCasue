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

echo -e "${BLUE}🚀 启动 PeCause 数据库服务${NC}"
echo -e "${BLUE}================================${NC}\n"

# ==================== 数据安全保护 ====================
echo -e "${YELLOW}🛡️  数据安全保护${NC}"
echo -e "${YELLOW}----------------${NC}"

MONGODB_DATA_DIR="./data-node-pecause"
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
echo -e "${BLUE}📊 步骤1: 启动 MongoDB (端口 27043)${NC}"
echo -e "${BLUE}----------------${NC}"

# 检查MongoDB是否已在运行
MONGODB_PID=$(pgrep -f "mongod --dbpath ./data-node-pecause --port 27043" 2>/dev/null || echo "")
if [ -n "$MONGODB_PID" ]; then
    echo -e "${YELLOW}⚠️  MongoDB已在运行 (PID: $MONGODB_PID)${NC}"
    
    # 验证连接
    if lsof -i :27043 > /dev/null 2>&1 && mongosh --port 27043 --eval "db.adminCommand('ping')" 2>&1 | grep -q "ok.*1"; then
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
    mongod --dbpath ./data-node-pecause --port 27043 --bind_ip_all --logpath ./logs/mongodb.log --nounixsocket &
    MONGODB_PID=$!
    echo -e "${GREEN}✅ MongoDB已启动 (PID: $MONGODB_PID)${NC}"
    
    # 等待MongoDB启动
    echo "⏳ 等待MongoDB启动（5秒）..."
    sleep 5
    
    # 验证连接
    RETRY_COUNT=0
    MAX_RETRIES=6
    while [ $RETRY_COUNT -lt $MAX_RETRIES ]; do
        # 先检查进程是否还在运行
        if ! ps -p $MONGODB_PID > /dev/null 2>&1; then
            echo -e "${RED}❌ MongoDB进程已退出${NC}"
            
            # 检查是否是数据库损坏问题
            if grep -q "WiredTiger metadata corruption\|Failed to start up WiredTiger\|Please read the documentation for starting MongoDB with --repair" ./logs/mongodb.log 2>/dev/null; then
                echo -e "${YELLOW}⚠️  检测到数据库文件损坏！${NC}"
                echo -e "${YELLOW}正在尝试修复数据库...${NC}"
                
                # 尝试修复数据库
                echo -e "${BLUE}执行修复命令: mongod --dbpath ./data-node-pecause --repair --nounixsocket${NC}"
                echo "⏳ 这可能需要几分钟时间，请耐心等待..."
                
                REPAIR_OUTPUT=$(mongod --dbpath ./data-node-pecause --repair --nounixsocket --logpath ./logs/mongodb-repair.log 2>&1)
                REPAIR_EXIT_CODE=$?
                
                # 显示修复输出的最后几行
                echo "$REPAIR_OUTPUT" | tail -10
                
                if [ $REPAIR_EXIT_CODE -eq 0 ]; then
                    echo -e "${GREEN}✅ 数据库修复完成，重新启动MongoDB...${NC}"
                    # 重新启动 MongoDB
                    mongod --dbpath ./data-node-pecause --port 27043 --bind_ip_all --logpath ./logs/mongodb.log --nounixsocket &
                    MONGODB_PID=$!
                    sleep 5
                    RETRY_COUNT=0  # 重置重试计数
                    continue
                else
                    echo -e "${RED}❌ 数据库修复失败${NC}"
                    echo -e "${YELLOW}💡 如果修复失败，您可能需要：${NC}"
                    echo -e "${YELLOW}   1. 备份数据: cp -r ./data-node-pecause ./data-node-pecause.backup${NC}"
                    echo -e "${YELLOW}   2. 删除损坏的数据: rm -rf ./data-node-pecause/*${NC}"
                    echo -e "${YELLOW}   3. 重新运行此脚本${NC}"
                    exit 1
                fi
            else
                echo -e "${RED}请检查日志: ./logs/mongodb.log${NC}"
                exit 1
            fi
        fi
        
        # 检查端口是否在监听
        if lsof -i :27043 > /dev/null 2>&1; then
            # 尝试使用 mongosh 连接（捕获段错误）
            if mongosh --port 27043 --eval "db.adminCommand('ping')" 2>&1 | grep -q "ok.*1"; then
                echo -e "${GREEN}✅ MongoDB连接成功${NC}"
                break
            fi
        fi
        
        RETRY_COUNT=$((RETRY_COUNT + 1))
        if [ $RETRY_COUNT -lt $MAX_RETRIES ]; then
            echo "⏳ 等待MongoDB就绪... ($RETRY_COUNT/$MAX_RETRIES)"
            sleep 2
        else
            echo -e "${RED}❌ MongoDB启动失败，请检查日志: ./logs/mongodb.log${NC}"
            echo -e "${YELLOW}💡 提示: 如果看到段错误，可能是 mongosh 版本问题，请尝试更新: brew upgrade mongosh${NC}"
            exit 1
        fi
    done
fi

# 检查端口监听
if lsof -i :27043 > /dev/null 2>&1; then
    echo -e "${GREEN}✅ MongoDB端口 27043 正在监听${NC}"
else
    echo -e "${RED}❌ MongoDB端口 27043 未监听${NC}"
fi

echo ""

# ==================== 检查并启动 VectorDB ====================
echo -e "${BLUE}🧠 步骤2: 启动 VectorDB (端口 5444)${NC}"
echo -e "${BLUE}----------------${NC}"

# 检查VectorDB容器是否已存在
VECTORDB_CONTAINER="pecause-vectordb-local"
if docker ps -a --format '{{.Names}}' | grep -q "^${VECTORDB_CONTAINER}$"; then
    if docker ps --format '{{.Names}}' | grep -q "^${VECTORDB_CONTAINER}$"; then
        echo -e "${YELLOW}⚠️  VectorDB容器已在运行${NC}"
        
        # 验证连接
        if docker exec "$VECTORDB_CONTAINER" pg_isready -U pecause_user -d pecause_vector > /dev/null 2>&1; then
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
    if docker volume ls --format '{{.Name}}' | grep -q "^pecause_pgdata2$"; then
        echo -e "${GREEN}✅ 发现现有数据卷: pecause_pgdata2${NC}"
    else
        echo -e "${YELLOW}⚠️  数据卷不存在，将创建新数据卷${NC}"
    fi
    
    docker run -d --name "$VECTORDB_CONTAINER" \
      -p 5444:5432 \
      -e POSTGRES_DB=pecause_vector \
      -e POSTGRES_USER=pecause_user \
      -e POSTGRES_PASSWORD=pecause_password \
      -v pecause_pgdata2:/var/lib/postgresql/data \
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
    if docker exec "$VECTORDB_CONTAINER" pg_isready -U pecause_user -d pecause_vector > /dev/null 2>&1; then
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
if lsof -i :5444 > /dev/null 2>&1 || nc -z localhost 5444 2>/dev/null; then
    echo -e "${GREEN}✅ VectorDB端口 5444 正在监听${NC}"
else
    echo -e "${YELLOW}⚠️  VectorDB端口 5444 可能未监听（容器可能仍在启动中）${NC}"
fi

echo ""

# ==================== 最终状态检查 ====================
echo -e "${BLUE}🔍 最终状态检查${NC}"
echo -e "${BLUE}----------------${NC}"

# MongoDB状态
if lsof -i :27043 > /dev/null 2>&1 && mongosh --port 27043 --eval "db.adminCommand('ping')" 2>&1 | grep -q "ok.*1"; then
    echo -e "${GREEN}✅ MongoDB: 运行中 (localhost:27043)${NC}"
else
    echo -e "${RED}❌ MongoDB: 未运行${NC}"
fi

# VectorDB状态
if docker exec "$VECTORDB_CONTAINER" pg_isready -U pecause_user -d pecause_vector > /dev/null 2>&1; then
    echo -e "${GREEN}✅ VectorDB: 运行中 (localhost:5444)${NC}"
else
    echo -e "${RED}❌ VectorDB: 未运行${NC}"
fi

echo ""

# ==================== 连接信息 ====================
echo -e "${BLUE}📋 数据库连接信息${NC}"
echo -e "${BLUE}----------------${NC}"
echo "MongoDB:"
echo "  URI: mongodb://localhost:27043/pecauseAi"
echo "  数据目录: ./data-node-pecause"
echo ""
echo "VectorDB (PostgreSQL + pgvector):"
echo "  Host: localhost"
echo "  Port: 5444"
echo "  Database: pecause_vector"
echo "  User: pecause_user"
echo "  Password: pecause_password"
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
