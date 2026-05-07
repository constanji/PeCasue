#!/bin/bash

# 脚本名称: stop-databases.sh
# 描述: 停止本地开发环境的数据库服务（MongoDB 和 VectorDB）
# 用法: ./stop-databases.sh

# 颜色定义
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}🛑 停止 PeCause 数据库服务${NC}"
echo -e "${BLUE}================================${NC}\n"

# ==================== 停止 MongoDB ====================
echo -e "${YELLOW}📊 停止 MongoDB...${NC}"

MONGODB_PID=$(pgrep -f "mongod --dbpath ./data-node-pecause --port 27043")
if [ -n "$MONGODB_PID" ]; then
    echo "发现MongoDB进程 (PID: $MONGODB_PID)"
    kill "$MONGODB_PID" 2>/dev/null
    
    # 等待进程结束
    sleep 2
    
    # 检查是否还在运行
    if pgrep -f "mongod --dbpath ./data-node-pecause --port 27043" > /dev/null; then
        echo -e "${YELLOW}⚠️  进程未正常退出，强制终止...${NC}"
        kill -9 "$MONGODB_PID" 2>/dev/null
        sleep 1
    fi
    
    if ! pgrep -f "mongod --dbpath ./data-node-pecause --port 27043" > /dev/null; then
        echo -e "${GREEN}✅ MongoDB已停止${NC}"
    else
        echo -e "${RED}❌ MongoDB停止失败${NC}"
    fi
else
    echo -e "${YELLOW}⚠️  未发现MongoDB进程${NC}"
fi

echo ""

# ==================== 停止 VectorDB ====================
echo -e "${YELLOW}🧠 停止 VectorDB...${NC}"

VECTORDB_CONTAINER="pecause-vectordb-local"
if docker ps --format '{{.Names}}' | grep -q "^${VECTORDB_CONTAINER}$"; then
    echo "发现VectorDB容器: $VECTORDB_CONTAINER"
    docker stop "$VECTORDB_CONTAINER" > /dev/null 2>&1
    
    if [ $? -eq 0 ]; then
        echo -e "${GREEN}✅ VectorDB容器已停止${NC}"
    else
        echo -e "${RED}❌ VectorDB容器停止失败${NC}"
    fi
elif docker ps -a --format '{{.Names}}' | grep -q "^${VECTORDB_CONTAINER}$"; then
    echo -e "${YELLOW}⚠️  VectorDB容器已停止${NC}"
else
    echo -e "${YELLOW}⚠️  未发现VectorDB容器${NC}"
fi

echo ""

# ==================== 最终状态 ====================
echo -e "${BLUE}🔍 最终状态${NC}"
echo -e "${BLUE}----------------${NC}"

# 检查MongoDB
if pgrep -f "mongod --dbpath ./data-node-pecause --port 27043" > /dev/null; then
    echo -e "${RED}❌ MongoDB: 仍在运行${NC}"
else
    echo -e "${GREEN}✅ MongoDB: 已停止${NC}"
fi

# 检查VectorDB
if docker ps --format '{{.Names}}' | grep -q "^${VECTORDB_CONTAINER}$"; then
    echo -e "${RED}❌ VectorDB: 仍在运行${NC}"
else
    echo -e "${GREEN}✅ VectorDB: 已停止${NC}"
fi

echo ""
echo -e "${GREEN}🎉 数据库服务已停止${NC}"
echo ""
echo -e "${YELLOW}💡 提示：${NC}"
echo "   - 数据已保存，不会被删除"
echo "   - 重新启动: ./start-databases.sh"
echo ""
