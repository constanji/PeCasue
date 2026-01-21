# 数据库服务管理指南

本指南介绍如何启动和管理本地开发环境所需的数据库服务。

## 📋 数据库服务

- **MongoDB**: 主数据库（端口 27033）
- **VectorDB**: 向量数据库 PostgreSQL + pgvector（端口 5434）

## 🚀 快速开始

### 启动所有数据库

```bash
./start-databases.sh
```

这个脚本会：
- ✅ 检查并启动 MongoDB（如果未运行）
- ✅ 检查并启动 VectorDB（如果未运行）
- ✅ 验证所有服务连接
- ✅ 显示连接信息

### 停止所有数据库

```bash
./stop-databases.sh
```

## 📊 MongoDB

### 启动方式

**方式1：使用脚本（推荐）**
```bash
./start-databases.sh
```

**方式2：手动启动**
```bash
mongod --dbpath ./data-node --port 27033 --bind_ip_all --logpath ./logs/mongodb.log &
```

### 连接信息

- **URI**: `mongodb://localhost:27033/BecauseAi`
- **数据目录**: `./data-node`
- **日志文件**: `./logs/mongodb.log`

### 验证连接

```bash
mongosh --port 27033 --eval "db.adminCommand('ping')"
```

### 停止

```bash
# 查找进程ID
pgrep -f "mongod --dbpath ./data-node --port 27033"

# 停止进程
kill <PID>

# 或使用脚本
./stop-databases.sh
```

## 🧠 VectorDB (PostgreSQL + pgvector)

### 启动方式

**方式1：使用脚本（推荐）**
```bash
./start-databases.sh
```

**方式2：手动启动**
```bash
docker run -d --name vectordb-local \
  -p 5434:5432 \
  -e POSTGRES_DB=mydatabase \
  -e POSTGRES_USER=myuser \
  -e POSTGRES_PASSWORD=mypassword \
  -v becausechat_pgdata2:/var/lib/postgresql/data \
  pgvector/pgvector:0.8.0-pg15-trixie
```

### 连接信息

- **Host**: `localhost`
- **Port**: `5434`
- **Database**: `mydatabase`
- **User**: `myuser`
- **Password**: `mypassword`

### 验证连接

```bash
# 检查容器状态
docker ps | grep vectordb-local

# 检查数据库连接
docker exec vectordb-local pg_isready -U myuser -d mydatabase

# 测试端口连接
nc -zv localhost 5434
```

### 停止

```bash
docker stop vectordb-local

# 或使用脚本
./stop-databases.sh
```

## 🔍 检查服务状态

### 检查所有数据库

```bash
./start-databases.sh
# 脚本会自动检查并显示状态
```

### 手动检查

**MongoDB:**
```bash
# 检查进程
pgrep -f "mongod --dbpath ./data-node --port 27033"

# 检查端口
lsof -i :27033

# 测试连接
mongosh --port 27033 --eval "db.adminCommand('ping')"
```

**VectorDB:**
```bash
# 检查容器
docker ps | grep vectordb-local

# 检查端口
lsof -i :5434
# 或
nc -zv localhost 5434

# 测试连接
docker exec vectordb-local pg_isready -U myuser -d mydatabase
```

## 🛡️ 数据安全

### 数据目录

- **MongoDB数据**: `./data-node/` - 本地目录，**数据会保留**
- **VectorDB数据**: Docker命名卷 `becausechat_pgdata2` - **数据会保留**

### 备份建议

```bash
# MongoDB备份
mongodump --port 27033 --db BecauseAi --out ./backup/mongodb-$(date +%Y%m%d)

# VectorDB备份
docker exec vectordb-local pg_dump -U myuser mydatabase > ./backup/vectordb-$(date +%Y%m%d).sql
```

## 🔧 故障排查

### MongoDB无法启动

1. **检查端口占用**
   ```bash
   lsof -i :27033
   ```

2. **检查数据目录权限**
   ```bash
   ls -la ./data-node
   ```

3. **查看日志**
   ```bash
   tail -f ./logs/mongodb.log
   ```

4. **清理锁文件（如果MongoDB异常退出）**
   ```bash
   rm ./data-node/mongod.lock
   ```

### VectorDB无法启动

1. **检查容器状态**
   ```bash
   docker ps -a | grep vectordb-local
   docker logs vectordb-local
   ```

2. **检查端口占用**
   ```bash
   lsof -i :5434
   ```

3. **重新创建容器**
   ```bash
   docker stop vectordb-local
   docker rm vectordb-local
   ./start-databases.sh
   ```

### 连接失败

1. **检查服务是否运行**
   ```bash
   ./start-databases.sh
   ```

2. **检查防火墙设置**
   ```bash
   # macOS
   sudo pfctl -s rules
   ```

3. **检查环境变量**
   ```bash
   # 后端会自动检测环境
   # 本地开发: localhost:5434
   # Docker环境: vectordb:5432
   ```

## 📝 环境变量

后端服务会自动检测环境并连接相应的数据库：

- **本地开发环境**: 使用 `localhost:27033` 和 `localhost:5434`
- **Docker容器环境**: 使用 `mongodb:27017` 和 `vectordb:5432`

无需手动配置，后端会自动适配。

## 🎯 常见使用场景

### 场景1：首次启动开发环境

```bash
# 1. 启动数据库
./start-databases.sh

# 2. 启动后端
npm run backend:dev

# 3. 启动前端（新终端）
npm run frontend:dev
```

### 场景2：重启数据库服务

```bash
# 停止
./stop-databases.sh

# 启动
./start-databases.sh
```

### 场景3：仅重启MongoDB

```bash
# 停止
kill $(pgrep -f "mongod --dbpath ./data-node --port 27033")

# 启动
mongod --dbpath ./data-node --port 27033 --bind_ip_all --logpath ./logs/mongodb.log &
```

### 场景4：仅重启VectorDB

```bash
# 停止
docker stop vectordb-local

# 启动
docker start vectordb-local
# 或
./start-databases.sh
```

## 📚 相关脚本

- `start-databases.sh` - 启动所有数据库服务（MongoDB + VectorDB）
- `stop-databases.sh` - 停止所有数据库服务

## 💡 提示

1. **数据持久化**: 所有数据都保存在本地，重启服务不会丢失数据
2. **端口冲突**: 如果端口被占用，脚本会提示，可以手动停止占用端口的进程
3. **健康检查**: 脚本会自动进行健康检查，确保服务正常启动
4. **日志查看**: MongoDB日志在 `./logs/mongodb.log`，VectorDB日志使用 `docker logs vectordb-local`

