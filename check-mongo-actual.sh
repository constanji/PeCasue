#!/bin/bash

echo "🔍 检查MongoDB实际监听状态"
echo "=========================="

# 1. 检查MongoDB进程状态
echo "1️⃣ 检查MongoDB进程状态..."
docker exec Because-MongoDB ps aux | grep mongod | grep -v grep

# 2. 检查MongoDB端口监听（在容器内部）
echo ""
echo "2️⃣ 检查MongoDB端口监听..."
echo "   从MongoDB容器内部检查..."
docker exec Because-MongoDB sh -c "
echo '检查netstat...'
netstat -tuln 2>/dev/null | grep 27017 || echo 'netstat不可用'

echo '检查ss...'
ss -tuln 2>/dev/null | grep 27017 || echo 'ss不可用'

echo '检查lsof...'
lsof -i :27017 2>/dev/null || echo 'lsof不可用'

echo '检查进程端口...'
ps aux | grep mongod | grep -v grep
"

# 3. 测试MongoDB从外部访问
echo ""
echo "3️⃣ 测试MongoDB从外部访问..."
MONGO_IP=$(docker inspect --format='{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' Because-MongoDB 2>/dev/null)
echo "   MongoDB IP: $MONGO_IP"

echo "   测试从API容器访问..."
docker exec Because-API sh -c "
echo '测试ping...'
ping -c 1 $MONGO_IP >/dev/null 2>&1 && echo '✅ ping成功' || echo '❌ ping失败'

echo '测试TCP连接...'
timeout 3 sh -c \"echo > /dev/tcp/$MONGO_IP/27017\" 2>/dev/null && echo '✅ TCP连接成功' || echo '❌ TCP连接失败'

echo '测试mongosh连接...'
mongosh --host $MONGO_IP --port 27017 --eval 'db.adminCommand(\"ping\")' 2>&1 | head -3
"

# 4. 检查MongoDB配置文件
echo ""
echo "4️⃣ 检查MongoDB配置文件..."
docker exec Because-MongoDB mongosh --eval "
try {
    const result = db.adminCommand('getCmdLineOpts');
    print('MongoDB启动参数:');
    print(JSON.stringify(result.parsed, null, 2));
} catch(e) {
    print('无法获取配置:', e.message);
}
" 2>&1

# 5. 检查MongoDB日志
echo ""
echo "5️⃣ 检查MongoDB最近启动日志..."
docker logs Because-MongoDB 2>&1 | grep -i "listening\|waiting\|bind\|port\|ready" | tail -10

echo ""
echo "🎉 检查完成！"
