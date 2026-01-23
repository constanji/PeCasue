#!/bin/bash

echo "🔍 测试直接IP连接"
echo "================"

# 获取MongoDB IP
MONGO_IP=$(docker inspect --format='{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' Because-MongoDB 2>/dev/null)
echo "MongoDB IP: $MONGO_IP"

# 测试1: 使用IP地址连接
echo ""
echo "1️⃣ 测试使用IP地址连接..."
docker exec Because-API node -e "
const net = require('net');
const socket = new net.Socket();
socket.setTimeout(5000);
socket.on('connect', () => {
    console.log('✅ IP连接成功！');
    socket.destroy();
    process.exit(0);
});
socket.on('error', (err) => {
    console.log('❌ IP连接失败:', err.code, err.message);
    process.exit(1);
});
socket.connect(27017, '$MONGO_IP');
setTimeout(() => {
    console.log('❌ 连接超时');
    socket.destroy();
    process.exit(1);
}, 5000);
" 2>&1

# 测试2: 使用服务名连接
echo ""
echo "2️⃣ 测试使用服务名连接..."
docker exec Because-API node -e "
const net = require('net');
const socket = new net.Socket();
socket.setTimeout(5000);
socket.on('connect', () => {
    console.log('✅ 服务名连接成功！');
    socket.destroy();
    process.exit(0);
});
socket.on('error', (err) => {
    console.log('❌ 服务名连接失败:', err.code, err.message);
    process.exit(1);
});
socket.connect(27017, 'mongodb');
setTimeout(() => {
    console.log('❌ 连接超时');
    socket.destroy();
    process.exit(1);
}, 5000);
" 2>&1

# 测试3: 如果IP连接成功，建议使用IP
if [ $? -eq 0 ] || [ $? -eq 1 ]; then
    echo ""
    echo "3️⃣ 如果IP连接成功但服务名失败，可以临时使用IP地址..."
    echo "   修改MONGO_URI为: mongodb://$MONGO_IP:27017/BecauseAi"
fi

echo ""
echo "🎉 测试完成！"
