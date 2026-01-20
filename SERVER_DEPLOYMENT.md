# BecauseChat 服务器部署指南

## 🚨 本地正常但服务器部署失败的常见原因

### 1. Docker版本不兼容
**问题**: 服务器Docker版本过旧或不兼容
**检查**:
```bash
docker --version
docker-compose --version
```
**要求**: Docker >= 20.10, Docker Compose >= 2.0

### 2. 端口冲突
**问题**: 服务器端口被其他服务占用
**检查**:
```bash
# 检查端口占用
sudo netstat -tulpn | grep -E ':(80|443|3080|5432|7700|27017)'
# 或
sudo lsof -i :80
```
**解决方案**: 修改 `deploy-compose.yml` 中的端口映射

### 3. CPU架构不兼容
**问题**: 服务器是ARM架构，但镜像只支持x86
**检查**:
```bash
uname -m  # 查看CPU架构
```
**解决方案**:
- 使用多架构镜像
- 或在 `deploy-compose.yml` 中指定平台:
```yaml
api:
  platform: linux/amd64
```

### 4. 文件权限问题
**问题**: 服务器文件权限与本地不同
**检查**:
```bash
ls -la data-node/ meili_data_v1.12/ logs/ uploads/
```
**解决方案**:
```bash
# 设置正确的权限
sudo chown -R 1000:1000 data-node/ meili_data_v1.12/ logs/ uploads/ images/
```

### 5. 内存/CPU资源不足
**问题**: 服务器资源不足导致容器启动失败
**检查**:
```bash
free -h
df -h
```
**要求**: 至少2GB内存，10GB磁盘空间

### 6. 网络配置问题
**问题**: 服务器防火墙阻止Docker网络通信
**检查**:
```bash
# 检查防火墙规则
sudo ufw status
sudo iptables -L
```
**解决方案**:
```bash
# 开放必要端口
sudo ufw allow 80
sudo ufw allow 443
sudo ufw allow 3080
```

## 🔧 服务器部署步骤

### 1. 环境准备
```bash
# 更新系统
sudo apt update && sudo apt upgrade -y

# 安装Docker
curl -fsSL https://get.docker.com -o get-docker.sh
sudo sh get-docker.sh

# 启动Docker服务
sudo systemctl start docker
sudo systemctl enable docker

# 添加用户到docker组（避免每次使用sudo）
sudo usermod -aG docker $USER
# 重新登录或运行: newgrp docker
```

### 2. 下载项目
```bash
git clone https://github.com/constanjin/Because.git
cd Because
```

### 3. 配置环境变量
```bash
cp .env.example .env
# 编辑 .env 文件，设置必要的环境变量
nano .env
```

### 4. 使用部署脚本
```bash
# 运行诊断和部署脚本
./deploy-server.sh
```

### 5. 手动部署（如果脚本失败）
```bash
# 清理可能的冲突
docker system prune -f

# 重新构建和启动
docker-compose -f deploy-compose.yml down --remove-orphans
docker-compose -f deploy-compose.yml up -d --build
```

## 🔍 故障排查

### 查看服务状态
```bash
docker-compose -f deploy-compose.yml ps
```

### 查看详细日志
```bash
# 查看所有服务日志
docker-compose -f deploy-compose.yml logs -f

# 查看特定服务日志
docker-compose -f deploy-compose.yml logs -f api
docker-compose -f deploy-compose.yml logs -f mongodb
```

### 检查容器资源使用
```bash
docker stats
```

### 测试服务连通性
```bash
# 测试API
curl http://localhost:3080/api/health

# 测试前端
curl http://localhost/

# 测试数据库连接
docker-compose -f deploy-compose.yml exec mongodb mongosh --eval "db.stats()"
```

## 🌐 生产环境配置

### 使用反向代理 (Nginx/Apache)
```nginx
# /etc/nginx/sites-available/becausechat
server {
    listen 80;
    server_name your-domain.com;

    location / {
        proxy_pass http://localhost:80;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
    }
}
```

### SSL证书配置
```bash
# 使用Let's Encrypt
sudo apt install certbot python3-certbot-nginx
sudo certbot --nginx -d your-domain.com
```

### 监控和日志
```bash
# 设置日志轮转
sudo apt install logrotate
# 配置logrotate规则...

# 监控容器
docker-compose -f deploy-compose.yml logs -f --tail=100 > app.log 2>&1 &
```

## 📞 获取帮助

如果问题仍然存在，请提供以下信息：
1. 服务器操作系统和版本
2. Docker版本信息
3. 完整的错误日志
4. `docker-compose -f deploy-compose.yml ps` 的输出
5. `docker-compose -f deploy-compose.yml logs` 的输出
