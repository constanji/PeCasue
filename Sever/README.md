# PeCause 服务器部署包（Sever）

两台业务镜像 **`pecause:latest`**、**`pecause-pipeline-svc:latest`** 与本目录一起放到服务器即可完成部署。

## 目录应具备内容

| 路径 | 说明 |
|------|------|
| `.env` | 首次可复制 `.env.example` 后填写（JWT、域名、密钥等）；勿提交仓库。 |
| `Because.yaml` | 应用主配置，与本包一并提供或可替换为你的环境版本。 |
| `specs/` | 挂载进 API；与开发仓库保持一致更新即可。 |
| `client/nginx.conf` | Nginx 反代配置（仅此为必需；不需要整仓前端源码）。 |
| **`pipeline-data/`** | Pipeline 持久化：**tasks.db、任务目录、rules** 等均在此（已含 `.gitkeep` 占位）。 |
| **`data-node-pecause/`** | MongoDB 数据目录（挂载名必须与 compose 一致）。 |
| `images/`、 `uploads/`、 `logs/` | 运行时数据；可由 `deploy.sh` 创建。 |

若你曾与旧部署一样只有 **`data-node/`**，首次运行 `./deploy.sh` 会将其 **重命名为 `data-node-pecause`**（仅在目标目录尚不存在时执行）。

可将镜像 tar 放在 **`images/*.tar`**（或 `*.tar.gz`），脚本在缺镜像时可 **自动执行 `docker load`**，或使用：

```bash
./deploy.sh --load-images
```

## 环境与命令

- 服务器需 **Docker + Docker Compose V2**。
- 启动：`chmod +x deploy.sh && ./deploy.sh`

compose 已对 **MongoDB、pipeline-svc、vectordb** 做 **healthcheck**，`api` 等三者健康后再启动，减少首开 502。

## 端口

- **`8180`**：HTTP（Nginx，浏览器入口）
- **`4182`**：API 直连

## 镜像构建（开发机）

```bash
docker buildx build … -t pecause:latest -f Dockerfile.multi .
docker buildx build … -t pecause-pipeline-svc:latest -f pipeline-svc/Dockerfile pipeline-svc
docker save -o pecause-latest.tar pecause:latest
docker save -o pecause-pipeline-svc-latest.tar pecause-pipeline-svc:latest
```

将 tar 拷贝到服务器的 `Sever/images/` 后按需 `./deploy.sh --load-images`。

## Pipeline 数据存哪？

`pipeline-svc` 容器内路径为 **`/data/pipeline`**，通过挂载绑定到 **`./pipeline-data`**，落在**宿主机**，删容器不会自动删这些数据；请将此目录纳入备份。
