<p align="center">
    <img src="client/public/assets/logo.svg" height="256">
  </a>
  <h1 align="center">    
  </h1>
</p>


# ✨ 核心功能

- 🖥️ **界面与体验**：灵感源自 ChatGPT，兼具增强型设计与丰富功能

- 🤖 **AI 模型选择**：
  - Anthropic（Claude）、AWS Bedrock、OpenAI、Azure OpenAI、Google、Vertex AI、OpenAI 响应 API（含 Azure）
  - 自定义端点：无需代理，即可使用任何兼容 OpenAI 的 API
  - 兼容本地及远程 AI 提供商：
    - Ollama、groq、Cohere、Mistral AI、Apple MLX、koboldcpp、together.ai
    - OpenRouter、Perplexity、ShuttleAI、Deepseek、通义千问（Qwen）等更多平台

- 🔧 **代码解释器 API**：
  - 安全沙箱执行环境，支持 Python、Node.js（JS/TS）、Go、C/C++、Java、PHP、Rust 及 Fortran
  - 无缝文件处理：直接上传、处理并下载文件
  - 隐私无忧：执行环境完全隔离，安全可靠

- 🔦 **智能代理与工具集成**：
  - 智能代理：
    - 无代码自定义助手：构建专业的 AI 驱动型辅助工具
    - 代理市场：发现并部署社区构建的智能代理
    - 协作共享：与特定用户及群组共享代理
    - 灵活可扩展：支持 MCP 服务器、各类工具、文件搜索、代码执行等功能
    - 多平台兼容：适配自定义端点、OpenAI、Azure、Anthropic、AWS Bedrock、Google、Vertex AI、响应 API 等
    - 工具支持模型上下文协议（MCP）

- 🔍 **网页搜索**：
  - 联网检索相关信息，增强 AI 上下文理解能力
  - 整合搜索提供商、内容抓取工具及结果重排器，优化搜索效果
  - 可自定义 Jina 重排：配置自定义 Jina API 地址用于重排服务

- 🪄 **生成式界面与代码产物**：
  - 代码产物支持在聊天中直接创建 React、HTML 及 Mermaid 图表

- 🎨 **图像生成与编辑**：
  - 文生图与图生图（支持 GPT-Image-1）
  - 文生图（支持 DALL-E（3/2）、Stable Diffusion 本地部署、Flux 或任何 MCP 服务器）
  - 输入提示词生成精美图像，或通过简单指令优化现有图像

- 💾 **预设与上下文管理**：
  - 创建、保存并共享自定义预设
  - 聊天过程中切换 AI 端点与预设
  - 编辑、重新提交消息，支持对话分支续聊
  - 创建并与特定用户及群组共享提示词
  - 消息与对话分支：高级上下文控制功能

- 💬 **多模态与文件交互**：
  - 上传并分析图像（支持 Claude 3、GPT-4.5、GPT-4o、o1、Llama-Vision 及 Gemini）📸
  - 文件对话（支持自定义端点、OpenAI、Azure、Anthropic、AWS Bedrock 及 Google）🗃️

- 🧠 **推理界面**：
  - 动态推理界面，适配思维链/推理型 AI 模型（如 DeepSeek-R1）

- 🎨 **可自定义界面**：
  - 可定制下拉菜单与界面布局，适配专业用户与新手用户需求

- 🗣️ **语音与音频功能**：
  - 语音转文字与文字转语音，支持免手动输入聊天
  - 自动发送并播放音频
  - 兼容 OpenAI、Azure OpenAI 及 Elevenlabs

- 📥 **对话导入与导出**：
  - 支持从 ChatGPT、Chatbot UI 导入对话
  - 支持以截图、Markdown、文本、JSON 格式导出对话

- 🔍 **搜索与发现**：
  - 搜索所有消息与对话

- 👥 **多用户与安全访问**：
  - 多用户支持，安全认证（兼容 OAuth2、LDAP 及邮箱登录）
  - 内置内容审核与 Token 消耗管理工具

---
> **提示**：如果在构建或启动过程中遇到错误，请参考 `docs/构建说明.md` 获取详细排查步骤。
## 🚀 快速开始

### 前置要求

- Node.js：推荐 v18+
- npm：v7+（支持 workspaces）
- Git
- Docker 与 Docker Compose（如使用 Docker 模式）

### 方式一：本地直接运行（适合深入开发）

1. **根目录安装依赖**
   ```bash
   npm install
   ```


2. **构建共享包**
   ```bash
    # 构建所有共享包（按顺序）
    npm run build:data-provider
    npm run build:data-schemas
    npm run build:api
    npm run build:client-package
    
    # 或者一次性构建所有包
    npm run build:packages
    ```
    
3. **构建Agent包**
    ```bash
    cd agents-because
    npm install           # 首次建议跑一下
    npm run build:dev     # 生成 dist/esm 和 dist/cjs
    ```

4. **配置环境变量**
   ```bash
   cp .env.example .env
   cp Because.yaml.example Because.yaml
   # 按需编辑 .env，配置数据库、密钥等
   ```

5. **启动后端（终端 1）**
   ```bash
   npm run backend:dev
   ```

6. **启动前端（终端 2）**
   ```bash
   npm run frontend:dev
   ```

7. **默认访问地址**
   - 开发前端：`http://localhost:3090`
   - 后端 API：`http://localhost:3080`



### 方式二：Docker 开发模式（测试版）

1. **克隆项目并进入目录**
   ```bash
   git clone https://github.com/constanji/Because
   cd Because
   ```

2. **配置环境变量**
   ```bash
   cp .env.example .env
   cp Because.yaml.example Because.yaml
   # 按需编辑 .env，配置数据库、密钥等
   ```

3. **构建镜像并启动服务**
   ```bash
   # 第一次或修改 Dockerfile 后，先构建镜像
   docker-compose -f docker-compose.dev.yml build
   # 然后启动所有服务（前端、后端、MongoDB、Meilisearch 等）
   docker-compose -f docker-compose.dev.yml up -d
   ```

4. **访问应用**
   - 前端（开发网关）：`http://pecause.localhost:4614`
   - 后端 API：`http://pecause.localhost:1245`

5. **查看 / 停止服务**
   ```bash
   # 查看日志
   docker-compose -f docker-compose.dev.yml logs -f

   # 停止服务
   docker-compose -f docker-compose.dev.yml down
   ```

### 方式三：Docker 生产环境部署（推荐）

#### 前置要求

- Docker：v20.10+
- Docker Compose：v2.0+
- 服务器内存：建议 4GB+（RAG ONNX 模型需要约 500MB 内存）
- 磁盘空间：建议 10GB+（包含模型文件和依赖）

#### 部署步骤

1. **克隆项目到服务器**
   ```bash
   git clone https://github.com/constanji/Because.git
   cd Because
   ```

2. **配置环境变量**
   ```bash
   # 复制环境变量模板
   cp .env.example .env
   cp Because.yaml.example Because.yaml
   
   # 编辑 .env 文件，配置必要的环境变量
   # 特别注意以下配置：
   # - MONGO_URI: MongoDB 连接字符串
   # - MEILI_MASTER_KEY: Meilisearch 主密钥
   # - 各种 API 密钥（OpenAI、Anthropic 等）
   # - USE_ONNX_EMBEDDING: 是否使用 ONNX 嵌入模型（默认 true）
   nano .env
   ```

3. **构建 Docker 镜像**
   ```bash
   # 方式 A：使用标准 Dockerfile（单阶段构建）
   docker build -t because:latest -f Dockerfile .
   
   # 方式 B：使用多阶段构建 Dockerfile（推荐，构建更快）
   docker build -t because:latest -f Dockerfile.multi .
   
   # 如果使用私有镜像仓库，可以推送到仓库
   # docker tag because:latest your-registry.com/because:latest
   # docker push your-registry.com/because:latest
   ```

4. **配置 docker-compose.yml**
   
   编辑 `docker-compose.yml`，确保以下配置正确：
   - **端口映射**：根据服务器实际情况调整 `API_PORT`、`MONGO_PORT`、`MEILI_PORT`、`VECTOR_DB_PORT`
   - **镜像名称**：如果使用自定义镜像，修改 `image` 字段
   - **环境变量**：确保 `.env` 文件中的变量正确传递

5. **启动所有服务**
   ```bash
   # 启动所有服务（API、MongoDB、Meilisearch、VectorDB）
   docker-compose up -d
   
   # 查看服务状态
   docker-compose ps
   
   # 查看日志
   docker-compose logs -f
   ```

6. **验证部署**
   ```bash
   # 检查 API 服务是否正常
   curl http://pecause.localhost:4180/api/health
   
   # 检查 MongoDB
   docker-compose exec mongodb mongosh --eval "db.version()"
   
   # 检查 Meilisearch
   curl http://localhost:8800/health
   ```

7. **访问应用**
   - 前端访问：`http://your-server-ip:4180`
   - API 端点：`http://your-server-ip:4180/api`

#### 注意事项

##### 1. RAG 嵌入式模型（ONNX）

项目使用本地 ONNX 模型进行文本向量化，模型文件位于：
- `api/server/services/RAG/onnx/embedding/resources/` - 嵌入模型
- `api/server/services/RAG/onnx/reranker/resources/` - 重排序模型

**确保模型文件已包含**：
- Dockerfile 会自动复制这些文件（通过 `COPY . .` 命令）
- 模型文件大小约 200-500MB，首次构建可能需要较长时间
- 如果模型文件缺失，RAG 功能会自动回退到其他嵌入方式（如 OpenAI API）

**环境变量配置**：
```bash
# .env 文件
USE_ONNX_EMBEDDING=true          # 启用 ONNX 嵌入（默认 true）
EMBEDDING_MODEL=onnx             # 使用 ONNX 模型
ALLOW_NO_EMBEDDING=false         # 是否允许无嵌入保存（生产环境建议 false）
```

##### 2. Plotly 可视化图表

项目使用 Plotly.js 生成可视化图表（通过 BeCauseSkills 的 chart-generation-tool）：
- Plotly 依赖已包含在 `api/package.json` 中（`plotly.js-dist-min`）
- Docker 构建时会自动安装
- 前端通过 CDN 加载 Plotly.js，无需额外配置

**确保功能正常**：
- 检查网络连接（前端需要加载 Plotly CDN）
- 如果内网环境，可以考虑将 Plotly.js 打包到前端构建中

##### 3. BeCauseSkills 工具集合

BeCauseSkills 包含多个智能工具（意图分类、RAG 检索、SQL 执行、图表生成等）：
- 工具代码位于 `BeCauseSkills/` 目录
- Dockerfile 会自动复制该目录
- 确保 `BeCauseSkills/index.js` 正确导出所有工具

**验证工具加载**：
```bash
# 进入容器检查
docker-compose exec api node -e "console.log(require('./BeCauseSkills'))"
```

##### 4. 数据持久化

确保以下目录已正确挂载（在 `docker-compose.yml` 中配置）：
- `./data-node` - MongoDB 数据目录
- `./meili_data_v1.12` - Meilisearch 数据目录
- `./logs` - 应用日志目录
- `./uploads` - 用户上传文件目录
- `./images` - 图片资源目录

##### 5. 性能优化建议

- **内存限制**：RAG ONNX 模型需要足够内存，建议容器内存限制至少 2GB
- **构建优化**：使用 `Dockerfile.multi` 多阶段构建，减少镜像大小
- **缓存利用**：合理使用 Docker 构建缓存，避免重复构建依赖

#### 更新和维护

```bash
# 拉取最新代码
git pull origin main

# 重新构建镜像
docker-compose build --no-cache

# 重启服务（零停机时间）
docker-compose up -d --force-recreate

# 查看资源使用情况
docker stats

# 清理未使用的镜像和容器
docker system prune -a
```

#### 故障排查

1. **容器启动失败**
   ```bash
   # 查看详细日志
   docker-compose logs api
   docker-compose logs mongodb
   ```

2. **RAG 功能异常**
   ```bash
   # 检查 ONNX 模型文件是否存在
   docker-compose exec api ls -lh api/server/services/RAG/onnx/embedding/resources/
   
   # 检查 @xenova/transformers 是否安装
   docker-compose exec api npm list @xenova/transformers
   ```

3. **图表无法显示**
   - 检查浏览器控制台是否有 Plotly.js 加载错误
   - 确认网络可以访问 Plotly CDN
   - 检查前端构建是否包含图表相关代码


   

---







