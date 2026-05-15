# pipeline-svc

PeCause 流水线 FastAPI sidecar。负责：

- 任务编排（多渠道独立执行 + 持久化）
- 解析器（账单 / 自有流水 / 客资 / 特殊渠道 / 境内日本 / 分摊基数 / 汇总）
- 智能体协作（AgentScope ReActAgent：Mapper / Password / Unknown / Compare / Copilot）
- 规则配置（账户/费项/汇率/处理表/特殊分行/模板/密码簿）
- 对比核对、观测运维

## 运行

```bash
# 本目录下
uv venv
uv pip install -e .
PIPELINE_DATA_DIR=./data uvicorn server.main:app --host 0.0.0.0 --port 8001 --reload
```

或使用 `python -m`：

```bash
python -m uvicorn server.main:app --host 0.0.0.0 --port 8001 --reload
```

## 与 PeCause 集成

- 前端通过 Vite 代理 `/api/pipeline/*` → `http://localhost:8001`（开发期）
- 生产期由 Node Express 反代 `/api/pipeline/*` 转发到本服务
- 鉴权由 Node 端的 `requireJwtAuth` + `requireRoles([ADMIN])` 完成；本服务信任来自反代的请求

## 数据目录

- `PIPELINE_DATA_DIR`（默认 `./data`）
  - `tasks.db` — SQLite（任务、生命周期事件、规则版本、对比、Agent 介入）
  - `tasks/{task_id}/` — 任务工作区
    - `meta.json`、`state.json`、`task.log`
    - `raw/`、`extracted/{channel}/`
    - `channels/{channel}/runs/{run_id}/`
    - `compare/{compare_id}/`、`agent_drafts/`
  - `rules/` — 规则文件（mapping/fx/rules/templates/password_book.enc）
    - `manifest.json` — 版本/修改人/时间

## Phase 进度

- [x] Phase 1 — FastAPI 骨架 + tasks.db + GET /tasks
- [x] Phase 2 — 上传与总览（zip 自动归类 + 多渠道表单上传）
- [x] Phase 3 — 渠道独立执行 + 解析器接口（占位实现，可逐步替换）
- [x] Phase 4 — SSE 日志流 / 校验报告 / 任务流
- [x] Phase 5 — 产物下载与上传替换（hash + dirty 标记 + 审计）
- [x] Phase 6 — 规则页 / 密码簿（Fernet 加密 + 版本号 + 回滚）
- [x] Phase 7 — Agent 工具集 + Copilot（password / unknown_channel / mapper / copilot）
- [x] Phase 8 — 对比核对（align/match/diff/summary/render + xlsx）
- [x] Phase 9 — 观测运维（KPI / 4 张图 / 实时事件流）
- [x] Phase 10 — 前端状态保持（URL ↔ Recoil） + 4 色主题打磨
- [x] Phase 11 — 端到端联调（见 `scripts/e2e_smoke.py`）

## E2E 验收

```bash
cd pipeline-svc
uv sync
uv run python scripts/e2e_smoke.py
```

预期输出包含每一阶段的 `[e2e]` 行，并以 `all checks passed ✅` 结尾。

## 路由总览（前缀 `/api/pipeline`）

| 区块  | 路径                                                                | 用途                              |
| ----- | ------------------------------------------------------------------- | --------------------------------- |
| 任务  | `GET/POST /tasks`、`GET /tasks/{tid}`、`GET /tasks/{tid}/timeline`  | 任务列表/详情/事件                 |
| 上传  | `POST /tasks/{tid}/upload-zip-auto`、`POST /tasks/{tid}/upload`     | 单包自动归类 / 多渠道表单         |
| 渠道  | `GET /tasks/{tid}/channels/{ch}`、`POST .../run`、`.../runs/{rid}`  | 渠道详情、独立执行、运行历史       |
| 文件  | `GET .../runs/{rid}/files/{name}`、`POST .../files/replace`         | 产物下载、源文件替换               |
| 规则  | `GET/PUT /rules/{kind}`、`GET .../versions`、`POST .../rollback`    | 7 种规则配置 + 版本回滚           |
| Agent | `POST /agent/ask`、`GET /agent/drafts/{tid}`                        | Copilot 问答 + 待审核草稿          |
| 对比  | `POST /compare`、`GET /compare/{cid}`、`GET /compare/{cid}/report`  | 创建对比 / 报告 / 下载 xlsx        |
| 观测  | `GET /observe/{kpi\|charts\|events}`                                | KPI / 图表 / 事件流                |
| SSE   | `GET /tasks/{tid}/stream`、`.../logs/stream`、`.../channels/.../`   | 实时任务/日志/渠道事件             |
