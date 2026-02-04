# 基准测试模块 (Benchmark)

本目录为 SQL 基准测试（BIRD 风格）所需的数据、评估脚本与知识库。

## 目录结构

- `data/` - 数据集与数据库
  - **必放**：`mini_dev_sqlite.json`、`mini_dev_mysql.json`、`mini_dev_postgresql.json`（与「数据库类型」选择一致时才会被加载）
  - **可选**：`dev_tables.json`
  - `dev_databases/` - 各库的 SQLite 文件，用于执行准确率（EX）评估，例如 `dev_databases/financial/financial.sqlite`
- 运行产生的预测与评估结果写入**项目根目录**下的 `benchmark_results/`（避免开发时 nodemon 因监听 `api/` 而重启），已加入 .gitignore
- `evaluation/` - Python 评估脚本（EX、R-VES、Soft F1），依赖见 `evaluation/requirements.txt` 与 `evaluation/INSTALL.md`
- `knowledge/` - 知识库：`schema/`、`knowledge/`、`qa_pairs/` 用于增强 prompt

## 所需文件与放置位置（来自 mini_dev-BIRD）

| 文件或目录 | 来源路径（mini_dev-BIRD） | 目标路径 |
|------------|---------------------------|----------|
| 数据集 JSON | `mini_dev_data/mini_dev_sqlite.json` | `api/benchmark/data/mini_dev_sqlite.json` |
| | `mini_dev_data/mini_dev_mysql.json` | `api/benchmark/data/mini_dev_mysql.json` |
| | `mini_dev_data/mini_dev_postgresql.json` | `api/benchmark/data/mini_dev_postgresql.json` |
| 表结构（可选） | `mini_dev_data/dev_tables.json` | `api/benchmark/data/dev_tables.json` |
| SQLite 库（EX 评估用） | `mini_dev_data/dev_databases/` 整目录 | `api/benchmark/data/dev_databases/` |

若本地有 `mini_dev-BIRD` 仓库，可一键复制：

```bash
cp /path/to/mini_dev-BIRD/mini_dev_data/mini_dev_*.json /path/to/BecauseChat/api/benchmark/data/
cp /path/to/mini_dev-BIRD/mini_dev_data/dev_tables.json /path/to/BecauseChat/api/benchmark/data/
cp -R /path/to/mini_dev-BIRD/mini_dev_data/dev_databases/* /path/to/BecauseChat/api/benchmark/data/dev_databases/
```

## 使用说明

1. 确保 `data/` 下有所选数据库类型对应的 `mini_dev_<sqlite|mysql|postgresql>.json`
2. 若需执行准确率（EX）评估，确保 `data/dev_databases/<db_id>/<db_id>.sqlite` 存在（如 `financial/financial.sqlite`）
3. 安装评估依赖：`cd evaluation && pip install -r requirements.txt`（见 `evaluation/INSTALL.md`）

前端入口：登录后管理员在「用户菜单 → 基准测试」中配置并运行测试。
