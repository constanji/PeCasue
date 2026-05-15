# allline / vendor `files/`（仅占位）

与 PeCause **真实业务数据**对齐方式：

| 用途 | 实际位置 |
|------|-----------|
| 任务上传的 QuickBI / CitiHK 源文件 | `data/tasks/{task_id}/extracted/allocation_base/`（与其它渠道一致） |
| QuickBI / CitiHK **模版与 mapping CSV** | `data/rules/files/allocation/`（规则页「分摊基数模版」与 mapping 文件） |
| 单次运行的 **生成物（xlsx/csv 等）** | `data/tasks/{task_id}/channels/allocation_base/runs/{run_id}/` |

本目录下的 `quickbi/`、`default_*`、`samples/` 仅保留 **默认路径占位 / CLI 回退**，不向仓库提交大型业务数据。可选地将「渠道映射」辅助簿 `成本分摊基数+输出模板(2).xlsx` 放于 `files/quickbi/`（不存在则仅用模版内 mapping，逻辑不变）。
