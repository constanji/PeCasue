# allocation_bundle（分摊基数 / allline 逻辑）

内置在 PeCause `pipeline-svc` 内，**不依赖**仓库外的 `2222`。

## 布局

- **`allline/`**：QuickBI / CitiHK / `ui_services`（默认常量路径指向 `allline/files/` 下占位目录；**真实产出落在任务的 `channels/.../runs/`**）。
- **`allline/files/`**：仅占位与可选辅助文件（说明见 `allline/files/README.md`）；**不包含**业务模版与全量 mapping（统一到 **`data/rules/files/allocation/`**）。
- **`cost_allocation/`**：CitiHK `citihk_core` 依赖的包。

## 与其它渠道对齐

- **输入**：`data/tasks/{tid}/extracted/allocation_base/`
- **输出**：`data/tasks/{tid}/channels/allocation_base/runs/{run_id}/`
- **规则侧模版**：`data/rules/files/allocation/quickbi|citihk/mapping`（由 `server.core.paths` 解析）

## 上游同步

```bash
rsync -a --delete --exclude '分摊基数/' \
  "<upstream>/分摊基数/" \
  "<repo>/pipeline-svc/vendor/allocation_bundle/allline/"
cp -R "<upstream>/分摊/cost_allocation/"* \
  "<repo>/pipeline-svc/vendor/allocation_bundle/cost_allocation/"
```

同步后请保留 **`CitiHKLine/citihk_core.py`** 顶部 `_COST_PARENT` / `cost_allocation` 的 PeCause 补丁；若覆盖了 **`QuickBILine/paths.py`**、**`ui_services/quickbi_service.py`**、**`citihk_core.py` 默认路径** 等与仓库不一致的改动，需按本仓库版本合并。

## 环境变量

- **`PECAUSE_ALLLINE_ALLOCATION_ROOT`**：覆盖 `allline` 根目录（默认 `vendor/allocation_bundle/allline`）。
