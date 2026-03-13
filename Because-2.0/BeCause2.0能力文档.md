# BeCause 问数 2.0 能力文档

> 更新日期：2025-03
> 版本：2.0

---

## 一、2.0 概览

BeCause 2.0 在 1.0 基础上新增**波动归因分析**能力，能够回答「为什么指标发生了变化」这类深层问题。核心升级包括：

| 能力域 | 1.0 | 2.0 |
|--------|-----|-----|
| 归因类型 | SQL 结构归因（表、字段、子句） | **维度归因 + 指标归因** |
| 归因算法 | 基于 SQL 解析的规则归因 | **Adtributor 算法**（解释力、简洁性、惊喜度） |
| 时间对比 | 不支持 | **基期/现期对比**，同比/环比/自定义周期 |
| 维度分析 | 简单字段类型识别 | **多维度下钻**，JS 散度计算惊喜度 |
| 指标归因 | 不支持 | **线性模型 / ElasticNet + 特征重要性** |
| 维度下钻 | 根据维度继续查询 | **层级关系下钻**（最多 10 条路径） |
| 统计信息 | 基础统计（sum/avg/max/min） | **解释力、简洁性、惊喜度量化** |

---

## 二、波动归因工具（核心能力）

### 2.1 工具定位

`fluctuation-attribution` 是 2.0 的核心新增工具，用于**指标异常波动的根因分析**。

**触发场景**：用户问「为什么涨了」「为什么跌了」「什么原因导致变化」「归因分析」「波动原因」「同比/环比变化」「异常原因」等。

**前置条件**：需要**基期数据**和**现期数据**（或带时间字段的完整数据集）。

---

### 2.2 三种分析模式

| 模式 | 说明 | 适用场景 |
|------|------|----------|
| `dimension` | 维度归因 | 哪个维度（地区/产品/渠道等）导致了变化 |
| `metric` | 指标归因 | 哪些子指标（客单价、订单数等）驱动了总指标变化 |
| `comprehensive` | 综合归因 | 同时进行维度和指标归因，默认推荐 |

---

### 2.3 维度归因（Adtributor 算法）

基于 Adtributor 论文的维度归因能力：

#### 量化指标

| 指标 | 英文 | 说明 | 范围 |
|------|------|------|------|
| 解释力 | Explanatory Power (EP) | 该维度能解释多少整体变化 | [0, 1] |
| 惊喜度 | Surprise | 维度分布在基期/现期的 JS 散度 | [0, 1] |
| 简洁性 | Parsimony | 用更少元素解释变化 | [0, 1] |
| Adtributor 评分 | - | 0.5×EP + 0.3×Surprise + 0.2×Parsimony | [0, 1] |

#### 算法实现

- **解释力**：`EP = 1 - (actual_change - predicted_change)² / actual_change²`
- **惊喜度**：基期与现期维度分布的 **Jensen-Shannon 散度**
- **简洁性**：`Parsimony = 1 - (解释元素数 / 总元素数)`
- **贡献分解**：将整体变化按维度值分解，计算每个维度值的贡献率

#### 维度下钻

- 沿维度层级逐层下钻，最多 **10 条归因路径**
- 每层计算 Adtributor 评分，筛选最显著的路径
- 支持多维度组合（如 地区 → 产品 → 渠道）

---

### 2.4 指标归因

用于将复合指标（如收入 = 客单价 × 订单数）分解为子指标贡献：

| 能力 | 说明 |
|------|------|
| 多元线性回归 | 最小二乘法，Y = β₀ + β₁X₁ + β₂X₂ + ... |
| ElasticNet 回归 | L1+L2 正则化，稀疏特征选择 |
| 特征重要性 | 置换重要性 + Shapley 值近似 |
| 指标相关性 | Pearson 相关系数矩阵 |

**输入**：`target_metric`（目标指标）、`component_metrics`（组成指标）

**输出**：各子指标的回归系数、R²、特征重要性排序

---

### 2.5 时间对比

| 类型 | 说明 |
|------|------|
| `year_over_year` | 同比 |
| `month_over_month` | 环比（月） |
| `week_over_week` | 环比（周） |
| `day_over_day` | 环比（日） |
| `custom` | 自定义时间区间（base_start/end, current_start/end） |

**自动能力**：时间字段检测、时间粒度识别（年/月/日）

---

### 2.6 输入参数

```json
{
  "analysis_type": "comprehensive",
  "base_data": [...],
  "current_data": [...],
  "metric_fields": ["revenue"],
  "dimension_fields": ["region", "product_category", "channel"],
  "time_field": "date",
  "time_comparison": {
    "type": "month_over_month",
    "base_start": "2026-02-01",
    "base_end": "2026-02-28",
    "current_start": "2026-03-01",
    "current_end": "2026-03-31"
  },
  "target_metric": "收入",
  "component_metrics": ["客单价", "订单数"],
  "weights": {
    "explanatoryPower": 0.5,
    "surprise": 0.3,
    "parsimony": 0.2
  },
  "max_drill_depth": 3
}
```

**数据提供方式**：
- 方式一：直接提供 `base_data` + `current_data`
- 方式二：提供 `full_data` + `time_field` + `time_comparison`，工具自动分割

---

### 2.7 输出结构

```json
{
  "analysis_type": "comprehensive",
  "time_comparison": {
    "overview": { "direction": "decrease", "changeRate": "-15.23%" },
    "metricComparisons": [...],
    "dimensionBreakdowns": [...]
  },
  "dimension_attribution": {
    "overview": { "baseTotal": 1000, "currentTotal": 850, "totalChange": -150 },
    "dimensionRanking": [
      {
        "dimension": "region",
        "explanatoryPower": 0.85,
        "surprise": 0.12,
        "parsimony": 0.80,
        "adtributorScore": 0.62,
        "topContributors": [...]
      }
    ],
    "drillPaths": [...],
    "summary": "..."
  },
  "metric_attribution": {
    "correlation": { "correlationMatrix": {...}, "keyFindings": [...] },
    "feature_importance": [...],
    "regression": { "method": "ElasticNet", "rSquared": 0.85, ... }
  },
  "conclusion": "...",
  "next_steps": [...]
}
```

---

### 2.8 典型使用场景

1. **销售额波动分析**：哪个地区/产品/渠道导致了收入下降？
2. **用户流失归因**：哪些用户特征与流失强相关？
3. **成本异常分析**：哪个部门/项目导致了成本飙升？
4. **KPI 变动追溯**：将复合 KPI 分解为子指标贡献

---

## 三、其他 2.0 增强工具

### 3.1 SQL 校验工具（sql-validation）

| 能力 | 说明 |
|------|------|
| 7 类关键字分类 | primary / join / clause / aggregate / scalar / comparison / arithmetic |
| 复杂度评估 | simple / moderate / complex / very_complex |
| 双盲 SQL 对比 | 结构相似度、关键字 Jaccard、结果匹配率 |
| 文本-知识-SQL 对齐 | question、evidence 与 SQL 的匹配度 |

### 3.2 结果分析工具（result-analysis）

| 能力 | 说明 |
|------|------|
| 统计分析 | 变异系数、分布类型识别 |
| 异常检测 | IQR 方法，自动标记异常值 |
| 时间趋势 | 线性回归趋势（方向、强度、R²） |
| 指标关联 | Pearson 相关矩阵 |
| 维度归因 | 提供 comparison_results 时，Adtributor 评分 |
| 智能建议 | 基于异常/趋势/相关性生成后续建议 |

**分析深度**：`basic` / `standard` / `deep`

---

## 四、工具选择指南

| 场景 | 推荐工具 |
|------|----------|
| 单次查询结果解释 | result-analysis (standard) |
| 发现异常值 | result-analysis (deep) |
| 时间趋势判断 | result-analysis (standard) |
| **为什么涨了/跌了** | **fluctuation-attribution** |
| **多维度下钻归因** | **fluctuation-attribution** |
| **指标分解（收入=单价×数量）** | **fluctuation-attribution** (metric) |
| **同比/环比对比** | **fluctuation-attribution** |

---

## 五、波动归因工作流

```
用户问：为什么本月收入下降了？

1. 生成现期 SQL → 执行
2. 生成基期 SQL → 执行
3. 调用 fluctuation-attribution（analysis_type: comprehensive）
4. 解读：维度排名 → 下钻路径 → 指标分解
5. 输出：归因结论 + 建议
```

---

## 六、技术实现模块

| 模块 | 路径 | 职责 |
|------|------|------|
| StatisticsEngine | `utils/statisticsEngine.js` | KL/JS 散度、EP、Parsimony、Surprise、贡献分解 |
| TimeComparison | `utils/timeComparison.js` | 时间分割、同比/环比、显著性 |
| DimensionDrillDown | `utils/dimensionDrillDown.js` | Adtributor 维度归因、下钻路径 |
| MetricAttribution | `utils/metricAttribution.js` | 线性回归、ElasticNet、特征重要性 |
| FluctuationAttributionTool | `fluctuation-attribution-tool/scripts/` | 工具入口， orchestration |

---

## 七、量化指标速查

| 指标 | 公式/说明 |
|------|-----------|
| JS 散度 | JSD(P‖Q) = 0.5·D_KL(P‖M) + 0.5·D_KL(Q‖M)，M=(P+Q)/2 |
| 解释力 | EP = 1 - (实际变化 - 预测变化)² / 实际变化² |
| 简洁性 | 1 - 解释元素数 / 总元素数 |
| Adtributor | 0.5×EP + 0.3×Surprise + 0.2×Parsimony |
