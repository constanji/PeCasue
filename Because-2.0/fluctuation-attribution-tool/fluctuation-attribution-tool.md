# 波动归因工具 (Fluctuation Attribution Tool)

## 概述

基于 Adtributor 算法的智能波动归因分析工具，当指标发生异常波动时，自动分析波动的根本原因。

## 核心能力

### 1. 维度归因 (Dimension Attribution)
- **Adtributor 算法**：综合解释力（EP）、惊喜度（Surprise）、简洁性（Parsimony）
- **JS 散度**：衡量维度在基期和现期的分布差异
- **贡献度分解**：将整体变化分解到每个维度值

### 2. 指标归因 (Metric Attribution)
- **多元线性回归**：分析各子指标对总指标的贡献
- **ElasticNet 正则化**：稀疏特征选择，找出关键驱动因素
- **特征重要性**：置换重要性 + Shapley 值近似
- **指标相关性**：Pearson 相关系数矩阵

### 3. 时间对比 (Time Comparison)
- 同比 (Year-over-Year)
- 环比 (Month-over-Month)
- 周比 (Week-over-Week)
- 日比 (Day-over-Day)
- 自定义时间区间

### 4. 维度下钻 (Dimension Drill-down)
- 逐层下钻（最多 10 条路径）
- 每层计算 Adtributor 评分
- 自动筛选最显著的归因路径

## 量化指标

| 指标 | 说明 | 范围 |
|------|------|------|
| 解释力 (EP) | 该维度能解释多少整体变化 | [0, 1] |
| 惊喜度 (Surprise) | 维度分布的 JS 散度 | [0, 1] |
| 简洁性 (Parsimony) | 用更少的元素解释变化 | [0, 1] |
| Adtributor 评分 | α·EP + β·Surprise + γ·Parsimony | [0, 1] |

## 输入参数

```json
{
  "analysis_type": "comprehensive",
  "base_data": [...],
  "current_data": [...],
  "metric_fields": ["revenue"],
  "dimension_fields": ["region", "product_category", "channel"],
  "weights": {
    "explanatoryPower": 0.5,
    "surprise": 0.3,
    "parsimony": 0.2
  }
}
```

## 输出结构

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

## 使用场景

1. **销售额波动分析**：哪个地区/产品/渠道导致了收入下降？
2. **用户流失归因**：哪些用户特征与流失强相关？
3. **成本异常分析**：哪个部门/项目导致了成本飙升？
4. **KPI 变动追溯**：将复合 KPI 分解为子指标贡献
