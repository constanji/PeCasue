# 结果分析工具 (Result Analysis Tool) - 增强版

## 概述

SQL查询结果深度分析工具，全面的数据分析引擎。

## 核心能力

### 1. 统计分析
- 基础统计量：总和、均值、中位数、标准差、最大/最小值
- 变异系数：衡量数据离散程度
- 分布类型识别：very_uniform / uniform / moderate / dispersed

### 2. 异常值检测
- 基于IQR（四分位距）方法
- 自动标记上界/下界异常
- 输出异常值位置和比例

### 3. 时间趋势检测
- 自动识别时间字段
- 线性回归趋势分析
- 趋势方向：increasing / decreasing / no_trend
- 趋势强度：strong / moderate / weak

### 4. 维度归因（Adtributor）
- 当提供 comparison_results（基期数据）时激活
- 解释力（Explanatory Power）
- 惊喜度（Surprise, JS散度）
- 简洁性（Parsimony）
- 综合 Adtributor 评分

### 5. 指标关联分析
- Pearson 相关系数矩阵
- 关键相关性发现（|r| > 0.7）

### 6. 智能后续建议
- 基于异常检测的调查建议
- 基于趋势的深入分析建议
- 基于相关性的因果分析建议
- 基于维度归因的下钻建议

## 分析深度

| 级别 | 包含内容 |
|------|----------|
| basic | SQL结构归因 + 字段分类 |
| standard | + 统计分析 + 异常检测 + 时间趋势 |
| deep | + 指标关联分析 + 维度归因 |

## 输入参数

```json
{
  "sql": "SELECT region, SUM(revenue) AS total_revenue FROM sales GROUP BY region",
  "results": [...],
  "analysis_depth": "deep",
  "comparison_results": [...]  // 可选：基期数据，用于波动归因
}
```

## 输出结构

```json
{
  "summary": "查询返回了 10 行数据...",
  "attribution": { "tables": [...], "columns": [...], "data_source": "..." },
  "statistics": {
    "total_revenue": {
      "count": 10, "sum": 50000, "mean": 5000, "std": 1200,
      "coefficientOfVariation": 0.24, "distribution": "uniform"
    }
  },
  "anomalies": [
    { "field": "total_revenue", "outlierCount": 1, "outlierRate": 0.1, ... }
  ],
  "time_trends": {
    "total_revenue": { "direction": "increasing", "slope": 0.05, "rSquared": 0.82, "strength": "strong" }
  },
  "correlation": { "correlationMatrix": {...}, "keyFindings": [...] },
  "dimension_attribution": {
    "dimensionRanking": [...],
    "drillPaths": [...],
    "summary": "..."
  },
  "key_insights": [...],
  "follow_up_suggestions": [...],
  "metadata": { "analysis_confidence": 0.85, "analysis_depth": "deep" }
}
```
