---
name: chart-generation-tool
description: 将SQL查询结果自动转换为可视化图表，支持多种图表类型和自动图表类型选择
category: visualization
version: 1.0
---

# Chart Generation Tool

## 概述

图表生成工具用于将 SQL 查询结果自动转换为可视化图表。工具会根据数据特征自动选择最合适的图表类型，并生成 Plotly 格式的图表配置。

**核心特性**：
- 自动图表类型选择（基于数据特征的启发式规则）
- 支持多种图表类型（柱状图、散点图、直方图、热力图、时间序列图、表格等）
- 智能数据处理（类型识别、数据清洗、聚合）
- Plotly 图表配置生成
- Vanna 品牌主题样式支持

## 核心能力

1. **自动图表类型选择**：根据数据特征（列数、数据类型、时间序列等）自动选择最合适的图表类型
2. **数据类型识别**：自动识别数值列、分类列、时间列
3. **数据预处理**：数据清洗、空值处理、数据采样
4. **多种图表类型**：支持表格、直方图、柱状图、散点图、热力图、时间序列图、分组柱状图
5. **图表配置生成**：生成标准的 Plotly JSON 配置，可直接用于前端渲染

## 输入参数

- `data` (array, 必需): SQL查询结果数组，**行式格式** `[{col1: val1, col2: val2}, ...]`，每行一个对象
- `title` (string, 可选): 图表标题，默认为 "Chart"
- `chart_type` (enum, 可选): 指定图表类型，如果不提供则自动选择
  - 可选值：`table`, `histogram`, `bar`, `scatter`, `heatmap`, `time_series`, `grouped_bar`, `generic`, `auto`（`auto` 或不传则自动选择）
- `x_axis` (string, 可选): X轴字段名，**建议在列名含中文或数值易混淆时显式指定**
- `y_axis` (string, 可选): Y轴字段名，**建议在列名含中文或数值易混淆时显式指定**
- `max_rows` (number, 可选): 最大行数限制，默认 1000，超过会自动采样

## 输出格式

```json
{
  "success": true,
  "chart": {
    "type": "plotly",
    "data": {
      "data": [...],
      "layout": {...}
    },
    "title": "图表标题",
    "config": {
      "data_shape": {
        "rows": 10,
        "columns": 2
      },
      "chart_type": "bar",
      "columns": ["category", "value"]
    }
  },
  "metadata": {
    "row_count": 10,
    "column_count": 2,
    "chart_type": "bar",
    "columns": ["category", "value"],
    "column_types": {
      "numeric": ["value"],
      "categorical": ["category"],
      "datetime": []
    }
  }
}
```

## 支持的图表类型

### 1. 表格 (table)
- **触发条件**：4+ 列
- **用途**：展示多列数据

### 2. 直方图 (histogram)
- **触发条件**：单个数值列
- **用途**：展示数值分布

### 3. 柱状图 (bar)
- **触发条件**：1 个分类列 + 1 个数值列
- **用途**：分类数据对比

### 4. 散点图 (scatter)
- **触发条件**：2 个数值列
- **用途**：两个数值变量的关系

### 5. 相关性热力图 (heatmap)
- **触发条件**：3+ 个数值列
- **用途**：展示多个数值变量之间的相关性

### 6. 时间序列图 (time_series)
- **触发条件**：包含时间列 + 数值列
- **用途**：展示时间趋势

### 7. 分组柱状图 (grouped_bar)
- **触发条件**：多个分类列
- **用途**：多维度分类数据对比

### 8. 通用图表 (generic)
- **触发条件**：其他情况
- **用途**：自动选择最合适的展示方式

## 执行流程

1. **数据预处理**
   - 数据清洗（处理 null、undefined）
   - 数据采样（如果超过 max_rows 限制）

2. **类型识别**
   - 识别数值列、分类列、时间列
   - 基于数据样本的启发式规则

3. **图表类型选择**
   - 如果指定了 `chart_type`，使用指定的类型
   - 否则根据数据特征自动选择

4. **图表配置生成**
   - 根据图表类型调用对应的生成函数
   - 应用 Vanna 品牌主题样式
   - 生成 Plotly JSON 配置

5. **返回结果**
   - 返回包含图表配置和元数据的 JSON

## 使用场景

### 1. SQL执行后自动生成图表

```javascript
// SQL执行后，自动生成可视化图表
const sqlResult = await sql_executor({
  sql: 'SELECT category, SUM(value) as total FROM sales GROUP BY category'
});

const chartResult = await chart_generation({
  data: sqlResult.rows,
  title: '销售数据统计'
});
```

### 2. 指定图表类型和坐标轴

```javascript
// 柱状图：含中文列名时建议显式指定 x_axis、y_axis
const chartResult = await chart_generation({
  data: [
    {"年龄区间": "20岁以下", "女性用户数量": 238, "占比": "9.00%"},
    {"年龄区间": "20-29岁", "女性用户数量": 511, "占比": "19.32%"}
  ],
  title: '各年龄段女性用户分布',
  chart_type: 'bar',
  x_axis: '年龄区间',
  y_axis: '女性用户数量'
});

// 散点图
const chartResult = await chart_generation({
  data: [...],
  title: '相关性分析',
  chart_type: 'scatter',
  x_axis: 'price',
  y_axis: 'sales'
});
```

### 3. 与结果分析工具结合

```javascript
// 先分析结果，再生成图表
const analysis = await result_analysis({
  sql: '...',
  results: [...]
});

const chart = await chart_generation({
  data: [...],
  title: analysis.summary
});
```

## 图表类型选择规则

工具使用以下启发式规则自动选择图表类型：

1. **4+ 列** → 表格
2. **时间列 + 数值列** → 时间序列图
3. **1个数值列** → 直方图
4. **1个分类列 + 1个数值列** → 柱状图
5. **2个数值列** → 散点图
6. **3+ 个数值列** → 相关性热力图
7. **多个分类列** → 分组柱状图
8. **其他情况** → 通用图表

## 技术实现

- **数据处理**：lodash + 自定义实现（类型识别、聚合、统计计算）
- **图表生成**：直接构建 Plotly JSON 配置（不依赖浏览器环境）
- **类型识别**：基于数据样本的启发式规则（采样前100行）
- **相关性计算**：皮尔逊相关系数（自定义实现）

## 数据类型识别规则

列类型基于数据样本自动识别（采样前 100 行）：

| 类型 | 识别条件 | 示例 |
|------|----------|------|
| **数值列** | `typeof === 'number'` 或字符串为**纯数字**（如 `"123"`, `"1,234.56"`） | `238`, `"123.45"` |
| **分类列** | 字符串含非数字字符（如 `"9.00%"`, `"20-29岁"`, `"20岁以下"`） | `"20岁以下"`, `"9.00%"` |
| **时间列** | 匹配日期格式（YYYY-MM-DD、ISO 等） | `"2024-01-01"` |

**注意**：`"9.00%"`、`"20-29岁"` 等会被识别为分类列。若自动识别不符合预期，请使用 `x_axis`、`y_axis` 显式指定轴字段。

## 注意事项

- **数据格式**：`data` 必须是**行数组**，每行 `{列名: 值}`，不要使用列对象 `{列名: [值数组]}` 格式
- **数据限制**：默认最大行数为 1000，超过会自动采样。可以通过 `max_rows` 参数调整
- **图表类型**：如果不指定 `chart_type`，工具会自动根据数据特征选择最合适的图表类型
- **显式指定轴**：当列名含中文、百分比等易混淆内容时，建议传入 `x_axis`、`y_axis` 避免类型误判
- **Plotly 配置**：返回的是 Plotly JSON 配置对象，前端需要使用 plotly.js 渲染
- **时间列识别**：支持常见的日期时间格式（YYYY-MM-DD, ISO格式等）

## 错误处理

如果图表生成失败，返回：

```json
{
  "success": false,
  "error": "错误信息",
  "metadata": {
    "row_count": 0,
    "column_count": 0,
    "chart_type": null
  }
}
```

常见错误：
- `数据为空，无法生成图表`: 输入数据为空
- `data参数必须是数组格式`: 使用了列对象格式，应改为行数组 `[{col: val}, ...]`
- `无效的图表类型`: 指定的图表类型不在支持列表中
- `柱状图需要至少一个分类列`: 分类列识别失败，可传入 `x_axis`、`y_axis` 显式指定
- `直方图需要至少一个数值列`: 数据不符合图表类型要求
- `散点图需要至少两个数值列`: 数据不符合图表类型要求

## 与前端集成

前端可以使用 plotly.js 渲染返回的图表配置：

```javascript
// 前端代码示例
const chartResult = JSON.parse(toolResult);
if (chartResult.success) {
  Plotly.newPlot('chart-container', chartResult.chart.data.data, chartResult.chart.data.layout);
}
```
