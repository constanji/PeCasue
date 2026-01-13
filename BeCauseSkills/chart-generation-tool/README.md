# Chart Generation Tool - 图表生成工具

## 实现完成情况

✅ **已完成的功能**：
- 数据处理模块（类型识别、数据清洗、聚合）
- 图表类型自动选择（基于数据特征的启发式规则）
- Plotly 图表配置生成（支持7种图表类型）
- 工具注册和集成
- 错误处理和日志记录

## 文件结构

```
chart-generation-tool/
├── scripts/
│   └── ChartGenerationTool.js      # 主工具类
├── utils/
│   ├── dataAnalyzer.js              # 数据分析工具
│   ├── chartTypeSelector.js         # 图表类型选择器
│   └── plotlyGenerator.js           # Plotly图表生成器
├── chart-generation-tool.md          # 工具说明文档
└── README.md                        # 本文件
```

## 依赖项

已在 `api/package.json` 中添加：
- `plotly.js-dist-min`: "^2.32.0" - Plotly 图表库
- `lodash`: "^4.17.21" - 数据处理工具（已存在）

## 使用方法

### 通过 BeCauseSkills 统一入口调用

```javascript
{
  command: 'chart-generation',
  arguments: JSON.stringify({
    data: [
      { category: 'A', value: 100 },
      { category: 'B', value: 200 },
      { category: 'C', value: 150 }
    ],
    title: '销售数据统计'
  })
}
```

### 直接使用工具类

```javascript
const ChartGenerationTool = require('./BeCauseSkills/chart-generation-tool/scripts/ChartGenerationTool');

const tool = new ChartGenerationTool();
const result = await tool._call({
  data: [...],
  title: '图表标题'
});
```

## 支持的图表类型

1. **table** - 表格（4+ 列）
2. **histogram** - 直方图（单个数值列）
3. **bar** - 柱状图（1分类 + 1数值）
4. **scatter** - 散点图（2个数值列）
5. **heatmap** - 相关性热力图（3+ 个数值列）
6. **time_series** - 时间序列图（包含时间维度）
7. **grouped_bar** - 分组柱状图（多个分类列）
8. **generic** - 通用图表（自动选择）

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
      "data_shape": { "rows": 10, "columns": 2 },
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

## 技术实现

- **数据处理**：lodash + 自定义实现
- **图表生成**：直接构建 Plotly JSON 配置（不依赖浏览器环境）
- **类型识别**：基于数据样本的启发式规则
- **图表类型选择**：参考 Vanna Python 实现的启发式规则

## 注意事项

1. **Plotly.js 使用**：在 Node.js 环境中，我们直接构建 Plotly JSON 配置对象，不需要实际渲染图表。前端可以使用 plotly.js 渲染这些配置。

2. **数据限制**：默认最大行数为 1000，超过会自动采样。可以通过 `max_rows` 参数调整。

3. **图表类型**：如果不指定 `chart_type`，工具会自动根据数据特征选择最合适的图表类型。

## 后续优化方向

- [ ] 支持更多图表类型（饼图、雷达图等）
- [ ] 支持自定义图表样式
- [ ] 支持图表交互配置
- [ ] 性能优化（大数据量处理）
- [ ] 图表预览功能


