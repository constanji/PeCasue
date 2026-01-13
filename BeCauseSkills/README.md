# BeCauseSkills - 智能问数工具集合

## 概述

BeCauseSkills 是重构后的智能问数工具系统，将原来的大而全的 `because` 工具拆分为多个独立、职责清晰的工具。

## 设计理念

根据 `doc.md` 中的设计思路，我们将完整的 Text-to-SQL 流程拆分为：

1. **意图识别** → `intent-classification-tool`
2. **RAG检索** → `rag-retrieval-tool`
3. **重排序** → `reranker-tool`
4. **SQL校验** → `sql-validation-tool`
5. **结果分析** → `result-analysis-tool`

**注意**：
- `sql-executor-tool` 已存在，无需重构
- `sql-generation-tool` 可选，SQL生成可以直接由 Agent 调用 LLM 完成

## 工具列表

### 1. intent-classification-tool

**职责**：分类用户查询意图（TEXT_TO_SQL / GENERAL / MISLEADING_QUERY）

**特点**：
- 集成RAG检索提高分类准确性
- 轻量级设计，只返回分类结果
- 支持上下文感知

**使用场景**：
- 用户查询的第一步，判断是否需要生成SQL

### 2. rag-retrieval-tool

**职责**：从知识库检索多源知识（语义模型、QA对、同义词、业务知识）

**特点**：
- 一次调用检索多种类型的知识
- 支持向量检索和混合检索
- 返回结构化的检索结果和相似度分数

**使用场景**：
- SQL生成前检索相关知识
- 意图分类时检索语义模型
- 业务理解时检索业务知识

### 3. reranker-tool

**职责**：对检索结果进行重排序优化

**特点**：
- 使用reranker模型提高相关性
- 支持增强重排序（结合多因素）
- 灵活配置权重和topK

**使用场景**：
- RAG检索后优化结果顺序
- 合并多源检索结果后统一重排序

### 4. sql-validation-tool

**职责**：SQL合法性/安全性检测

**特点**：
- 禁止危险操作（DROP、DELETE等）
- 基本语法检查
- 可选的Schema验证
- 风险评估

**使用场景**：
- SQL生成后执行前校验
- 用户输入SQL验证
- 安全审计

### 5. sql-executor-tool（重构版 v2.0）

**职责**：执行SQL查询并返回结果和归因分析

**特点**：
- ✅ **动态数据源切换**：根据Agent配置的 `data_source_id` 动态连接数据库
- ✅ **多数据库支持**：支持MySQL和PostgreSQL
- ✅ **不再依赖sql-api**：直接连接数据库，不再通过独立的sql-api服务
- ✅ **连接池管理**：使用连接池管理数据库连接，提高性能
- ✅ **安全性检查**：禁止危险操作，只允许SELECT查询
- ✅ **归因分析**：提供详细的数据来源说明

**使用场景**：
- SQL生成后执行查询
- 数据分析查询
- 报表生成

**重要更新**：
- 不再依赖独立的 `sql-api` 服务
- 支持通过Agent配置动态切换数据源
- 支持前端数据源管理功能

### 6. result-analysis-tool

**职责**：分析SQL查询结果，提供归因分析和后续建议

**特点**：
- 结果解释和业务洞察
- 关键维度识别
- 清晰的归因说明
- 智能的后续建议

**使用场景**：
- SQL执行后分析结果
- 生成数据报告
- 帮助用户理解查询结果

### 7. chart-generation-tool

**职责**：将SQL查询结果自动转换为可视化图表

**特点**：
- ✅ 自动图表类型选择（基于数据特征）
- ✅ 支持多种图表类型（柱状图、散点图、直方图、热力图、时间序列图、表格等）
- ✅ 智能数据处理（类型识别、数据清洗、聚合）
- ✅ Plotly 图表配置生成
- ✅ 主题样式支持

**支持的图表类型**：
1. **表格**：4+ 列时自动使用表格展示
2. **直方图**：单个数值列
3. **柱状图**：1 个分类列 + 1 个数值列
4. **散点图**：2 个数值列
5. **相关性热力图**：3+ 个数值列
6. **时间序列图**：包含时间维度
7. **分组柱状图**：多个分类列

**使用场景**：
- SQL执行后自动生成可视化图表
- 数据分析和报表生成
- 提升用户体验和数据理解

## 工具结构

每个工具都遵循 skill 结构：

```
tool-name/
├── tool-name.md          # 核心文件，包含指令和元数据（frontmatter + body）
├── scripts/              # 可执行代码
│   └── ToolName.js       # JavaScript 工具类实现
├── references/           # 文档参考（可选）
└── assets/              # 资源文件（可选）
```

## 使用方式

### 1. 注册工具

在 `api/app/clients/tools/util/handleTools.js` 中注册：

```javascript
const {
  IntentClassificationTool,
  RAGRetrievalTool,
  RerankerTool,
  SQLValidationTool,
  ResultAnalysisTool,
} = require('~/BeCauseSkills');

const toolConstructors = {
  // ... 其他工具
  intent_classification: IntentClassificationTool,
  rag_retrieval: RAGRetrievalTool,
  reranker: RerankerTool,
  sql_validation: SQLValidationTool,
  result_analysis: ResultAnalysisTool,
  // 保留原有工具
  sql_executor: SqlExecutor,
};
```

### 2. Agent 配置

在 Agent 配置中添加需要的工具，并配置数据源：

```json
{
  "name": "智能问数Agent",
  "tools": [
    "intent_classification",
    "rag_retrieval",
    "sql_validation",
    "sql_executor",
    "result_analysis"
  ],
  "data_source_id": "datasource-123"
}
```

**重要**：`sql_executor` 工具需要Agent配置 `data_source_id` 才能正常工作。

### 3. 典型工作流

```
用户查询
  ↓
intent_classification (判断意图)
  ↓
rag_retrieval (检索相关知识)
  ↓
reranker (重排序，可选)
  ↓
Agent 调用 LLM 生成 SQL
  ↓
sql_validation (校验SQL)
  ↓
sql_executor (执行SQL)
  ↓
result_analysis (分析结果)
  ↓
chart_generation (生成图表，可选)
```

## 优势对比

### 原有 because 工具的问题

1. **Token占用大**：所有命令模板一次性传给LLM
2. **RAG集成不直接**：需要在工具内部调用RAG，不够灵活
3. **职责不清**：一个工具包含多个命令，职责混乱
4. **硬编码注册**：添加新命令需要修改代码

### 新工具系统的优势

1. **Token占用少**：每个工具只包含必要的schema和description
2. **RAG深度集成**：RAG检索作为独立工具，可以灵活使用
3. **职责清晰**：每个工具只做一件事
4. **易于扩展**：添加新工具只需创建新的skill文件夹

## 迁移指南

### 从 because 工具迁移

1. **意图分类**：
   - 原来：`because({ command: 'intent-classification', arguments: query })`
   - 现在：`intent_classification({ query })`

2. **RAG检索**：
   - 原来：在 because 工具内部调用RAG
   - 现在：直接调用 `rag_retrieval({ query, types, top_k })`

3. **SQL生成**：
   - 原来：`because({ command: 'sql-generation', arguments: query })`
   - 现在：Agent直接调用LLM生成SQL（或创建独立的sql-generation-tool）

4. **SQL校验**：
   - 原来：在sql_executor中简单校验
   - 现在：独立的 `sql_validation` 工具，功能更强大

5. **结果分析**：
   - 原来：在sql_executor中简单归因
   - 现在：独立的 `result_analysis` 工具，分析更深入

## 后续工作

1. **创建 sql-generation-tool**（可选）
   - 如果希望SQL生成也作为独立工具
   - 或者继续由Agent直接调用LLM

2. **完善工具实现**
   - 改进意图分类的LLM调用
   - 增强结果分析的智能程度
   - 优化RAG检索的性能

3. **工具注册机制优化**
   - 支持自动扫描BeCauseSkills目录
   - 支持热加载新工具

4. **文档完善**
   - 添加更多使用示例
   - 添加最佳实践指南

## 参考文档

- `doc.md`: 工具设计思路和架构说明
- `.because/`: 原有的命令模板和提示词模板
- `api/server/services/RAG/`: RAG服务实现

