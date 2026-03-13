/**
 * BeCauseSkills 2.0 - 智能问数工具集合（重构版）
 * 
 * 重构后的智能问数工具系统，新增波动归因能力：
 * 1. intent-classification-tool: 意图分类
 * 2. rag-retrieval-tool: RAG知识检索
 * 3. database-schema-tool: 数据库Schema获取
 * 4. reranker-tool: 结果重排序
 * 5. sql-validation-tool: SQL校验（增强：7类关键字分类 + 双盲对比 + 对齐检查）
 * 6. result-analysis-tool: 结果分析（增强：Adtributor归因 + 异常检测 + 趋势分析）
 * 7. sql-executor-tool: SQL执行
 * 8. chart-generation-tool: 图表生成
 * 9. fluctuation-attribution-tool: 波动归因（新增：维度归因 + 指标归因 + 时间对比 + 下钻）
 * 
 * 新增核心算法模块：
 * - utils/statisticsEngine: 统计算法引擎（KL/JS散度、解释力、简洁性、惊喜度）
 * - utils/timeComparison: 时间对比模块（同比/环比/自定义）
 * - utils/dimensionDrillDown: 维度下钻模块（Adtributor算法）
 * - utils/metricAttribution: 指标归因模块（线性回归/ElasticNet/特征重要性）
 */

const IntentClassificationTool = require('./intent-classification-tool/scripts/IntentClassificationTool');
const RAGRetrievalTool = require('./rag-retrieval-tool/scripts/RAGRetrievalTool');
const DatabaseSchemaTool = require('./database-schema-tool/scripts/DatabaseSchemaTool');
const RerankerTool = require('./reranker-tool/scripts/RerankerTool');
const SQLValidationTool = require('./sql-validation-tool/scripts/SQLValidationTool');
const ResultAnalysisTool = require('./result-analysis-tool/scripts/ResultAnalysisTool');
const SqlExecutorTool = require('./sql-executor-tool/scripts/SqlExecutorTool');
const ChartGenerationTool = require('./chart-generation-tool/scripts/ChartGenerationTool');
const FluctuationAttributionTool = require('./fluctuation-attribution-tool/scripts/FluctuationAttributionTool');

const StatisticsEngine = require('./utils/statisticsEngine');
const TimeComparison = require('./utils/timeComparison');
const DimensionDrillDown = require('./utils/dimensionDrillDown');
const MetricAttribution = require('./utils/metricAttribution');

module.exports = {
  // 工具类
  IntentClassificationTool,
  RAGRetrievalTool,
  DatabaseSchemaTool,
  RerankerTool,
  SQLValidationTool,
  ResultAnalysisTool,
  SqlExecutorTool,
  ChartGenerationTool,
  FluctuationAttributionTool,

  // 算法模块（供外部直接使用）
  StatisticsEngine,
  TimeComparison,
  DimensionDrillDown,
  MetricAttribution,
};
