/**
 * BeCauseSkills - 智能问数工具集合
 * 
 * 重构后的智能问数工具系统，将原来的大而全的 because 工具拆分为多个独立工具：
 * 1. intent-classification-tool: 意图分类
 * 2. rag-retrieval-tool: RAG知识检索
 * 3. database-schema-tool: 数据库Schema获取
 * 4. reranker-tool: 结果重排序
 * 5. sql-validation-tool: SQL校验
 * 6. result-analysis-tool: 结果分析
 * 7. sql-executor-tool: SQL执行
 * 
 * 优势：
 * - 每个工具职责单一，token占用少
 * - 深度集成RAG服务，检索更高效
 * - 工具可独立使用，也可组合使用
 * - skill结构，易于维护和扩展
 */

const IntentClassificationTool = require('./intent-classification-tool/scripts/IntentClassificationTool');
const RAGRetrievalTool = require('./rag-retrieval-tool/scripts/RAGRetrievalTool');
const DatabaseSchemaTool = require('./database-schema-tool/scripts/DatabaseSchemaTool');
const RerankerTool = require('./reranker-tool/scripts/RerankerTool');
const SQLValidationTool = require('./sql-validation-tool/scripts/SQLValidationTool');
const ResultAnalysisTool = require('./result-analysis-tool/scripts/ResultAnalysisTool');
const SqlExecutorTool = require('./sql-executor-tool/scripts/SqlExecutorTool');
const ChartGenerationTool = require('./chart-generation-tool/scripts/ChartGenerationTool');

module.exports = {
  IntentClassificationTool,
  RAGRetrievalTool,
  DatabaseSchemaTool,
  RerankerTool,
  SQLValidationTool,
  ResultAnalysisTool,
  SqlExecutorTool,
  ChartGenerationTool,
};

