const { Tool } = require('@langchain/core/tools');
const { z } = require('zod');
const { logger } = require('@because/data-schemas');
const path = require('path');

// 导入 Because-2.0 重构后的工具集
const projectRoot = path.resolve(__dirname, '../../../../..');
const BeCauseSkills2 = require(path.join(projectRoot, 'Because-2.0'));

/**
 * BeCause问数工具 2.0 - 统一的智能问数工具入口（增强版）
 * 
 * 相比 1.0 版本新增：
 * - fluctuation-attribution: 波动归因（Adtributor算法，维度归因+指标归因+时间对比+下钻）
 * - sql-validation 增强: 7类关键字分类 + 双盲SQL对比 + 文本-知识-SQL对齐检查
 * - result-analysis 增强: Adtributor归因 + 异常检测 + 趋势分析 + 指标关联
 * 
 * 新增核心算法：
 * - JS散度 / KL散度
 * - 解释力（Explanatory Power）、惊喜度（Surprise）、简洁性（Parsimony）
 * - ElasticNet回归 + 特征重要性（SHAP近似）
 * - 多维度下钻（最多10条路径）
 * - 同比/环比/自定义时间对比
 */
class BeCauseSkillsTool2 extends Tool {
  name = 'because_skills_2';

  description =
    'BeCause问数工具2.0 - 智能问数（自然语言转SQL）的完整能力集，新增波动归因能力。' +
    'Commands: intent-classification (意图分类), rag-retrieval (RAG知识检索), ' +
    'database-schema (数据库Schema获取), reranker (结果重排序), ' +
    'sql-validation (SQL校验，支持7类关键字分类+双盲对比), ' +
    'result-analysis (结果分析，支持Adtributor归因+异常检测+趋势分析), ' +
    'sql-executor (SQL执行), chart-generation (图表生成), ' +
    'fluctuation-attribution (波动归因，维度归因+指标归因+时间对比+下钻)。';

  schema = z.object({
    command: z.enum([
      'intent-classification',
      'rag-retrieval',
      'database-schema',
      'reranker',
      'sql-validation',
      'result-analysis',
      'sql-executor',
      'chart-generation',
      'fluctuation-attribution',
    ]),
    arguments: z
      .string()
      .optional()
      .describe('命令参数，JSON字符串格式，包含各命令所需的参数'),
  });

  constructor(fields = {}) {
    super();
    this.userId = fields.userId || 'system';
    this.req = fields.req;
    this.projectRoot = fields.projectRoot || process.cwd();
    this.conversation = fields.conversation;

    // 基准测试模式：仅暴露允许的子命令
    const allowed = fields.req?.body?._benchmarkAllowedCommands;
    if (Array.isArray(allowed) && allowed.length > 0) {
      this.schema = z.object({
        command: z.enum(allowed),
        arguments: z.string().optional().describe('命令参数，JSON字符串格式，包含各命令所需的参数'),
      });
    }

    // 初始化各个子工具实例
    this.tools = {
      'intent-classification': new BeCauseSkills2.IntentClassificationTool({
        userId: this.userId,
        req: this.req,
        conversation: this.conversation,
      }),
      'rag-retrieval': new BeCauseSkills2.RAGRetrievalTool({
        userId: this.userId,
        req: this.req,
        conversation: this.conversation,
      }),
      'database-schema': new BeCauseSkills2.DatabaseSchemaTool({
        userId: this.userId,
        req: this.req,
        conversation: this.conversation,
      }),
      'reranker': new BeCauseSkills2.RerankerTool({
        userId: this.userId,
        req: this.req,
      }),
      'sql-validation': new BeCauseSkills2.SQLValidationTool({
        userId: this.userId,
        req: this.req,
      }),
      'result-analysis': new BeCauseSkills2.ResultAnalysisTool({
        userId: this.userId,
        req: this.req,
      }),
      'sql-executor': new BeCauseSkills2.SqlExecutorTool({
        userId: this.userId,
        req: this.req,
        conversation: this.conversation,
      }),
      'chart-generation': new BeCauseSkills2.ChartGenerationTool({
        userId: this.userId,
        req: this.req,
      }),
      'fluctuation-attribution': new BeCauseSkills2.FluctuationAttributionTool({
        userId: this.userId,
        req: this.req,
      }),
    };
  }

  parseArguments(argsString) {
    logger.info('[BeCauseSkillsTool2] parseArguments called with:', {
      argsStringLength: argsString?.length || 0,
      argsStringPreview: argsString?.substring(0, 200) + (argsString?.length > 200 ? '...' : ''),
    });

    if (!argsString || !argsString.trim()) {
      logger.warn('[BeCauseSkillsTool2] argsString is empty or null');
      return {};
    }
    try {
      const parsed = JSON.parse(argsString);
      logger.info('[BeCauseSkillsTool2] Successfully parsed arguments:', {
        hasData: !!parsed.data,
        dataType: typeof parsed.data,
        dataLength: Array.isArray(parsed.data) ? parsed.data.length : 'N/A',
        keys: Object.keys(parsed),
      });
      return parsed;
    } catch (error) {
      const preview = typeof argsString === 'string' ? argsString.substring(0, 500) : String(argsString);
      logger.error('[BeCauseSkillsTool2] Failed to parse arguments:', {
        error: error.message,
        argsStringPreview: preview,
      });
      return {};
    }
  }

  async _call(input) {
    const startTime = Date.now();
    try {
      const { command, arguments: argsString } = input || {};

      if (!command) {
        logger.warn('[BeCauseSkillsTool2] command 参数缺失');
        return JSON.stringify({ success: false, error: 'command 参数缺失' }, null, 2);
      }

      logger.info('[BeCauseSkillsTool2] ========== 开始调用 ==========');
      logger.info(`[BeCauseSkillsTool2] Command: ${command}, UserId: ${this.userId}`);

      const tool = this.tools[command];
      if (!tool) {
        return JSON.stringify(
          { success: false, error: `未知命令: ${command}` },
          null, 2,
        );
      }

      const args = this.parseArguments(argsString);

      // chart-generation：自动先执行SQL获取数据
      if (command === 'chart-generation' && args.sql && !args.data) {
        logger.info('[BeCauseSkillsTool2] chart-generation需要SQL查询，先执行sql-executor...');

        try {
          const sqlExecutor = this.tools['sql-executor'];
          if (!sqlExecutor) {
            throw new Error('sql-executor工具不可用');
          }

          const sqlResult = await sqlExecutor._call({ sql: args.sql });

          let sqlData;
          if (typeof sqlResult === 'string') {
            const parsed = JSON.parse(sqlResult);
            // SqlExecutorTool 返回的是 rows 字段
            sqlData = parsed.rows || parsed.data;
          } else if (sqlResult) {
            sqlData = sqlResult.rows || sqlResult.data;
          }

          if (!sqlData || !Array.isArray(sqlData)) {
            throw new Error('SQL执行结果无效');
          }

          args.data = sqlData;
          logger.info('[BeCauseSkillsTool2] SQL查询成功，获得数据:', {
            rowCount: sqlData.length,
            columns: sqlData.length > 0 ? Object.keys(sqlData[0]) : [],
          });
        } catch (sqlError) {
          logger.error('[BeCauseSkillsTool2] SQL查询失败:', sqlError);
          return JSON.stringify({
            success: false,
            error: `SQL查询失败: ${sqlError.message}`,
            original_sql: args.sql,
          }, null, 2);
        }
      }

      // fluctuation-attribution：自动执行SQL获取基期/现期数据
      if (command === 'fluctuation-attribution') {
        args._sqlExecutor = this.tools['sql-executor'];
      }

      const result = await tool._call(args);

      const duration = Date.now() - startTime;
      logger.info(`[BeCauseSkillsTool2] 执行完成，耗时: ${duration}ms`);
      logger.info('[BeCauseSkillsTool2] ========== 调用完成 ==========');

      return result;
    } catch (err) {
      const duration = Date.now() - startTime;
      logger.error(`[BeCauseSkillsTool2] 执行错误 (耗时: ${duration}ms):`, err);
      return JSON.stringify(
        { success: false, error: err.message || 'BeCause问数工具2.0执行失败' },
        null, 2,
      );
    }
  }
}

module.exports = BeCauseSkillsTool2;
