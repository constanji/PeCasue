const { Tool } = require('@langchain/core/tools');
const { z } = require('zod');
const { logger } = require('@because/data-schemas');
const path = require('path');

// 导入 BeCauseSkills 中的各个工具
// 使用相对路径从api/app/clients/tools/structured访问项目根目录的BeCauseSkills
// __dirname = api/app/clients/tools/structured
// 需要回到项目根目录: ../../../../BeCauseSkills
// 但实际路径是: 从 api/app/clients/tools/structured 到项目根目录需要 5 级向上
const projectRoot = path.resolve(__dirname, '../../../../..');
const BeCauseSkills = require(path.join(projectRoot, 'BeCauseSkills'));

/**
 * BeCause问数工具 - 统一的智能问数工具入口
 * 
 * 这是一个统一的工具入口，内部集成了 BeCauseSkills 中的所有工具能力：
 * - 意图分类 (intent-classification)
 * - RAG知识检索 (rag-retrieval)
 * - 数据库Schema获取 (database-schema)
 * - 结果重排序 (reranker)
 * - SQL校验 (sql-validation)
 * - 结果分析 (result-analysis)
 * - SQL执行 (sql-executor)
 * - 图表生成 (chart-generation)
 * 
 * 通过统一的 command 参数来调用不同的子工具能力。
 */
class BeCauseSkillsTool extends Tool {
  name = 'because_skills';

  description =
    'BeCause问数工具 - 智能问数（自然语言转SQL）的完整能力集。' +
    'Commands: intent-classification (意图分类), rag-retrieval (RAG知识检索), ' +
    'database-schema (数据库Schema获取), reranker (结果重排序), sql-validation (SQL校验), ' +
    'result-analysis (结果分析), sql-executor (SQL执行), chart-generation (图表生成)。' +
    '此工具集成了RAG服务、知识库检索、SQL生成与执行、数据可视化等完整问数流程。';

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

    // 基准测试模式：仅暴露允许的子命令，避免模型调用无关工具浪费时间
    const allowed = fields.req?.body?._benchmarkAllowedCommands;
    if (Array.isArray(allowed) && allowed.length > 0) {
      this.schema = z.object({
        command: z.enum(allowed),
        arguments: z.string().optional().describe('命令参数，JSON字符串格式，包含各命令所需的参数'),
      });
    }

    // 初始化各个子工具实例
    this.tools = {
      'intent-classification': new BeCauseSkills.IntentClassificationTool({
        userId: this.userId,
        req: this.req,
        conversation: this.conversation, // 传递conversation给IntentClassificationTool
      }),
      'rag-retrieval': new BeCauseSkills.RAGRetrievalTool({
        userId: this.userId,
        req: this.req,
        conversation: this.conversation, // 传递conversation给RAGRetrievalTool
      }),
      'database-schema': new BeCauseSkills.DatabaseSchemaTool({
        userId: this.userId,
        req: this.req,
        conversation: this.conversation, // 传递conversation给DatabaseSchemaTool
      }),
      'reranker': new BeCauseSkills.RerankerTool({
        userId: this.userId,
        req: this.req,
      }),
      'sql-validation': new BeCauseSkills.SQLValidationTool({
        userId: this.userId,
        req: this.req,
      }),
      'result-analysis': new BeCauseSkills.ResultAnalysisTool({
        userId: this.userId,
        req: this.req,
      }),
      'sql-executor': new BeCauseSkills.SqlExecutorTool({
        userId: this.userId,
        req: this.req,
        conversation: this.conversation, // 传递conversation给SqlExecutorTool
      }),
      'chart-generation': new BeCauseSkills.ChartGenerationTool({
        userId: this.userId,
        req: this.req,
      }),
    };
  }

  /**
   * 解析 arguments 参数
   */
  parseArguments(argsString) {
    logger.info('[BeCauseSkillsTool] parseArguments called with:', {
      argsStringLength: argsString?.length || 0,
      argsStringPreview: argsString?.substring(0, 200) + (argsString?.length > 200 ? '...' : ''),
    });

    if (!argsString || !argsString.trim()) {
      logger.warn('[BeCauseSkillsTool] argsString is empty or null');
      return {};
    }
    try {
      const parsed = JSON.parse(argsString);
      logger.info('[BeCauseSkillsTool] Successfully parsed arguments:', {
        hasData: !!parsed.data,
        dataType: typeof parsed.data,
        dataLength: Array.isArray(parsed.data) ? parsed.data.length : 'N/A',
        keys: Object.keys(parsed),
      });
      return parsed;
    } catch (error) {
      const preview = typeof argsString === 'string' ? argsString.substring(0, 500) : String(argsString);
      logger.error('[BeCauseSkillsTool] Failed to parse arguments:', {
        error: error.message,
        argsStringPreview: preview,
      });
      return {};
    }
  }

  /**
   * @override
   */
  async _call(input) {
    const startTime = Date.now();
    try {
      const { command, arguments: argsString } = input || {};

      if (!command) {
        logger.warn('[BeCauseSkillsTool] command 参数缺失');
        return JSON.stringify({ success: false, error: 'command 参数缺失' }, null, 2);
      }

      logger.info('[BeCauseSkillsTool] ========== 开始调用 ==========');
      logger.info(`[BeCauseSkillsTool] Command: ${command}, UserId: ${this.userId}`);

      const tool = this.tools[command];
      if (!tool) {
        return JSON.stringify(
          {
            success: false,
            error: `未知命令: ${command}`,
          },
          null,
          2,
        );
      }

      const args = this.parseArguments(argsString);

      // 特殊处理：如果调用chart-generation且包含sql但没有data，自动先执行SQL查询
      if (command === 'chart-generation' && args.sql && !args.data) {
        logger.info('[BeCauseSkillsTool] chart-generation需要SQL查询，先执行sql-executor...');

        try {
          // 先调用sql-executor执行查询
          const sqlExecutor = this.tools['sql-executor'];
          if (!sqlExecutor) {
            throw new Error('sql-executor工具不可用');
          }

          const sqlArgs = { sql: args.sql };
          const sqlResult = await sqlExecutor._call(sqlArgs);

          // 解析SQL执行结果
          let sqlData;
          if (typeof sqlResult === 'string') {
            const parsed = JSON.parse(sqlResult);
            sqlData = parsed.data;
          } else if (sqlResult && sqlResult.data) {
            sqlData = sqlResult.data;
          }

          if (!sqlData || !Array.isArray(sqlData)) {
            throw new Error('SQL执行结果无效');
          }

          // 将SQL查询结果添加到chart-generation的参数中
          args.data = sqlData;
          logger.info('[BeCauseSkillsTool] SQL查询成功，获得数据:', {
            rowCount: sqlData.length,
            columns: sqlData.length > 0 ? Object.keys(sqlData[0]) : []
          });

        } catch (sqlError) {
          logger.error('[BeCauseSkillsTool] SQL查询失败:', sqlError);
          // 如果SQL查询失败，返回错误信息而不是继续执行
          return JSON.stringify({
            success: false,
            error: `SQL查询失败: ${sqlError.message}`,
            original_sql: args.sql
          }, null, 2);
        }
      }

      // 调用对应的子工具
      const result = await tool._call(args);

      const duration = Date.now() - startTime;
      logger.info(`[BeCauseSkillsTool] 执行完成，耗时: ${duration}ms`);
      logger.info('[BeCauseSkillsTool] ========== 调用完成 ==========');

      return result;
    } catch (err) {
      const duration = Date.now() - startTime;
      logger.error(`[BeCauseSkillsTool] 执行错误 (耗时: ${duration}ms):`, err);
      return JSON.stringify(
        {
          success: false,
          error: err.message || 'BeCause问数工具执行失败',
        },
        null,
        2,
      );
    }
  }
}

module.exports = BeCauseSkillsTool;

