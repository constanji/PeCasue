const { Tool } = require('@langchain/core/tools');
const { z } = require('zod');
const { logger } = require('@because/data-schemas');
const path = require('path');

// 延迟加载RAGService，避免路径别名问题
let RAGService = null;
function loadRAGService() {
  if (!RAGService) {
    try {
      RAGService = require('~/server/services/RAG').RAGService;
    } catch (e) {
      RAGService = require(path.resolve(__dirname, '../../../api/server/services/RAG')).RAGService;
    }
  }
  return RAGService;
}

/**
 * Intent Classification Tool - 意图分类工具
 * 
 * 轻量级意图分类工具，使用RAG检索提高分类准确性
 * 只返回分类结果，不包含冗长的模板说明
 */
class IntentClassificationTool extends Tool {
  name = 'intent_classification';

  description =
    '分类用户查询意图：TEXT_TO_SQL（需要生成SQL）、GENERAL（数据库一般问题）、MISLEADING_QUERY（无关查询）。' +
    '自动使用RAG检索相关知识提高分类准确性。';

  schema = z.object({
    query: z
      .string()
      .min(1)
      .describe('用户查询文本'),
    use_rag: z
      .boolean()
      .optional()
      .default(true)
      .describe('是否使用RAG检索提高分类准确性，默认启用'),
    top_k: z
      .number()
      .int()
      .positive()
      .max(10)
      .optional()
      .default(5)
      .describe('RAG检索返回数量，默认5'),
    entity_id: z
      .string()
      .optional()
      .describe('实体ID过滤（数据源ID）'),
    use_schema_fallback: z
      .boolean()
      .optional()
      .default(true)
      .describe('当RAG检索不到语义模型时，是否使用database_schema作为后备方案，默认启用'),
  });

  constructor(fields = {}) {
    super();
    this.userId = fields.userId || 'system';
    this.req = fields.req;
    this.conversation = fields.conversation; // 保存conversation信息
    this.ragService = null; // 延迟初始化
  }

  /**
   * 获取RAGService实例（延迟加载）
   */
  getRAGService() {
    if (!this.ragService) {
      const RAGServiceClass = loadRAGService();
      this.ragService = new RAGServiceClass();
    }
    return this.ragService;
  }

  /**
   * 调用RAG服务检索相关知识（直接调用RAGService，不通过HTTP）
   */
  async retrieveRAGKnowledge(query, userId, topK = 5, entityId = null) {
    try {
      const ragService = this.getRAGService();
      
      const result = await ragService.query({
        query,
        userId,
        options: {
          types: ['semantic_model', 'qa_pair', 'business_knowledge'],
          topK,
          useReranking: true,
          entityId,
        },
      });

      return result;
    } catch (error) {
      logger.warn('[IntentClassificationTool] RAG检索失败:', error.message);
      return null;
    }
  }

  /**
   * 获取数据库Schema（作为RAG检索的后备方案）
   * 直接连接数据库获取Schema，不依赖sql-api服务
   */
  async getDatabaseSchema(dataSourceId = null) {
    try {
      // 如果没有提供dataSourceId，尝试获取
      if (!dataSourceId) {
        dataSourceId = await this.getEntityId({});
      }

      if (!dataSourceId) {
        logger.warn('[IntentClassificationTool] 未提供数据源ID，无法获取Schema');
        return null;
      }

      // 延迟加载DatabaseSchemaTool，避免循环依赖
      let DatabaseSchemaTool = null;
      try {
        DatabaseSchemaTool = require('../../database-schema-tool/scripts/DatabaseSchemaTool');
      } catch (e) {
        DatabaseSchemaTool = require(path.resolve(__dirname, '../../database-schema-tool/scripts/DatabaseSchemaTool'));
      }

      // 创建DatabaseSchemaTool实例并调用
      const schemaTool = new DatabaseSchemaTool({
        userId: this.userId,
        req: this.req,
        conversation: this.conversation,
      });

      const result = await schemaTool._call({
        format: 'semantic',
        data_source_id: dataSourceId,
      });

      const parsedResult = JSON.parse(result);
      if (parsedResult.success && parsedResult.semantic_models) {
        // 转换为checkQueryRelevance需要的格式
        return {
          database: parsedResult.database,
          schema: parsedResult.semantic_models.reduce((acc, model) => {
            acc[model.name] = {
              columns: model.columns.map(col => ({
                column_name: col.name,
                data_type: col.type,
                is_nullable: col.nullable ? 'YES' : 'NO',
                column_key: col.key,
                column_comment: col.comment,
                column_default: col.default,
              })),
              indexes: model.indexes || [],
            };
            return acc;
          }, {}),
        };
      }

      return null;
    } catch (error) {
      logger.warn('[IntentClassificationTool] 获取数据库Schema失败:', error.message);
      return null;
    }
  }

  /**
   * 检查查询是否与数据库表/列相关
   */
  checkQueryRelevance(query, schemaData) {
    if (!schemaData) return { relevant: false };

    const queryLower = query.toLowerCase();
    const tableNames = [];
    const columnNames = [];

    // 提取所有表名和列名
    if (schemaData.schema) {
      for (const [tableName, tableInfo] of Object.entries(schemaData.schema)) {
        tableNames.push(tableName.toLowerCase());
        if (tableInfo.columns) {
          columnNames.push(...tableInfo.columns.map(col => col.column_name.toLowerCase()));
        }
      }
    } else if (schemaData.table) {
      tableNames.push(schemaData.table.toLowerCase());
      if (schemaData.columns) {
        columnNames.push(...schemaData.columns.map(col => col.column_name.toLowerCase()));
      }
    }

    // 检查查询中是否包含表名或列名
    const matchedTables = tableNames.filter(tableName => 
      queryLower.includes(tableName)
    );
    const matchedColumns = columnNames.filter(columnName => 
      queryLower.includes(columnName)
    );

    return {
      relevant: matchedTables.length > 0 || matchedColumns.length > 0,
      matched_tables: matchedTables,
      matched_columns: matchedColumns.slice(0, 5), // 只返回前5个匹配的列
    };
  }

  /**
   * 基于RAG结果和查询文本分析意图
   */
  analyzeIntent(query, ragResults, schemaRelevance = null) {
    const ragContext = {
      semantic_models_found: false,
      qa_pairs_found: false,
      business_knowledge_found: false,
      schema_relevance: schemaRelevance,
    };

    if (ragResults && ragResults.results) {
      const results = ragResults.results;
      ragContext.semantic_models_found = results.some(r => r.type === 'semantic_model');
      ragContext.qa_pairs_found = results.some(r => r.type === 'qa_pair');
      ragContext.business_knowledge_found = results.some(r => r.type === 'business_knowledge');
    }

    // 简单的意图判断逻辑（实际应该调用LLM）
    let intent = 'MISLEADING_QUERY';
    let confidence = 0.3;
    let reasoning = '';

    // 如果找到语义模型，更可能是TEXT_TO_SQL
    if (ragContext.semantic_models_found) {
      intent = 'TEXT_TO_SQL';
      confidence = 0.8;
      reasoning = '检索到相关数据库语义模型';
    } 
    // 如果RAG检索失败但通过database_schema发现查询与数据库相关
    else if (schemaRelevance && schemaRelevance.relevant) {
      intent = 'TEXT_TO_SQL';
      confidence = 0.7;
      reasoning = `查询与数据库表/列相关（${schemaRelevance.matched_tables.length}个表，${schemaRelevance.matched_columns.length}个列）`;
    }
    // 如果找到QA对或业务知识，可能是GENERAL
    else if (ragContext.qa_pairs_found || ragContext.business_knowledge_found) {
      intent = 'GENERAL';
      confidence = 0.6;
      reasoning = '检索到相关问答或业务知识';
    } else {
      // 检查查询文本中的关键词
      const sqlKeywords = ['查询', '统计', '显示', '列出', '计算', 'select', 'count', 'sum', 'avg', 'max', 'min'];
      const hasSqlKeywords = sqlKeywords.some(keyword => 
        query.toLowerCase().includes(keyword.toLowerCase())
      );

      if (hasSqlKeywords) {
        intent = 'TEXT_TO_SQL';
        confidence = 0.5;
        reasoning = '查询包含SQL相关关键词';
      } else {
        intent = 'MISLEADING_QUERY';
        confidence = 0.3;
        reasoning = '未找到相关数据库信息';
      }
    }

    return {
      intent,
      confidence,
      reasoning,
      rag_context: ragContext,
    };
  }

  /**
   * 清理数据源ID字符串，移除多余的引号和空白字符
   */
  cleanDataSourceId(id) {
    if (!id || typeof id !== 'string') {
      return id;
    }
    // 移除字符串两端的引号和空白字符
    return id.trim().replace(/^["']+|["']+$/g, '');
  }

  /**
   * 获取entityId（从conversation或input）
   */
  async getEntityId(input) {
    // 优先使用input中的entity_id
    if (input.entity_id) {
      return this.cleanDataSourceId(input.entity_id);
    }

    // 从conversation中获取数据源ID
    if (this.conversation) {
      // 优先使用conversation.data_source_id
      if (this.conversation.data_source_id) {
        return this.cleanDataSourceId(this.conversation.data_source_id);
      }
      
      // 如果conversation有project_id，从项目获取data_source_id
      if (this.conversation.project_id) {
        try {
          let getProjectById = null;
          try {
            getProjectById = require('~/models/Project').getProjectById;
          } catch (e) {
            getProjectById = require(path.resolve(__dirname, '../../../api/models/Project')).getProjectById;
          }
          const projectId = this.cleanDataSourceId(this.conversation.project_id);
          const project = await getProjectById(projectId);
          if (project && project.data_source_id) {
            return project.data_source_id.toString();
          }
        } catch (error) {
          logger.warn('[IntentClassificationTool] 获取项目数据源失败:', error.message);
        }
      }
    }

    return null;
  }

  /**
   * @override
   */
  async _call(input) {
    const { query, use_rag = true, top_k = 5, use_schema_fallback = true } = input;

    try {
      logger.info('[IntentClassificationTool] 开始意图分类:', { query: query.substring(0, 50) });

      let ragResults = null;
      if (use_rag) {
        const userId = this.userId || 'system';
        const entityId = await this.getEntityId(input);
        ragResults = await this.retrieveRAGKnowledge(query, userId, top_k, entityId);
      }

      // 如果RAG检索不到语义模型，且启用了schema后备方案，尝试获取数据库schema
      let schemaRelevance = null;
      const ragContext = ragResults && ragResults.results 
        ? { semantic_models_found: ragResults.results.some(r => r.type === 'semantic_model') }
        : { semantic_models_found: false };

      if (use_schema_fallback && !ragContext.semantic_models_found) {
        try {
          const entityId = await this.getEntityId(input);
          const schemaData = await this.getDatabaseSchema(entityId);
          if (schemaData) {
            schemaRelevance = this.checkQueryRelevance(query, schemaData);
            logger.info('[IntentClassificationTool] 使用database_schema后备方案:', {
              relevant: schemaRelevance.relevant,
              matched_tables: schemaRelevance.matched_tables.length,
              matched_columns: schemaRelevance.matched_columns.length,
            });
          }
        } catch (error) {
          logger.warn('[IntentClassificationTool] Schema后备方案失败:', error.message);
        }
      }

      const analysis = this.analyzeIntent(query, ragResults, schemaRelevance);

      const result = {
        intent: analysis.intent,
        confidence: analysis.confidence,
        reasoning: analysis.reasoning,
        rephrased_question: query, // 简化处理，实际应调用LLM重述
        rag_context: analysis.rag_context,
      };

      logger.info('[IntentClassificationTool] 分类完成:', result);

      return JSON.stringify(result, null, 2);
    } catch (error) {
      logger.error('[IntentClassificationTool] 分类失败:', error);
      return JSON.stringify(
        {
          intent: 'MISLEADING_QUERY',
          confidence: 0.0,
          reasoning: `分类失败: ${error.message}`,
          error: error.message,
        },
        null,
        2,
      );
    }
  }
}

module.exports = IntentClassificationTool;

