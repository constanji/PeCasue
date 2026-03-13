const { Tool } = require('@langchain/core/tools');
const { z } = require('zod');
const { logger } = require('@because/data-schemas');
const path = require('path');
// 延迟加载RerankingService，避免路径别名问题
let RerankingService = null;
function loadRerankingService() {
  if (!RerankingService) {
    try {
      // 尝试使用路径别名（在api目录上下文中有效）
      RerankingService = require('~/server/services/RAG').RerankingService;
    } catch (e) {
      // 如果路径别名无效，使用相对路径
      RerankingService = require(path.resolve(__dirname, '../../../api/server/services/RAG')).RerankingService;
    }
  }
  return RerankingService;
}

/**
 * Reranker Tool - 重排序工具
 * 
 * 对检索结果进行重排序优化，使用reranker模型提高相关性
 */
class RerankerTool extends Tool {
  name = 'reranker';

  description =
    '对检索结果进行重排序优化，使用reranker模型提高相关性。' +
    '支持基础重排序和增强重排序（结合相似度、类型权重、时效性等多因素）。';

  schema = z.object({
    query: z
      .string()
      .min(1)
      .describe('原始查询文本'),
    results: z
      .array(z.any())
      .min(1)
      .describe('检索结果数组，每个结果应包含 content 或 text 字段'),
    top_k: z
      .number()
      .int()
      .positive()
      .max(50)
      .optional()
      .default(10)
      .describe('返回前K个结果，默认10'),
    enhanced: z
      .boolean()
      .optional()
      .default(false)
      .describe('是否使用增强重排序，默认关闭'),
    weights: z
      .object({
        similarity_weight: z.number().min(0).max(1).optional().default(0.7),
        type_weight: z.number().min(0).max(1).optional().default(0.2),
        recency_weight: z.number().min(0).max(1).optional().default(0.1),
      })
      .optional()
      .describe('增强重排序的权重配置'),
  });

  constructor(fields = {}) {
    super();
    this.rerankingService = null; // 延迟初始化
  }

  /**
   * 获取RerankingService实例（延迟加载）
   */
  getRerankingService() {
    if (!this.rerankingService) {
      const RerankingServiceClass = loadRerankingService();
      this.rerankingService = new RerankingServiceClass();
    }
    return this.rerankingService;
  }

  /**
   * 标准化检索结果格式
   */
  normalizeResults(results) {
    return results.map((result, index) => {
      // 确保有content或text字段
      const content = result.content || result.text || '';
      
      return {
        ...result,
        content,
        text: content,
        score: result.score || result.similarity || 0,
        similarity: result.similarity || result.score || 0,
        originalIndex: index,
      };
    });
  }

  /**
   * @override
   */
  async _call(input) {
    const {
      query,
      results,
      top_k = 10,
      enhanced = false,
      weights,
    } = input;

    try {
      logger.info('[RerankerTool] 开始重排序:', {
        query: query.substring(0, 50),
        resultsCount: results.length,
        topK: top_k,
        enhanced,
      });

      // 标准化结果格式
      const normalizedResults = this.normalizeResults(results);

      let rerankedResults;
      let rerankerType = 'default';

      const rerankingService = this.getRerankingService();
      
      if (enhanced) {
        // 增强重排序
        rerankedResults = await rerankingService.enhancedRerank({
          query,
          results: normalizedResults,
          topK: top_k,
          weights,
        });
        rerankerType = rerankingService.reranker?.type || 'enhanced';
      } else {
        // 基础重排序
        rerankedResults = await rerankingService.rerank({
          query,
          results: normalizedResults,
          topK: top_k,
        });
        rerankerType = rerankingService.reranker?.type || 'default';
      }

      // 添加排名信息
      const finalResults = rerankedResults.map((result, index) => ({
        ...result,
        rank: index + 1,
        reranked: true,
        enhanced: enhanced,
      }));

      const result = {
        query,
        reranked_results: finalResults,
        total: finalResults.length,
        metadata: {
          reranker_type: rerankerType,
          enhanced,
          original_count: results.length,
        },
      };

      logger.info('[RerankerTool] 重排序完成:', {
        total: finalResults.length,
        rerankerType,
        enhanced,
      });

      return JSON.stringify(result, null, 2);
    } catch (error) {
      logger.error('[RerankerTool] 重排序失败:', error);
      
      // 失败时返回默认排序结果
      const sortedResults = results
        .map((result, index) => ({
          ...result,
          rank: index + 1,
          reranked: false,
          score: result.score || result.similarity || 0,
        }))
        .sort((a, b) => (b.score || 0) - (a.score || 0))
        .slice(0, top_k);

      return JSON.stringify(
        {
          query,
          reranked_results: sortedResults,
          total: sortedResults.length,
          error: error.message,
          metadata: {
            reranker_type: 'default',
            enhanced: false,
            original_count: results.length,
            fallback: true,
          },
        },
        null,
        2,
      );
    }
  }
}

module.exports = RerankerTool;

