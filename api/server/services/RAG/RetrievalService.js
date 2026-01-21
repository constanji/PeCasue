const { logger } = require('@because/data-schemas');
const mongoose = require('mongoose');
const { createModels } = require('@because/data-schemas');
const VectorDBService = require('./VectorDBService');

// 确保模型已创建（如果还没有）
let KnowledgeEntry;
try {
  const models = require('~/db/models');
  KnowledgeEntry = models.KnowledgeEntry;
  if (!KnowledgeEntry) {
    // 如果模型未导出，直接创建
    const createdModels = createModels(mongoose);
    KnowledgeEntry = createdModels.KnowledgeEntry;
  }
} catch (e) {
  // 如果从 db/models 导入失败，直接创建模型
  const createdModels = createModels(mongoose);
  KnowledgeEntry = createdModels.KnowledgeEntry;
}
// 从编译后的包中导入，或使用本地 JavaScript 文件
let KnowledgeType;
try {
  KnowledgeType = require('@because/data-schemas/schema/knowledgeBase').KnowledgeType;
} catch (e) {
  try {
    KnowledgeType = require('../../../../packages/data-schemas/src/schema/knowledgeBase').KnowledgeType;
  } catch (e2) {
    KnowledgeType = {
      SEMANTIC_MODEL: 'semantic_model',
      QA_PAIR: 'qa_pair',
      SYNONYM: 'synonym',
      BUSINESS_KNOWLEDGE: 'business_knowledge',
      FILE: 'file',
    };
  }
}
const EmbeddingService = require('./EmbeddingService');

/**
 * 向量检索服务
 * 负责从知识库中检索相关内容
 */
class RetrievalService {
  constructor() {
    this.embeddingService = new EmbeddingService();
    this.vectorDBService = new VectorDBService();
    this.ragApiUrl = process.env.RAG_API_URL; // 可选，仅用于文件检索
    this.useVectorDB = process.env.USE_VECTOR_DB !== 'false'; // 默认启用向量数据库
  }

  /**
   * 计算余弦相似度
   * @param {number[]} vec1 - 向量1
   * @param {number[]} vec2 - 向量2
   * @returns {number} 相似度分数（0-1）
   */
  cosineSimilarity(vec1, vec2) {
    if (vec1.length !== vec2.length) {
      throw new Error('Vectors must have the same length');
    }

    let dotProduct = 0;
    let norm1 = 0;
    let norm2 = 0;

    for (let i = 0; i < vec1.length; i++) {
      dotProduct += vec1[i] * vec2[i];
      norm1 += vec1[i] * vec1[i];
      norm2 += vec2[i] * vec2[i];
    }

    const denominator = Math.sqrt(norm1) * Math.sqrt(norm2);
    if (denominator === 0) {
      return 0;
    }

    return dotProduct / denominator;
  }

  /**
   * 从知识库中检索相关内容
   * @param {Object} params
   * @param {string} params.query - 查询文本
   * @param {string} params.userId - 用户ID
   * @param {string[]} [params.types] - 要检索的知识类型数组
   * @param {string} [params.entityId] - 实体ID过滤
   * @param {number} [params.topK] - 返回前K个结果
   * @param {number} [params.minScore] - 最小相似度分数
   * @returns {Promise<Array>} 检索结果数组
   */
  async retrieveFromKnowledgeBase({ query, userId, types, entityId, topK = 10, minScore = 0.5 }) {
    try {
      // 1. 将查询文本向量化
      logger.info(`[RetrievalService] 开始向量化查询文本: "${query.substring(0, 50)}${query.length > 50 ? '...' : ''}"`);
      const queryEmbedding = await this.embeddingService.embedText(query, userId);
      
      if (!queryEmbedding) {
        logger.warn('[RetrievalService] 查询文本向量化失败，无法进行检索');
        return [];
      }
      
      logger.info(`[RetrievalService] 查询文本向量化完成 (维度: ${queryEmbedding.length})`);

      // 2. 优先使用向量数据库进行高效相似度搜索
      if (this.useVectorDB) {
        try {
          const searchTypes = types && types.length > 0 ? types.join(', ') : '全部类型';
          const isolationInfo = entityId ? `, entityId: ${entityId} (数据源隔离)` : ' (无数据源隔离)';
          logger.info(`[RetrievalService] 开始在向量数据库中搜索相似向量 (类型: ${searchTypes}, topK: ${topK * 2}, minScore: ${minScore})${isolationInfo}`);
          const vectorResults = await this.vectorDBService.searchSimilar({
            queryEmbedding,
            types,
            entityId, 
            topK: topK * 2, // 检索更多结果以便后续过滤
            minScore,
          });

          // 从 MongoDB 获取完整信息
          if (vectorResults.length > 0) {
            const knowledgeEntryIds = vectorResults.map(r => r.knowledgeEntryId);
            const knowledgeEntries = await KnowledgeEntry.find({
              _id: { $in: knowledgeEntryIds },
            })
              .select('type title content embedding metadata user')
              .lean();

            // 合并向量数据库的相似度分数和 MongoDB 的完整信息
            // 注意：entityId过滤已在向量数据库层面完成，这里不需要再次过滤
            const resultsMap = new Map(knowledgeEntries.map(e => [e._id.toString(), e]));
            const scoredResults = vectorResults
              .map(vectorResult => {
                const entry = resultsMap.get(vectorResult.knowledgeEntryId);
                if (!entry) return null;

                // 双重检查确保entityId匹配
                if (entityId) {
                  const entityIdStr = typeof entityId === 'string' ? entityId : String(entityId);
                  const storedEntityIdStr = entry.metadata?.entity_id ? String(entry.metadata.entity_id) : null;

                  if (storedEntityIdStr !== entityIdStr) {
                    logger.warn(`[RetrievalService] 发现entityId不匹配的条目: ${vectorResult.knowledgeEntryId}, 期望: ${entityIdStr}, 实际: ${storedEntityIdStr}`);
                    return null;
                  }
                }

                return {
                  ...entry,
                  score: vectorResult.score,
                  similarity: vectorResult.score,
                };
              })
              .filter(result => result !== null)
              .slice(0, topK);

            logger.info(`[RetrievalService] 从向量数据库检索到 ${scoredResults.length} 个相关结果${isolationInfo}`);
            return scoredResults;
          }
        } catch (vectorError) {
          logger.warn('[RetrievalService] VectorDB search failed, falling back to MongoDB:', vectorError.message);
        }
      }

      // 3. 回退到 MongoDB 中的向量相似度计算
      logger.info('[RetrievalService] 使用MongoDB进行向量相似度计算（向量数据库不可用或回退）');
      const queryConditions = {};

      if (types && types.length > 0) {
        queryConditions.type = { $in: types };
      }

      if (entityId) {
        queryConditions['metadata.entity_id'] = entityId;
      }

      const knowledgeEntries = await KnowledgeEntry.find(queryConditions)
        .select('type title content embedding metadata user')
        .lean();

      if (knowledgeEntries.length === 0) {
        logger.info('[RetrievalService] 未找到相关知识条目');
        return [];
      }

      // 4. 计算相似度并排序
      const scoredResults = knowledgeEntries
        .map(entry => {
          if (!entry.embedding || entry.embedding.length === 0) {
            return null;
          }

          const score = this.cosineSimilarity(queryEmbedding, entry.embedding);
          return {
            ...entry,
            score,
            similarity: score,
          };
        })
        .filter(result => result !== null && result.score >= minScore)
        .sort((a, b) => b.score - a.score)
        .slice(0, topK);

      logger.info(`[RetrievalService] 从MongoDB检索到 ${scoredResults.length} 个相关结果`);

      return scoredResults;
    } catch (error) {
      logger.error('[RetrievalService] 检索失败:', error);
      throw error;
    }
  }

  /**
   * 从文件向量库中检索
   * 支持两种模式：
   * 1. 本地模式（优先）：直接查询file_vectors表（如果USE_VECTOR_DB启用）
   * 2. RAG API模式（回退）：通过RAG_API_URL调用外部服务
   * 
   * @param {Object} params
   * @param {string} params.query - 查询文本
   * @param {string} params.fileId - 文件ID
   * @param {string} params.userId - 用户ID
   * @param {string} [params.entityId] - 实体ID（数据源隔离，可选）
   * @param {number} [params.k] - 返回前K个结果
   * @returns {Promise<Array>} 检索结果数组
   */
  async retrieveFromFiles({ query, fileId, userId, entityId, k = 4 }) {
    // 优先使用本地向量数据库
    if (this.useVectorDB) {
      try {
        logger.info(`[RetrievalService] 使用本地向量数据库检索文件: fileId=${fileId}`);
        
        // 1. 将查询文本向量化
        const queryEmbedding = await this.embeddingService.embedText(query, userId);
        
        if (!queryEmbedding) {
          logger.warn('[RetrievalService] 查询文本向量化失败，无法进行文件检索');
          // 回退到RAG API
          return await this.retrieveFromFilesViaRAGAPI({ query, fileId, userId, k });
        }

        // 2. 从file_vectors表中检索
        const vectorResults = await this.vectorDBService.searchFileVectors({
          queryEmbedding,
          fileId, // 指定file_id，只检索该文件的chunk
          entityId, // 数据源隔离
          topK: k,
          minScore: 0.5,
        });

        // 3. 转换格式以统一返回
        const results = vectorResults.map(result => ({
          type: KnowledgeType.FILE,
          title: result.metadata?.filename || result.metadata?.source?.split('/').pop() || '文件',
          content: result.content,
          score: result.score,
          similarity: result.similarity,
          metadata: {
            file_id: result.fileId,
            filename: result.metadata?.filename || result.metadata?.source?.split('/').pop(),
            chunk_index: result.chunkIndex,
            page: result.metadata?.page || null,
            entity_id: result.metadata?.entity_id || entityId,
          },
        }));

        logger.info(`[RetrievalService] 从本地向量数据库检索到 ${results.length} 个文件chunk`);
        return results;
      } catch (vectorError) {
        logger.warn('[RetrievalService] 本地向量数据库检索失败，回退到RAG API:', vectorError.message);
        // 回退到RAG API
        return await this.retrieveFromFilesViaRAGAPI({ query, fileId, userId, k });
      }
    }

    // 回退到RAG API
    return await this.retrieveFromFilesViaRAGAPI({ query, fileId, userId, k });
  }

  /**
   * 通过RAG API检索文件（回退方案）
   * @param {Object} params
   * @param {string} params.query - 查询文本
   * @param {string} params.fileId - 文件ID
   * @param {string} params.userId - 用户ID
   * @param {number} [params.k] - 返回前K个结果
   * @returns {Promise<Array>} 检索结果数组
   */
  async retrieveFromFilesViaRAGAPI({ query, fileId, userId, k = 4 }) {
    if (!this.ragApiUrl) {
      logger.warn('[RetrievalService] RAG_API_URL not configured, file retrieval disabled');
      return [];
    }

    try {
      const axios = require('axios');
      const { generateShortLivedToken } = require('@because/api');
      const jwtToken = generateShortLivedToken(userId);

      const response = await axios.post(
        `${this.ragApiUrl}/query`,
        {
          file_id: fileId,
          query,
          k,
        },
        {
          headers: {
            Authorization: `Bearer ${jwtToken}`,
            'Content-Type': 'application/json',
          },
        }
      );

      // 转换格式以统一返回
      const results = (response.data || []).map(([docInfo, distance]) => ({
        type: KnowledgeType.FILE,
        title: docInfo.metadata?.source?.split('/').pop() || '文件',
        content: docInfo.page_content,
        score: 1.0 - distance, // 将距离转换为相似度分数
        similarity: 1.0 - distance,
        metadata: {
          file_id: fileId,
          filename: docInfo.metadata?.source?.split('/').pop(),
          page: docInfo.metadata?.page || null,
        },
      }));

      return results;
    } catch (error) {
      logger.error('[RetrievalService] RAG API文件检索失败:', error);
      // 失败时返回空数组，不抛出错误
      return [];
    }
  }

  /**
   * 混合检索：从知识库和文件中检索
   * 智能文件检索策略：
   * 1. 如果指定了fileIds，先尝试用file_id检索（只检索指定文件的chunk）
   * 2. 如果fileIds检索结果为空或没有fileIds，使用跨文件相似度检索（检索所有文件的chunk）
   * 
   * @param {Object} params
   * @param {string} params.query - 查询文本
   * @param {string} params.userId - 用户ID
   * @param {string[]} [params.fileIds] - 文件ID数组（可选，如果提供则优先检索指定文件）
   * @param {string[]} [params.types] - 知识类型数组
   * @param {string} [params.entityId] - 实体ID
   * @param {number} [params.topK] - 总返回数量
   * @returns {Promise<Array>} 混合检索结果
   */
  async hybridRetrieve({ query, userId, fileIds, types, entityId, topK = 10 }) {
    try {
      const promises = [];

      // 1. 从知识库检索
      promises.push(
        this.retrieveFromKnowledgeBase({
          query,
          userId,
          types,
          entityId,
          topK: Math.ceil(topK * 0.7), // 70% 来自知识库
        })
      );

      // 2. 智能文件检索策略
      let fileResults = [];
      const fileTopK = Math.ceil(topK * 0.3); // 30% 来自文件

      if (fileIds && fileIds.length > 0) {
        // 策略1：如果指定了fileIds，先尝试用file_id检索（只检索指定文件的chunk）
        logger.info(`[RetrievalService] 使用file_id检索模式，检索 ${fileIds.length} 个指定文件`);
        
        const filePromises = fileIds.map(fileId =>
          this.retrieveFromFiles({
            query,
            fileId, // 指定file_id，只检索该文件的chunk
            userId,
            entityId,
            k: Math.ceil(fileTopK / fileIds.length),
          })
        );
        
        const specifiedFileResults = await Promise.all(filePromises);
        fileResults = specifiedFileResults.flat();

        // 如果指定文件的检索结果为空或太少，回退到跨文件相似度检索
        if (fileResults.length === 0 || fileResults.length < Math.ceil(fileTopK * 0.5)) {
          logger.info(`[RetrievalService] 指定文件检索结果不足（${fileResults.length}个），回退到跨文件相似度检索`);
          
          // 策略2：跨文件相似度检索（不指定file_id，检索所有文件的chunk）
          if (this.useVectorDB) {
            try {
              const queryEmbedding = await this.embeddingService.embedText(query, userId);
              if (queryEmbedding) {
                const crossFileResults = await this.vectorDBService.searchFileVectors({
                  queryEmbedding,
                  // 不指定fileId，检索所有文件
                  fileId: null,
                  entityId,
                  topK: fileTopK,
                  minScore: 0.5,
                });

                // 转换格式
                const formattedCrossFileResults = crossFileResults.map(result => ({
                  type: KnowledgeType.FILE,
                  title: result.metadata?.filename || result.metadata?.source?.split('/').pop() || '文件',
                  content: result.content,
                  score: result.score,
                  similarity: result.similarity,
                  metadata: {
                    file_id: result.fileId,
                    filename: result.metadata?.filename || result.metadata?.source?.split('/').pop(),
                    chunk_index: result.chunkIndex,
                    page: result.metadata?.page || null,
                    entity_id: result.metadata?.entity_id || entityId,
                  },
                }));

                // 合并结果：优先使用指定文件的结果，补充跨文件检索的结果
                const existingFileIds = new Set(fileResults.map(r => r.metadata?.file_id));
                const additionalResults = formattedCrossFileResults.filter(
                  r => !existingFileIds.has(r.metadata?.file_id)
                );
                
                fileResults = [...fileResults, ...additionalResults]
                  .sort((a, b) => b.score - a.score)
                  .slice(0, fileTopK);

                logger.info(`[RetrievalService] 跨文件检索补充了 ${additionalResults.length} 个结果，总计 ${fileResults.length} 个文件结果`);
              }
            } catch (crossFileError) {
              logger.warn('[RetrievalService] 跨文件相似度检索失败，使用指定文件结果:', crossFileError.message);
            }
          }
        } else {
          logger.info(`[RetrievalService] 指定文件检索成功，返回 ${fileResults.length} 个结果`);
        }
      } else {
        // 策略3：如果没有指定fileIds，直接使用跨文件相似度检索
        logger.info('[RetrievalService] 未指定fileIds，使用跨文件相似度检索');
        
        if (this.useVectorDB) {
          try {
            const queryEmbedding = await this.embeddingService.embedText(query, userId);
            if (queryEmbedding) {
              const crossFileResults = await this.vectorDBService.searchFileVectors({
                queryEmbedding,
                fileId: null, // 不指定fileId，检索所有文件
                entityId,
                topK: fileTopK,
                minScore: 0.5,
              });

              // 转换格式
              fileResults = crossFileResults.map(result => ({
                type: KnowledgeType.FILE,
                title: result.metadata?.filename || result.metadata?.source?.split('/').pop() || '文件',
                content: result.content,
                score: result.score,
                similarity: result.similarity,
                metadata: {
                  file_id: result.fileId,
                  filename: result.metadata?.filename || result.metadata?.source?.split('/').pop(),
                  chunk_index: result.chunkIndex,
                  page: result.metadata?.page || null,
                  entity_id: result.metadata?.entity_id || entityId,
                },
              }));

              logger.info(`[RetrievalService] 跨文件检索返回 ${fileResults.length} 个结果`);
            }
          } catch (crossFileError) {
            logger.warn('[RetrievalService] 跨文件相似度检索失败:', crossFileError.message);
          }
        }
      }

      // 3. 等待知识库检索完成
      const knowledgeResults = await Promise.all(promises);
      const allResults = [...knowledgeResults.flat(), ...fileResults];

      // 4. 按相似度排序并返回前K个
      const sortedResults = allResults
        .sort((a, b) => b.score - a.score)
        .slice(0, topK);

      logger.info(`[RetrievalService] 混合检索返回 ${sortedResults.length} 个结果（知识库: ${knowledgeResults.flat().length}，文件: ${fileResults.length}）`);

      return sortedResults;
    } catch (error) {
      logger.error('[RetrievalService] 混合检索失败:', error);
      throw error;
    }
  }
}

module.exports = RetrievalService;

