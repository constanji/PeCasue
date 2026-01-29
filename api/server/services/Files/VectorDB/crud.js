const fs = require('fs');
const axios = require('axios');
const FormData = require('form-data');
const { logger } = require('@because/data-schemas');
const { FileSources } = require('@because/data-provider');
const { logAxiosError, generateShortLivedToken, parseText } = require('@because/api');
const { fixFilenameEncoding } = require('~/server/utils/files');

// 配置参数（可通过环境变量覆盖）
const CHUNK_SIZE = parseInt(process.env.RAG_CHUNK_SIZE || '1500', 10);
const CHUNK_OVERLAP = parseInt(process.env.RAG_CHUNK_OVERLAP || '100', 10);
const EMBEDDING_BATCH_SIZE = parseInt(process.env.RAG_EMBEDDING_BATCH_SIZE || '50', 10); // 每批处理的块数

/**
 * Deletes a file from the vector database. This function takes a file object, constructs the full path, and
 * verifies the path's validity before deleting the file. If the path is invalid, an error is thrown.
 *
 * @param {ServerRequest} req - The request object from Express.
 * @param {MongoFile} file - The file object to be deleted. It should have a `filepath` property that is
 *                           a string representing the path of the file relative to the publicPath.
 *
 * @returns {Promise<void>}
 *          A promise that resolves when the file has been successfully deleted, or throws an error if the
 *          file path is invalid or if there is an error in deletion.
 */
const deleteVectors = async (req, file) => {
  if (!file.embedded) {
    return;
  }


  if (process.env.RAG_API_URL) {
    try {
      const jwtToken = generateShortLivedToken(req.user.id);

      return await axios.delete(`${process.env.RAG_API_URL}/documents`, {
        headers: {
          Authorization: `Bearer ${jwtToken}`,
          'Content-Type': 'application/json',
          accept: 'application/json',
        },
        data: [file.file_id],
      });
    } catch (error) {
      logAxiosError({
        error,
        message: 'Error deleting vectors from RAG API',
      });
      if (
        error.response &&
        error.response.status !== 404 &&
        (error.response.status < 200 || error.response.status >= 300)
      ) {
        logger.warn('Error deleting vectors from RAG API, trying local deletion');

      } else {
        return; 
      }
    }
  }

  // 使用本地向量数据库删除
  try {
    const VectorDBService = require('../../RAG/VectorDBService');
    const vectorDBService = new VectorDBService();
    await vectorDBService.initialize();
    const pool = vectorDBService.getPool();

    const result = await pool.query('DELETE FROM file_vectors WHERE file_id = $1', [
      file.file_id,
    ]);
    logger.info(
      `[deleteVectors] 从本地向量数据库删除文件向量: file_id=${file.file_id}, 删除行数=${result.rowCount}`
    );
  } catch (error) {
    logger.error('[deleteVectors] 本地向量删除失败:', error);
    // 不抛出错误，允许文件删除继续
  }
};

/**
 * Uploads a file to the configured Vector database
 *
 * @param {Object} params - The params object.
 * @param {Object} params.req - The request object from Express. It should have a `user` property with an `id` representing the user
 * @param {Express.Multer.File} params.file - The file object, which is part of the request. The file object should
 *                                     have a `path` property that points to the location of the uploaded file.
 * @param {string} params.file_id - The file ID.
 * @param {string} [params.entity_id] - The entity ID for shared resources.
 * @param {Object} [params.storageMetadata] - Storage metadata for dual storage pattern.
 *
 * @returns {Promise<{ filepath: string, bytes: number }>}
 *          A promise that resolves to an object containing:
 *            - filepath: The path where the file is saved.
 *            - bytes: The size of the file in bytes.
 */
/**
 * 清理文本：移除 null 字符和无效 UTF-8 字符
 * @param {string} text - 原始文本
 * @returns {string} 清理后的文本
 */
function cleanText(text) {
  if (!text) return '';
  // 移除 null 字符
  text = text.replace(/\x00/g, '');
  // 移除无效 UTF-8 字符：使用 Buffer 的 ignore 选项
  try {
    return Buffer.from(text, 'utf8').toString('utf8');
  } catch {
    // 如果转换失败，尝试移除无效字符
    return text.replace(/[\uFFFD\u0000-\u0008\u000B\u000C\u000E-\u001F\u007F-\u009F]/g, '');
  }
}

/**
 * 智能文本分块函数（类似 RecursiveCharacterTextSplitter）
 * 优先在段落、句子边界断开，保持语义完整性
 * @param {string} text - 要分块的文本
 * @param {number} chunkSize - 每个块的目标大小（字符数）
 * @param {number} chunkOverlap - 块之间的重叠（字符数）
 * @returns {string[]} 文本块数组
 */
function splitTextIntoChunks(text, chunkSize = CHUNK_SIZE, chunkOverlap = CHUNK_OVERLAP) {
  if (!text || text.length === 0) {
    return [];
  }

  // 清理文本
  text = cleanText(text);

  const chunks = [];
  let startIndex = 0;

  // 分隔符优先级：从大到小，优先在更大语义单元断开
  const separators = [
    '\n\n',      // 段落分隔
    '\n',        // 行分隔
    '。',        // 中文句号
    '. ',        // 英文句号+空格
    '！',        // 中文感叹号
    '! ',        // 英文感叹号+空格
    '？',        // 中文问号
    '? ',        // 英文问号+空格
    '；',        // 中文分号
    '; ',        // 英文分号+空格
    '，',        // 中文逗号
    ', ',        // 英文逗号+空格
    ' ',         // 空格
    '',          // 字符边界（最后手段）
  ];

  while (startIndex < text.length) {
    let endIndex = Math.min(startIndex + chunkSize, text.length);
    let chunkText = text.slice(startIndex, endIndex);

    // 如果不是最后一块，尝试在合适的分隔符位置断开
    if (endIndex < text.length) {
      let bestSeparatorIndex = -1;
      let bestSeparatorLength = 0;

      // 按优先级查找分隔符
      for (const separator of separators) {
        if (separator === '') {
          // 字符边界：直接使用当前位置
          bestSeparatorIndex = endIndex;
          bestSeparatorLength = 0;
          break;
        }

        const index = chunkText.lastIndexOf(separator);
        if (index !== -1 && index > chunkText.length * 0.3) {
          // 只在块的后 70% 部分查找，避免块太小
          const separatorEnd = index + separator.length;
          if (separatorEnd > bestSeparatorIndex) {
            bestSeparatorIndex = separatorEnd;
            bestSeparatorLength = separator.length;
          }
        }
      }

      if (bestSeparatorIndex !== -1) {
        endIndex = startIndex + bestSeparatorIndex;
        chunkText = text.slice(startIndex, endIndex);
      }
    }

    chunkText = chunkText.trim();
    if (chunkText.length > 0) {
      chunks.push(chunkText);
    }

    // 移动到下一个块的起始位置（考虑重叠）
    if (chunks.length > 0) {
      // 计算重叠：从当前块的末尾向前取 overlap 个字符
      const overlapStart = Math.max(0, endIndex - chunkOverlap);
      startIndex = overlapStart;
    } else {
      startIndex = endIndex;
    }

    // 防止无限循环
    if (startIndex >= text.length) break;
    if (startIndex === endIndex && endIndex < text.length) {
      // 如果没有找到合适的分隔符，强制前进
      startIndex = endIndex;
    }
  }

  return chunks;
}

/**
 * 批量处理文档块：分批向量化和插入，减少内存占用
 * @param {Object} params
 * @returns {Promise<number>} 成功插入的块数
 */
async function processChunksInBatches({
  chunks,
  embeddings,
  chunkMetadataList = [], //可选的chunk metadata列表
  file_id,
  userId,
  entity_id,
  file,
  fixedFilename, // 修复后文件名
  storageMetadata,
  pool,
  embeddingService,
  batchSize = EMBEDDING_BATCH_SIZE,
}) {
  const totalChunks = chunks.length;
  const numBatches = Math.ceil(totalChunks / batchSize);
  let insertedCount = 0;
  const insertedChunkIndices = [];

  logger.info(
    `[uploadVectorsLocal] 开始批量处理: ${totalChunks} 个块，分 ${numBatches} 批，每批 ${batchSize} 个`
  );

  try {
    for (let batchIdx = 0; batchIdx < numBatches; batchIdx++) {
      const startIdx = batchIdx * batchSize;
      const endIdx = Math.min(startIdx + batchSize, totalChunks);
      const batchChunks = chunks.slice(startIdx, endIdx);
      const batchEmbeddings = embeddings.slice(startIdx, endIdx);
      const batchMetadata = chunkMetadataList.slice(startIdx, endIdx);

      logger.info(
        `[uploadVectorsLocal] 处理批次 ${batchIdx + 1}/${numBatches}: 块 ${startIdx}-${endIdx - 1}`
      );

      // 批量插入当前批次的向量
      const insertPromises = batchChunks.map((chunk, idx) => {
        const globalIdx = startIdx + idx;
        const embeddingStr = `[${batchEmbeddings[idx].join(',')}]`;
        if (chunk.includes('\u0000')) {
          logger.warn(`[processChunksInBatches] 检测到 NUL 字符，正在清理: chunk_index=${globalIdx}`);
          chunk = chunk.replace(/\u0000/g, '');
        }
        
        // 合并基础metadata和chunk-specific metadata
        const baseMetadata = {
          entity_id: entity_id || null,
          filename: fixedFilename, 
          mimetype: file.mimetype,
          ...(storageMetadata || {}),
        };
        
        // 如果有chunk-specific metadata，合并它
        const chunkMetadata = batchMetadata && batchMetadata[idx] 
          ? batchMetadata[idx] 
          : { chunk_index: globalIdx };
        
        const metadata = {
          ...baseMetadata,
          ...chunkMetadata,
          chunk_index: globalIdx, // 确保chunk_index是全局索引
        };

        return pool.query(
          `INSERT INTO file_vectors (file_id, user_id, entity_id, chunk_index, content, embedding, metadata)
           VALUES ($1, $2, $3, $4, $5, $6::vector, $7::jsonb)`,
          [
            file_id,
            userId,
            entity_id || null,
            globalIdx,
            chunk,
            embeddingStr,
            JSON.stringify(metadata),
          ]
        );
      });

      await Promise.all(insertPromises);
      insertedCount += batchChunks.length;
      insertedChunkIndices.push(...Array.from({ length: batchChunks.length }, (_, i) => startIdx + i));

      logger.info(
        `[uploadVectorsLocal] 批次 ${batchIdx + 1}/${numBatches} 完成: 已插入 ${insertedCount}/${totalChunks} 个块`
      );
    }

    logger.info(`[uploadVectorsLocal] 所有批次处理完成: 共插入 ${insertedCount} 个块`);
    return insertedCount;
  } catch (error) {
    logger.error(`[uploadVectorsLocal] 批量处理失败，开始回滚: ${error.message}`);
    // 回滚：删除已插入的块
    if (insertedChunkIndices.length > 0) {
      try {
        await pool.query('DELETE FROM file_vectors WHERE file_id = $1', [file_id]);
        logger.info(`[uploadVectorsLocal] 回滚完成: 已删除 ${insertedChunkIndices.length} 个块`);
      } catch (rollbackError) {
        logger.error(`[uploadVectorsLocal] 回滚失败: ${rollbackError.message}`);
      }
    }
    throw error;
  }
}

/**
 * 使用本地服务进行文件向量化（改进版：支持批量处理和错误回滚）
 * @param {Object} params
 * @returns {Promise<{ filepath: string, bytes: number, embedded: boolean }>}
 */
async function uploadVectorsLocal({ req, file, file_id, entity_id, storageMetadata }) {
  const VectorDBService = require('../../RAG/VectorDBService');
  const ONNXEmbeddingService = require('../../RAG/ONNXEmbeddingService');
  
  let vectorDBService = null;
  let pool = null;
  let insertedCount = 0;

  try {
    // 0. 验证文件路径
    if (!file || !file.path) {
      throw new Error('文件对象无效：缺少文件路径');
    }
    
    const fs = require('fs');
    if (!fs.existsSync(file.path)) {
      throw new Error(`文件不存在: ${file.path}`);
    }
    
    // 🔥 修复文件名编码问题（Latin1 → UTF-8）
    const fixedFilename = fixFilenameEncoding(file.originalname);
    logger.info(`[uploadVectorsLocal] 开始解析文件: ${fixedFilename} (路径: ${file.path})`);
    
    // 1. 解析文件文本（如果是PDF或Word，使用专用解析服务）
    let chunks = [];
    let chunkMetadataList = []; // 存储每个块的metadata
    const isPDF = file.mimetype === 'application/pdf' || fixedFilename?.toLowerCase().endsWith('.pdf');
    const isWord = file.mimetype === 'application/vnd.openxmlformats-officedocument.wordprocessingml.document' ||
                   file.mimetype === 'application/msword' ||
                   fixedFilename?.toLowerCase().endsWith('.docx') ||
                   fixedFilename?.toLowerCase().endsWith('.doc');
    
    if (isPDF) {
      // 使用PDFParseService解析PDF
      try {
        const PDFParseService = require('../../RAG/PDFParseService');
        const pdfService = new PDFParseService();
        await pdfService.initialize();
        
        logger.info(`[uploadVectorsLocal] 使用PDFParseService解析PDF文件`);
        const pdfChunks = await pdfService.parsePDF(file.path, {
          chunkSize: CHUNK_SIZE,
          chunkOverlap: CHUNK_OVERLAP,
          fileMetadata: {
            file_id: file_id,
            filename: fixedFilename, // 使用修复后的文件名
            mimetype: file.mimetype,
          },
        });
        
        // 提取文本和metadata
        chunks = pdfChunks.map(chunk => chunk.text);
        chunkMetadataList = pdfChunks.map(chunk => chunk.metadata);
        
        logger.info(`[uploadVectorsLocal] PDF解析成功: ${pdfChunks.length} 个块`);
      } catch (pdfError) {
        logger.warn(`[uploadVectorsLocal] PDF解析失败，回退到普通文本解析: ${pdfError.message}`);
        // 回退到普通文本解析
        const { text, bytes } = await parseText({ req, file, file_id: file_id });
        if (!text || text.trim().length === 0) {
          throw new Error('文件解析后没有文本内容');
        }
        chunks = splitTextIntoChunks(text, CHUNK_SIZE, CHUNK_OVERLAP);
        // 为普通文本文件创建默认metadata
        chunkMetadataList = chunks.map((_, idx) => ({
          chunk_index: idx,
          source: 'text',
        }));
        logger.info(`[uploadVectorsLocal] 文件已分块: ${chunks.length} 个块`);
      }
    } else if (isWord) {
      // 使用WordParseService解析Word文档
      try {
        const WordParseService = require('../../RAG/WordParseService');
        const wordService = new WordParseService();
        await wordService.initialize();
        
        logger.info(`[uploadVectorsLocal] 使用WordParseService解析Word文件`);
        const wordChunks = await wordService.parseWordDocument(file.path, {
          chunkSize: CHUNK_SIZE,
          chunkOverlap: CHUNK_OVERLAP,
          fileMetadata: {
            file_id: file_id,
            filename: fixedFilename, // 使用修复后的文件名
            mimetype: file.mimetype,
          },
        });
        
        // 提取文本和metadata
        chunks = wordChunks.map(chunk => chunk.text);
        chunkMetadataList = wordChunks.map(chunk => chunk.metadata);
        
        logger.info(`[uploadVectorsLocal] Word解析成功: ${wordChunks.length} 个块`);
      } catch (wordError) {
        logger.warn(`[uploadVectorsLocal] Word解析失败，回退到普通文本解析: ${wordError.message}`);
        // 回退到普通文本解析
        const { text, bytes } = await parseText({ req, file, file_id: file_id });
        if (!text || text.trim().length === 0) {
          throw new Error('文件解析后没有文本内容');
        }
        chunks = splitTextIntoChunks(text, CHUNK_SIZE, CHUNK_OVERLAP);
        // 为普通文本文件创建默认metadata
        chunkMetadataList = chunks.map((_, idx) => ({
          chunk_index: idx,
          source: 'text',
        }));
        logger.info(`[uploadVectorsLocal] 文件已分块: ${chunks.length} 个块`);
      }
    } else {
      // 普通文本文件解析
      const { text, bytes } = await parseText({ req, file, file_id: file_id });
      if (!text || text.trim().length === 0) {
        throw new Error('文件解析后没有文本内容');
      }
      
      logger.info(`[uploadVectorsLocal] 文件解析成功: ${bytes} 字节`);

      // 2. 文本分块（使用配置的参数）
      logger.info(
        `[uploadVectorsLocal] 使用分块参数: chunkSize=${CHUNK_SIZE}, chunkOverlap=${CHUNK_OVERLAP}`
      );
      chunks = splitTextIntoChunks(text, CHUNK_SIZE, CHUNK_OVERLAP);
      // 为普通文本文件创建默认metadata
      chunkMetadataList = chunks.map((_, idx) => ({
        chunk_index: idx,
        source: 'text',
      }));
      logger.info(`[uploadVectorsLocal] 文件已分块: ${chunks.length} 个块`);
    }

    if (chunks.length === 0) {
      throw new Error('文件分块后没有内容');
    }

    // 3. 初始化本地向量化服务
    logger.info('[uploadVectorsLocal] 初始化 ONNX 嵌入服务...');
    const embeddingService = new ONNXEmbeddingService();
    try {
      await embeddingService.initialize();
      logger.info('[uploadVectorsLocal] ONNX 嵌入服务初始化成功');
    } catch (initError) {
      logger.error('[uploadVectorsLocal] ONNX 嵌入服务初始化失败:', initError);
      throw new Error(`ONNX 嵌入服务初始化失败: ${initError.message}`);
    }

    // 4. 批量向量化（分批处理以减少内存占用）
    logger.info(`[uploadVectorsLocal] 开始向量化 ${chunks.length} 个文本块`);
    const embeddings = await embeddingService.embedTexts(chunks);
    logger.info(`[uploadVectorsLocal] 向量化完成，共 ${embeddings.length} 个向量`);

    if (embeddings.length !== chunks.length) {
      throw new Error(`向量化结果数量不匹配: 期望 ${chunks.length}，实际 ${embeddings.length}`);
    }

    // 5. 初始化向量数据库服务
    logger.info('[uploadVectorsLocal] 初始化向量数据库服务...');
    vectorDBService = new VectorDBService();
    try {
      await vectorDBService.initialize();
      logger.info('[uploadVectorsLocal] 向量数据库服务初始化成功');
    } catch (dbError) {
      logger.error('[uploadVectorsLocal] 向量数据库服务初始化失败:', dbError);
      throw new Error(`向量数据库服务初始化失败: ${dbError.message}`);
    }
    
    pool = vectorDBService.getPool();
    const userId = req.user?.id;
    
    if (!userId) {
      throw new Error('用户ID不存在，无法上传向量');
    }
    
    logger.info(`[uploadVectorsLocal] 用户ID: ${userId}, 实体ID: ${entity_id || '无'}`);

    // 6. 删除该文件的旧向量（如果存在）
    logger.info(`[uploadVectorsLocal] 清理旧向量: file_id=${file_id}`);
    await pool.query('DELETE FROM file_vectors WHERE file_id = $1', [file_id]);

    // 7. 批量插入向量（支持分批处理）
    insertedCount = await processChunksInBatches({
      chunks,
      embeddings,
      chunkMetadataList, // 传递chunk metadata列表
      file_id,
      userId,
      entity_id,
      file,
      fixedFilename, // 🔥 传递修复后的文件名
      storageMetadata,
      pool,
      embeddingService,
      batchSize: EMBEDDING_BATCH_SIZE,
    });

    logger.info(
      `[uploadVectorsLocal] 文件向量化完成: ${fixedFilename} (${insertedCount} 个块)`
    );

    return {
      bytes: file.size,
      filename: fixedFilename, // 使用修复后的文件名
      filepath: FileSources.vectordb,
      embedded: true,
    };
  } catch (error) {
    logger.error('[uploadVectorsLocal] 本地向量化失败:', error);
    logger.error('[uploadVectorsLocal] 错误堆栈:', error.stack);
    
    // 如果已经部分插入，尝试清理
    if (pool && insertedCount > 0) {
      try {
        logger.warn(`[uploadVectorsLocal] 清理部分插入的数据: file_id=${file_id}`);
        await pool.query('DELETE FROM file_vectors WHERE file_id = $1', [file_id]);
      } catch (cleanupError) {
        logger.error(`[uploadVectorsLocal] 清理失败: ${cleanupError.message}`);
      }
    }

    // 提供更详细的错误信息
    let errorMessage = `本地文件向量化失败: ${error.message}`;
    
    // 根据错误类型提供更具体的提示
    if (error.message.includes('not found') || error.message.includes('not exist')) {
      if (error.message.includes('ONNX') || error.message.includes('model')) {
        errorMessage += '。请确保 ONNX 模型文件存在于 api/server/services/RAG/onnx/embedding/resources/';
      } else if (error.message.includes('@xenova/transformers')) {
        errorMessage += '。请运行: cd api && npm install @xenova/transformers';
      } else if (error.message.includes('pg') || error.message.includes('database')) {
        errorMessage += '。请检查向量数据库连接配置和 pgvector 扩展是否已启用';
      }
    } else if (error.message.includes('dimension')) {
      errorMessage += '。请检查 EMBEDDING_DIMENSION 环境变量是否与模型输出维度匹配';
    } else if (error.message.includes('连接') || error.message.includes('connection')) {
      errorMessage += '。请检查向量数据库服务是否正常运行';
    }

    throw new Error(errorMessage);
  }
}

async function uploadVectors({ req, file, file_id, entity_id, storageMetadata }) {
  // 🔥 修复文件名编码问题
  const fixedFilename = fixFilenameEncoding(file.originalname);
  logger.info(`[uploadVectors] 开始处理文件向量化: ${fixedFilename}, file_id=${file_id}, entity_id=${entity_id || '无'}`);
  
  // 如果未配置 RAG_API_URL，使用本地向量化服务
  if (!process.env.RAG_API_URL) {
    logger.info('[uploadVectors] RAG_API_URL 未配置，使用本地向量化服务');
    try {
      const result = await uploadVectorsLocal({ req, file, file_id, entity_id, storageMetadata });
      logger.info(`[uploadVectors] 本地向量化成功: ${fixedFilename}`);
      return result;
    } catch (error) {
      logger.error(`[uploadVectors] 本地向量化失败: ${fixedFilename}`, error);
      throw error;
    }
  }

  // 使用外部 RAG API
  try {
    const jwtToken = generateShortLivedToken(req.user.id);
    const formData = new FormData();
    formData.append('file_id', file_id);
    formData.append('file', fs.createReadStream(file.path));
    if (entity_id != null && entity_id) {
      formData.append('entity_id', entity_id);
    }

    // Include storage metadata for RAG API to store with embeddings
    if (storageMetadata) {
      formData.append('storage_metadata', JSON.stringify(storageMetadata));
    }

    const formHeaders = formData.getHeaders();

    const response = await axios.post(`${process.env.RAG_API_URL}/embed`, formData, {
      headers: {
        Authorization: `Bearer ${jwtToken}`,
        accept: 'application/json',
        ...formHeaders,
      },
    });

    const responseData = response.data;
    logger.debug('Response from embedding file', responseData);

    if (responseData.known_type === false) {
      throw new Error(`File embedding failed. The filetype ${file.mimetype} is not supported`);
    }

    if (!responseData.status) {
      throw new Error('File embedding failed.');
    }

    return {
      bytes: file.size,
      filename: fixedFilename, // 使用修复后的文件名
      filepath: FileSources.vectordb,
      embedded: Boolean(responseData.known_type),
    };
  } catch (error) {
    logAxiosError({
      error,
      message: 'Error uploading vectors',
    });
    
    // 如果外部 RAG API 失败，尝试使用本地服务作为回退
    logger.warn('[uploadVectors] 外部 RAG API 失败，尝试使用本地向量化服务作为回退');
    try {
      return await uploadVectorsLocal({ req, file, file_id, entity_id, storageMetadata });
    } catch (localError) {
      logger.error('[uploadVectors] 本地向量化服务也失败:', localError);
      throw new Error(error.message || 'An error occurred during file upload.');
    }
  }
}

module.exports = {
  deleteVectors,
  uploadVectors,
};
