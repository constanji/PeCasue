const fs = require('fs').promises;
const express = require('express');
const { EnvVar } = require('@because/agents');
const { logger } = require('@because/data-schemas');
const {
  Time,
  isUUID,
  CacheKeys,
  FileSources,
  ResourceType,
  EModelEndpoint,
  PermissionBits,
  checkOpenAIStorage,
  isAssistantsEndpoint,
} = require('@because/data-provider');
const {
  filterFile,
  processFileUpload,
  processDeleteRequest,
  processAgentFileUpload,
} = require('~/server/services/Files/process');
const { fileAccess } = require('~/server/middleware/accessResources/fileAccess');
const { getStrategyFunctions } = require('~/server/services/Files/strategies');
const { getOpenAIClient } = require('~/server/controllers/assistants/helpers');
const { checkPermission } = require('~/server/services/PermissionService');
const { loadAuthValues } = require('~/server/services/Tools/credentials');
const { refreshS3FileUrls } = require('~/server/services/Files/S3/crud');
const { hasAccessToFilesViaAgent } = require('~/server/services/Files');
const { getFiles, batchUpdateFiles } = require('~/models/File');
const { cleanFileName, fixFilenameEncoding } = require('~/server/utils/files');
const { getAssistant } = require('~/models/Assistant');
const { getAgent } = require('~/models/Agent');
const { getLogStores } = require('~/cache');
const { Readable } = require('stream');

const router = express.Router();

router.get('/', async (req, res) => {
  try {
    const appConfig = req.config;
    const files = await getFiles({ user: req.user.id });
    if (appConfig.fileStrategy === FileSources.s3) {
      try {
        const cache = getLogStores(CacheKeys.S3_EXPIRY_INTERVAL);
        const alreadyChecked = await cache.get(req.user.id);
        if (!alreadyChecked) {
          await refreshS3FileUrls(files, batchUpdateFiles);
          await cache.set(req.user.id, true, Time.THIRTY_MINUTES);
        }
      } catch (error) {
        logger.warn('[/files] Error refreshing S3 file URLs:', error);
      }
    }
    // 🔥 显式设置 UTF-8 charset，防止前端二次错误解码
    res.setHeader('Content-Type', 'application/json; charset=utf-8');
    res.status(200).send(files);
  } catch (error) {
    logger.error('[/files] Error getting files:', error);
    res.setHeader('Content-Type', 'application/json; charset=utf-8');
    res.status(400).json({ message: 'Error in request', error: error.message });
  }
});

/**
 * Get files specific to an agent
 * @route GET /files/agent/:agent_id
 * @param {string} agent_id - The agent ID to get files for
 * @returns {Promise<TFile[]>} Array of files attached to the agent
 */
router.get('/agent/:agent_id', async (req, res) => {
  try {
    const { agent_id } = req.params;
    const userId = req.user.id;

    if (!agent_id) {
      return res.status(400).json({ error: 'Agent ID is required' });
    }

    const agent = await getAgent({ id: agent_id });
    if (!agent) {
      return res.status(200).json([]);
    }

    if (agent.author.toString() !== userId) {
      const hasEditPermission = await checkPermission({
        userId,
        role: req.user.role,
        resourceType: ResourceType.AGENT,
        resourceId: agent._id,
        requiredPermission: PermissionBits.EDIT,
      });

      if (!hasEditPermission) {
        return res.status(200).json([]);
      }
    }

    const agentFileIds = [];
    if (agent.tool_resources) {
      for (const [, resource] of Object.entries(agent.tool_resources)) {
        if (resource?.file_ids && Array.isArray(resource.file_ids)) {
          agentFileIds.push(...resource.file_ids);
        }
      }
    }

    if (agentFileIds.length === 0) {
      return res.status(200).json([]);
    }

    const files = await getFiles({ file_id: { $in: agentFileIds } }, null, { text: 0 });

    res.setHeader('Content-Type', 'application/json; charset=utf-8');
    res.status(200).json(files);
  } catch (error) {
    logger.error('[/files/agent/:agent_id] Error fetching agent files:', error);
    res.setHeader('Content-Type', 'application/json; charset=utf-8');
    res.status(500).json({ error: 'Failed to fetch agent files' });
  }
});

router.get('/config', async (req, res) => {
  try {
    const appConfig = req.config;
    res.status(200).json(appConfig.fileConfig);
  } catch (error) {
    logger.error('[/files] Error getting fileConfig', error);
    res.status(400).json({ message: 'Error in request', error: error.message });
  }
});

router.delete('/', async (req, res) => {
  try {
    const { files: _files } = req.body;

    /** @type {MongoFile[]} */
    const files = _files.filter((file) => {
      if (!file.file_id) {
        return false;
      }
      if (!file.filepath) {
        return false;
      }

      if (/^(file|assistant)-/.test(file.file_id)) {
        return true;
      }

      return isUUID.safeParse(file.file_id).success;
    });

    if (files.length === 0) {
      res.status(204).json({ message: 'Nothing provided to delete' });
      return;
    }

    const fileIds = files.map((file) => file.file_id);
    const dbFiles = await getFiles({ file_id: { $in: fileIds } });

    const ownedFiles = [];
    const nonOwnedFiles = [];

    for (const file of dbFiles) {
      if (file.user.toString() === req.user.id.toString()) {
        ownedFiles.push(file);
      } else {
        nonOwnedFiles.push(file);
      }
    }

    if (nonOwnedFiles.length === 0) {
      await processDeleteRequest({ req, files: ownedFiles });
      logger.debug(
        `[/files] Files deleted successfully: ${ownedFiles
          .filter((f) => f.file_id)
          .map((f) => f.file_id)
          .join(', ')}`,
      );
      res.status(200).json({ message: 'Files deleted successfully' });
      return;
    }

    let authorizedFiles = [...ownedFiles];
    let unauthorizedFiles = [];

    if (req.body.agent_id && nonOwnedFiles.length > 0) {
      const nonOwnedFileIds = nonOwnedFiles.map((f) => f.file_id);
      const accessMap = await hasAccessToFilesViaAgent({
        userId: req.user.id,
        role: req.user.role,
        fileIds: nonOwnedFileIds,
        agentId: req.body.agent_id,
        isDelete: true,
      });

      for (const file of nonOwnedFiles) {
        if (accessMap.get(file.file_id)) {
          authorizedFiles.push(file);
        } else {
          unauthorizedFiles.push(file);
        }
      }
    } else {
      unauthorizedFiles = nonOwnedFiles;
    }

    if (unauthorizedFiles.length > 0) {
      return res.status(403).json({
        message: 'You can only delete files you have access to',
        unauthorizedFiles: unauthorizedFiles.map((f) => f.file_id),
      });
    }

    /* Handle agent unlinking even if no valid files to delete */
    if (req.body.agent_id && req.body.tool_resource && dbFiles.length === 0) {
      const agent = await getAgent({
        id: req.body.agent_id,
      });

      const toolResourceFiles = agent.tool_resources?.[req.body.tool_resource]?.file_ids ?? [];
      const agentFiles = files.filter((f) => toolResourceFiles.includes(f.file_id));

      await processDeleteRequest({ req, files: agentFiles });
      res.status(200).json({ message: 'File associations removed successfully from agent' });
      return;
    }

    /* Handle assistant unlinking even if no valid files to delete */
    if (req.body.assistant_id && req.body.tool_resource && dbFiles.length === 0) {
      const assistant = await getAssistant({
        id: req.body.assistant_id,
      });

      const toolResourceFiles = assistant.tool_resources?.[req.body.tool_resource]?.file_ids ?? [];
      const assistantFiles = files.filter((f) => toolResourceFiles.includes(f.file_id));

      await processDeleteRequest({ req, files: assistantFiles });
      res.status(200).json({ message: 'File associations removed successfully from assistant' });
      return;
    } else if (
      req.body.assistant_id &&
      req.body.files?.[0]?.filepath === EModelEndpoint.azureAssistants
    ) {
      await processDeleteRequest({ req, files: req.body.files });
      return res
        .status(200)
        .json({ message: 'File associations removed successfully from Azure Assistant' });
    }

    await processDeleteRequest({ req, files: authorizedFiles });

    logger.debug(
      `[/files] Files deleted successfully: ${authorizedFiles
        .filter((f) => f.file_id)
        .map((f) => f.file_id)
        .join(', ')}`,
    );
    res.status(200).json({ message: 'Files deleted successfully' });
  } catch (error) {
    logger.error('[/files] Error deleting files:', error);
    res.status(400).json({ message: 'Error in request', error: error.message });
  }
});

function isValidID(str) {
  return /^[A-Za-z0-9_-]{21}$/.test(str);
}

router.get('/code/download/:session_id/:fileId', async (req, res) => {
  try {
    const { session_id, fileId } = req.params;
    const logPrefix = `Session ID: ${session_id} | File ID: ${fileId} | Code output download requested by user `;
    logger.debug(logPrefix);

    if (!session_id || !fileId) {
      return res.status(400).send('Bad request');
    }

    if (!isValidID(session_id) || !isValidID(fileId)) {
      logger.debug(`${logPrefix} invalid session_id or fileId`);
      return res.status(400).send('Bad request');
    }

    const { getDownloadStream } = getStrategyFunctions(FileSources.execute_code);
    if (!getDownloadStream) {
      logger.warn(
        `${logPrefix} has no stream method implemented for ${FileSources.execute_code} source`,
      );
      return res.status(501).send('Not Implemented');
    }

    const result = await loadAuthValues({ userId: req.user.id, authFields: [EnvVar.CODE_API_KEY] });

    /** @type {AxiosResponse<ReadableStream> | undefined} */
    const response = await getDownloadStream(
      `${session_id}/${fileId}`,
      result[EnvVar.CODE_API_KEY],
    );
    res.set(response.headers);
    response.data.pipe(res);
  } catch (error) {
    logger.error('Error downloading file:', error);
    res.status(500).send('Error downloading file');
  }
});

router.get('/download/:userId/:file_id', fileAccess, async (req, res) => {
  try {
    const { userId, file_id } = req.params;
    logger.debug(`File download requested by user ${userId}: ${file_id}`);

    // Access already validated by fileAccess middleware
    const file = req.fileAccess.file;

    // 🔥 如果文件已向量化，直接返回数据库中的文本内容（用于查看详情）
    // 不需要检查 query 参数或 Accept header，因为用户明确要求返回数据库内容
    if (file.embedded) {
      try {
        const VectorDBService = require('~/server/services/RAG/VectorDBService');
        const vectorDBService = new VectorDBService();
        await vectorDBService.initialize();
        const pool = vectorDBService.getPool();

        // 查询该文件的所有chunks
        const result = await pool.query(
          `SELECT chunk_index, content, metadata 
           FROM file_vectors 
           WHERE file_id = $1 
           ORDER BY chunk_index ASC`,
          [file_id]
        );

        if (result.rows && result.rows.length > 0) {
          // 合并所有chunks的文本内容
          const chunks = result.rows.map(row => ({
            index: row.chunk_index,
            content: row.content,
            metadata: row.metadata,
          }));

          // 按chunk_index排序并合并
          chunks.sort((a, b) => a.index - b.index);
          const fullText = chunks.map(chunk => chunk.content).join('\n\n');

          logger.info(`[files/download] 返回向量化文件的文本内容: ${file_id}, ${chunks.length} 个chunks`);

          // 🔥 显式设置 UTF-8 charset，返回纯文本
          res.setHeader('Content-Type', 'text/plain; charset=utf-8');
          res.status(200).send(fullText);
          return;
        } else {
          logger.warn(`[files/download] 文件已向量化但未找到chunks: ${file_id}，回退到文件下载`);
        }
      } catch (vectorError) {
        logger.warn(`[files/download] 从向量数据库获取文本失败，回退到文件下载:`, vectorError.message);
        // 回退到文件下载
      }
    }

    if (checkOpenAIStorage(file.source) && !file.model) {
      logger.warn(`File download requested by user ${userId} has no associated model: ${file_id}`);
      return res.status(400).send('The model used when creating this file is not available');
    }

    const { getDownloadStream } = getStrategyFunctions(file.source);
    if (!getDownloadStream) {
      logger.warn(
        `File download requested by user ${userId} has no stream method implemented: ${file.source}`,
      );
      return res.status(501).send('Not Implemented');
    }

    const setHeaders = () => {
      // 🔥 先修复文件名编码问题（Latin1 → UTF-8）
      const fixedFilename = fixFilenameEncoding(file.filename);
      // 然后清理文件名（移除UUID前缀等）
      const cleanedFilename = cleanFileName(fixedFilename);
      
      // 🔥 修复Content-Disposition header编码问题
      // HTTP header 中不能直接包含非ASCII字符，必须使用RFC 5987格式
      // 即使是在引号内，非ASCII字符也会导致 "Invalid character in header content" 错误
      let contentDisposition;
      
      // 🔥 HTTP header 值中不能包含以下字符（即使是在引号内）：
      // - 非ASCII字符（\x80-\xFF）
      // - 控制字符（\x00-\x1F，除了 \t）
      // - 某些特殊字符：\x7F, \x22 (双引号), \x5C (反斜杠)
      
      // 检查是否包含非ASCII字符
      const hasNonASCII = /[^\x00-\x7F]/.test(cleanedFilename);
      
      if (hasNonASCII) {
        // 包含非ASCII字符：必须使用RFC 5987格式
        // fallback filename 只包含安全的ASCII字符（字母、数字、点、下划线、连字符）
        const asciiFallback = cleanedFilename
          .replace(/[^\x00-\x7F]/g, '_')  // 非ASCII字符替换为下划线
          .replace(/[^a-zA-Z0-9._-]/g, '_') // 其他特殊字符替换为下划线
          .substring(0, 100); // 限制长度，避免header过长
        
        // RFC 5987格式：filename*=charset'lang'value
        // 其中 charset 是 UTF-8，lang 是空字符串，value 是 URL编码的文件名
        const encodedFilename = encodeURIComponent(cleanedFilename);
        
        // 确保 header 值中没有任何非法字符
        // 移除所有控制字符和非法字符
        const safeHeader = `attachment; filename="${asciiFallback}"; filename*=UTF-8''${encodedFilename}`
          .replace(/[\x00-\x1F\x7F]/g, '') // 移除控制字符
          .replace(/[\x22\x5C]/g, ''); // 移除双引号和反斜杠（虽然不应该出现）
        
        contentDisposition = safeHeader;
      } else {
        // 只包含ASCII字符：检查是否有需要转义的特殊字符
        // HTTP header 值中的双引号需要转义，但 filename 参数的值在引号内，所以引号本身需要转义
        const safeFilename = cleanedFilename
          .replace(/"/g, '\\"')  // 转义双引号
          .replace(/\\/g, '\\\\') // 转义反斜杠
          .replace(/[\x00-\x1F\x7F]/g, ''); // 移除控制字符
        
        contentDisposition = `attachment; filename="${safeFilename}"`;
      }
      
      res.setHeader('Content-Disposition', contentDisposition);
      res.setHeader('Content-Type', 'application/octet-stream');
      res.setHeader('X-File-Metadata', JSON.stringify(file));
    };

    if (checkOpenAIStorage(file.source)) {
      req.body = { model: file.model };
      const endpointMap = {
        [FileSources.openai]: EModelEndpoint.assistants,
        [FileSources.azure]: EModelEndpoint.azureAssistants,
      };
      const { openai } = await getOpenAIClient({
        req,
        res,
        overrideEndpoint: endpointMap[file.source],
      });
      logger.debug(`Downloading file ${file_id} from OpenAI`);
      const passThrough = await getDownloadStream(file_id, openai);
      setHeaders();
      logger.debug(`File ${file_id} downloaded from OpenAI`);

      // Handle both Node.js and Web streams
      const stream =
        passThrough.body && typeof passThrough.body.getReader === 'function'
          ? Readable.fromWeb(passThrough.body)
          : passThrough.body;

      stream.pipe(res);
    } else {
      const fileStream = await getDownloadStream(req, file.filepath);

      fileStream.on('error', (streamError) => {
        logger.error('[DOWNLOAD ROUTE] Stream error:', streamError);
      });

      setHeaders();
      fileStream.pipe(res);
    }
  } catch (error) {
    logger.error('[DOWNLOAD ROUTE] Error downloading file:', error);
    res.status(500).send('Error downloading file');
  }
});

/**
 * GET /files/text/:userId/:file_id
 * 获取文件的文本内容（用于已向量化的文件）
 * 返回解析后的文本内容，而不是原始文件流
 */
router.get('/text/:userId/:file_id', fileAccess, async (req, res) => {
  try {
    const { userId, file_id } = req.params;
    logger.debug(`File text content requested by user ${userId}: ${file_id}`);

    // Access already validated by fileAccess middleware
    const file = req.fileAccess.file;

    // 如果文件已经向量化，从向量数据库获取文本内容
    if (file.embedded) {
      try {
        const VectorDBService = require('~/server/services/RAG/VectorDBService');
        const vectorDBService = new VectorDBService();
        await vectorDBService.initialize();
        const pool = vectorDBService.getPool();

        // 查询该文件的所有chunks
        const result = await pool.query(
          `SELECT chunk_index, content, metadata 
           FROM file_vectors 
           WHERE file_id = $1 
           ORDER BY chunk_index ASC`,
          [file_id]
        );

        if (result.rows && result.rows.length > 0) {
          // 合并所有chunks的文本内容
          const chunks = result.rows.map(row => ({
            index: row.chunk_index,
            content: row.content,
            metadata: row.metadata,
          }));

          // 按chunk_index排序并合并
          chunks.sort((a, b) => a.index - b.index);
          const fullText = chunks.map(chunk => chunk.content).join('\n\n');

          // 🔥 显式设置 UTF-8 charset
          res.setHeader('Content-Type', 'text/plain; charset=utf-8');
          res.status(200).send(fullText);
          return;
        }
      } catch (vectorError) {
        logger.warn(`[files/text] Failed to get text from vector DB for file ${file_id}, falling back to file download:`, vectorError.message);
        // 如果向量数据库查询失败，回退到原始文件解析
      }
    }

    // 如果文件未向量化或向量数据库查询失败，尝试解析原始文件
    try {
      const { parseText } = require('@because/api');
      const { getStrategyFunctions } = require('~/server/services/Files/strategies');
      const { getDownloadStream } = getStrategyFunctions(file.source);
      
      if (!getDownloadStream) {
        return res.status(501).json({ error: 'File text extraction not supported for this file source' });
      }

      // 获取文件流并解析为文本
      const fileStream = await getDownloadStream(req, file.filepath);
      const fs = require('fs');
      const path = require('path');
      const os = require('os');
      
      // 将流保存到临时文件
      const tempPath = path.join(os.tmpdir(), `file_${file_id}_${Date.now()}`);
      const writeStream = fs.createWriteStream(tempPath);
      
      await new Promise((resolve, reject) => {
        fileStream.pipe(writeStream);
        fileStream.on('end', resolve);
        fileStream.on('error', reject);
        writeStream.on('error', reject);
      });

      // 解析文件为文本
      const mockFile = {
        path: tempPath,
        originalname: file.filename,
        mimetype: file.type,
        size: file.bytes,
      };

      const { text } = await parseText({ req, file: mockFile, file_id: file_id });

      // 清理临时文件
      try {
        fs.unlinkSync(tempPath);
      } catch (e) {
        logger.warn(`[files/text] Failed to delete temp file: ${tempPath}`, e.message);
      }

      // 🔥 显式设置 UTF-8 charset
      res.setHeader('Content-Type', 'text/plain; charset=utf-8');
      res.status(200).send(text);
    } catch (parseError) {
      logger.error(`[files/text] Failed to parse file ${file_id}:`, parseError);
      res.status(500).json({ 
        error: '无法解析文件内容',
        message: parseError.message 
      });
    }
  } catch (error) {
    logger.error('[files/text] Error getting file text content:', error);
    res.status(500).json({ 
      error: '获取文件文本内容失败',
      message: error.message 
    });
  }
});

router.post('/', async (req, res) => {
  const metadata = req.body;
  let cleanup = true;

  try {
    filterFile({ req });

    metadata.temp_file_id = metadata.file_id;
    metadata.file_id = req.file_id;

    if (isAssistantsEndpoint(metadata.endpoint)) {
      return await processFileUpload({ req, res, metadata });
    }

    return await processAgentFileUpload({ req, res, metadata });
  } catch (error) {
    let message = 'Error processing file';
    logger.error('[/files] Error processing file:', error);
    logger.error('[/files] Error stack:', error.stack);
    logger.error('[/files] Error details:', {
      message: error.message,
      name: error.name,
      code: error.code,
      file: req.file?.originalname,
      file_id: req.file_id,
    });

    if (error.message?.includes('file_ids')) {
      message += ': ' + error.message;
    }

    if (
      error.message?.includes('Invalid file format') ||
      error.message?.includes('No OCR result') ||
      error.message?.includes('exceeds token limit')
    ) {
      message = error.message;
    }

    // 传递更详细的错误信息（特别是向量化相关的错误）
    if (
      error.message?.includes('向量化') || 
      error.message?.includes('embedding') || 
      error.message?.includes('ONNX') ||
      error.message?.includes('本地文件向量化失败') ||
      error.message?.includes('文件存储失败') ||
      error.message?.includes('文件向量化失败')
    ) {
      message = error.message;
    }
    
    // 如果是未知错误，至少传递错误消息
    if (message === 'Error processing file' && error.message) {
      message = error.message;
    }
    
    // 特别处理 file_id 相关的错误
    if (error.message?.includes('No file_id provided') || error.message?.includes('file_id')) {
      message = error.message;
    }

    try {
      await fs.unlink(req.file.path);
      cleanup = false;
    } catch (error) {
      logger.error('[/files] Error deleting file:', error);
    }
    res.status(500).json({ message });
  } finally {
    if (cleanup) {
      try {
        await fs.unlink(req.file.path);
      } catch (error) {
        logger.error('[/files] Error deleting file after file processing:', error);
      }
    } else {
      logger.debug('[/files] File processing completed without cleanup');
    }
  }
});

module.exports = router;
