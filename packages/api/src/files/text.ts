import axios from 'axios';
import FormData from 'form-data';
import { createReadStream } from 'fs';
import { logger } from '@because/data-schemas';
import { FileSources } from '@because/data-provider';
import type { ServerRequest } from '~/types';
import { logAxiosError, readFileAsString } from '~/utils';
import { generateShortLivedToken } from '~/crypto/jwt';

/**
 * Attempts to parse text using RAG API, falls back to native text parsing
 * @param params - The parameters object
 * @param params.req - The Express request object
 * @param params.file - The uploaded file
 * @param params.file_id - The file ID
 * @returns
 */
export async function parseText({
  req,
  file,
  file_id,
}: {
  req: ServerRequest;
  file: Express.Multer.File;
  file_id: string;
}): Promise<{ text: string; bytes: number; source: string }> {
  if (!process.env.RAG_API_URL) {
    logger.debug('[parseText] RAG_API_URL not defined, falling back to native text parsing');
    return parseTextNative(file);
  }

  const userId = req.user?.id;
  if (!userId) {
    logger.debug('[parseText] No user ID provided, falling back to native text parsing');
    return parseTextNative(file);
  }

  try {
    const healthResponse = await axios.get(`${process.env.RAG_API_URL}/health`, {
      timeout: 10000,
    });
    if (healthResponse?.statusText !== 'OK' && healthResponse?.status !== 200) {
      logger.debug('[parseText] RAG API health check failed, falling back to native parsing');
      return parseTextNative(file);
    }
  } catch (healthError) {
    logAxiosError({
      message: '[parseText] RAG API health check failed, falling back to native parsing:',
      error: healthError,
    });
    return parseTextNative(file);
  }

  try {
    const jwtToken = generateShortLivedToken(userId);
    const formData = new FormData();
    formData.append('file_id', file_id);
    formData.append('file', createReadStream(file.path));

    const formHeaders = formData.getHeaders();

    const response = await axios.post(`${process.env.RAG_API_URL}/text`, formData, {
      headers: {
        Authorization: `Bearer ${jwtToken}`,
        accept: 'application/json',
        ...formHeaders,
      },
      timeout: 300000,
    });

    const responseData = response.data;
    logger.debug(`[parseText] RAG API completed successfully (${response.status})`);

    if (!('text' in responseData)) {
      throw new Error('RAG API did not return parsed text');
    }

    return {
      text: responseData.text,
      bytes: Buffer.byteLength(responseData.text, 'utf8'),
      source: FileSources.text,
    };
  } catch (error) {
    logAxiosError({
      message: '[parseText] RAG API text parsing failed, falling back to native parsing',
      error,
    });
    return parseTextNative(file);
  }
}

/**
 * Native JavaScript text parsing fallback
 * Simple text file reading - complex formats handled by RAG API
 * @param file - The uploaded file
 * @returns
 */
export async function parseTextNative(file: Express.Multer.File): Promise<{
  text: string;
  bytes: number;
  source: string;
}> {
  if (!file || !file.path) {
    throw new Error('文件对象无效：缺少文件路径');
  }

  // 如果是PDF文件，尝试使用PDF解析服务
  // 注意：PDF文件的详细解析（包括分块和metadata）主要在 uploadVectorsLocal 函数中处理
  // 这里只做基本的文本提取，用于非向量化场景
  if (file.mimetype === 'application/pdf' || file.originalname?.toLowerCase().endsWith('.pdf')) {
    try {

      let PDFParseService;
      try {
        PDFParseService = require('~/server/services/RAG/PDFParseService');
      } catch (e) {
        const path = require('path');
        const serverPath = path.resolve(process.cwd(), 'api/server');
        PDFParseService = require(path.join(serverPath, 'services/RAG/PDFParseService'));
      }
      
      const pdfService = new PDFParseService();
      await pdfService.initialize();

      // 解析PDF
      const parseResult = await pdfService.parseTextPDF(file.path);
      
      logger.info(`[parseTextNative] PDF解析成功: ${parseResult.pages} 页, ${parseResult.text.length} 字符`);

      return {
        text: parseResult.text,
        bytes: Buffer.byteLength(parseResult.text, 'utf8'),
        source: FileSources.text,
      };
    } catch (pdfError) {
      logger.warn('[parseTextNative] PDF解析失败:', pdfError.message);
      // 如果PDF解析失败，抛出错误（PDF文件不能直接作为文本读取）
      throw new Error(`PDF解析失败: ${pdfError.message}`);
    }
  }

  // 如果是Word文件，尝试使用Word解析服务
  // 注意：Word文件的详细解析（包括分块和metadata）主要在 uploadVectorsLocal 函数中处理
  // 这里只做基本的文本提取，用于非向量化场景
  const isWord = file.mimetype === 'application/vnd.openxmlformats-officedocument.wordprocessingml.document' ||
                 file.mimetype === 'application/msword' ||
                 file.originalname?.toLowerCase().endsWith('.docx') ||
                 file.originalname?.toLowerCase().endsWith('.doc');
  
  if (isWord) {
    try {
      let WordParseService;
      try {
        WordParseService = require('~/server/services/RAG/WordParseService');
      } catch (e) {
        const path = require('path');
        const serverPath = path.resolve(process.cwd(), 'api/server');
        WordParseService = require(path.join(serverPath, 'services/RAG/WordParseService'));
      }
      
      const wordService = new WordParseService();
      await wordService.initialize();

      // 解析Word文档
      const parseResult = await wordService.parseWord(file.path);
      
      logger.info(`[parseTextNative] Word解析成功: ${parseResult.text.length} 字符`);

      return {
        text: parseResult.text,
        bytes: Buffer.byteLength(parseResult.text, 'utf8'),
        source: FileSources.text,
      };
    } catch (wordError) {
      logger.warn('[parseTextNative] Word解析失败:', wordError.message);
      // 如果Word解析失败，抛出错误（Word文件不能直接作为文本读取）
      throw new Error(`Word解析失败: ${wordError.message}`);
    }
  }


  try {
    const { content: text, bytes } = await readFileAsString(file.path, {
      fileSize: file.size,
    });

    return {
      text,
      bytes,
      source: FileSources.text,
    };
  } catch (error) {
    logger.error('[parseTextNative] 文件读取失败:', error);
    if (error.message?.includes('ENOENT') || error.message?.includes('not found')) {
      throw new Error(`文件不存在或已被删除: ${file.path}`);
    }
    throw error;
  }
}
