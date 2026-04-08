const fs = require('fs');
const { logger } = require('@because/data-schemas');


class ExcelParseService {
  constructor() {
    this.XLSX = null;
    this.initialized = false;
  }

  async initialize() {
    if (this.initialized) {
      return;
    }

    try {
      this.XLSX = require('xlsx');
      this.initialized = true;
      logger.info('[ExcelParseService] xlsx (SheetJS) 加载成功');
    } catch (error) {
      if (error.code === 'MODULE_NOT_FOUND') {
        logger.error('[ExcelParseService] xlsx 未安装');
        throw new Error('Excel解析库未安装。请安装：npm install xlsx');
      }
      throw error;
    }
  }

  sanitizeText(text) {
    if (!text) return '';

    return text
      .replace(/\u0000/g, '')
      .replace(/[\u0001-\u0008\u000B\u000C\u000E-\u001F\u007F]/g, '')
      .replace(/\uFFFD/g, '');
  }

  /**
   * 解析 Excel 文件（.xlsx / .xls）
   * 将所有 sheet 转为可读文本（Markdown 表格格式）
   *
   * @param {string|Buffer} excelPathOrBuffer - 文件路径或 Buffer
   * @returns {Promise<{text: string, metadata: Object}>}
   */
  async parseExcel(excelPathOrBuffer) {
    if (!this.initialized) {
      await this.initialize();
    }

    try {
      let buffer;

      if (Buffer.isBuffer(excelPathOrBuffer)) {
        buffer = excelPathOrBuffer;
      } else if (typeof excelPathOrBuffer === 'string') {
        buffer = fs.readFileSync(excelPathOrBuffer);
      } else {
        throw new Error('无效的Excel输入：必须是文件路径或Buffer');
      }

      const workbook = this.XLSX.read(buffer, {
        type: 'buffer',
        cellDates: true,
        cellNF: true,
        cellText: true,
      });

      if (!workbook || !workbook.SheetNames || workbook.SheetNames.length === 0) {
        throw new Error('Excel解析失败：工作簿为空或无工作表');
      }

      const sheetTexts = [];
      const sheetMeta = [];

      for (const sheetName of workbook.SheetNames) {
        const sheet = workbook.Sheets[sheetName];
        if (!sheet) continue;

        const range = this.XLSX.utils.decode_range(sheet['!ref'] || 'A1');
        const rowCount = range.e.r - range.s.r + 1;
        const colCount = range.e.c - range.s.c + 1;

        const jsonData = this.XLSX.utils.sheet_to_json(sheet, {
          header: 1,
          defval: '',
          blankrows: false,
          raw: false,
        });

        if (!jsonData || jsonData.length === 0) continue;

        const mdTable = this._jsonToMarkdownTable(jsonData, sheetName);
        if (mdTable) {
          sheetTexts.push(mdTable);
          sheetMeta.push({
            sheet_name: sheetName,
            rows: rowCount,
            cols: colCount,
            data_rows: jsonData.length,
          });
        }
      }

      if (sheetTexts.length === 0) {
        throw new Error('Excel解析失败：所有工作表均为空');
      }

      const fullText = sheetTexts.join('\n\n');

      const metadata = {
        parse_method: 'xlsx-sheetjs',
        sheet_count: workbook.SheetNames.length,
        sheets: sheetMeta,
      };

      return {
        text: this.sanitizeText(fullText),
        metadata,
      };
    } catch (error) {
      logger.error('[ExcelParseService] Excel解析失败:', error);
      throw new Error(`Excel解析失败: ${error.message}`);
    }
  }

  /**
   * 将二维数组转为 Markdown 表格文本
   * 第一行作为表头，其余为数据行
   */
  _jsonToMarkdownTable(jsonData, sheetName) {
    if (!jsonData || jsonData.length === 0) return '';

    const lines = [];
    lines.push(`### Sheet: ${sheetName}`);
    lines.push('');

    const headers = jsonData[0].map(h => String(h ?? '').trim());
    const hasHeaders = headers.some(h => h.length > 0);

    if (!hasHeaders && jsonData.length <= 1) return '';

    const colCount = Math.max(...jsonData.map(r => r.length));
    const normalizedHeaders = [];
    for (let i = 0; i < colCount; i++) {
      normalizedHeaders.push(headers[i] || `Col${i + 1}`);
    }

    lines.push('| ' + normalizedHeaders.join(' | ') + ' |');
    lines.push('| ' + normalizedHeaders.map(() => '---').join(' | ') + ' |');

    const dataRows = jsonData.slice(1);
    for (const row of dataRows) {
      const cells = [];
      for (let i = 0; i < colCount; i++) {
        const val = row[i] != null ? String(row[i]).trim() : '';
        cells.push(val.replace(/\|/g, '\\|').replace(/\n/g, ' '));
      }
      lines.push('| ' + cells.join(' | ') + ' |');
    }

    return lines.join('\n');
  }

  cleanText(text) {
    if (!text) return '';

    return text
      .replace(/\n{3,}/g, '\n\n')
      .replace(/[ \t]+/g, ' ')
      .trim();
  }

  /**
   * 完整解析流程：Excel → 文本 → 清理 → 分块
   *
   * @param {string|Buffer} excelPathOrBuffer
   * @param {Object} options
   * @param {number} options.chunkSize
   * @param {number} options.chunkOverlap
   * @param {Object} options.fileMetadata
   * @returns {Promise<Array<{text: string, metadata: Object}>>}
   */
  async parseExcelDocument(excelPathOrBuffer, options = {}) {
    if (!this.initialized) {
      await this.initialize();
    }

    try {
      logger.info('[ExcelParseService] 开始解析Excel文件');
      const parseResult = await this.parseExcel(excelPathOrBuffer);

      logger.info('[ExcelParseService] 开始清理文本');
      const cleanedText = this.cleanText(parseResult.text);

      const {
        chunkSize = 1000,
        chunkOverlap = 150,
        fileMetadata = {},
      } = options;

      logger.info(`[ExcelParseService] 开始分块: chunkSize=${chunkSize}, chunkOverlap=${chunkOverlap}`);
      const chunks = this.chunkText(cleanedText, {
        chunkSize,
        chunkOverlap,
        fileMetadata: {
          ...fileMetadata,
          ...parseResult.metadata,
        },
      });

      logger.info(`[ExcelParseService] Excel解析完成: ${chunks.length} 个块`);
      return chunks;
    } catch (error) {
      logger.error('[ExcelParseService] Excel解析失败:', error);
      throw error;
    }
  }

  chunkText(text, options = {}) {
    const {
      chunkSize = 1000,
      chunkOverlap = 150,
      fileMetadata = {},
    } = options;

    if (!text || text.trim().length === 0) {
      return [];
    }

    const chunks = [];
    let startIndex = 0;

    const separators = [
      '\n\n',
      '\n',
      '。',
      '. ',
      '！',
      '! ',
      '？',
      '? ',
      '；',
      '; ',
      '，',
      ', ',
      ' ',
      '',
    ];

    while (startIndex < text.length) {
      let endIndex = Math.min(startIndex + chunkSize, text.length);
      let chunkText = text.slice(startIndex, endIndex);

      if (endIndex < text.length) {
        let bestSeparatorIndex = -1;

        for (const separator of separators) {
          if (separator === '') {
            bestSeparatorIndex = endIndex;
            break;
          }

          const index = chunkText.lastIndexOf(separator);
          if (index !== -1 && index > chunkText.length * 0.3) {
            const separatorEnd = index + separator.length;
            if (separatorEnd > bestSeparatorIndex) {
              bestSeparatorIndex = separatorEnd;
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
        chunkText = this.sanitizeText(chunkText);

        if (chunkText.length > 0) {
          chunks.push({
            text: chunkText,
            metadata: {
              ...fileMetadata,
              chunk_index: chunks.length,
              source: 'excel',
              parse_method: fileMetadata.parse_method || 'xlsx-sheetjs',
            },
          });
        }
      }

      if (chunks.length > 0) {
        const overlapStart = Math.max(0, endIndex - chunkOverlap);
        startIndex = overlapStart;
      } else {
        startIndex = endIndex;
      }

      if (startIndex >= text.length) break;
      if (startIndex === endIndex && endIndex < text.length) {
        startIndex = endIndex;
      }
    }

    return chunks;
  }
}

module.exports = ExcelParseService;
