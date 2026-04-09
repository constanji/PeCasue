const { Tool } = require('@langchain/core/tools');
const { z } = require('zod');
const { logger } = require('@because/data-schemas');
const { generateFromSpec } = require('~/server/services/Files/ExcelGenerateService');
const { findFileById } = require('~/models/File');

const sheetSchema = z.object({
  name: z.string().min(1).max(31).describe('工作表名称（1～31 字符，符合 Excel 限制）'),
  columns: z.array(z.string()).min(1).describe('表头列名，与每行 cells 数量一致'),
  rows: z.array(z.array(z.any())).describe('数据行，每行长度须与 columns 一致'),
  styles: z.record(z.string(), z.any()).optional().describe('可选：freezeFirstRow、headerBold、headerBg、columnWidths、numberFormats 等'),
  formulas: z
    .array(
      z.object({
        ref: z.string().describe('单元格引用，如 A2'),
        formula: z.string().describe('以 = 开头的公式字符串'),
      }),
    )
    .optional(),
});

/**
 * Agent 工具：根据 JSON 规范生成 .xlsx，写入用户文件空间并返回 file_id。
 *
 * 附件推送采用「工具内直推」策略——不依赖 on_tool_end → createToolEndCallback 管线
 * （LangGraph streamEvents 对 ToolMessage.artifact 的传递不稳定），
 * 而是在 _call 内直接写 SSE `event: attachment` 并 push 到 req._artifactPromises。
 */
class GenerateExcel extends Tool {
  name = 'generate_excel';

  description =
    '根据结构化 JSON 规范生成 Excel（.xlsx）文件并保存到当前用户空间。' +
    '返回 file_id、文件名、大小与 MIME；前端或后续流程可用 file_id 通过文件下载接口获取原始文件。' +
    '每个 sheet 需包含 name、columns（表头）、rows（二维数组，列数与 columns 一致）；可选 ttlHours 控制过期时间（小时）、context 写入文件元数据。';

  schema = z.object({
    sheets: z.array(sheetSchema).min(1).describe('至少一个工作表的规范数组'),
    fileName: z
      .string()
      .optional()
      .describe('生成文件名（可不含 .xlsx，服务端会补全并做安全处理）；默认 export.xlsx'),
    ttlHours: z
      .number()
      .int()
      .min(1)
      .optional()
      .describe('可选：文件保留小时数（1～8760），不设则走服务端默认过期策略'),
    context: z
      .record(z.string(), z.unknown())
      .optional()
      .describe('可选：任意 JSON 对象，写入文件 metadata，便于追溯生成原因'),
  });

  constructor(fields = {}) {
    super();
    this.req = fields.req;
    this.res = fields.res;
  }

  /**
   * 工具内部直接推送附件到 SSE 流 + artifactPromises（绕过 on_tool_end 管线）。
   * @param {object} file       findFileById 返回的完整 Mongo 文档
   * @param {string} messageId  助手消息 ID（configurable.requestBody.messageId）
   * @param {string} conversationId
   * @param {string} toolCallId
   */
  _emitFileAttachment(file, messageId, conversationId, toolCallId) {
    const attachment = Object.assign({}, file, {
      messageId,
      conversationId,
      toolCallId,
    });

    if (this.res) {
      try {
        this.res.write(`event: attachment\ndata: ${JSON.stringify(attachment)}\n\n`);
        logger.info(
          `[GenerateExcel] SSE attachment 已写入 file_id=${file.file_id} messageId=${messageId} toolCallId=${toolCallId}`,
        );
      } catch (err) {
        logger.warn(`[GenerateExcel] SSE attachment 写入失败: ${err.message}`);
      }
    }

    const artifactPromises = this.req?._artifactPromises;
    if (Array.isArray(artifactPromises)) {
      artifactPromises.push(Promise.resolve(attachment));
    }

    if (!this.req._pushedExcelFileIds) {
      this.req._pushedExcelFileIds = new Set();
    }
    this.req._pushedExcelFileIds.add(file.file_id);
  }

  /**
   * @override
   * 接收三个参数以获取 LangGraph 运行时 config（第三参）。
   * 仍返回 [content, artifact]（content_and_artifact 格式），artifact 作为 on_tool_end 的后备。
   * @returns {Promise<[string, object|undefined]>}
   */
  async _call(input, runManagerOrConfig, _config) {
    // @langchain/core 的 tool() 包装器只传 (input, childConfig) 两个参数,
    // config 在第二参而非第三参
    const config = _config ?? runManagerOrConfig;

    if (!this.req?.user?.id) {
      const content = JSON.stringify(
        { success: false, error: '缺少已登录用户上下文，无法生成文件。' },
        null,
        2,
      );
      return [content, undefined];
    }

    try {
      const result = await generateFromSpec(this.req, input);
      logger.info('[GenerateExcel] 文件生成完成', { file_id: result.file_id, filename: result.filename });

      const configurable = config?.configurable ?? {};
      const messageId =
        configurable.requestBody?.messageId ?? configurable.run_id;
      const conversationId = configurable.thread_id;
      const toolCallId = config?.toolCall?.id ?? '';

      logger.info(
        `[GenerateExcel] config 诊断 hasConfig=${!!config} messageId=${messageId ?? 'null'} ` +
        `conversationId=${conversationId ?? 'null'} toolCallId=${toolCallId || 'null'} ` +
        `configurableKeys=[${Object.keys(configurable)}]`,
      );

      if (messageId) {
        try {
          const file = await findFileById(result.file_id, { user: this.req.user.id });
          if (file?.filepath) {
            this._emitFileAttachment(file, messageId, conversationId, toolCallId);
          } else {
            logger.warn(`[GenerateExcel] findFileById 未找到或缺少 filepath file_id=${result.file_id}`);
          }
        } catch (err) {
          logger.error(`[GenerateExcel] 附件推送失败: ${err.message}`);
        }
      } else {
        logger.warn('[GenerateExcel] 无法获取 messageId，跳过附件直推');
      }

      const content = JSON.stringify(
        {
          success: true,
          file_id: result.file_id,
          filename: result.filename,
          size: result.size,
          mime: result.mime,
          note: '文件已作为对话附件展示，用户可直接预览或下载。',
        },
        null,
        2,
      );
      const artifact = {
        generate_excel: {
          file_id: result.file_id,
          filename: result.filename,
          size: result.size,
          mime: result.mime,
        },
      };
      return [content, artifact];
    } catch (error) {
      logger.error('[GenerateExcel] 生成失败', { message: error.message, stack: error.stack });
      const content = JSON.stringify(
        { success: false, error: error.message || 'Excel 生成失败', statusCode: error.statusCode },
        null,
        2,
      );
      return [content, undefined];
    }
  }
}

module.exports = GenerateExcel;
