const fs = require('fs').promises;
const path = require('path');
const ExcelJS = require('exceljs');
const { v4 } = require('uuid');
const { logger } = require('@because/data-schemas');
const { FileSources, FileContext, removeNullishValues } = require('@because/data-provider');
const { sanitizeFilename } = require('@because/api');
const { File } = require('~/db/models');
const { createFile } = require('~/models/File');

const MIME_XLSX = 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet';

const DEFAULT_LIMITS = {
  maxSheets: 32,
  maxRowsPerSheet: 10000,
  maxCols: 256,
  maxColumnNameLength: 200,
  maxBodyBytes: Number(process.env.MAX_GENERATE_EXCEL_BODY_BYTES) || 2 * 1024 * 1024,
};

/** 仅允许常见 Excel 公式字符，防止注入 */
const FORMULA_SAFE = /^=[A-Za-z0-9_:(),.+\-*/\s"<>&%$]+$/;

function validationError(message, statusCode = 400) {
  const e = new Error(message);
  e.statusCode = statusCode;
  return e;
}

function validateFormulaString(f) {
  if (typeof f !== 'string' || f.length === 0) {
    return false;
  }
  const trimmed = f.trim();
  if (!trimmed.startsWith('=')) {
    return false;
  }
  return FORMULA_SAFE.test(trimmed) && trimmed.length <= 1024;
}

/**
 * @param {unknown} body
 * @param {number} rawBodyBytes
 */
function validateSpec(body, rawBodyBytes) {
  if (rawBodyBytes > DEFAULT_LIMITS.maxBodyBytes) {
    throw validationError(
      `请求体超过限制（${DEFAULT_LIMITS.maxBodyBytes} 字节）`,
      413,
    );
  }
  if (!body || typeof body !== 'object') {
    throw validationError('请求体必须为 JSON 对象');
  }
  const { sheets, fileName, ttlHours } = body;
  if (!Array.isArray(sheets) || sheets.length === 0) {
    throw validationError('sheets 必须为非空数组');
  }
  if (sheets.length > DEFAULT_LIMITS.maxSheets) {
    throw validationError(`工作表数量不能超过 ${DEFAULT_LIMITS.maxSheets}`);
  }
  if (ttlHours != null && (typeof ttlHours !== 'number' || ttlHours < 1 || ttlHours > 24 * 365)) {
    throw validationError('ttlHours 必须为 1～8760（小时）之间的数字');
  }

  for (let s = 0; s < sheets.length; s++) {
    const sh = sheets[s];
    if (!sh || typeof sh !== 'object') {
      throw validationError(`sheets[${s}] 无效`);
    }
    const name = typeof sh.name === 'string' ? sh.name.trim() : '';
    if (!name || name.length > 31) {
      throw validationError(`sheets[${s}].name 必填且长度 1～31（Excel 限制）`);
    }
    const cols = sh.columns;
    const rows = sh.rows;
    if (!Array.isArray(cols) || cols.length === 0) {
      throw validationError(`sheets[${s}].columns 必须为非空数组`);
    }
    if (cols.length > DEFAULT_LIMITS.maxCols) {
      throw validationError(`sheets[${s}] 列数不能超过 ${DEFAULT_LIMITS.maxCols}`);
    }
    for (const c of cols) {
      if (typeof c !== 'string' || c.length > DEFAULT_LIMITS.maxColumnNameLength) {
        throw validationError(
          `sheets[${s}].columns 每项须为字符串且长度 ≤ ${DEFAULT_LIMITS.maxColumnNameLength}`,
        );
      }
    }
    if (!Array.isArray(rows)) {
      throw validationError(`sheets[${s}].rows 必须为数组`);
    }
    if (rows.length > DEFAULT_LIMITS.maxRowsPerSheet) {
      throw validationError(
        `sheets[${s}] 行数不能超过 ${DEFAULT_LIMITS.maxRowsPerSheet}`,
      );
    }
    for (let r = 0; r < rows.length; r++) {
      const row = rows[r];
      if (!Array.isArray(row)) {
        throw validationError(`sheets[${s}].rows[${r}] 必须为数组`);
      }
      if (row.length < cols.length) {
        while (row.length < cols.length) {
          row.push(null);
        }
      } else if (row.length > cols.length) {
        rows[r] = row.slice(0, cols.length);
      }
    }
    if (Array.isArray(sh.formulas)) {
      for (let i = 0; i < sh.formulas.length; i++) {
        const f = sh.formulas[i];
        if (!f || typeof f.ref !== 'string' || typeof f.formula !== 'string') {
          throw validationError(`sheets[${s}].formulas[${i}] 须含 ref、formula 字符串`);
        }
        if (!validateFormulaString(f.formula)) {
          throw validationError(`sheets[${s}].formulas[${i}].formula 格式不合法`);
        }
      }
    }
  }

  return { sheets, fileName, ttlHours, context: body.context };
}

/**
 * @param {import('exceljs').Worksheet} ws
 * @param {object} styles
 * @param {number} colCount
 */
function applySheetStyles(ws, styles, colCount) {
  if (!styles || typeof styles !== 'object') {
    return;
  }
  if (styles.freezeFirstRow) {
    ws.views = [{ state: 'frozen', ySplit: 1 }];
  }
  if (Array.isArray(styles.columnWidths)) {
    styles.columnWidths.forEach((w, i) => {
      if (typeof w === 'number' && w > 0 && i < colCount) {
        ws.getColumn(i + 1).width = w;
      }
    });
  }
  if (styles.numberFormats && typeof styles.numberFormats === 'object') {
    for (const [key, fmt] of Object.entries(styles.numberFormats)) {
      if (typeof fmt !== 'string') {
        continue;
      }
      const col = /^\d+$/.test(key) ? parseInt(key, 10) : key;
      try {
        if (typeof col === 'number' && col >= 1 && col <= colCount) {
          ws.getColumn(col).numFmt = fmt;
        } else if (typeof col === 'string' && /^[A-Za-z]+$/.test(col)) {
          ws.getColumn(col).numFmt = fmt;
        }
      } catch (e) {
        logger.warn('[ExcelGenerateService] 跳过无效列格式', key, e.message);
      }
    }
  }
}

/**
 * @param {object} spec
 * @returns {Promise<Buffer>}
 */
async function buildWorkbookBuffer(spec) {
  const workbook = new ExcelJS.Workbook();
  for (const sheetSpec of spec.sheets) {
    const sheetName = sheetSpec.name.substring(0, 31);
    const ws = workbook.addWorksheet(sheetName);
    const headerRow = ws.addRow(sheetSpec.columns);
    if (sheetSpec.styles?.headerBold !== false) {
      headerRow.font = { ...(headerRow.font || {}), bold: true };
    }
    if (sheetSpec.styles?.headerBg && /^#[0-9A-Fa-f]{6}$/.test(sheetSpec.styles.headerBg)) {
      const argb = `FF${sheetSpec.styles.headerBg.slice(1)}`;
      headerRow.eachCell((cell) => {
        cell.fill = {
          type: 'pattern',
          pattern: 'solid',
          fgColor: { argb },
        };
      });
    }
    for (const row of sheetSpec.rows) {
      ws.addRow(row);
    }
    applySheetStyles(ws, sheetSpec.styles, sheetSpec.columns.length);

    if (Array.isArray(sheetSpec.formulas)) {
      for (const { ref, formula } of sheetSpec.formulas) {
        const cell = ws.getCell(ref);
        const f = formula.trim().startsWith('=') ? formula.trim().slice(1) : formula.trim();
        cell.value = { formula: f, result: undefined };
      }
    }
  }
  return workbook.xlsx.writeBuffer();
}

function ensureXlsxFilename(name) {
  let base = typeof name === 'string' && name.trim() ? name.trim() : 'export';
  base = base.replace(/[/\\]/g, '');
  if (!base.toLowerCase().endsWith('.xlsx')) {
    base += '.xlsx';
  }
  return sanitizeFilename(base);
}

/**
 * 从 JSON 规范生成 Excel 并入库（本地 uploads）。
 *
 * @param {import('express').Request} req
 * @param {object} body
 * @returns {Promise<{ file_id: string, filename: string, size: number, mime: string }>}
 */
async function generateFromSpec(req, body) {
  const rawBytes = Buffer.byteLength(JSON.stringify(body || {}), 'utf8');
  const spec = validateSpec(body, rawBytes);

  const file_id = v4();
  const filename = ensureXlsxFilename(spec.fileName);
  const buffer = await buildWorkbookBuffer(spec);
  const bytes = buffer.length;

  const appConfig = req.config;
  const { uploads } = appConfig.paths;
  const userPath = path.join(uploads, req.user.id);
  await fs.mkdir(userPath, { recursive: true });

  const diskName = `${file_id}__${filename}`;
  const absolutePath = path.join(userPath, diskName);
  await fs.writeFile(absolutePath, buffer);

  const filepath = path.posix.join('/', 'uploads', req.user.id, diskName);

  const fileInfo = removeNullishValues({
    user: req.user.id,
    file_id,
    bytes,
    filepath,
    filename,
    type: MIME_XLSX,
    source: FileSources.local,
    context: FileContext.message_attachment,
    embedded: false,
    metadata: {
      generatedExcel: true,
      context: spec.context ?? undefined,
    },
  });

  /** 无 ttlHours 时使用默认 1h 过期（与 createFile 行为一致）；有 ttlHours 则先禁用默认再写入自定义 expiresAt */
  await createFile(fileInfo, spec.ttlHours != null);

  if (spec.ttlHours != null) {
    const expiresAt = new Date(Date.now() + spec.ttlHours * 3600 * 1000);
    await File.findOneAndUpdate({ file_id }, { $set: { expiresAt } });
  }

  logger.info(`[ExcelGenerateService] 已生成 Excel file_id=${file_id} bytes=${bytes}`);

  return {
    file_id,
    filename,
    size: bytes,
    mime: MIME_XLSX,
  };
}

module.exports = {
  generateFromSpec,
  validateSpec,
  DEFAULT_LIMITS,
};
