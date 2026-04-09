jest.mock('@because/data-schemas', () => ({
  logger: { info: jest.fn(), warn: jest.fn(), error: jest.fn(), debug: jest.fn() },
}));

jest.mock('~/server/services/Files/ExcelGenerateService', () => ({
  generateFromSpec: jest.fn(),
}));

jest.mock('~/models/File', () => ({
  findFileById: jest.fn(),
}));

const { generateFromSpec } = require('~/server/services/Files/ExcelGenerateService');
const { findFileById } = require('~/models/File');
const GenerateExcel = require('./GenerateExcel');

const minimalInput = {
  sheets: [
    {
      name: 'S1',
      columns: ['A'],
      rows: [[1]],
    },
  ],
};

describe('GenerateExcel tool', () => {
  beforeEach(() => {
    generateFromSpec.mockReset();
    findFileById.mockReset();
  });

  it('returns JSON error when req.user is missing', async () => {
    const tool = new GenerateExcel({});
    const out = await tool._call(minimalInput);
    expect(Array.isArray(out)).toBe(true);
    const parsed = JSON.parse(out[0]);
    expect(parsed.success).toBe(false);
    expect(parsed.error).toMatch(/缺少/);
    expect(out[1]).toBeUndefined();
    expect(generateFromSpec).not.toHaveBeenCalled();
  });

  it('calls generateFromSpec and returns file_id + artifact', async () => {
    generateFromSpec.mockResolvedValue({
      file_id: 'fid-1',
      filename: 'a.xlsx',
      size: 100,
      mime: 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
    });
    const tool = new GenerateExcel({
      req: { user: { id: 'user-1' } },
    });
    const out = await tool._call(minimalInput);
    expect(Array.isArray(out)).toBe(true);
    const parsed = JSON.parse(out[0]);
    expect(parsed.success).toBe(true);
    expect(parsed.file_id).toBe('fid-1');
    expect(out[1].generate_excel.file_id).toBe('fid-1');
    expect(generateFromSpec).toHaveBeenCalledWith(
      expect.objectContaining({ user: { id: 'user-1' } }),
      minimalInput,
    );
  });

  it('returns JSON error when generateFromSpec throws', async () => {
    generateFromSpec.mockRejectedValue(Object.assign(new Error('bad spec'), { statusCode: 400 }));
    const tool = new GenerateExcel({
      req: { user: { id: 'user-1' } },
    });
    const out = await tool._call(minimalInput);
    const parsed = JSON.parse(out[0]);
    expect(parsed.success).toBe(false);
    expect(parsed.error).toBe('bad spec');
    expect(parsed.statusCode).toBe(400);
    expect(out[1]).toBeUndefined();
  });

  it('writes SSE attachment and pushes to _artifactPromises when config has messageId', async () => {
    generateFromSpec.mockResolvedValue({
      file_id: 'fid-2',
      filename: 'report.xlsx',
      size: 500,
      mime: 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
    });
    findFileById.mockResolvedValue({
      file_id: 'fid-2',
      filename: 'report.xlsx',
      filepath: '/uploads/user-1/fid-2__report.xlsx',
      user: 'user-1',
    });

    const artifactPromises = [];
    const resWrite = jest.fn();
    const tool = new GenerateExcel({
      req: { user: { id: 'user-1' }, _artifactPromises: artifactPromises },
      res: { write: resWrite },
    });

    const config = {
      configurable: {
        requestBody: { messageId: 'msg-abc' },
        thread_id: 'conv-xyz',
      },
      toolCall: { id: 'call_123' },
    };

    // @langchain/core tool() wrapper 只传 (input, childConfig), config 在第二参
    const out = await tool._call(minimalInput, config);

    expect(resWrite).toHaveBeenCalledTimes(1);
    const written = resWrite.mock.calls[0][0];
    expect(written).toMatch(/^event: attachment\ndata: /);
    const payload = JSON.parse(written.replace('event: attachment\ndata: ', '').replace('\n\n', ''));
    expect(payload.file_id).toBe('fid-2');
    expect(payload.messageId).toBe('msg-abc');
    expect(payload.conversationId).toBe('conv-xyz');
    expect(payload.toolCallId).toBe('call_123');

    expect(artifactPromises).toHaveLength(1);
    const resolved = await artifactPromises[0];
    expect(resolved.file_id).toBe('fid-2');
    expect(resolved.toolCallId).toBe('call_123');

    expect(tool.req._pushedExcelFileIds).toBeInstanceOf(Set);
    expect(tool.req._pushedExcelFileIds.has('fid-2')).toBe(true);

    const parsed = JSON.parse(out[0]);
    expect(parsed.success).toBe(true);
    expect(out[1].generate_excel.file_id).toBe('fid-2');
  });

  it('skips SSE when config has no messageId', async () => {
    generateFromSpec.mockResolvedValue({
      file_id: 'fid-3',
      filename: 'x.xlsx',
      size: 10,
      mime: 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
    });

    const resWrite = jest.fn();
    const tool = new GenerateExcel({
      req: { user: { id: 'user-1' } },
      res: { write: resWrite },
    });

    const out = await tool._call(minimalInput, {});

    expect(resWrite).not.toHaveBeenCalled();
    expect(findFileById).not.toHaveBeenCalled();
    const parsed = JSON.parse(out[0]);
    expect(parsed.success).toBe(true);
  });
});
