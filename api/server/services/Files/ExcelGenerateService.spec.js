jest.mock('@because/data-schemas', () => ({
  logger: { info: jest.fn(), warn: jest.fn(), error: jest.fn(), debug: jest.fn() },
}));

jest.mock('@because/api', () => ({
  sanitizeFilename: (n) => n,
}));

jest.mock('~/models/File', () => ({
  createFile: jest.fn(),
}));

jest.mock('~/db/models', () => ({
  File: { findOneAndUpdate: jest.fn() },
}));

const { validateSpec, DEFAULT_LIMITS } = require('./ExcelGenerateService');

describe('ExcelGenerateService.validateSpec', () => {
  const validBody = () => ({
    fileName: 'test.xlsx',
    sheets: [
      {
        name: 'Sheet1',
        columns: ['A', 'B'],
        rows: [
          [1, 2],
          [3, 4],
        ],
      },
    ],
  });

  it('accepts minimal valid spec', () => {
    const b = validBody();
    const raw = Buffer.byteLength(JSON.stringify(b), 'utf8');
    const r = validateSpec(b, raw);
    expect(r.sheets).toHaveLength(1);
  });

  it('rejects empty sheets', () => {
    expect(() => validateSpec({ sheets: [] }, 10)).toThrow(/非空数组/);
  });

  it('pads short rows and truncates long rows to match column count', () => {
    const b = validBody();
    b.sheets[0].rows = [[1], [10, 20, 30]];
    const raw = Buffer.byteLength(JSON.stringify(b), 'utf8');
    const r = validateSpec(b, raw);
    expect(r.sheets[0].rows[0]).toEqual([1, null]);
    expect(r.sheets[0].rows[1]).toEqual([10, 20]);
  });

  it('rejects oversized body', () => {
    const b = validBody();
    expect(() => validateSpec(b, DEFAULT_LIMITS.maxBodyBytes + 1)).toThrow(/请求体超过限制/);
  });
});
