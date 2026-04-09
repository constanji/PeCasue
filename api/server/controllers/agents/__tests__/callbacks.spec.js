const { Tools } = require('@because/data-provider');

// Mock all dependencies before requiring the module
jest.mock('nanoid', () => ({
  nanoid: jest.fn(() => 'mock-id'),
}));

jest.mock('@because/api', () => ({
  sendEvent: jest.fn(),
}));

jest.mock('@because/data-schemas', () => ({
  logger: {
    info: jest.fn(),
    warn: jest.fn(),
    error: jest.fn(),
    debug: jest.fn(),
  },
}));

jest.mock('@because/agents', () => ({
  EnvVar: { CODE_API_KEY: 'CODE_API_KEY' },
  Providers: { GOOGLE: 'google' },
  GraphEvents: {},
  getMessageId: jest.fn(),
  ToolEndHandler: jest.fn(),
  handleToolCalls: jest.fn(),
  ChatModelStreamHandler: jest.fn(),
}));

jest.mock('~/server/services/Files/Citations', () => ({
  processFileCitations: jest.fn(),
}));

jest.mock('~/server/services/Files/Code/process', () => ({
  processCodeOutput: jest.fn(),
}));

jest.mock('~/server/services/Tools/credentials', () => ({
  loadAuthValues: jest.fn(),
}));

jest.mock('~/server/services/Files/process', () => ({
  saveBase64Image: jest.fn(),
}));

jest.mock('~/models/File', () => ({
  findFileById: jest.fn(),
}));

describe('createToolEndCallback', () => {
  let req, res, artifactPromises, createToolEndCallback;
  let logger;

  beforeEach(() => {
    jest.clearAllMocks();

    // Get the mocked logger
    logger = require('@because/data-schemas').logger;

    // Now require the module after all mocks are set up
    const callbacks = require('../callbacks');
    createToolEndCallback = callbacks.createToolEndCallback;

    req = {
      user: { id: 'user123' },
    };
    res = {
      headersSent: false,
      write: jest.fn(),
    };
    artifactPromises = [];
  });

  describe('ui_resources artifact handling', () => {
    it('should process ui_resources artifact and return attachment when headers not sent', async () => {
      const toolEndCallback = createToolEndCallback({ req, res, artifactPromises });

      const output = {
        tool_call_id: 'tool123',
        artifact: {
          [Tools.ui_resources]: {
            data: {
              0: { type: 'button', label: 'Click me' },
              1: { type: 'input', placeholder: 'Enter text' },
            },
          },
        },
      };

      const metadata = {
        run_id: 'run456',
        thread_id: 'thread789',
      };

      await toolEndCallback({ output }, metadata);

      // Wait for all promises to resolve
      const results = await Promise.all(artifactPromises);

      // When headers are not sent, it returns attachment without writing
      expect(res.write).not.toHaveBeenCalled();

      const attachment = results[0];
      expect(attachment).toEqual({
        type: Tools.ui_resources,
        messageId: 'run456',
        toolCallId: 'tool123',
        conversationId: 'thread789',
        [Tools.ui_resources]: {
          0: { type: 'button', label: 'Click me' },
          1: { type: 'input', placeholder: 'Enter text' },
        },
      });
    });

    it('should write to response when headers are already sent', async () => {
      res.headersSent = true;
      const toolEndCallback = createToolEndCallback({ req, res, artifactPromises });

      const output = {
        tool_call_id: 'tool123',
        artifact: {
          [Tools.ui_resources]: {
            data: {
              0: { type: 'carousel', items: [] },
            },
          },
        },
      };

      const metadata = {
        run_id: 'run456',
        thread_id: 'thread789',
      };

      await toolEndCallback({ output }, metadata);
      const results = await Promise.all(artifactPromises);

      expect(res.write).toHaveBeenCalled();
      expect(results[0]).toEqual({
        type: Tools.ui_resources,
        messageId: 'run456',
        toolCallId: 'tool123',
        conversationId: 'thread789',
        [Tools.ui_resources]: {
          0: { type: 'carousel', items: [] },
        },
      });
    });

    it('should handle errors when processing ui_resources', async () => {
      const toolEndCallback = createToolEndCallback({ req, res, artifactPromises });

      // Mock res.write to throw an error
      res.headersSent = true;
      res.write.mockImplementation(() => {
        throw new Error('Write failed');
      });

      const output = {
        tool_call_id: 'tool123',
        artifact: {
          [Tools.ui_resources]: {
            data: {
              0: { type: 'test' },
            },
          },
        },
      };

      const metadata = {
        run_id: 'run456',
        thread_id: 'thread789',
      };

      await toolEndCallback({ output }, metadata);
      const results = await Promise.all(artifactPromises);

      expect(logger.error).toHaveBeenCalledWith(
        'Error processing artifact content:',
        expect.any(Error),
      );
      expect(results[0]).toBeNull();
    });

    it('should handle multiple artifacts including ui_resources', async () => {
      const toolEndCallback = createToolEndCallback({ req, res, artifactPromises });

      const output = {
        tool_call_id: 'tool123',
        artifact: {
          [Tools.ui_resources]: {
            data: {
              0: { type: 'chart', data: [] },
            },
          },
          [Tools.web_search]: {
            results: ['result1', 'result2'],
          },
        },
      };

      const metadata = {
        run_id: 'run456',
        thread_id: 'thread789',
      };

      await toolEndCallback({ output }, metadata);
      const results = await Promise.all(artifactPromises);

      // Both ui_resources and web_search should be processed
      expect(artifactPromises).toHaveLength(2);
      expect(results).toHaveLength(2);

      // Check ui_resources attachment
      const uiResourceAttachment = results.find((r) => r?.type === Tools.ui_resources);
      expect(uiResourceAttachment).toBeTruthy();
      expect(uiResourceAttachment[Tools.ui_resources]).toEqual({
        0: { type: 'chart', data: [] },
      });

      // Check web_search attachment
      const webSearchAttachment = results.find((r) => r?.type === Tools.web_search);
      expect(webSearchAttachment).toBeTruthy();
      expect(webSearchAttachment[Tools.web_search]).toEqual({
        results: ['result1', 'result2'],
      });
    });

    it('should not process artifacts when output has no artifacts', async () => {
      const toolEndCallback = createToolEndCallback({ req, res, artifactPromises });

      const output = {
        tool_call_id: 'tool123',
        content: 'Some regular content',
        // No artifact property
      };

      const metadata = {
        run_id: 'run456',
        thread_id: 'thread789',
      };

      await toolEndCallback({ output }, metadata);

      expect(artifactPromises).toHaveLength(0);
      expect(res.write).not.toHaveBeenCalled();
    });
  });

  describe('generate_excel artifact handling', () => {
    it('should attach file metadata when generate_excel artifact has file_id', async () => {
      const { findFileById } = require('~/models/File');
      findFileById.mockResolvedValue({
        file_id: '76396f18-48ce-4693-908f-e80074f12301',
        filename: 'summary.xlsx',
        filepath: '/uploads/user123/76396f18-48ce-4693-908f-e80074f12301__summary.xlsx',
        type: 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        user: 'user123',
      });

      const toolEndCallback = createToolEndCallback({ req, res, artifactPromises });

      const output = {
        tool_call_id: 'call-gen-1',
        artifact: {
          generate_excel: {
            file_id: '76396f18-48ce-4693-908f-e80074f12301',
            filename: 'summary.xlsx',
            size: 2048,
            mime: 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
          },
        },
      };

      const metadata = {
        run_id: 'run456',
        thread_id: 'thread789',
      };

      await toolEndCallback({ output }, metadata);
      const results = await Promise.all(artifactPromises);

      expect(findFileById).toHaveBeenCalledWith('76396f18-48ce-4693-908f-e80074f12301', {
        user: 'user123',
      });
      expect(results[0]).toMatchObject({
        file_id: '76396f18-48ce-4693-908f-e80074f12301',
        filepath: expect.any(String),
        messageId: 'run456',
        toolCallId: 'call-gen-1',
        conversationId: 'thread789',
      });
    });

    it('should attach from tool content JSON when artifact is missing (stream ToolMessage)', async () => {
      const { findFileById } = require('~/models/File');
      findFileById.mockResolvedValue({
        file_id: '779cf9d5-0947-4940-85e0-777722a6fd76',
        filename: '归纳.xlsx',
        filepath: '/uploads/user123/779cf9d5__归纳.xlsx',
        user: 'user123',
      });

      const toolEndCallback = createToolEndCallback({ req, res, artifactPromises });

      const output = {
        name: 'generate_excel',
        tool_call_id: 'call-2',
        content: JSON.stringify({
          success: true,
          file_id: '779cf9d5-0947-4940-85e0-777722a6fd76',
          filename: '归纳.xlsx',
          mime: 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        }),
      };

      await toolEndCallback({ output }, {
        run_id: 'run456',
        thread_id: 'thread789',
      });
      const results = await Promise.all(artifactPromises);

      expect(findFileById).toHaveBeenCalledWith('779cf9d5-0947-4940-85e0-777722a6fd76', {
        user: 'user123',
      });
      expect(results[0]).toMatchObject({
        file_id: '779cf9d5-0947-4940-85e0-777722a6fd76',
        messageId: 'run456',
        toolCallId: 'call-2',
      });
    });

    it('should attach when output is a plain string (no ToolMessage wrapper)', async () => {
      const { findFileById } = require('~/models/File');
      findFileById.mockResolvedValue({
        file_id: 'plain-str-fid',
        filename: 'plain.xlsx',
        filepath: '/uploads/user123/plain.xlsx',
        user: 'user123',
      });

      const toolEndCallback = createToolEndCallback({ req, res, artifactPromises });

      const rawContent = JSON.stringify({
        success: true,
        file_id: 'plain-str-fid',
        filename: 'plain.xlsx',
        mime: 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
      });

      await toolEndCallback({ output: rawContent }, {
        run_id: 'run456',
        thread_id: 'thread789',
      });
      const results = await Promise.all(artifactPromises);

      expect(findFileById).toHaveBeenCalledWith('plain-str-fid', { user: 'user123' });
      expect(results[0]).toMatchObject({
        file_id: 'plain-str-fid',
        messageId: 'run456',
      });
    });
  });

  describe('edge cases', () => {
    it('should handle empty ui_resources data object', async () => {
      const toolEndCallback = createToolEndCallback({ req, res, artifactPromises });

      const output = {
        tool_call_id: 'tool123',
        artifact: {
          [Tools.ui_resources]: {
            data: {},
          },
        },
      };

      const metadata = {
        run_id: 'run456',
        thread_id: 'thread789',
      };

      await toolEndCallback({ output }, metadata);
      const results = await Promise.all(artifactPromises);

      expect(results[0]).toEqual({
        type: Tools.ui_resources,
        messageId: 'run456',
        toolCallId: 'tool123',
        conversationId: 'thread789',
        [Tools.ui_resources]: {},
      });
    });

    it('should handle ui_resources with complex nested data', async () => {
      const toolEndCallback = createToolEndCallback({ req, res, artifactPromises });

      const complexData = {
        0: {
          type: 'form',
          fields: [
            { name: 'field1', type: 'text', required: true },
            { name: 'field2', type: 'select', options: ['a', 'b', 'c'] },
          ],
          nested: {
            deep: {
              value: 123,
              array: [1, 2, 3],
            },
          },
        },
      };

      const output = {
        tool_call_id: 'tool123',
        artifact: {
          [Tools.ui_resources]: {
            data: complexData,
          },
        },
      };

      const metadata = {
        run_id: 'run456',
        thread_id: 'thread789',
      };

      await toolEndCallback({ output }, metadata);
      const results = await Promise.all(artifactPromises);

      expect(results[0][Tools.ui_resources]).toEqual(complexData);
    });

    it('should handle when output is undefined', async () => {
      const toolEndCallback = createToolEndCallback({ req, res, artifactPromises });

      const metadata = {
        run_id: 'run456',
        thread_id: 'thread789',
      };

      await toolEndCallback({ output: undefined }, metadata);

      expect(artifactPromises).toHaveLength(0);
      expect(res.write).not.toHaveBeenCalled();
    });

    it('should handle when data parameter is undefined', async () => {
      const toolEndCallback = createToolEndCallback({ req, res, artifactPromises });

      const metadata = {
        run_id: 'run456',
        thread_id: 'thread789',
      };

      await toolEndCallback(undefined, metadata);

      expect(artifactPromises).toHaveLength(0);
      expect(res.write).not.toHaveBeenCalled();
    });
  });
});
