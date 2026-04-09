import { useCallback, useState } from 'react';
import { useNavigate } from 'react-router-dom';
import { ChevronLeft, Loader2 } from 'lucide-react';
import {
  Button,
  OGDialog,
  OGDialogContent,
  OGDialogHeader,
  OGDialogTitle,
} from '@because/client';
import { dataService } from '@because/data-provider';
import { useAuthContext } from '~/hooks/AuthContext';
import useAuthRedirect from './useAuthRedirect';
import SpreadsheetNativeViewer from '~/components/Chat/Messages/Content/Parts/FilePreview/SpreadsheetNativeViewer';

const DEFAULT_SPEC = `{
  "fileName": "demo-from-web.xlsx",
  "sheets": [
    {
      "name": "销售",
      "columns": ["区域", "金额", "备注"],
      "rows": [
        ["华东", 1200, "Q1"],
        ["华北", 800, "Q1"]
      ],
      "styles": { "freezeFirstRow": true, "headerBold": true }
    }
  ]
}`;

function parseApiError(err: unknown): string {
  if (err && typeof err === 'object' && 'response' in err) {
    const r = (err as { response?: { data?: unknown; status?: number } }).response;
    const d = r?.data;
    if (d && typeof d === 'object' && 'message' in d && typeof (d as { message: string }).message === 'string') {
      return (d as { message: string }).message;
    }
    if (typeof d === 'string') {
      return d;
    }
  }
  if (err instanceof Error) {
    return err.message;
  }
  return '请求失败';
}

/**
 * Excel 生成联调页：提交 JSON → `generateExcel` → 下载 / 原生表格预览（与 Agent 返回 file_id 后的流程一致）。
 */
export default function ExcelGenerateDemo() {
  useAuthRedirect();
  const navigate = useNavigate();
  const { user, isAuthenticated } = useAuthContext();

  const [text, setText] = useState(DEFAULT_SPEC);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [genResult, setGenResult] = useState<{
    file_id: string;
    filename: string;
    size: number;
    mime: string;
  } | null>(null);

  const [previewOpen, setPreviewOpen] = useState(false);
  const [previewBlob, setPreviewBlob] = useState<Blob | null>(null);
  const [previewLoading, setPreviewLoading] = useState(false);

  const handleGenerate = async () => {
    setError(null);
    setGenResult(null);
    let body: unknown;
    try {
      body = JSON.parse(text);
    } catch {
      setError('JSON 无法解析，请检查语法');
      return;
    }
    setLoading(true);
    try {
      const result = await dataService.generateExcel(body as Parameters<typeof dataService.generateExcel>[0]);
      setGenResult(result);
    } catch (err) {
      setError(parseApiError(err));
    } finally {
      setLoading(false);
    }
  };

  const handleDownload = useCallback(async () => {
    if (!user?.id || !genResult) {
      return;
    }
    const res = await dataService.getFileDownload(user.id, genResult.file_id, { original: true });
    const blob = res.data as Blob;
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = genResult.filename;
    a.click();
    URL.revokeObjectURL(url);
  }, [user?.id, genResult]);

  const handleOpenPreview = useCallback(async () => {
    if (!user?.id || !genResult) {
      return;
    }
    setPreviewLoading(true);
    setPreviewBlob(null);
    setError(null);
    try {
      const res = await dataService.getFileDownload(user.id, genResult.file_id, { original: true });
      setPreviewBlob(res.data as Blob);
      setPreviewOpen(true);
    } catch (err) {
      setError(parseApiError(err));
    } finally {
      setPreviewLoading(false);
    }
  }, [user?.id, genResult]);

  const onPreviewClose = useCallback((open: boolean) => {
    setPreviewOpen(open);
    if (!open) {
      setPreviewBlob(null);
    }
  }, []);

  if (!isAuthenticated) {
    return null;
  }

  return (
    <div className="flex h-full w-full flex-col overflow-hidden bg-background">
      <div className="mb-4 px-4 pt-4">
        <div className="flex items-start justify-between gap-4">
          <div>
            <h1 className="text-2xl font-semibold text-text-primary">Excel 生成联调</h1>
            <p className="mt-1 text-sm text-text-secondary">
              调用 <code className="rounded bg-surface-tertiary px-1">POST /api/files/generate-excel</code>，与 Agent
              工具 <code className="rounded bg-surface-tertiary px-1">generate_excel</code> 使用同一套服务端逻辑。
            </p>
          </div>
          <button
            type="button"
            className="btn btn-neutral border-token-border-light relative flex shrink-0 items-center gap-2 rounded-lg px-3 py-2"
            onClick={() => navigate('/c/new')}
            aria-label="返回"
          >
            <ChevronLeft className="h-4 w-4" />
            <span>返回</span>
          </button>
        </div>
      </div>

      <div className="min-h-0 flex-1 overflow-y-auto px-4 pb-6">
        <div className="mx-auto flex max-w-4xl flex-col gap-4">
          <div className="rounded-lg border border-border-light bg-surface-secondary p-4 shadow-sm">
            <label htmlFor="excel-spec-json" className="mb-2 block text-sm font-medium text-text-primary">
              请求体 JSON
            </label>
            <textarea
              id="excel-spec-json"
              className="border-token-border-light focus:ring-token-ring w-full rounded-md border bg-surface-primary px-3 py-2 font-mono text-sm text-text-primary focus:outline-none focus:ring-2"
              rows={16}
              value={text}
              onChange={(e) => setText(e.target.value)}
              spellCheck={false}
            />
            <div className="mt-3 flex flex-wrap gap-2">
              <Button type="button" onClick={handleGenerate} disabled={loading}>
                {loading ? (
                  <>
                    <Loader2 className="mr-2 h-4 w-4 animate-spin" />
                    生成中…
                  </>
                ) : (
                  '生成 Excel'
                )}
              </Button>
            </div>
          </div>

          {error && (
            <div className="rounded-lg border border-red-200 bg-red-50 px-4 py-3 text-sm text-red-800 dark:border-red-900 dark:bg-red-950/40 dark:text-red-200">
              {error}
            </div>
          )}

          {genResult && (
            <div className="rounded-lg border border-border-light bg-surface-secondary p-4 shadow-sm">
              <h2 className="mb-2 text-lg font-semibold text-text-primary">生成结果</h2>
              <dl className="grid grid-cols-1 gap-2 text-sm sm:grid-cols-2">
                <div>
                  <dt className="text-text-secondary">file_id</dt>
                  <dd className="font-mono text-text-primary">{genResult.file_id}</dd>
                </div>
                <div>
                  <dt className="text-text-secondary">filename</dt>
                  <dd className="font-mono text-text-primary">{genResult.filename}</dd>
                </div>
                <div>
                  <dt className="text-text-secondary">size</dt>
                  <dd className="text-text-primary">{genResult.size} 字节</dd>
                </div>
                <div>
                  <dt className="text-text-secondary">mime</dt>
                  <dd className="font-mono text-text-primary">{genResult.mime}</dd>
                </div>
              </dl>
              <div className="mt-4 flex flex-wrap gap-2">
                <Button type="button" variant="outline" onClick={handleDownload}>
                  下载
                </Button>
                <Button type="button" variant="secondary" onClick={handleOpenPreview} disabled={previewLoading}>
                  {previewLoading ? (
                    <>
                      <Loader2 className="mr-2 h-4 w-4 animate-spin" />
                      加载预览…
                    </>
                  ) : (
                    '表格预览'
                  )}
                </Button>
              </div>
            </div>
          )}
        </div>
      </div>

      <OGDialog open={previewOpen} onOpenChange={onPreviewClose}>
        <OGDialogContent className="flex max-h-[88vh] max-w-4xl flex-col gap-3 overflow-hidden p-4">
          <OGDialogHeader>
            <OGDialogTitle>表格预览</OGDialogTitle>
          </OGDialogHeader>
          <div className="min-h-0 flex-1 overflow-auto rounded-md border border-border-light bg-surface-primary p-2">
            {previewBlob ? (
              <SpreadsheetNativeViewer
                blob={previewBlob}
                onParseError={(msg) => console.warn('[ExcelGenerateDemo] spreadsheet parse', msg)}
              />
            ) : (
              <p className="text-sm text-text-secondary">无数据</p>
            )}
          </div>
        </OGDialogContent>
      </OGDialog>
    </div>
  );
}
