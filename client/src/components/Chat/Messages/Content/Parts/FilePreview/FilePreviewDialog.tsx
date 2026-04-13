import { useCallback, useEffect, useMemo, useState } from 'react';
import {
  Button,
  OGDialog,
  OGDialogContent,
  OGDialogFooter,
  OGDialogHeader,
  OGDialogTitle,
} from '@because/client';
import { dataService } from '@because/data-provider';
import { useFileDownload } from '~/data-provider';
import type { TAttachment, TFile } from '@because/data-provider';
import { useToastContext } from '@because/client';
import { Loader2 } from 'lucide-react';
import { useAuthContext } from '~/hooks/AuthContext';
import { useLocalize } from '~/hooks';
import PdfNativeViewer from './PdfNativeViewer';
import { needsNativeBlob, resolvePreviewKind } from './resolvePreviewKind';
import SpreadsheetNativeViewer from './SpreadsheetNativeViewer';
import TextFallbackViewer from './TextFallbackViewer';

type FilePreviewDialogProps = {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  attachment: Partial<TAttachment>;
};

function parseErrorMessage(err: unknown): string {
  if (err && typeof err === 'object' && 'response' in err) {
    const r = (err as { response?: { data?: unknown } }).response;
    const d = r?.data;
    if (
      d &&
      typeof d === 'object' &&
      'message' in d &&
      typeof (d as { message: string }).message === 'string'
    ) {
      return (d as { message: string }).message;
    }
    if (typeof d === 'string') {
      return d;
    }
  }
  if (err instanceof Error) {
    return err.message;
  }
  return 'Error';
}

/**
 * 附件原生预览壳：按类型在中间区域切换 Pdf / 表格 / 文本兜底（/files/text），与解析管线独立。
 */
export default function FilePreviewDialog({ open, onOpenChange, attachment }: FilePreviewDialogProps) {
  const localize = useLocalize();
  const { showToast } = useToastContext();
  const { user } = useAuthContext();
  const file_id = (attachment as TFile).file_id;
  const filename = attachment.filename ?? '';

  const { refetch: downloadOriginal } = useFileDownload(user?.id, file_id, { original: true });

  const kind = useMemo(
    () => resolvePreviewKind(attachment.type, attachment.filename),
    [attachment.type, attachment.filename],
  );

  const [textFallback, setTextFallback] = useState(false);
  const [blobLoading, setBlobLoading] = useState(false);
  const [downloadError, setDownloadError] = useState<string | null>(null);
  const [blobUrl, setBlobUrl] = useState<string | null>(null);
  const [previewBlob, setPreviewBlob] = useState<Blob | null>(null);

  const handleDownload = useCallback(
    async (e: React.MouseEvent<HTMLButtonElement>) => {
      e.preventDefault();
      if (!user?.id || !file_id) {
        showToast({ status: 'error', message: localize('com_ui_download_error') });
        return;
      }
      try {
        const result = await downloadOriginal();
        const url = result.data;
        if (url == null || url === '') {
          showToast({ status: 'error', message: localize('com_ui_download_error') });
          return;
        }
        const link = document.createElement('a');
        link.href = url;
        link.setAttribute('download', filename);
        document.body.appendChild(link);
        link.click();
        document.body.removeChild(link);
        window.URL.revokeObjectURL(url);
      } catch (err) {
        console.error('[FilePreviewDialog] download failed', err);
        showToast({ status: 'error', message: localize('com_ui_download_error') });
      }
    },
    [user?.id, file_id, downloadOriginal, filename, localize, showToast],
  );

  const onPdfRenderError = useCallback((msg: string) => {
    console.warn('[FilePreviewDialog] PDF 原生预览失败，回退 /files/text:', msg);
    setTextFallback(true);
  }, []);

  const onSpreadsheetParseError = useCallback((msg: string) => {
    console.warn('[FilePreviewDialog] 表格原生预览失败，回退 /files/text:', msg);
    setTextFallback(true);
  }, []);

  useEffect(() => {
    if (!open) {
      setTextFallback(false);
      setDownloadError(null);
      setBlobLoading(false);
      setPreviewBlob(null);
      setBlobUrl((prev) => {
        if (prev) {
          URL.revokeObjectURL(prev);
        }
        return null;
      });
    }
  }, [open]);

  useEffect(() => {
    if (!open || !user?.id || !file_id) {
      return;
    }

    if (!needsNativeBlob(kind)) {
      setBlobLoading(false);
      setDownloadError(null);
      setPreviewBlob(null);
      setBlobUrl((prev) => {
        if (prev) {
          URL.revokeObjectURL(prev);
        }
        return null;
      });
      return;
    }

    let cancelled = false;
    let createdUrl: string | null = null;

    setTextFallback(false);
    setDownloadError(null);
    setBlobLoading(true);
    setPreviewBlob(null);
    setBlobUrl((prev) => {
      if (prev) {
        URL.revokeObjectURL(prev);
      }
      return null;
    });

    void dataService
      .getFileDownload(user.id, file_id, { original: true })
      .then((res) => {
        if (cancelled) {
          return;
        }
        const blob = res.data as Blob;
        setPreviewBlob(blob);
        createdUrl = URL.createObjectURL(blob);
        setBlobUrl(createdUrl);
      })
      .catch((e) => {
        if (!cancelled) {
          setDownloadError(parseErrorMessage(e));
        }
      })
      .finally(() => {
        if (!cancelled) {
          setBlobLoading(false);
        }
      });

    return () => {
      cancelled = true;
      if (createdUrl) {
        URL.revokeObjectURL(createdUrl);
      }
    };
  }, [open, file_id, user?.id, kind]);

  const showTextOnly =
    textFallback ||
    !needsNativeBlob(kind) ||
    (needsNativeBlob(kind) && (downloadError != null || (blobUrl == null && !blobLoading)));

  const showPdfNative =
    kind === 'pdf' && needsNativeBlob(kind) && blobUrl != null && !textFallback && !downloadError;
  const showSheetNative =
    kind === 'spreadsheet' &&
    needsNativeBlob(kind) &&
    previewBlob != null &&
    !textFallback &&
    !downloadError;

  const needIdError = open && (!file_id || !user?.id);

  return (
    <OGDialog open={open} onOpenChange={onOpenChange}>
      <OGDialogContent className="flex max-h-[88vh] max-w-3xl flex-col gap-3 overflow-hidden p-4 sm:max-w-3xl">
        <OGDialogHeader>
          <OGDialogTitle className="truncate pr-8 text-left" title={filename}>
            {localize('com_ui_preview')}: {filename}
          </OGDialogTitle>
        </OGDialogHeader>
        <div className="min-h-[12rem] max-h-[min(60vh,32rem)] overflow-auto rounded-lg border border-border-light bg-surface-secondary p-3 text-sm text-text-primary">
          {needIdError && (
            <p className="text-red-600 dark:text-red-400">{localize('com_ui_file_preview_need_id')}</p>
          )}
          {!needIdError && needsNativeBlob(kind) && blobLoading && (
            <div className="flex items-center gap-2 text-text-secondary">
              <Loader2 className="h-5 w-5 animate-spin" aria-hidden />
              <span>{localize('com_ui_loading')}</span>
            </div>
          )}
          {!needIdError && !blobLoading && showPdfNative && blobUrl && (
            <PdfNativeViewer fileUrl={blobUrl} onRenderError={onPdfRenderError} />
          )}
          {!needIdError && !blobLoading && showSheetNative && previewBlob && (
            <SpreadsheetNativeViewer blob={previewBlob} onParseError={onSpreadsheetParseError} />
          )}
          {!needIdError && !blobLoading && showTextOnly && file_id && user?.id && (
            <>
              {downloadError != null && (
                <p className="mb-2 text-xs text-amber-700 dark:text-amber-300">
                  原件加载失败（{downloadError}），已回退为解析文本预览。
                </p>
              )}
              {textFallback && kind !== 'word' && kind !== 'text' && kind !== 'unsupported' && (
                <p className="mb-2 text-xs text-text-secondary">原生预览不可用，已回退为解析文本预览。</p>
              )}
              {kind === 'word' && (
                <p className="mb-2 text-xs text-text-secondary">
                  Word 原生预览开发中，当前为解析文本预览。
                </p>
              )}
              <TextFallbackViewer open={open} userId={user.id} file_id={file_id} />
            </>
          )}
        </div>
        <OGDialogFooter className="gap-2 sm:justify-end">
          <Button
            type="button"
            variant="outline"
            onClick={(e) => {
              void handleDownload(e);
            }}
          >
            {localize('com_ui_download')}
          </Button>
          <Button type="button" onClick={() => onOpenChange(false)}>
            {localize('com_ui_close')}
          </Button>
        </OGDialogFooter>
      </OGDialogContent>
    </OGDialog>
  );
}
