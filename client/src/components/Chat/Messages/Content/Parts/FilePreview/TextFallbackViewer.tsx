import { useEffect, useState } from 'react';
import type { AxiosResponse } from 'axios';
import { apiBaseUrl, request } from '@because/data-provider';
import { Loader2 } from 'lucide-react';
import { useLocalize } from '~/hooks';

const MAX_PREVIEW_CHARS = 120_000;

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

type TextFallbackViewerProps = {
  open: boolean;
  userId: string;
  file_id: string;
};

/**
 * 通过 `/files/text` 拉取解析后的纯文本（RAG/Agent 同源接口），供原生预览失败或未实现格式兜底。
 */
export default function TextFallbackViewer({ open, userId, file_id }: TextFallbackViewerProps) {
  const localize = useLocalize();
  const [text, setText] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    if (!open) {
      setText(null);
      setError(null);
      setLoading(false);
      return;
    }

    let cancelled = false;
    setLoading(true);
    setError(null);
    setText(null);

    const url = `${apiBaseUrl()}/api/files/text/${encodeURIComponent(userId)}/${encodeURIComponent(file_id)}`;
    void request
      .getResponse(url, {
        responseType: 'text',
        headers: { Accept: 'text/plain, application/json' },
      })
      .then((res) => {
        if (!cancelled) {
          const r = res as AxiosResponse<string>;
          const raw = typeof r.data === 'string' ? r.data : String(r.data ?? '');
          setText(raw);
        }
      })
      .catch((e) => {
        if (!cancelled) {
          setError(parseErrorMessage(e));
        }
      })
      .finally(() => {
        if (!cancelled) {
          setLoading(false);
        }
      });

    return () => {
      cancelled = true;
    };
  }, [open, file_id, userId]);

  const displayText =
    text != null && text.length > MAX_PREVIEW_CHARS
      ? text.slice(0, MAX_PREVIEW_CHARS) + '\n\n…'
      : text;

  if (loading) {
    return (
      <div className="flex items-center gap-2 text-text-secondary">
        <Loader2 className="h-5 w-5 animate-spin" aria-hidden />
        <span>{localize('com_ui_loading')}</span>
      </div>
    );
  }

  if (error) {
    return <p className="whitespace-pre-wrap text-red-600 dark:text-red-400">{error}</p>;
  }

  return (
    <>
      <pre className="whitespace-pre-wrap break-words font-sans">{displayText ?? ''}</pre>
      {text != null && text.length > MAX_PREVIEW_CHARS && (
        <p className="mt-3 border-t border-border-light pt-2 text-text-secondary">
          {localize('com_ui_file_preview_truncated')}
        </p>
      )}
    </>
  );
}
