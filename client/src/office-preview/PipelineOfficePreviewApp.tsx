import { useEffect, useMemo, useRef, useState } from 'react';
import { Download, Loader2, FileWarning, Info, RefreshCw, X } from 'lucide-react';
import { convertDocument, initX2T, initX2TScript } from '~/lib/office/x2t';
import {
  createDocEditor,
  destroyDocEditor,
  loadEditorApi,
  type OnlyOfficeEditor,
} from '~/lib/office/docEditor';
import {
  buildPipelineArtifactFetchInit,
  fetchPipelinePreviewBlob,
  readPipelineOfficeAuthorizationFromHash,
} from '~/lib/office/fetchPipelinePreviewBlob';
import {
  officePreviewErrorMessage,
  saveAllocationMergeUploadOutput,
  savePipelineRunOutput,
} from '~/lib/office/savePipelineRunOutput';
import { triggerBrowserDownload } from '~/lib/office/previewUtils';

type SupportedKind = 'xlsx' | 'xls' | 'xlsm' | 'csv' | 'docx' | 'doc' | 'pptx' | 'ppt';

type PreviewQuery = {
  fileUrl: string | null;
  storageName: string | null;
  displayTitle: string | null;
  fileKind: string | null;
  scope: string | null;
  taskId: string | null;
  channelId: string | null;
  runId: string | null;
};

function readQuery(): PreviewQuery {
  const params = new URLSearchParams(window.location.search);
  return {
    fileUrl: params.get('fileUrl'),
    storageName: params.get('storageName'),
    displayTitle: params.get('displayTitle'),
    fileKind: params.get('fileKind'),
    scope: params.get('scope'),
    taskId: params.get('taskId'),
    channelId: params.get('channelId'),
    runId: params.get('runId'),
  };
}

function isSupportedKind(kind: string | null): kind is SupportedKind {
  return (
    kind !== null &&
    ['xlsx', 'xls', 'xlsm', 'csv', 'docx', 'doc', 'pptx', 'ppt'].includes(kind.toLowerCase())
  );
}

function getEditorDocumentType(kind: SupportedKind): 'word' | 'cell' | 'slide' {
  if (['docx', 'doc'].includes(kind)) return 'word';
  if (['pptx', 'ppt'].includes(kind)) return 'slide';
  return 'cell';
}

function requestClose() {
  if (window.parent && window.parent !== window) {
    window.parent.postMessage({ type: 'office-preview-close' }, window.location.origin);
    return;
  }
  window.close();
}

function notifySaved(payload: { taskId: string | null; channelId: string | null }) {
  if (window.parent && window.parent !== window) {
    window.parent.postMessage(
      { type: 'office-preview-saved', ...payload },
      window.location.origin,
    );
  }
}

function notifyError(message: string) {
  if (window.parent && window.parent !== window) {
    window.parent.postMessage({ type: 'office-preview-error', message }, window.location.origin);
  }
}

export function PipelineOfficePreviewApp() {
  const query = useMemo(() => readQuery(), []);
  const editorRef = useRef<OnlyOfficeEditor | null>(null);
  const mediaRef = useRef<Record<string, string>>({});
  const previewTimersRef = useRef<{ stuck?: ReturnType<typeof setTimeout> }>({});
  const [status, setStatus] = useState<'loading' | 'ready' | 'error'>('loading');
  const [error, setError] = useState<string | null>(null);
  const [isSaving, setIsSaving] = useState(false);
  const [reloadKey, setReloadKey] = useState(0);

  const headerTitle =
    query.displayTitle?.trim() || query.storageName || 'Office 文件预览';

  useEffect(() => {
    document.title = headerTitle;
  }, [headerTitle]);

  const clearPreviewTimers = () => {
    const t = previewTimersRef.current;
    if (t.stuck !== undefined) {
      clearTimeout(t.stuck);
      t.stuck = undefined;
    }
  };

  const downloadCurrentFile = async () => {
    if (!query.fileUrl || !query.storageName) return;
    try {
      const res = await fetch(
        query.fileUrl,
        buildPipelineArtifactFetchInit(undefined, readPipelineOfficeAuthorizationFromHash()),
      );
      if (!res.ok) throw new Error(`下载失败: ${res.status}`);
      const blob = await res.blob();
      triggerBrowserDownload(blob, query.storageName);
    } catch (downloadError) {
      const message = officePreviewErrorMessage(downloadError, '下载失败');
      setError(message);
      notifyError(message);
    }
  };

  useEffect(() => {
    let cancelled = false;
    const abortController = new AbortController();

    const run = async () => {
      setStatus('loading');
      setError(null);

      const isMergeUpload = query.scope === 'allocation_merge_upload';

      if (query.scope !== 'pipeline_run' && !isMergeUpload) {
        setStatus('error');
        setError('仅支持 pipeline_run / allocation_merge_upload 预览上下文');
        return;
      }
      if (
        !query.fileUrl ||
        !query.taskId ||
        !query.storageName ||
        (!isMergeUpload && (!query.channelId || !query.runId))
      ) {
        setStatus('error');
        setError(
          isMergeUpload
            ? '缺少 fileUrl / taskId / storageName 参数'
            : '缺少 fileUrl / taskId / channelId / runId / storageName 参数',
        );
        return;
      }

      if (!isSupportedKind(query.fileKind)) {
        setStatus('error');
        setError(`暂不支持 ${query.fileKind || '未知'} 类型的 OnlyOffice 在线预览`);
        return;
      }

      try {
        const file = await fetchPipelinePreviewBlob(
          query.fileUrl,
          query.storageName,
          query.fileKind,
          { signal: abortController.signal },
        );

        if (cancelled) return;

        const fileType = query.fileKind!.toLowerCase() as SupportedKind;

        await initX2TScript();
        await loadEditorApi();
        await initX2T();

        const converted = await convertDocument(file);

        if (cancelled) return;

        destroyDocEditor(editorRef.current);
        editorRef.current = null;

        mediaRef.current = converted.media || {};

        clearPreviewTimers();
        previewTimersRef.current.stuck = window.setTimeout(() => {
          if (!cancelled) {
            setStatus('error');
            setError(
              '编辑器加载超时。请检查 Network 里 web-apps、sdkjs、wasm 是否 404。首次加载 x2t.wasm 体积较大。',
            );
          }
        }, 120_000);

        let editorInstance: OnlyOfficeEditor | null = null;
        editorInstance = createDocEditor('onlyoffice-editor', {
          width: '100%',
          height: '100%',
          documentType: getEditorDocumentType(fileType),
          document: {
            title: headerTitle,
            url: query.storageName,
            fileType,
            permissions: {
              edit: true,
              chat: false,
              protect: false,
            },
          },
          editorConfig: {
            lang: 'zh',
            customization: {
              help: false,
              about: false,
              hideRightMenu: true,
              features: {
                spellcheck: {
                  change: false,
                },
              },
              anonymous: {
                request: false,
                label: 'Guest',
              },
            },
          },
          events: {
            onAppReady: () => {
              if (cancelled) return;
              const ed = editorInstance;
              if (!ed) return;
              try {
                if (Object.keys(mediaRef.current).length > 0) {
                  ed.sendCommand({
                    command: 'asc_setImageUrls',
                    data: { urls: mediaRef.current },
                  });
                }
                ed.sendCommand({
                  command: 'asc_openDocument',
                  data: { buf: converted.bin },
                });
                clearPreviewTimers();
                setStatus('ready');
              } catch (openErr) {
                clearPreviewTimers();
                setStatus('error');
                setError(openErr instanceof Error ? openErr.message : '打开文档失败');
              }
            },
            onSave: async (event: {
              data?: { data?: { data?: Uint8Array }; option?: { outputformat?: number } };
            }) => {
              const saveData = event.data?.data?.data;
              const outputFormat = event.data?.option?.outputformat;
              const ed = editorInstance;
              try {
                if (saveData && typeof outputFormat === 'number' && query.taskId && query.storageName) {
                  setIsSaving(true);
                  if (query.scope === 'allocation_merge_upload') {
                    await saveAllocationMergeUploadOutput({
                      taskId: query.taskId,
                      storageName: query.storageName,
                      originalFileName: query.storageName,
                      outputFormat,
                      saveData,
                    });
                    notifySaved({
                      taskId: query.taskId,
                      channelId: 'allocation_base',
                    });
                  } else if (query.channelId && query.runId) {
                    await savePipelineRunOutput({
                      taskId: query.taskId,
                      channelId: query.channelId,
                      runId: query.runId,
                      storageName: query.storageName,
                      originalFileName: query.storageName,
                      outputFormat,
                      saveData,
                    });
                    notifySaved({
                      taskId: query.taskId,
                      channelId: query.channelId,
                    });
                  }
                }
                ed?.sendCommand({
                  command: 'asc_onSaveCallback',
                  data: { err_code: 0 },
                });
              } catch (saveError) {
                const msg = officePreviewErrorMessage(saveError, '保存失败');
                setError(msg);
                notifyError(msg);
                ed?.sendCommand({
                  command: 'asc_onSaveCallback',
                  data: { err_code: 1 },
                });
              } finally {
                setIsSaving(false);
              }
            },
          },
        });
        editorRef.current = editorInstance;
      } catch (err) {
        clearPreviewTimers();
        if (cancelled || abortController.signal.aborted) return;
        setStatus('error');
        setError(err instanceof Error ? err.message : '预览页初始化失败');
      }
    };

    void run();

    return () => {
      cancelled = true;
      clearPreviewTimers();
      abortController.abort();
      destroyDocEditor(editorRef.current);
      editorRef.current = null;
      Object.values(mediaRef.current).forEach((url) => {
        if (typeof url === 'string' && url.startsWith('blob:')) {
          URL.revokeObjectURL(url);
        }
      });
      mediaRef.current = {};
    };
  }, [query, reloadKey, headerTitle]);

  return (
    <div className="h-screen w-screen overflow-hidden bg-zinc-950 text-zinc-100">
      <div className="flex h-full min-h-0 flex-col">
        <div className="flex items-center justify-between border-b border-zinc-800 bg-zinc-950/95 px-4 py-3">
          <div className="min-w-0">
            <div className="truncate text-sm font-semibold text-zinc-100">{headerTitle}</div>
            <div className="truncate text-xs text-zinc-500">
              {query.storageName && query.storageName !== headerTitle ? query.storageName : ''}
              {query.storageName && query.storageName !== headerTitle ? ' · ' : ''}
              流水线产物 · OnlyOffice
            </div>
          </div>
          <div className="flex items-center gap-2">
            <button
              type="button"
              onClick={() => void downloadCurrentFile()}
              disabled={isSaving || !query.fileUrl}
              className="inline-flex items-center gap-2 rounded-md border border-zinc-700 bg-zinc-900 px-3 py-1.5 text-xs text-zinc-200 hover:bg-zinc-800 disabled:opacity-50"
            >
              <Download className="h-3.5 w-3.5" /> 下载
            </button>
            <button
              type="button"
              onClick={() => setReloadKey((value) => value + 1)}
              disabled={isSaving}
              className="inline-flex items-center gap-2 rounded-md border border-zinc-700 bg-zinc-900 px-3 py-1.5 text-xs text-zinc-200 hover:bg-zinc-800 disabled:opacity-50"
            >
              <RefreshCw className={`h-3.5 w-3.5 ${isSaving ? 'animate-spin' : ''}`} />
              {isSaving ? '保存中…' : '刷新'}
            </button>
            <button
              type="button"
              onClick={requestClose}
              className="inline-flex items-center gap-2 rounded-md border border-zinc-700 bg-zinc-900 px-3 py-1.5 text-xs text-zinc-200 hover:bg-zinc-800"
            >
              <X className="h-3.5 w-3.5" /> 关闭
            </button>
          </div>
        </div>

        <div className="flex items-center gap-2 border-b border-zinc-800 px-4 py-2 text-xs text-zinc-300">
          <Info className="h-3.5 w-3.5 shrink-0 text-blue-300" />
          <span>修改后请使用 Ctrl+S / 保存；保存后会写回当前 run 的产物文件。</span>
        </div>

        <div className="relative flex min-h-0 flex-1 flex-col bg-zinc-900">
          <div id="onlyoffice-editor" className="min-h-0 flex-1" />

          {status === 'loading' && (
            <div className="absolute inset-0 z-10 flex flex-col items-center justify-center gap-3 bg-zinc-950/90">
              <Loader2 className="h-8 w-8 animate-spin text-blue-400" />
              <div className="text-sm text-zinc-300">正在加载 Office 编辑器…</div>
            </div>
          )}

          {status === 'error' && (
            <div className="absolute inset-0 z-10 flex items-center justify-center bg-zinc-950/95 p-6">
              <div className="max-w-lg rounded-xl border border-red-900/50 bg-red-950/30 p-6 text-center">
                <div className="mx-auto mb-3 flex h-12 w-12 items-center justify-center rounded-full bg-red-500/15 text-red-300">
                  <FileWarning className="h-6 w-6" />
                </div>
                <div className="text-base font-semibold text-red-100">预览加载失败</div>
                <div className="mt-2 break-words text-sm text-red-200/80">{error}</div>
                {error && /conversion failed|Aborted|abort/i.test(error) && (
                  <div className="mt-3 border-t border-red-800/40 pt-3 text-left text-xs leading-relaxed text-red-200/70">
                    多为表格过大或浏览器内存限制导致 WASM 转换中断。请优先点击「下载」在本地 Excel
                    中查看或删减后重传；若本次任务生成了仅含汇总的轻量 xlsx，可用该文件再次在线预览。
                  </div>
                )}
                <div className="mt-4 flex flex-wrap items-center justify-center gap-2">
                  <button
                    type="button"
                    onClick={() => setReloadKey((value) => value + 1)}
                    className="inline-flex items-center gap-2 rounded-md bg-red-700 px-3 py-2 text-sm text-white hover:bg-red-600"
                  >
                    <RefreshCw className="h-4 w-4" /> 重试
                  </button>
                </div>
              </div>
            </div>
          )}
        </div>
      </div>
    </div>
  );
}
