import React, { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import {
  ChevronDown,
  ChevronRight,
  ChevronUp,
  Download,
  ExternalLink,
  FileText,
  Upload,
} from 'lucide-react';
import { OGDialog, OGDialogContent } from '@because/client';
import { PipelineApi, useReplaceRunOutputFile } from '~/data-provider';
import { downloadPipelineArtifactUrl } from '~/lib/office/fetchPipelinePreviewBlob';
import { useAuthContext } from '~/hooks/AuthContext';
import { cn } from '~/utils';
import {
  pipelineArtifactDisplayTitle,
  pipelineArtifactRoleLabel,
  pipelineArtifactTechnicalName,
} from './pipelineArtifactLabels';

const PREVIEWABLE_EXT = /\.(xlsx|xls|xlsm|csv)$/i;

interface OutputFileLike {
  file_id?: string;
  name: string;
  role?: string;
  size?: number;
  created_at?: string;
}

interface XlsxPreviewProps {
  taskId: string;
  channelId: string;
  runId: string;
  outputFiles: OutputFileLike[];
  /** 顶栏折叠区标题，默认「产物」（分摊页等与外层「文件」tab 一致时可传「文件」） */
  artifactDockTitle?: string;
  /** 第一节列表的小标题，默认「最终合并产物」 */
  primarySectionTitle?: string;
  /** 主产物文件卡片右上角补充信息（如生成时间、来源 run），仅作用于「最终产出」行 */
  primaryOutputRowMeta?: React.ReactNode;
}

export function buildPipelineOfficeIframeSrc(
  taskId: string,
  channelId: string,
  runId: string,
  storageName: string,
  displayTitle: string,
  fileKind: string,
  opts?: { authorization?: string },
): string {
  const path = PipelineApi.runFileDownloadUrl(taskId, channelId, runId, storageName);
  const origin = typeof window !== 'undefined' ? window.location.origin : '';
  const fileUrl = `${origin}${path}`;
  const u = new URLSearchParams({
    scope: 'pipeline_run',
    fileUrl,
    fileKind: fileKind.toLowerCase(),
    storageName,
    displayTitle,
    taskId,
    channelId,
    runId,
    v: String(Date.now()),
  });
  let out = `/office-preview.html?${u.toString()}`;
  const authz = opts?.authorization?.trim();
  if (authz) {
    out += `#authorization=${encodeURIComponent(
      authz.startsWith('Bearer ') ? authz : `Bearer ${authz}`,
    )}`;
  }
  return out;
}

function fileKindFromName(name: string): string {
  const m = name.match(/\.([^.]+)$/i);
  return (m?.[1] ?? 'xlsx').toLowerCase();
}

export function ReplaceFinalOutputButton({
  taskId,
  channelId,
  runId,
  outputFileName,
}: {
  taskId: string;
  channelId: string;
  runId: string;
  outputFileName: string;
}) {
  const inputRef = useRef<HTMLInputElement | null>(null);
  const [info, setInfo] = useState<string | null>(null);
  const replace = useReplaceRunOutputFile(taskId, channelId, runId);

  return (
    <span className="inline-flex flex-col items-end gap-0.5">
      <input
        ref={inputRef}
        type="file"
        className="hidden"
        title={`替换产物「${outputFileName}」（请选择同名文件）`}
        aria-label={`替换产物 ${outputFileName}`}
        onChange={async (e) => {
          const file = e.target.files?.[0];
          if (!file) return;
          if (file.name !== outputFileName) {
            setInfo(`须选择与磁盘产物同名文件「${outputFileName}」，当前为「${file.name}」`);
            if (inputRef.current) inputRef.current.value = '';
            return;
          }
          try {
            const res = await replace.mutateAsync({ outputFileName, file });
            const oldP =
              res.old_sha256 != null && res.old_sha256 !== ''
                ? res.old_sha256.slice(0, 8)
                : '—';
            setInfo(
              res.old_sha256 != null && res.new_sha256 === res.old_sha256
                ? '内容相同（hash 未变）'
                : `已替换 (sha ${oldP}→${res.new_sha256.slice(0, 8)})`,
            );
          } catch (err) {
            setInfo((err as Error).message);
          } finally {
            if (inputRef.current) inputRef.current.value = '';
          }
        }}
      />
      <button
        type="button"
        onClick={() => inputRef.current?.click()}
        disabled={replace.isLoading}
        className="inline-flex items-center gap-1 rounded-md border border-border-light px-2 py-1 text-xs text-text-secondary hover:bg-surface-hover disabled:opacity-50"
      >
        <Upload className="h-3 w-3" />
        {replace.isLoading ? '上传…' : '替换'}
      </button>
      {info ? (
        <span className="max-w-[15rem] text-right text-[10px] leading-snug text-text-secondary">
          {info}
        </span>
      ) : null}
    </span>
  );
}

/** role 缺省时视为与后端一致的最终产出（output）。 */
function partitionOutputs(files: OutputFileLike[]) {
  const sorted = [...files].sort((a, b) => a.name.localeCompare(b.name));
  const finalMerged = sorted.filter((f) => (f.role ?? 'output') === 'output');
  const nonFinal = sorted.filter((f) => (f.role ?? 'output') !== 'output');
  const midfiles = nonFinal.filter((f) => f.role === 'midfile');
  const auxiliary = nonFinal.filter((f) => f.role !== 'midfile');
  return { finalMerged, midfiles, auxiliary };
}

export default function XlsxPreview({
  taskId,
  channelId,
  runId,
  outputFiles,
  artifactDockTitle = '产物',
  primarySectionTitle = '最终合并产物',
  primaryOutputRowMeta,
}: XlsxPreviewProps) {
  const [dockOpen, setDockOpen] = useState(true);
  const [intermediateOpen, setIntermediateOpen] = useState(false);
  const [officeOpen, setOfficeOpen] = useState(false);
  const [officeSrc, setOfficeSrc] = useState('');

  const { token } = useAuthContext();

  const { finalMerged, midfiles, auxiliary } = useMemo(
    () => partitionOutputs(outputFiles),
    [outputFiles],
  );

  const intermediateCount = midfiles.length + auxiliary.length;

  const openOffice = useCallback(
    (storageName: string, displayTitle: string) => {
      const kind = fileKindFromName(storageName);
      const authz = token ? `Bearer ${token}` : undefined;
      setOfficeSrc(
        buildPipelineOfficeIframeSrc(
          taskId,
          channelId,
          runId,
          storageName,
          displayTitle,
          kind,
          { authorization: authz },
        ),
      );
      setOfficeOpen(true);
    },
    [taskId, channelId, runId, token],
  );

  useEffect(() => {
    const onMessage = (ev: MessageEvent) => {
      if (ev.origin !== window.location.origin) return;
      const t = (ev.data as { type?: string })?.type;
      if (t === 'office-preview-close') {
        setOfficeOpen(false);
      }
    };
    window.addEventListener('message', onMessage);
    return () => window.removeEventListener('message', onMessage);
  }, []);

  const renderFileRow = (
    f: OutputFileLike,
    opts?: { showReplace?: boolean; topRightMeta?: React.ReactNode },
  ) => {
    const tech = pipelineArtifactTechnicalName(f.name);
    const title = pipelineArtifactDisplayTitle(f.name);
    const canOffice = PREVIEWABLE_EXT.test(f.name);
    const href = PipelineApi.runFileDownloadUrl(taskId, channelId, runId, f.name);
    const showReplace = !!opts?.showReplace;
    const topRightMeta = opts?.topRightMeta;
    return (
      <div
        key={f.file_id ?? f.name}
        className="rounded-md border border-border-light bg-surface-primary px-3 py-2 text-text-primary"
      >
        <div className="flex flex-wrap items-start justify-between gap-3">
          <div className="min-w-0 flex-1">
            <div className="truncate text-sm font-medium text-text-primary">{title}</div>
            <div className="mt-0.5 truncate font-mono text-[11px] text-text-secondary">
              {tech}
              <>
                {' '}
                · <span>{pipelineArtifactRoleLabel(f.role)}</span>
              </>
              {typeof f.size === 'number' && (
                <>
                  {' '}
                  · {(f.size / 1024).toFixed(1)} KB
                </>
              )}
              {f.created_at && (
                <>
                  {' '}
                  · 生成于 {new Date(f.created_at).toLocaleString('zh-CN', { month: '2-digit', day: '2-digit', hour: '2-digit', minute: '2-digit', second: '2-digit' })}
                </>
              )}
            </div>
          </div>
          <div className="flex max-w-full shrink-0 flex-col items-end gap-2">
            {topRightMeta ? (
              <div className="max-w-[min(100%,20rem)] text-right text-[11px] leading-snug text-text-secondary">
                {topRightMeta}
              </div>
            ) : null}
            <div className="flex flex-wrap justify-end gap-2">
              {canOffice && (
                <button
                  type="button"
                  onClick={() => openOffice(f.name, title)}
                  className="inline-flex items-center gap-1.5 rounded-md border border-border-light bg-surface-secondary px-3 py-1.5 text-xs font-medium text-text-primary hover:bg-surface-hover"
                >
                  <ExternalLink className="h-3.5 w-3.5" />
                  打开
                </button>
              )}
              <button
                type="button"
                onClick={() => {
                  void downloadPipelineArtifactUrl(href, f.name).catch((e) =>
                    window.alert((e as Error).message),
                  );
                }}
                className="inline-flex items-center gap-1.5 rounded-md border border-green-500/40 bg-green-500/10 px-3 py-1.5 text-xs font-medium text-green-400 hover:bg-green-500/15"
              >
                <Download className="h-3.5 w-3.5" />
                下载
              </button>
              {showReplace ? (
                <ReplaceFinalOutputButton
                  taskId={taskId}
                  channelId={channelId}
                  runId={runId}
                  outputFileName={f.name}
                />
              ) : null}
            </div>
          </div>
        </div>
      </div>
    );
  };

  if (outputFiles.length === 0) {
    return null;
  }

  return (
    <>
      <div className="flex h-full min-h-0 flex-col overflow-hidden bg-surface-secondary">
        <div className="flex shrink-0 items-center justify-between px-4 py-2">
          <button
            type="button"
            onClick={() => setDockOpen((v) => !v)}
            className="inline-flex items-center gap-1.5 text-xs font-medium text-text-primary"
            aria-expanded={dockOpen}
            aria-label={dockOpen ? `收起${artifactDockTitle}面板` : `展开${artifactDockTitle}面板`}
          >
            {dockOpen ? (
              <ChevronDown className="h-3.5 w-3.5" />
            ) : (
              <ChevronUp className="h-3.5 w-3.5" />
            )}
            <FileText className="h-3.5 w-3.5 text-green-400" />
            {artifactDockTitle}
            <span className="font-normal text-text-secondary">
              （{outputFiles.length} 个文件）
            </span>
          </button>
        </div>

        {dockOpen && (
          <div className="min-h-0 flex-1 overflow-y-auto px-4 pb-4">
            <div className="space-y-4">
              <section className="space-y-2">
                <div className="text-[11px] font-semibold uppercase tracking-wide text-text-secondary">
                  {primarySectionTitle}
                  <span className="ml-1.5 font-normal normal-case text-text-secondary">
                    （{finalMerged.length}）
                  </span>
                </div>
                {finalMerged.length === 0 ? (
                  <div className="rounded-md border border-dashed border-border-medium px-3 py-4 text-center text-xs text-text-secondary">
                    暂无最终合并产出（最终产出类文件）
                  </div>
                ) : (
                  <div className="space-y-2">
                    {finalMerged.map((f) =>
                      renderFileRow(f, {
                        showReplace: true,
                        topRightMeta: primaryOutputRowMeta ?? undefined,
                      }),
                    )}
                  </div>
                )}
              </section>

              {intermediateCount > 0 && (
                <section className="space-y-2 border-t border-border-light pt-4">
                  <button
                    type="button"
                    onClick={() => setIntermediateOpen((v) => !v)}
                    className="flex w-full items-center gap-2 text-left text-[11px] font-semibold uppercase tracking-wide text-text-secondary hover:text-text-primary"
                    aria-expanded={intermediateOpen}
                  >
                    {intermediateOpen ? (
                      <ChevronDown className="h-3.5 w-3.5 shrink-0" />
                    ) : (
                      <ChevronRight className="h-3.5 w-3.5 shrink-0" />
                    )}
                    <span>
                      中间产物
                      <span className="ml-1.5 font-normal normal-case text-text-secondary">
                        （各银行临时表等 · {midfiles.length} 个）
                      </span>
                      {auxiliary.length > 0 ? (
                        <span className="ml-1.5 font-normal normal-case text-text-secondary">
                          · 日志 / 清单 {auxiliary.length} 个
                        </span>
                      ) : null}
                    </span>
                  </button>

                  {intermediateOpen && (
                    <div className="space-y-3 pl-1">
                      {midfiles.some((mf) => /_temp\.xlsx$/i.test(mf.name)) ? (
                        <p className="text-[10px] leading-relaxed text-text-secondary">
                          账单渠道：<span className="font-mono">*_temp.xlsx</span>{' '}
                          一般为<strong className="text-text-primary/90">
                            按银行或单次分发解析得到的临时汇总
                          </strong>
                          ，落在磁盘子目录 <span className="font-mono">midfile/</span>{' '}
                          ；顶部「账单合并最终结果」才是全流程对齐后的总表。
                        </p>
                      ) : midfiles.some(
                          (mf) =>
                            mf.name.endsWith('_内转.csv') || mf.name.endsWith('_ach_return.csv'),
                        ) ? (
                        <p className="text-[10px] leading-relaxed text-text-secondary">
                          以下为与主工作簿同批生成的 <span className="font-mono">CSV</span>{' '}
                          明细侧车；签发与下游仍以顶部「最终合并产物」中的工作簿为准。
                        </p>
                      ) : null}
                      {midfiles.length > 0 && (
                        <div className="space-y-2">
                          {midfiles.map((mf) => renderFileRow(mf, { showReplace: true }))}
                        </div>
                      )}
                      {auxiliary.length > 0 && (
                        <div className="space-y-2">
                          {midfiles.length > 0 ? (
                            <div className="text-[10px] font-medium text-text-secondary">
                              日志与清单
                            </div>
                          ) : null}
                          {auxiliary.map(renderFileRow)}
                        </div>
                      )}
                    </div>
                  )}
                </section>
              )}
            </div>
          </div>
        )}
      </div>

      <OGDialog open={officeOpen} onOpenChange={setOfficeOpen}>
        <OGDialogContent
          showCloseButton={false}
          className={cn(
            'fixed inset-0 left-0 top-0 z-50 h-screen w-screen max-w-none translate-x-0 translate-y-0 rounded-none border-0 bg-black p-0',
            'overflow-hidden sm:max-w-none',
          )}
        >
          {officeOpen && officeSrc ? (
            <iframe
              key={officeSrc}
              title="Office 编辑器"
              src={officeSrc}
              className="h-full w-full border-0 bg-zinc-950"
            />
          ) : null}
        </OGDialogContent>
      </OGDialog>
    </>
  );
}
