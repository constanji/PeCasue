import React, { useCallback, useMemo, useRef, useState } from 'react';
import {
  ChevronRight,
  Download,
  FileText,
  Folder,
  Loader2,
  Upload,
} from 'lucide-react';
import {
  PipelineApi,
  useReplaceSourceFile,
  useChannelPrescan,
  type PipelineBillPrescanResponse,
  type PipelineClassificationFile,
  type PipelineOwnFlowPrescanResponse,
} from '~/data-provider';
import { downloadPipelineArtifactUrl } from '~/lib/office/fetchPipelinePreviewBlob';

function fmtBytes(n: number): string {
  if (n < 1024) return `${n} B`;
  if (n < 1024 * 1024) return `${(n / 1024).toFixed(1)} KB`;
  return `${(n / (1024 * 1024)).toFixed(2)} MB`;
}

/** 目录树节点（segment + pathPrefix 便于折叠状态键） */
type DirNode = {
  segment: string;
  pathPrefix: string;
  subdirs: Map<string, DirNode>;
  files: PipelineClassificationFile[];
};

function emptyRootDir(): DirNode {
  return { segment: '', pathPrefix: '', subdirs: new Map(), files: [] };
}

function insertClassificationFile(root: DirNode, f: PipelineClassificationFile): void {
  const partsActual = f.rel_path.split(/[/\\]+/).filter(Boolean);
  const partsDisplay = (f.display_rel_path ?? f.rel_path).split(/[/\\]+/).filter(Boolean);
  if (partsActual.length === 0) return;
  const dirPartsActual = partsActual.slice(0, -1);
  let node = root;
  for (let i = 0; i < dirPartsActual.length; i++) {
    const act = dirPartsActual[i]!;
    const disp = partsDisplay[i] ?? act;
    const prefix = dirPartsActual.slice(0, i + 1).join('/');
    if (!node.subdirs.has(act)) {
      node.subdirs.set(act, {
        segment: disp,
        pathPrefix: prefix,
        subdirs: new Map(),
        files: [],
      });
    }
    node = node.subdirs.get(act)!;
  }
  node.files.push(f);
}

function buildClassificationTree(files: PipelineClassificationFile[]): DirNode {
  const root = emptyRootDir();
  for (const f of files) insertClassificationFile(root, f);
  return root;
}

function sortedSubdirs(node: DirNode): DirNode[] {
  return [...node.subdirs.values()].sort((a, b) => a.segment.localeCompare(b.segment));
}

function sortedFilesInDir(node: DirNode): PipelineClassificationFile[] {
  return [...node.files].sort((a, b) => a.rel_path.localeCompare(b.rel_path));
}

function fileBasename(relPath: string): string {
  const parts = relPath.split(/[/\\]+/).filter(Boolean);
  return parts[parts.length - 1] ?? relPath;
}

function countFilesUnder(node: DirNode): number {
  let n = node.files.length;
  for (const s of node.subdirs.values()) n += countFilesUnder(s);
  return n;
}

function ClassificationFileRow({
  taskId,
  channelId,
  f,
  depth,
}: {
  taskId: string;
  channelId: string;
  f: PipelineClassificationFile;
  depth: number;
}) {
  const pad = 8 + depth * 16;
  const fileChannelId = f.source_channel_id ?? channelId;
  return (
    <tr className="border-t border-border-light">
      <td className="py-2 text-text-primary" style={{ paddingLeft: pad }}>
        <div className="flex min-w-0 items-center gap-2 pr-2">
          <FileText className="h-3.5 w-3.5 shrink-0 text-text-secondary" />
          <span className="truncate font-mono text-xs" title={f.rel_path}>
            {fileChannelId !== channelId ? (
              <span className="text-[10px] text-text-tertiary">{fileChannelId}/</span>
            ) : null}
            {fileBasename(f.display_rel_path ?? f.rel_path)}
          </span>
        </div>
      </td>
      <td className="px-3 py-2 text-right text-xs text-text-secondary">{fmtBytes(f.size)}</td>
      <td className="px-3 py-2 text-right">
        <div className="inline-flex items-center justify-end gap-2">
          <button
            type="button"
            onClick={() => {
              const path = f.rel_path;
              const dl = PipelineApi.sourceFileDownloadUrl(taskId, fileChannelId, path);
              const base = fileBasename(f.display_rel_path ?? path);
              void downloadPipelineArtifactUrl(dl, base).catch((e) =>
                window.alert((e as Error).message),
              );
            }}
            className="inline-flex items-center gap-1 rounded-md border border-border-light px-2 py-1 text-xs text-text-secondary hover:bg-surface-hover"
          >
            <Download className="h-3 w-3" />
            下载
          </button>
          <ReplaceButton taskId={taskId} channelId={fileChannelId} relPath={f.rel_path} />
        </div>
      </td>
    </tr>
  );
}

function ClassificationFolderSubtree({
  taskId,
  channelId,
  node,
  depth,
  expanded,
  toggle,
}: {
  taskId: string;
  channelId: string;
  node: DirNode;
  depth: number;
  expanded: Record<string, boolean>;
  toggle: (pathPrefix: string) => void;
}) {
  const open = !!expanded[node.pathPrefix];
  const subs = sortedSubdirs(node);
  const fs = sortedFilesInDir(node);
  const pad = 8 + depth * 16;
  const totalFiles = countFilesUnder(node);

  return (
    <>
      <tr className="border-t border-border-light bg-surface-secondary/30 hover:bg-surface-secondary/50">
        <td className="py-2 text-text-primary" colSpan={3} style={{ paddingLeft: pad }}>
          <button
            type="button"
            onClick={() => toggle(node.pathPrefix)}
            className="inline-flex max-w-full items-center gap-1 rounded px-1 py-0.5 text-left text-xs font-medium text-text-primary hover:text-green-500"
            aria-expanded={open}
          >
            <ChevronRight
              className={`h-4 w-4 shrink-0 text-text-secondary transition-transform ${open ? 'rotate-90' : ''}`}
              aria-hidden
            />
            <Folder className="h-3.5 w-3.5 shrink-0 text-amber-500/90" aria-hidden />
            <span className="truncate">{node.segment}</span>
            <span className="shrink-0 font-normal text-text-secondary">
              · {totalFiles} 个文件
              {subs.length > 0 ? ` · ${subs.length} 个子文件夹` : ''}
            </span>
          </button>
        </td>
      </tr>
      {open &&
        subs.map((child) => (
          <ClassificationFolderSubtree
            key={child.pathPrefix}
            taskId={taskId}
            channelId={channelId}
            node={child}
            depth={depth + 1}
            expanded={expanded}
            toggle={toggle}
          />
        ))}
      {open &&
        fs.map((f) => (
          <ClassificationFileRow
            key={f.rel_path}
            taskId={taskId}
            channelId={channelId}
            f={f}
            depth={depth + 1}
          />
        ))}
    </>
  );
}

function ClassificationFilesTreeBody({
  taskId,
  channelId,
  files,
}: {
  taskId: string;
  channelId: string;
  files: PipelineClassificationFile[];
}) {
  const root = useMemo(() => buildClassificationTree(files), [files]);
  const [expanded, setExpanded] = useState<Record<string, boolean>>({});
  const toggle = useCallback((pathPrefix: string) => {
    setExpanded((prev) => ({ ...prev, [pathPrefix]: !prev[pathPrefix] }));
  }, []);

  const rootDirs = sortedSubdirs(root);
  const rootFiles = sortedFilesInDir(root);

  return (
    <tbody>
      {rootDirs.map((d) => (
        <ClassificationFolderSubtree
          key={d.pathPrefix}
          taskId={taskId}
          channelId={channelId}
          node={d}
          depth={0}
          expanded={expanded}
          toggle={toggle}
        />
      ))}
      {rootFiles.map((f) => (
        <ClassificationFileRow key={f.rel_path} taskId={taskId} channelId={channelId} f={f} depth={0} />
      ))}
    </tbody>
  );
}

function ReplaceButton({
  taskId,
  channelId,
  relPath,
}: {
  taskId: string;
  channelId: string;
  relPath: string;
}) {
  const inputRef = useRef<HTMLInputElement | null>(null);
  const [info, setInfo] = useState<string | null>(null);
  const replace = useReplaceSourceFile(taskId, channelId);
  return (
    <span className="inline-flex items-center gap-1.5">
      <input
        ref={inputRef}
        type="file"
        title={`替换 ${relPath}`}
        aria-label={`替换 ${relPath}`}
        className="hidden"
        onChange={async (e) => {
          const file = e.target.files?.[0];
          if (!file) return;
          try {
            const res = await replace.mutateAsync({ relPath, file });
            setInfo(
              res.is_changed
                ? `已替换 (sha ${res.old_sha256.slice(0, 6)}→${res.new_sha256.slice(0, 6)})`
                : '内容相同（hash 未变）',
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
      {info && <span className="text-xs text-text-secondary">{info}</span>}
    </span>
  );
}

function BillPrescanBlock({ data }: { data: PipelineBillPrescanResponse }) {
  return (
    <section className="rounded-lg border border-border-light bg-surface-primary">
      <div className="border-b border-border-light px-3 py-2">
        <h3 className="text-sm font-semibold text-text-primary">预扫结果</h3>
        <p className="mt-0.5 text-[11px] leading-snug text-text-secondary">
          按{' '}
          <span className="font-mono text-[10px] text-green-500/90">
            zhangdan/folder_match + zhangdan/all.py · BANK_SCRIPTS
          </span>{' '}
          识别各个账单子文件夹及脚本分发键；含嵌套（如 2026.03账单/CITI账单）。
        </p>
      </div>
      {data.folders.length === 0 ? (
        <div className="px-3 py-4 text-xs text-text-secondary">
          未命中任何「XX账单」式银行目录名；请对照{' '}
          <span className="font-mono">pipeline-svc/server/parsers/_legacy/zhangdan/all.py</span>{' '}
          检查文件夹命名。
        </div>
      ) : (
        <div className="overflow-x-auto">
          <table className="w-full min-w-[640px] text-sm">
            <thead className="bg-surface-secondary text-left text-[11px] text-text-secondary">
              <tr>
                <th className="px-3 py-2 font-medium">folder_name</th>
                <th className="px-3 py-2 font-medium">folder_path</th>
                <th className="px-3 py-2 font-medium">bank_key</th>
                <th className="px-3 py-2 text-right font-medium">file_count</th>
              </tr>
            </thead>
            <tbody>
              {data.folders.map((row) => (
                <tr key={row.folder_path} className="border-t border-border-light">
                  <td className="px-3 py-2 text-text-primary">{row.folder_name}</td>
                  <td className="px-3 py-2 font-mono text-[11px] text-text-secondary">
                    {row.folder_path}
                  </td>
                  <td className="px-3 py-2 font-mono text-xs text-green-500/90">{row.bank_key}</td>
                  <td className="px-3 py-2 text-right font-mono text-xs text-text-primary">
                    {row.file_count}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
      {data.folders.length === 0 && data.top_level_non_bank_dirs.length > 0 && (
        <div className="border-t border-border-light bg-amber-500/5 px-3 py-2 text-[11px] leading-relaxed text-amber-200/90">
          <div className="font-medium text-amber-300/95">解压根下第一层未命中银行目录名</div>
          <ul className="mt-1 list-inside list-disc space-y-0.5 text-text-secondary">
            {data.top_level_non_bank_dirs.map((d) => (
              <li key={d.folder_path}>
                <span className="font-mono text-text-primary">{d.folder_path}</span> — {d.hint}
              </li>
            ))}
          </ul>
          <p className="mt-2 text-text-secondary">
            已在整棵目录树内递归查找「XX账单」式文件夹，仍未识别任何银行目录。若顶层实为月份包（如 2026.03账单），请展开核对其下各家子文件夹命名是否与{' '}
            <span className="font-mono">zhangdan/folder_match.py</span> 中别名规则一致。
          </p>
        </div>
      )}
    </section>
  );
}

function OwnFlowPrescanBlock({ data }: { data: PipelineOwnFlowPrescanResponse }) {
  return (
    <section className="rounded-lg border border-border-light bg-surface-primary">
      <div className="border-b border-border-light px-3 py-2">
        <h3 className="text-sm font-semibold text-text-primary">
          已识别来源（将进入流水线解析）
        </h3>
        <p className="mt-0.5 text-[11px] leading-snug text-text-secondary">
          与{' '}
          <span className="font-mono text-[10px] text-green-500/90">own_flow_pkg/discovery.scan_inventory</span>{' '}
          一致：逐文件读取维度；未列入表格的文件见下方告警。
        </p>
      </div>
      {data.sources.length === 0 ? (
        <div className="px-3 py-4 text-xs text-text-secondary">
          当前目录下未发现会被流水线认领的来源文件。
        </div>
      ) : (
        <div className="max-h-[min(480px,58vh)] overflow-auto overscroll-y-contain">
          <table className="w-full min-w-[720px] text-sm">
            <thead className="sticky top-0 z-[1] bg-surface-secondary text-left text-[11px] text-text-secondary shadow-sm">
              <tr>
                <th className="w-10 px-2 py-2 text-right font-medium">#</th>
                <th className="px-3 py-2 font-medium">来源</th>
                <th className="px-3 py-2 font-medium">文件</th>
                <th className="px-3 py-2 text-right font-medium">行数</th>
                <th className="px-3 py-2 text-right font-medium">列数</th>
              </tr>
            </thead>
            <tbody>
              {data.sources.map((row) => (
                <tr key={`${row.index}-${row.rel_path}-${row.file}`} className="border-t border-border-light">
                  <td className="px-2 py-2 text-right font-mono text-[11px] text-text-secondary">
                    {row.index}
                  </td>
                  <td className="px-3 py-2 font-mono text-xs text-green-500/90">{row.source ?? '—'}</td>
                  <td className="max-w-[280px] px-3 py-2">
                    <div className="truncate font-mono text-[11px] text-text-primary" title={row.file ?? ''}>
                      {row.file ?? '—'}
                    </div>
                    {row.rel_path ? (
                      <div className="truncate font-mono text-[10px] text-text-secondary" title={row.rel_path}>
                        {row.rel_path}
                      </div>
                    ) : null}
                  </td>
                  <td className="px-3 py-2 text-right font-mono text-xs text-text-primary">
                    {row.row_count ?? '—'}
                  </td>
                  <td className="px-3 py-2 text-right font-mono text-xs text-text-primary">
                    {row.col_count ?? '—'}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
      {data.warnings.length > 0 && (
        <div className="border-t border-border-light px-3 py-2">
          <div className="text-[11px] font-medium text-amber-300/95">未纳入解析的文件</div>
          <ul className="mt-1 space-y-1 text-[11px] text-text-secondary">
            {data.warnings.map((w, i) => (
              <li key={`${w.rel_path}-${i}`} className="rounded-md bg-surface-secondary/60 px-2 py-1">
                <span className="font-mono text-text-primary">{w.rel_path ?? '—'}</span>
                {w.reason ? <span className="text-text-secondary"> · {w.reason}</span> : null}
                {w.detail ? (
                  <div className="mt-0.5 text-[10px] leading-snug text-text-secondary">{w.detail}</div>
                ) : null}
              </li>
            ))}
          </ul>
        </div>
      )}
    </section>
  );
}

export default function ChannelFilesTab({
  taskId,
  channelId,
  files,
  directoryCaption,
}: {
  taskId: string;
  channelId: string;
  files: PipelineClassificationFile[];
  /** 若提供则替换默认的「当前渠道目录」单一路径说明（如多目录联扫） */
  directoryCaption?: string | null;
}) {
  const prescanEnabled = channelId === 'bill' || channelId === 'own_flow';
  const prescan = useChannelPrescan(taskId, channelId);

  return (
    <div className="space-y-4 p-4">
      {prescanEnabled && (
        <div className="space-y-3">
          {prescan.isLoading && (
            <div className="flex items-center gap-2 rounded-lg border border-border-light bg-surface-primary px-3 py-2 text-xs text-text-secondary">
              <Loader2 className="h-3.5 w-3.5 animate-spin text-green-500" aria-hidden />
              正在生成预扫统计…
            </div>
          )}
          {prescan.error && (
            <div className="rounded-lg border border-red-500/40 bg-red-500/10 px-3 py-2 text-xs text-red-200">
              {(prescan.error as Error).message}
            </div>
          )}
          {prescan.data?.kind === 'bill' && <BillPrescanBlock data={prescan.data} />}
          {prescan.data?.kind === 'own_flow' && <OwnFlowPrescanBlock data={prescan.data} />}
        </div>
      )}

      <div className="rounded-lg border border-border-light bg-surface-primary p-3 text-xs text-text-secondary">
        当前渠道目录：
        {directoryCaption != null && directoryCaption !== '' ? (
          <span className="text-text-primary"> {directoryCaption}</span>
        ) : (
          <span className="font-mono">
            {' '}
            data/tasks/{taskId.slice(0, 8)}…/extracted/{channelId}/
          </span>
        )}
      </div>
      {files.length === 0 ? (
        <div className="flex items-center gap-2 rounded-lg border border-dashed border-border-medium p-6 text-sm text-text-secondary">
          <Folder className="h-5 w-5" />
          暂无文件，请回到“总览”使用整包上传或在此渠道单独上传。
        </div>
      ) : (
        <div className="overflow-hidden rounded-lg border border-border-light">
          <div className="border-b border-border-light bg-surface-secondary px-3 py-2 text-xs font-medium text-text-secondary">
            <span>原始文件列表</span>
            <span className="ml-2 font-normal text-[11px] text-text-secondary/80">
              顶层：文件夹在上、根目录独立文件在下；展开后同样先子文件夹、后本目录文件。
            </span>
          </div>
          <table className="w-full text-sm">
            <thead className="bg-surface-secondary text-xs text-text-secondary">
              <tr>
                <th className="px-3 py-2 text-left">文件</th>
                <th className="px-3 py-2 text-right">大小</th>
                <th className="px-3 py-2 text-right">操作</th>
              </tr>
            </thead>
            <ClassificationFilesTreeBody taskId={taskId} channelId={channelId} files={files} />
          </table>
        </div>
      )}
    </div>
  );
}
