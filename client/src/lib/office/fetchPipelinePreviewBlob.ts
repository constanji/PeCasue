import axios from 'axios';
import { extractFilenameFromDisposition, triggerBrowserDownload } from '~/lib/office/previewUtils';

/**
 * 父窗口在打开 office-preview iframe 时把 Bearer 放在 URL hash（不随请求发往服务器、referer）。
 * 独立入口的 iframe 内无 AuthContext，axios.defaults 往往为空，生产环境必须经过 Node JWT 才能访问 /api/pipeline。
 */
export function readPipelineOfficeAuthorizationFromHash(): string | null {
  if (typeof window === 'undefined') return null;
  const h = window.location.hash?.replace(/^#/, '') ?? '';
  if (!h) return null;
  try {
    return new URLSearchParams(h).get('authorization');
  } catch {
    return null;
  }
}

/** 流水线产物下载走 /api/pipeline，需 Bearer；hash 优先（iframe），否则回落 axios.defaults（Vite 直连 sidecar 时代码路径）。 */
export function buildPipelineArtifactFetchInit(
  signal?: AbortSignal,
  authorizationOverride?: string | null,
): RequestInit {
  const fromAxios = axios.defaults.headers.common?.Authorization;
  const raw =
    authorizationOverride != null && String(authorizationOverride).trim() !== ''
      ? String(authorizationOverride).trim()
      : typeof fromAxios === 'string' && fromAxios.trim() !== ''
        ? fromAxios.trim()
        : '';
  const headers: Record<string, string> = {};
  if (raw) {
    headers.Authorization = raw.startsWith('Bearer ') ? raw : `Bearer ${raw}`;
  }
  return {
    credentials: 'include',
    signal,
    ...(Object.keys(headers).length ? { headers } : {}),
  };
}

function guessFileName(fileUrl: string, fileKind?: string | null): string {
  const pathPart = fileUrl.split('?')[0] || '';
  const baseName = decodeURIComponent(pathPart.split('/').pop() || 'preview');
  if (baseName.includes('.')) return baseName;
  return fileKind ? `${baseName}.${fileKind}` : `${baseName}.xlsx`;
}

export type FetchPipelinePreviewBlobInit = {
  signal?: AbortSignal;
  /** 通常为 hash 解析结果；与 axios 二选一即可 */
  authorization?: string | null;
};

/** Fetch run artifact or any same-origin pipeline URL（生产经 Node 反代时需 JWT）。 */
export async function fetchPipelinePreviewBlob(
  fileUrl: string,
  storageName: string,
  fileKind?: string | null,
  init?: FetchPipelinePreviewBlobInit,
): Promise<File> {
  const signal = init?.signal;
  const auth = init?.authorization ?? readPipelineOfficeAuthorizationFromHash();
  const response = await fetch(fileUrl, buildPipelineArtifactFetchInit(signal, auth));
  if (!response.ok) {
    throw new Error(`加载文件失败: ${response.status}`);
  }
  const blob = await response.blob();
  const resolvedName =
    extractFilenameFromDisposition(response.headers.get('content-disposition')) ||
    storageName ||
    guessFileName(fileUrl, fileKind);
  const mimeType =
    blob.type || (fileKind === 'csv' ? 'text/csv' : 'application/octet-stream');
  return new File([blob], resolvedName, { type: mimeType });
}

/** 生产环境不可使用裸 <a href> 下载 /api/pipeline/*（不会带 Bearer）。 */
export async function downloadPipelineArtifactUrl(
  url: string,
  filenameFallback: string,
): Promise<void> {
  const hashAuth = readPipelineOfficeAuthorizationFromHash();
  const res = await fetch(url, buildPipelineArtifactFetchInit(undefined, hashAuth));
  if (!res.ok) {
    let detail = '';
    try {
      const t = await res.text();
      if (t) detail = t.length > 160 ? `${t.slice(0, 160)}…` : t;
    } catch {
      /* ignore */
    }
    throw new Error(`下载失败: ${res.status}${detail ? ` · ${detail}` : ''}`);
  }
  const blob = await res.blob();
  const name =
    extractFilenameFromDisposition(res.headers.get('content-disposition')) ||
    filenameFallback;
  triggerBrowserDownload(blob, name);
}
