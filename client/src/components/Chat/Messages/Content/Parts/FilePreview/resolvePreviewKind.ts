/** 与原生预览路由一致：决定拉取原件 Blob 还是直接走 /files/text */
export type PreviewKind = 'pdf' | 'spreadsheet' | 'word' | 'text' | 'unsupported';

/**
 * 根据 MIME 与扩展名解析预览类型（扩展名在 MIME 缺失时兜底）。
 */
export function resolvePreviewKind(mime?: string | null, filename?: string | null): PreviewKind {
  const m = (mime ?? '').toLowerCase();
  const ext = filename?.split('.').pop()?.toLowerCase() ?? '';

  if (m === 'application/pdf' || ext === 'pdf') {
    return 'pdf';
  }
  if (
    m === 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet' ||
    m === 'application/vnd.ms-excel' ||
    ext === 'xlsx' ||
    ext === 'xls'
  ) {
    return 'spreadsheet';
  }
  if (
    m === 'application/vnd.openxmlformats-officedocument.wordprocessingml.document' ||
    m === 'application/msword' ||
    ext === 'docx' ||
    ext === 'doc'
  ) {
    return 'word';
  }
  if (
    m.startsWith('text/') ||
    ext === 'txt' ||
    ext === 'csv' ||
    ext === 'md' ||
    ext === 'json' ||
    ext === 'xml'
  ) {
    return 'text';
  }
  return 'unsupported';
}

export function needsNativeBlob(kind: PreviewKind): boolean {
  return kind === 'pdf' || kind === 'spreadsheet';
}
