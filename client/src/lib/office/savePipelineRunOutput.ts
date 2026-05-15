import { c_oAscFileType2, convertBinToDocument } from '~/lib/office/x2t';

function toArrayBuffer(input: Uint8Array): ArrayBuffer {
  return input.buffer.slice(input.byteOffset, input.byteOffset + input.byteLength) as ArrayBuffer;
}

const API_PREFIX = '/api/pipeline';

export async function savePipelineRunOutput(target: {
  taskId: string;
  channelId: string;
  runId: string;
  /** Registered ``output_files[].name`` — must match task state. */
  storageName: string;
  originalFileName: string;
  outputFormat: number;
  saveData: Uint8Array;
}): Promise<unknown> {
  const targetExt = c_oAscFileType2[target.outputFormat];
  if (!targetExt) {
    throw new Error(`不支持的输出格式: ${target.outputFormat}`);
  }

  const converted = await convertBinToDocument(
    target.saveData,
    target.originalFileName,
    targetExt,
  );
  const blob = new Blob([toArrayBuffer(converted.data)], {
    type: converted.fileName.endsWith('.csv') ? 'text/csv' : 'application/octet-stream',
  });
  const file = new File([blob], target.storageName, { type: blob.type });

  const fd = new FormData();
  fd.append('name', target.storageName);
  fd.append('file', file);

  const url = `${API_PREFIX}/tasks/${encodeURIComponent(target.taskId)}/channels/${encodeURIComponent(
    target.channelId,
  )}/runs/${encodeURIComponent(target.runId)}/files/replace`;

  const res = await fetch(url, {
    method: 'POST',
    credentials: 'include',
    body: fd,
  });

  if (!res.ok) {
    let msg = `保存失败: ${res.status}`;
    try {
      const detail = (await res.json()) as { detail?: unknown; message?: unknown };
      if (typeof detail.message === 'string' && detail.message.trim()) msg = detail.message;
      else if (typeof detail.detail === 'string') msg = detail.detail;
    } catch {
      /* ignore */
    }
    throw new Error(msg);
  }

  return res.json();
}

export async function saveAllocationMergeUploadOutput(target: {
  taskId: string;
  /** Must match the workbook basename on server (OnlyOffice storageName). */
  storageName: string;
  originalFileName: string;
  outputFormat: number;
  saveData: Uint8Array;
}): Promise<unknown> {
  const targetExt = c_oAscFileType2[target.outputFormat];
  if (!targetExt) {
    throw new Error(`不支持的输出格式: ${target.outputFormat}`);
  }

  const converted = await convertBinToDocument(
    target.saveData,
    target.originalFileName,
    targetExt,
  );
  const blob = new Blob([toArrayBuffer(converted.data)], {
    type: converted.fileName.endsWith('.csv') ? 'text/csv' : 'application/octet-stream',
  });
  const file = new File([blob], target.storageName, { type: blob.type });

  const fd = new FormData();
  fd.append('file', file);

  const url = `${API_PREFIX}/tasks/${encodeURIComponent(
    target.taskId,
  )}/channels/allocation_base/upload-merge-base`;

  const res = await fetch(url, {
    method: 'POST',
    credentials: 'include',
    body: fd,
  });

  if (!res.ok) {
    let msg = `保存失败: ${res.status}`;
    try {
      const detail = (await res.json()) as { detail?: unknown; message?: unknown };
      if (typeof detail.message === 'string' && detail.message.trim()) msg = detail.message;
      else if (typeof detail.detail === 'string') msg = detail.detail;
    } catch {
      /* ignore */
    }
    throw new Error(msg);
  }

  return res.json();
}

export function officePreviewErrorMessage(err: unknown, fallback: string): string {
  if (err instanceof Error && err.message.trim()) return err.message;
  return fallback;
}
