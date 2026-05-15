import { useEffect, useMemo, useState, type ReactNode } from 'react';
import { Loader2 } from 'lucide-react';
import * as XLSX from 'xlsx';
import { useLocalize } from '~/hooks';

const MAX_ROWS = 400;
const MAX_COLS = 64;

type SpreadsheetNativeViewerProps = {
  blob: Blob;
  onParseError: (message: string) => void;
};

function sheetToTable(ws: XLSX.WorkSheet): ReactNode {
  const ref = ws['!ref'];
  if (!ref) {
    return <p className="text-text-secondary">（空工作表）</p>;
  }
  const range = XLSX.utils.decode_range(ref);
  const rows: ReactNode[] = [];
  const lastRow = Math.min(range.e.r, range.s.r + MAX_ROWS - 1);
  const lastCol = Math.min(range.e.c, range.s.c + MAX_COLS - 1);

  for (let R = range.s.r; R <= lastRow; R++) {
    const cells: ReactNode[] = [];
    for (let C = range.s.c; C <= lastCol; C++) {
      const addr = XLSX.utils.encode_cell({ r: R, c: C });
      const cell = ws[addr];
      let display = '';
      if (cell) {
        if (cell.w != null) {
          display = String(cell.w);
        } else if (cell.v != null) {
          display = String(cell.v);
        }
      }
      cells.push(
        <td key={addr} className="border border-border-light px-2 py-0.5 align-top text-xs">
          {display}
        </td>,
      );
    }
    rows.push(
      <tr key={R} className="bg-surface-secondary even:bg-surface-primary">
        {cells}
      </tr>,
    );
  }

  return (
    <div className="overflow-auto">
      <table className="w-max min-w-full border-collapse border border-border-light text-text-primary">
        <tbody>{rows}</tbody>
      </table>
      {(range.e.r - range.s.r + 1 > MAX_ROWS || range.e.c - range.s.c + 1 > MAX_COLS) && (
        <p className="mt-2 text-xs text-text-secondary">
          仅展示前 {MAX_ROWS} 行 × {MAX_COLS} 列，完整内容请下载后用 Excel 打开。
        </p>
      )}
    </div>
  );
}

/**
 * SheetJS 读入 xlsx/xls，以 HTML 表格形式做原生预览（图表/公式等无法完整呈现）。
 */
export default function SpreadsheetNativeViewer({ blob, onParseError }: SpreadsheetNativeViewerProps) {
  const localize = useLocalize();
  const [arrayBuffer, setArrayBuffer] = useState<ArrayBuffer | null>(null);
  const [bufferLoading, setBufferLoading] = useState(true);

  useEffect(() => {
    let cancelled = false;
    setBufferLoading(true);
    setArrayBuffer(null);
    void blob.arrayBuffer().then((ab) => {
      if (!cancelled) {
        setArrayBuffer(ab);
        setBufferLoading(false);
      }
    });
    return () => {
      cancelled = true;
    };
  }, [blob]);

  const model = useMemo(() => {
    if (!arrayBuffer) {
      return null;
    }
    try {
      const wb = XLSX.read(arrayBuffer, {
        type: 'array',
        cellDates: true,
        sheetRows: MAX_ROWS + 2,
      });
      return { wb } as const;
    } catch (e) {
      const msg = e instanceof Error ? e.message : String(e);
      return { error: msg } as const;
    }
  }, [arrayBuffer]);

  useEffect(() => {
    if (model && 'error' in model && model.error) {
      onParseError(model.error);
    }
  }, [model, onParseError]);

  const [sheetIndex, setSheetIndex] = useState(0);

  if (bufferLoading || !arrayBuffer) {
    return (
      <div className="flex items-center gap-2 text-text-secondary">
        <Loader2 className="h-5 w-5 animate-spin" aria-hidden />
        <span>{localize('com_ui_loading')}</span>
      </div>
    );
  }

  if (!model || 'error' in model) {
    return null;
  }

  const { wb } = model;
  const names = wb.SheetNames;
  if (names.length === 0) {
    return <p className="text-text-secondary">（无工作表）</p>;
  }

  const safeIndex = Math.min(sheetIndex, names.length - 1);
  const name = names[safeIndex];
  const ws = wb.Sheets[name];
  if (!ws) {
    return <p className="text-text-secondary">（工作表缺失）</p>;
  }

  return (
    <div className="flex min-h-0 flex-1 flex-col gap-2">
      {names.length > 1 && (
        <label className="flex flex-wrap items-center gap-2 text-xs text-text-secondary">
          <span>工作表</span>
          <select
            className="rounded border border-border-light bg-surface-primary px-2 py-1 text-sm text-text-primary"
            value={safeIndex}
            onChange={(e) => setSheetIndex(Number(e.target.value))}
          >
            {names.map((n, i) => (
              <option key={n} value={i}>
                {n}
              </option>
            ))}
          </select>
        </label>
      )}
      {sheetToTable(ws)}
    </div>
  );
}
