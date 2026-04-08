import { useMemo, useState } from 'react';
import { Document, Page, pdfjs } from 'react-pdf';
import workerUrl from 'pdfjs-dist/build/pdf.worker.min.mjs?url';
import 'react-pdf/dist/Page/AnnotationLayer.css';
import 'react-pdf/dist/Page/TextLayer.css';

if (typeof window !== 'undefined' && !pdfjs.GlobalWorkerOptions.workerSrc) {
  pdfjs.GlobalWorkerOptions.workerSrc = workerUrl;
}

type PdfNativeViewerProps = {
  fileUrl: string;
  onRenderError: (message: string) => void;
};

/**
 * 使用 PDF.js（react-pdf）在浏览器内渲染 PDF，保留版式与嵌入内容。
 */
export default function PdfNativeViewer({ fileUrl, onRenderError }: PdfNativeViewerProps) {
  const [numPages, setNumPages] = useState(0);

  const file = useMemo(() => fileUrl, [fileUrl]);

  return (
    <Document
      file={file}
      className="flex flex-col items-center gap-4"
      onLoadError={(e) => onRenderError(e.message || String(e))}
      onLoadSuccess={({ numPages: n }) => setNumPages(n)}
      loading={null}
    >
      {Array.from({ length: numPages }, (_, i) => (
        <Page
          key={i + 1}
          pageNumber={i + 1}
          className="shadow-sm"
          width={Math.min(900, typeof window !== 'undefined' ? window.innerWidth - 120 : 900)}
          renderTextLayer
          renderAnnotationLayer
        />
      ))}
    </Document>
  );
}
