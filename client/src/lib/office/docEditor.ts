let docsApiPromise: Promise<void> | null = null;

export type OnlyOfficeEditor = {
  destroyEditor?: () => void;
  sendCommand: (payload: { command: string; data?: unknown }) => void;
};

export async function loadEditorApi(): Promise<void> {
  if ((window as Window & { DocsAPI?: unknown }).DocsAPI) {
    return;
  }

  if (!docsApiPromise) {
    docsApiPromise = new Promise<void>((resolve, reject) => {
      const script = document.createElement('script');
      const base =
        typeof window !== 'undefined'
          ? `${window.location.origin}/`
          : `${import.meta.env.BASE_URL || '/'}`;
      script.src = new URL('web-apps/apps/api/documents/api.js', base).href;
      script.onload = () => resolve();
      script.onerror = () => {
        docsApiPromise = null;
        reject(new Error('无法加载 OnlyOffice API 脚本'));
      };
      document.head.appendChild(script);
    });
  }

  await docsApiPromise;
}

export function createDocEditor(containerId: string, config: unknown): OnlyOfficeEditor {
  const DocsAPI = (window as Window & {
    DocsAPI?: {
      DocEditor: new (target: string, editorConfig: unknown) => OnlyOfficeEditor;
    };
  }).DocsAPI;

  if (!DocsAPI?.DocEditor) {
    throw new Error('OnlyOffice DocsAPI 未初始化');
  }

  return new DocsAPI.DocEditor(containerId, config);
}

export function destroyDocEditor(editor: OnlyOfficeEditor | null | undefined): void {
  if (editor && typeof editor.destroyEditor === 'function') {
    editor.destroyEditor();
  }
}
