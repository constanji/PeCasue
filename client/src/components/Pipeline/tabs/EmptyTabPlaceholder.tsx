import React from 'react';

interface EmptyTabPlaceholderProps {
  icon: React.ReactNode;
  title: string;
  description: string;
  hints?: string[];
}

export default function EmptyTabPlaceholder({
  icon,
  title,
  description,
  hints,
}: EmptyTabPlaceholderProps) {
  return (
    <div className="flex h-full w-full items-center justify-center px-4 py-8">
      <div className="max-w-2xl rounded-2xl border border-border-light bg-surface-primary p-8 shadow-sm">
        <div className="flex items-start gap-4">
          <div className="flex h-16 w-16 flex-shrink-0 items-center justify-center rounded-xl border border-border-light bg-green-500/10 text-green-500">
            {icon}
          </div>
          <div className="flex-1">
            <h2 className="text-lg font-semibold text-text-primary">{title}</h2>
            <p className="mt-2 text-sm leading-relaxed text-text-secondary">{description}</p>
            {hints && hints.length > 0 && (
              <ul className="mt-4 space-y-1.5">
                {hints.map((hint) => (
                  <li
                    key={hint}
                    className="flex items-start gap-2 text-sm text-text-secondary"
                  >
                    <span className="mt-1.5 inline-block h-1.5 w-1.5 flex-shrink-0 rounded-full bg-green-500" />
                    <span>{hint}</span>
                  </li>
                ))}
              </ul>
            )}
            <div className="mt-5 inline-flex items-center gap-2 rounded-md border border-border-light px-2.5 py-1 text-xs text-text-secondary">
              <span className="inline-block h-1.5 w-1.5 rounded-full bg-green-500" />
              当前为 Phase 0 骨架，尚未联通后端
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}
