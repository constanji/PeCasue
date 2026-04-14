import { memo } from 'react';
import type { SummaryContentPart } from '@because/data-provider';
import { ContentTypes } from '@because/data-provider';

function summaryText(part: SummaryContentPart): string {
  if (typeof part.text === 'string' && part.text.trim().length > 0) {
    return part.text;
  }
  if (Array.isArray(part.content)) {
    return part.content
      .filter((b) => b?.type === ContentTypes.TEXT && typeof b.text === 'string')
      .map((b) => b.text)
      .join('\n');
  }
  return '';
}

const Summary = memo(({ part }: { part: SummaryContentPart }) => {
  const text = summaryText(part);
  if (!text) {
    return null;
  }
  return (
    <div
      className="border-token-border-medium bg-surface-secondary/60 my-2 rounded-lg border px-3 py-2 text-sm"
      role="region"
      aria-label="Conversation summary"
    >
      <div className="text-token-text-secondary mb-1 text-xs font-medium">上下文摘要</div>
      <div className="text-token-text-primary whitespace-pre-wrap">{text}</div>
    </div>
  );
});

Summary.displayName = 'Summary';

export default Summary;
