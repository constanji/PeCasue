import React, { memo, useMemo } from 'react';
import remarkGfm from 'remark-gfm';
import remarkMath from 'remark-math';
import supersub from 'remark-supersub';
import rehypeKatex from 'rehype-katex';
import { useRecoilValue } from 'recoil';
import ReactMarkdown from 'react-markdown';
import rehypeHighlight from 'rehype-highlight';
import remarkDirective from 'remark-directive';
import type { Pluggable } from 'unified';
import { Citation, CompositeCitation, HighlightedText } from '~/components/Web/Citation';
import { Artifact, artifactPlugin } from '~/components/Artifacts/Artifact';
import { ArtifactProvider, CodeBlockProvider } from '~/Providers';
import MarkdownErrorBoundary from './MarkdownErrorBoundary';
import { langSubset, preprocessLaTeX } from '~/utils';
import { unicodeCitation } from '~/components/Web';
import { code, a, p, img, chart } from './MarkdownComponents';
import store from '~/store';
import { preprocessChartMarkers, hasChartMarkers } from './ChartRenderer';

type TContentProps = {
  content: string;
  isLatestMessage: boolean;
  attachments?: unknown[];
};

const Markdown = memo(({ content = '', isLatestMessage }: TContentProps) => {
  const LaTeXParsing = useRecoilValue<boolean>(store.LaTeXParsing);
  const isInitializing = content === '';

  const processedContent = useMemo(() => {
    if (isInitializing) {
      return '';
    }

    let processed = content;

    // 处理 LaTeX
    if (LaTeXParsing) {
      processed = preprocessLaTeX(processed);
    }

    if (hasChartMarkers(processed)) {
      processed = preprocessChartMarkers(processed);
    }

    return processed;
  }, [content, LaTeXParsing, isInitializing]);

  const rehypePlugins = useMemo(
    () => [
      [rehypeKatex],
      [
        rehypeHighlight,
        {
          detect: true,
          ignoreMissing: true,
          subset: langSubset,
        },
      ],
    ],
    [],
  );

  const remarkPlugins: Pluggable[] = [
    supersub,
    remarkGfm,
    remarkDirective,
    artifactPlugin,
    [remarkMath, { singleDollarTextMath: false }],
    unicodeCitation,
  ];

  if (isInitializing) {
    return (
      <div className="absolute">
        <p className="relative">
          <span className={isLatestMessage ? 'result-thinking' : ''} />
        </p>
      </div>
    );
  }

  return (
    <MarkdownErrorBoundary content={content} codeExecution={true}>
      <ArtifactProvider>
        <CodeBlockProvider>
          <ReactMarkdown
            /** @ts-ignore */
            remarkPlugins={remarkPlugins}
            /* @ts-ignore */
            rehypePlugins={rehypePlugins}
            components={
              {
                code,
                a,
                p,
                img,
                div: chart,
                artifact: Artifact,
                citation: Citation,
                'highlighted-text': HighlightedText,
                'composite-citation': CompositeCitation,
              } as {
                [nodeType: string]: React.ElementType;
              }
            }
          >
            {processedContent}
          </ReactMarkdown>
        </CodeBlockProvider>
      </ArtifactProvider>
    </MarkdownErrorBoundary>
  );
});

export default Markdown;
