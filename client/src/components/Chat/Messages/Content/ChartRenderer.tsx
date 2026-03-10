import React, { useEffect, useRef } from 'react';

interface ChartRendererProps {
  chartId: string;
  title: string;
  data: unknown[];
  layout: Record<string, unknown>;
}

/**
 * ChartRenderer 组件用于在对话中渲染内联图表
 * 它会在文本中的 [chart:标题:chartId] 标记处显示对应的Plotly图表
 */
export const ChartRenderer: React.FC<ChartRendererProps> = ({
  chartId,
  title,
  data,
  layout,
}) => {
  const containerRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    let isMounted = true;

    if (!data || !Array.isArray(data) || data.length === 0) {
      if (containerRef.current && isMounted) {
        containerRef.current.innerHTML =
          '<div style="display:flex;align-items:center;justify-content:center;height:200px;color:#d32f2f;">图表数据无效</div>';
      }
      return;
    }

    const doRender = () => {
      if (!isMounted || !containerRef.current || !window.Plotly) return;
      try {
        window.Plotly.newPlot(
          containerRef.current,
          data,
          { ...layout, autosize: true },
          { responsive: true, displayModeBar: 'hover', displaylogo: false },
        );
      } catch (error) {
        console.error('[ChartRenderer] render failed:', error);
      }
    };

    const scheduleRender = () => {
      requestAnimationFrame(() => {
        if (isMounted) doRender();
      });
    };

    const loadAndRender = () => {
      if (window.Plotly) {
        scheduleRender();
        return;
      }
      const existing = document.querySelector(
        'script[src*="plotly"]',
      ) as HTMLScriptElement | null;
      if (existing) {
        if (existing.dataset.loaded === '1') {
          scheduleRender();
        } else {
          existing.addEventListener('load', scheduleRender);
        }
      } else {
        const script = document.createElement('script');
        script.src = 'https://cdn.plot.ly/plotly-2.35.2.min.js';
        script.onload = () => {
          script.dataset.loaded = '1';
          scheduleRender();
        };
        script.onerror = () =>
          console.error('[ChartRenderer] Failed to load Plotly');
        document.head.appendChild(script);
      }
    };

    loadAndRender();

    const resizeObserver = new ResizeObserver(() => {
      if (window.Plotly && containerRef.current && isMounted) {
        try {
          window.Plotly.Plots.resize(containerRef.current);
        } catch { /* ignore */ }
      }
    });
    if (containerRef.current?.parentElement) {
      resizeObserver.observe(containerRef.current.parentElement);
    }

    return () => {
      isMounted = false;
      resizeObserver.disconnect();
    };
  }, [chartId, data, layout]);

  return (
    <div
      className="inline-chart-container"
      style={{
        width: '100%',
        minWidth: 0,
        margin: '12px 0',
        padding: '16px',
        border: '1px solid var(--border-light, #e0e0e0)',
        borderRadius: '8px',
        background: 'var(--surface-primary, #ffffff)',
        alignSelf: 'stretch',
      }}
    >
      {title && (
        <h3
          style={{
            marginTop: 0,
            marginBottom: '8px',
            color: 'var(--text-primary, #023d60)',
            fontSize: '15px',
            fontWeight: '600',
          }}
        >
          {title}
        </h3>
      )}
      <div
        id={chartId}
        ref={containerRef}
        style={{
          width: '100%',
          height: '400px',
        }}
      />
    </div>
  );
};

export type ExtractedChartData = {
  chartId: string;
  title: string;
  data: unknown[];
  layout: Record<string, unknown>;
};

/**
 * 直接从 tool output 内容字符串中提取图表数据 (vanna-inspired)
 *
 * langchain 的 Tool.invoke 会将工具返回的整个对象 JSON.stringify 为 ToolMessage.content，
 * 所以前端收到的 output 可能是以下两种格式之一：
 *
 * 格式 A (直接): `JSON_BLOB\n\n[chart:Title:chartId]`
 * 格式 B (包装): `{"content":"JSON_BLOB\n\n[chart:...]","_chartData":{...},"artifact":{...}}`
 *
 * 优先从格式 B 的 `_chartData` 中提取（最可靠），回退到格式 A。
 */
export const extractChartDataFromToolOutput = (
  output: string,
): ExtractedChartData | null => {
  if (!output) {
    return null;
  }

  try {
    const wrapper = JSON.parse(output);
    if (wrapper._chartData?.data && wrapper._chartData?.layout && wrapper._chartData?.chartId) {
      return {
        chartId: wrapper._chartData.chartId,
        title: wrapper._chartData.title || '',
        data: wrapper._chartData.data,
        layout: wrapper._chartData.layout,
      };
    }
    if (typeof wrapper.content === 'string') {
      return extractFromDirectFormat(wrapper.content);
    }
  } catch {
    // output 不是 JSON，尝试直接格式
  }

  return extractFromDirectFormat(output);
};

function extractFromDirectFormat(text: string): ExtractedChartData | null {
  try {
    const markerMatch = text.match(/\[chart:([^\]:]+):([^\]]+)\]/);
    if (!markerMatch) {
      return null;
    }

    const title = markerMatch[1];
    const chartId = markerMatch[2];
    const jsonEnd = text.indexOf('[chart:');
    const jsonStr = text.substring(0, jsonEnd).trim();

    const parsed = JSON.parse(jsonStr);
    if (!parsed.success || !parsed.chart) {
      return null;
    }

    const chartPayload = parsed.chart.data;
    if (!chartPayload?.data || !chartPayload?.layout) {
      return null;
    }

    return {
      chartId,
      title: title || parsed.chart.title || '',
      data: chartPayload.data,
      layout: chartPayload.layout,
    };
  } catch {
    return null;
  }
}


/**
 * 处理文本中的图表标记，将其替换为占位 HTML
 * 无论是否找到数据都生成占位符，实际渲染由 chart 组件处理
 */
export const preprocessChartMarkers = (
  text: string,
): string => {
  if (!text) {
    return text;
  }

  const chartRegex = /\[chart:([^\]:]+):([^\]]+)\]/g;
  return text.replace(chartRegex, (_match, title, chartId) => {
    return `<div class="chart-placeholder" data-chart-id="${chartId}" data-title="${title}"></div>`;
  });
};

/**
 * 检查文本是否包含图表标记
 */
export const hasChartMarkers = (text: string): boolean => {
  const chartRegex = /\[chart:([^\]:]+):([^\]]+)\]/g;
  return chartRegex.test(text);
};

// 扩展 window 对象以支持 Plotly
declare global {
  interface Window {
    Plotly?: any;
  }
}
