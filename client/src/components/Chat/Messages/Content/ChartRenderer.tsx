import React, { useEffect, useRef } from 'react';
import type { TAttachment } from '@because/data-provider';

interface ChartRendererProps {
  chartId: string;
  title: string;
  data: any[];
  layout: any;
  attachments?: TAttachment[];
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
  attachments = []
}) => {
  console.log('[ChartRenderer] Component called with:', { chartId, title, hasData: !!data, hasLayout: !!layout, attachmentsCount: attachments.length });
  const containerRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    let isMounted = true;

    // 检查数据有效性
    if (!data || !Array.isArray(data) || data.length === 0) {
      console.warn('[ChartRenderer] Invalid chart data:', data);
      if (containerRef.current && isMounted) {
        containerRef.current.innerHTML = `
          <div style="display: flex; align-items: center; justify-content: center; height: 200px; color: #d32f2f; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;">
            图表数据无效
          </div>
        `;
      }
      return;
    }

    // 检查 Plotly 是否已加载
    const renderChart = () => {
      if (!isMounted || !containerRef.current) return;

      if (window.Plotly) {
        try {
          window.Plotly.newPlot(chartId, data, {
            ...layout,
            autosize: true,
            responsive: true
          }, { responsive: true });
        } catch (error) {
          console.error('[ChartRenderer] Failed to render chart:', error);
          if (containerRef.current && isMounted) {
            containerRef.current.innerHTML = `
              <div style="display: flex; align-items: center; justify-content: center; height: 200px; color: #d32f2f; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;">
                图表渲染失败: ${error.message || '未知错误'}
              </div>
            `;
          }
        }
      } else if (containerRef.current) {
        // 如果 Plotly 还没加载，显示加载中
        containerRef.current.innerHTML = `
          <div style="display: flex; align-items: center; justify-content: center; height: 200px; color: #666; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;">
            <div style="text-align: center;">
              <div>图表加载中...</div>
              <div style="font-size: 12px; margin-top: 8px; color: #999;">正在加载 Plotly 库</div>
            </div>
          </div>
        `;
      }
    };

    // 如果 Plotly 已经加载，直接渲染
    if (window.Plotly) {
      renderChart();
    } else {
      // 检查是否已经有加载中的脚本
      const existingScript = document.querySelector('script[src="https://cdn.plot.ly/plotly-latest.min.js"]');
      if (existingScript) {
        // 如果脚本已经在加载中，等待它完成
        existingScript.addEventListener('load', renderChart);
        existingScript.addEventListener('error', () => {
          if (containerRef.current && isMounted) {
            containerRef.current.innerHTML = `
              <div style="display: flex; align-items: center; justify-content: center; height: 200px; color: #d32f2f; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;">
                图表库加载失败，请检查网络连接
              </div>
            `;
          }
        });
      } else {
        // 否则创建新的脚本标签
        const script = document.createElement('script');
        script.src = 'https://cdn.plot.ly/plotly-latest.min.js';
        script.onload = renderChart;
        script.onerror = () => {
          console.error('[ChartRenderer] Failed to load Plotly library');
          if (containerRef.current && isMounted) {
            containerRef.current.innerHTML = `
              <div style="display: flex; align-items: center; justify-content: center; height: 200px; color: #d32f2f; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;">
                <div style="text-align: center;">
                  <div>图表库加载失败</div>
                  <div style="font-size: 12px; margin-top: 8px; color: #999;">请检查网络连接后刷新页面</div>
                </div>
              </div>
            `;
          }
        };
        document.head.appendChild(script);
      }
    }

    // 监听窗口大小变化，重新渲染图表
    const handleResize = () => {
      if (window.Plotly && containerRef.current && isMounted) {
        try {
          window.Plotly.Plots.resize(chartId);
        } catch (error) {
          console.warn('[ChartRenderer] Failed to resize chart:', error);
        }
      }
    };

    window.addEventListener('resize', handleResize);

    return () => {
      isMounted = false;
      window.removeEventListener('resize', handleResize);
    };
  }, [chartId, data, layout]);

  return (
    <div
      className="inline-chart-container"
      style={{
        margin: '20px 0',
        padding: '20px',
        border: '1px solid #e0e0e0',
        borderRadius: '8px',
        background: '#ffffff'
      }}
    >
      {title && (
        <h3
          style={{
            marginTop: 0,
            color: '#023d60',
            fontFamily: '-apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif',
            fontSize: '16px',
            fontWeight: '600'
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
          height: '400px'
        }}
      />
    </div>
  );
};

/**
 * 从附件中查找图表数据
 */
export const findChartDataFromAttachments = (
  chartId: string,
  attachments: TAttachment[] = []
): { title: string; data: any[]; layout: any } | null => {
  console.log('[findChartDataFromAttachments] Looking for chartId:', chartId, 'in', attachments.length, 'attachments');

  for (const attachment of attachments) {
    console.log('[findChartDataFromAttachments] Checking attachment:', {
      type: attachment.type,
      hasChartData: !!attachment._chartData,
      chartDataId: attachment._chartData?.chartId,
      hasUiResources: !!attachment.ui_resources
    });
    // 优先检查 _chartData 字段（直接在附件中）
    if (attachment._chartData && attachment._chartData.chartId === chartId) {
      const chartData = attachment._chartData;
      console.log('[findChartDataFromAttachments] Found _chartData match:', {
        title: chartData.title,
        hasData: !!chartData.data,
        hasLayout: !!chartData.layout,
        dataLength: chartData.data?.length
      });
      if (chartData.data && chartData.layout) {
        console.log('[findChartDataFromAttachments] Returning chart data from _chartData');
        return {
          title: chartData.title || '',
          data: chartData.data,
          layout: chartData.layout
        };
      }
    }

    // 检查 ui_resources 中的结构化数据
    if (attachment.type === 'ui_resources' && attachment.ui_resources) {
      const uiResources = Array.isArray(attachment.ui_resources)
        ? attachment.ui_resources
        : attachment.ui_resources.data || [];

      for (const resource of uiResources) {
        if (resource.chartId === chartId && resource.type === 'text/html') {
          // 优先使用结构化数据（如果有的话）
          if (resource.data && resource.layout) {
            return {
              title: resource.title || '',
              data: resource.data,
              layout: resource.layout
            };
          }

          // 回退到从 HTML 中解析数据
          try {
            const htmlContent = resource.text || '';
            const dataMatch = htmlContent.match(/const chartData = (\[[\s\S]*?\]);/);
            const layoutMatch = htmlContent.match(/const chartLayout = (\{[\s\S]*?\});/);
            const titleMatch = htmlContent.match(/<h2>([^<]+)<\/h2>/);

            if (dataMatch && layoutMatch) {
              const data = JSON.parse(dataMatch[1]);
              const layout = JSON.parse(layoutMatch[1]);
              const title = titleMatch ? titleMatch[1] : '';

              return { title, data, layout };
            }
          } catch (error) {
            console.error('Failed to parse chart data from HTML:', error);
          }
        }
      }
    }
  }

  return null;
};

/**
 * 处理文本中的图表标记，将其替换为特殊的 markdown 语法
 */
export const preprocessChartMarkers = (
  text: string,
  attachments: TAttachment[] = []
): string => {
  if (!text) return text;

  const chartRegex = /\[chart:([^\]:]+):([^\]]+)\]/g;
  return text.replace(chartRegex, (match, title, chartId) => {
    const chartData = findChartDataFromAttachments(chartId, attachments);
    if (chartData) {
      // 返回特殊的 markdown 语法，后面会通过自定义组件处理
      return `<div class="chart-placeholder" data-chart-id="${chartId}" data-title="${title}"></div>`;
    } else {
      // 如果找不到图表数据，返回错误提示
      return `<div class="chart-error">图表数据未找到: ${title}</div>`;
    }
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
