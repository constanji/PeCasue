const { logger } = require('@because/data-schemas');

/**
 * The `addCharts` function processes chart markers in tool outputs and embeds
 * the corresponding HTML charts directly into the response message text.
 * This allows charts to be rendered inline within the conversation flow,
 * similar to how images are handled.
 *
 * @function
 * @module addCharts
 *
 * @param {Array.<Object>} intermediateSteps - An array of objects, each containing a tool observation
 * @param {Object} responseMessage - An object containing the text property which might have chart markers
 *
 * @property {string} intermediateSteps[].observation - The observation string which might contain chart data
 * @property {string} responseMessage.text - The text which might contain chart markers like [chart:Title:chartId]
 *
 * @example
 *
 * const intermediateSteps = [
 *   { observation: '{"success": true, "_chartData": {"marker": "[chart:Test Chart:chart-123]", "html": "<html>...</html>"}}' }
 * ];
 * const responseMessage = { text: 'Here is the chart: [chart:Test Chart:chart-123]' };
 *
 * addCharts(intermediateSteps, responseMessage);
 *
 * // responseMessage.text will now contain the embedded HTML chart
 *
 * @returns {void}
 */
function addCharts(intermediateSteps, responseMessage) {
  if (!intermediateSteps || !responseMessage || !responseMessage.text) {
    return;
  }

  const chartDataMap = new Map();

  // 从 intermediateSteps 中收集所有图表数据
  intermediateSteps.forEach((step) => {
    const { observation } = step;
    if (!observation) {
      return;
    }

    try {
      // 尝试解析 JSON 格式的 observation
      let parsed;
      if (typeof observation === 'string') {
        parsed = JSON.parse(observation);
      } else {
        parsed = observation;
      }

      // 检查是否有图表数据
      if (parsed && parsed._chartData) {
        const { marker, chartId, title, html, data, layout } = parsed._chartData;
        if (marker && (html || (data && layout))) {
          chartDataMap.set(marker, {
            chartId,
            title,
            html,
            data,
            layout,
            marker
          });
          logger.debug('[addCharts] Found chart data:', { marker, chartId, title });
        }
      }
    } catch (e) {
      // 如果不是 JSON，尝试从文本中提取图表标记
      const chartRegex = /\[chart:[^\]]+\]/g;
      const matches = observation.match(chartRegex);
      if (matches) {
        matches.forEach(marker => {
          // 如果文本中包含图表标记，我们假设图表数据在其他地方
          logger.debug('[addCharts] Found chart marker in text:', marker);
        });
      }
    }
  });

  if (chartDataMap.size === 0) {
    return;
  }

  // 替换响应文本中的图表标记为实际的HTML
  let updatedText = responseMessage.text;
  let hasReplacements = false;

  chartDataMap.forEach((chartData, marker) => {
    if (updatedText.includes(marker)) {
      // 创建一个简化的内联图表HTML（不包含完整的HTML文档结构）
      const inlineChartHtml = `
        <div class="inline-chart-container" style="margin: 20px 0; padding: 20px; border: 1px solid #e0e0e0; border-radius: 8px; background: #ffffff;">
          <h3 style="margin-top: 0; color: #023d60; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;">${chartData.title}</h3>
          <div id="${chartData.chartId}" style="width: 100%; height: 400px;"></div>
          <script>
            (function() {
              const chartData = ${JSON.stringify(chartData.data || [])};
              const chartLayout = ${JSON.stringify(chartData.layout || {})};
              if (typeof Plotly !== 'undefined') {
                Plotly.newPlot('${chartData.chartId}', chartData, chartLayout, {responsive: true});
              } else {
                document.getElementById('${chartData.chartId}').innerHTML =
                  '<div style="display: flex; align-items: center; justify-content: center; height: 100%; color: #666;">图表加载中... 请确保网络连接正常</div>';
              }
            })();
          </script>
        </div>
      `;

      updatedText = updatedText.replace(marker, inlineChartHtml);
      hasReplacements = true;
      logger.debug('[addCharts] Replaced chart marker with inline HTML:', marker);
    }
  });

  if (hasReplacements) {
    responseMessage.text = updatedText;
    logger.debug('[addCharts] Successfully embedded charts in response text');
  }
}

module.exports = addCharts;
