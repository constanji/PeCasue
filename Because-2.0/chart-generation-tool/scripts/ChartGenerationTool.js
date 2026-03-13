const { Tool } = require('@langchain/core/tools');
const { z } = require('zod');
const { logger } = require('@because/data-schemas');
const { Tools } = require('@because/data-provider');
const DataAnalyzer = require('../utils/dataAnalyzer');
const ChartTypeSelector = require('../utils/chartTypeSelector');
const PlotlyChartGenerator = require('../utils/plotlyGenerator');

/**
 * Chart Generation Tool - 图表生成工具
 * 
 * 将 SQL 查询结果自动转换为可视化图表
 */
class ChartGenerationTool extends Tool {
  name = 'chart_generation';

  description =
    '图表生成工具，根据SQL查询结果自动生成可视化图表。' +
    '工具会智能分析数据特征，自动选择最合适的图表类型（柱状图、散点图、直方图、热力图、时间序列图、表格等）。' +
    '支持自动图表类型选择，也支持手动指定图表类型。';

  schema = z.object({
    data: z
      .array(z.record(z.any()))
      .min(1)
      .describe('SQL查询结果数据数组，格式为 [{col1: val1, col2: val2}, ...]'),
    title: z
      .string()
      .optional()
      .describe('图表标题，默认为 "Chart"'),
    chart_type: z
      .enum([
        'table',
        'histogram',
        'bar',
        'scatter',
        'heatmap',
        'time_series',
        'grouped_bar',
        'generic',
        'auto',
      ])
      .optional()
      .describe('指定图表类型（可选），不提供或传 "auto" 则自动选择'),
    x_axis: z
      .string()
      .optional()
      .describe('X轴字段名（可选，用于指定图表类型时）'),
    y_axis: z
      .string()
      .optional()
      .describe('Y轴字段名（可选，用于指定图表类型时）'),
    max_rows: z
      .number()
      .int()
      .positive()
      .max(10000)
      .optional()
      .describe('最大行数限制，默认1000，超过会自动采样'),
  });

  constructor(fields = {}) {
    super();
    this.dataAnalyzer = new DataAnalyzer();
    this.chartTypeSelector = new ChartTypeSelector();
    this.plotlyGenerator = new PlotlyChartGenerator();
  }

  /**
   * 重写 invoke 方法以支持返回 artifact
   * @override
   */
  async invoke(input, config) {
    const result = await this._call(input);
    
    // 如果返回的是包含 artifact 的对象，直接返回
    if (result && typeof result === 'object' && result.artifact) {
      return result;
    }
    
    // 否则返回标准格式
    return {
      content: typeof result === 'string' ? result : JSON.stringify(result),
    };
  }

  /**
   * @override
   */
  async _call(input) {
    const startTime = Date.now();
    try {
      logger.info('[ChartGenerationTool] 收到输入参数:', {
        hasData: !!input.data,
        dataType: typeof input.data,
        dataLength: Array.isArray(input.data) ? input.data.length : 'N/A',
        title: input.title,
        chartType: input.chart_type,
        xAxis: input.x_axis,
        yAxis: input.y_axis,
        keys: Object.keys(input)
      });

      const { data, title = 'Chart', chart_type, x_axis, y_axis, max_rows = 1000 } = input;

      // 检查data参数
      if (!data) {
        throw new Error('data参数不能为空，必须提供SQL查询结果数据');
      }

      if (!Array.isArray(data)) {
        throw new Error('data参数必须是数组格式');
      }

      logger.info('[ChartGenerationTool] 开始生成图表:', {
        dataRows: data.length,
        title,
        chartType: chart_type || 'auto',
      });

      // 1. 数据清洗和限制
      let cleanedData = this.dataAnalyzer.cleanData(data);
      cleanedData = this.dataAnalyzer.limitRows(cleanedData, max_rows);

      if (cleanedData.length === 0) {
        throw new Error('数据为空，无法生成图表');
      }

      // 2. 识别列类型
      const columnTypes = this.dataAnalyzer.identifyColumnTypes(cleanedData);
      logger.info('[ChartGenerationTool] 列类型识别:', columnTypes);

      // 3. 选择图表类型（"auto" 或不传时自动选择）
      let selectedChartType = chart_type === 'auto' || !chart_type ? null : chart_type;
      if (!selectedChartType) {
        selectedChartType = this.chartTypeSelector.selectChartType(cleanedData, columnTypes);
        logger.info(`[ChartGenerationTool] 自动选择图表类型: ${selectedChartType}`);
      } else if (!this.chartTypeSelector.isValidChartType(selectedChartType)) {
        throw new Error(`无效的图表类型: ${selectedChartType}`);
      }

      // 4. 生成图表配置
      const chartConfig = this._generateChartConfig(
        cleanedData,
        selectedChartType,
        columnTypes,
        title,
        x_axis,
        y_axis,
      );

      // 强制确保柱状图使用绿色
      if (selectedChartType === 'bar' && chartConfig.data && chartConfig.data[0]) {
        chartConfig.data[0].marker = chartConfig.data[0].marker || {};
        chartConfig.data[0].marker.color = '#15a8a8';
      }

      // 5. 构建返回结果
      const columns = Object.keys(cleanedData[0]);
      const result = {
        success: true,
        chart: {
          type: 'plotly',
          data: chartConfig,
          config: {
            data_shape: {
              rows: cleanedData.length,
              columns: columns.length,
            },
            chart_type: selectedChartType,
            columns: columns,
          },
        },
        metadata: {
          row_count: cleanedData.length,
          column_count: columns.length,
          chart_type: selectedChartType,
          columns: columns,
          column_types: columnTypes,
        },
      };

      const duration = Date.now() - startTime;
      logger.info(`[ChartGenerationTool] 图表生成完成，耗时: ${duration}ms`);

      // 生成包含 Plotly 图表的 HTML
      const chartId = `chart-${Date.now()}-${Math.random().toString(36).substr(2, 9)}`;
      const plotlyHtml = `
<!DOCTYPE html>
<html>
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <script src="https://cdn.plot.ly/plotly-latest.min.js"></script>
  <style>
    body {
      margin: 0;
      padding: 0;
      font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', 'Roboto', 'Oxygen', 'Ubuntu', 'Cantarell', 'Fira Sans', 'Droid Sans', 'Helvetica Neue', sans-serif;
      background: #ffffff;
      overflow: hidden;
    }
    #${chartId} {
      width: 100%;
      height: 480px;
      max-height: 80vh;
      margin: 0;
      padding: 0;
    }
  </style>
</head>
<body>
  <div id="${chartId}"></div>
  <script>
    const chartData = ${JSON.stringify(chartConfig.data)};
    const chartLayout = ${JSON.stringify(chartConfig.layout)};
    Plotly.newPlot('${chartId}', chartData, chartLayout, {responsive: true});
  </script>
</body>
</html>
      `.trim();

      // 返回包含 artifact 的对象，用于前端渲染图表
      // 生成一个唯一的 URI 用于标识这个图表资源
      const chartUri = `ui://chart-${Date.now()}-${Math.random().toString(36).substr(2, 9)}`;
      
      // 创建图表标记，让LLM可以在对话中引用
      const chartMarker = `[chart:${title}:${chartId}]`;

      const returnValue = {
        content: `${JSON.stringify(result, null, 2)}\n\n${chartMarker}`,
        artifact: {
          [Tools.ui_resources]: {
            data: [
              {
                type: 'text/html',
                uri: chartUri,
                text: plotlyHtml,
                mimeType: 'text/html',
                title: title,
                data: chartConfig.data,
                layout: chartConfig.layout,
                chartId: chartId,
              },
            ],
          },
        },
        // 添加特殊字段用于前端处理（保留用于向后兼容）
        _chartData: {
          marker: chartMarker,
          chartId: chartId,
          title: title,
          data: chartConfig.data,
          layout: chartConfig.layout,
          // 注意：html 字段已移除，因为前端现在主要通过 artifact.ui_resources 来渲染
          // 如果需要，可以从 artifact.ui_resources.data[0].text 获取
        },
      };

      // 记录返回结果的结构
      logger.info('[ChartGenerationTool] 返回结果结构:', {
        hasContent: !!returnValue.content,
        hasArtifact: !!returnValue.artifact,
        artifactKeys: returnValue.artifact ? Object.keys(returnValue.artifact) : [],
        hasChartData: !!returnValue._chartData,
        chartDataKeys: returnValue._chartData ? Object.keys(returnValue._chartData) : [],
        uiResourcesCount: returnValue.artifact?.[Tools.ui_resources]?.data?.length ?? 0,
      });

      return returnValue;
    } catch (error) {
      const duration = Date.now() - startTime;
      logger.error(`[ChartGenerationTool] 图表生成失败 (耗时: ${duration}ms):`, error);

      return JSON.stringify(
        {
          success: false,
          error: error.message || '图表生成失败',
          metadata: {
            row_count: input.data?.length || 0,
            column_count: 0,
            chart_type: null,
          },
        },
        null,
        2,
      );
    }
  }

  /**
   * 生成图表配置
   * @private
   */
  _generateChartConfig(data, chartType, columnTypes, title, xAxis, yAxis) {
    const { numeric, categorical, datetime } = columnTypes;

    switch (chartType) {
      case 'table':
        return this.plotlyGenerator.createTable(data, title);

      case 'histogram':
        if (numeric.length === 0) {
          throw new Error('直方图需要至少一个数值列');
        }
        return this.plotlyGenerator.createHistogram(data, numeric[0], title);

      case 'bar':
        if (xAxis && yAxis) {
          return this.plotlyGenerator.createBarChart(data, xAxis, yAxis, title);
        }
        if (categorical.length > 0 && numeric.length > 0) {
          return this.plotlyGenerator.createBarChart(
            data,
            categorical[0],
            numeric[0],
            title,
          );
        }
        if (categorical.length > 0) {
          // 值计数柱状图
          const counts = this.dataAnalyzer.valueCounts(data, categorical[0]);
          return this.plotlyGenerator.createBarChart(
            counts.map((c) => ({ [categorical[0]]: c.value, count: c.count })),
            categorical[0],
            'count',
            title,
          );
        }
        throw new Error('柱状图需要至少一个分类列');

      case 'scatter':
        if (xAxis && yAxis) {
          return this.plotlyGenerator.createScatterPlot(data, xAxis, yAxis, title);
        }
        if (numeric.length >= 2) {
          return this.plotlyGenerator.createScatterPlot(data, numeric[0], numeric[1], title);
        }
        throw new Error('散点图需要至少两个数值列');

      case 'heatmap':
        if (numeric.length < 3) {
          throw new Error('相关性热力图需要至少三个数值列');
        }
        return this.plotlyGenerator.createCorrelationHeatmap(data, numeric, title);

      case 'time_series':
        if (datetime.length === 0 || numeric.length === 0) {
          throw new Error('时间序列图需要至少一个时间列和一个数值列');
        }
        return this.plotlyGenerator.createTimeSeriesChart(
          data,
          datetime[0],
          numeric,
          title,
        );

      case 'grouped_bar':
        if (categorical.length < 2) {
          throw new Error('分组柱状图需要至少两个分类列');
        }
        return this.plotlyGenerator.createGroupedBarChart(data, categorical, title);

      case 'generic':
        const columns = Object.keys(data[0]);
        if (columns.length >= 2) {
          return this.plotlyGenerator.createGenericChart(data, columns[0], columns[1], title);
        }
        throw new Error('通用图表需要至少两列数据');

      default:
        throw new Error(`不支持的图表类型: ${chartType}`);
    }
  }
}

module.exports = ChartGenerationTool;

