/**
 * Plotly Chart Generator - Plotly 图表生成器
 * 
 * 封装各种图表类型的生成逻辑，参考 Vanna 的 Python 实现
 * 
 * 注意：plotly.js-dist-min 主要用于浏览器环境，在 Node.js 中我们直接构建 Plotly JSON 配置
 * 不需要实际渲染图表，只需要生成配置对象即可
 */

class PlotlyChartGenerator {
  constructor() {

    this.THEME_COLORS = {
      navy: '#023d60',
      cream: '#e7e1cf',
      teal: '#15a8a8',
      orange: '#fe5d26',
      magenta: '#bf1363',
    };

    // 图表颜色调色板 - 绿色渐变
    this.COLOR_PALETTE = ['#15a8a8', '#0f8a7a', '#0a6b5e', '#054c42'];
  }

  /**
   * 应用标准布局样式
   * @param {Object} layout - Plotly layout 对象
   * @returns {Object} 更新后的 layout
   */
  applyStandardLayout(layout = {}) {
    // 如果传入的title是默认值"Chart"或为空，则不显示标题
    // 否则保留传入的标题
    let finalTitle = undefined;
    if (layout.title) {
      const titleText = typeof layout.title === 'string' ? layout.title : layout.title.text;
      if (titleText && titleText !== 'Chart') {
        finalTitle = {
          ...(typeof layout.title === 'string' ? { text: layout.title } : layout.title),
          font: { color: this.THEME_COLORS.navy },
        };
      }
    }
    
    return {
      ...layout,
      title: finalTitle,
      font: { color: this.THEME_COLORS.navy },
      autosize: true,
      colorway: this.COLOR_PALETTE,
      // 优化上下留白
      margin: {
        l: 60,  // 左边距
        r: 20,  // 右边距
        t: 50,  // 上边距
        b: 50,  // 下边距
        pad: 0,  // 内边距
      },
    };
  }

  /**
   * 创建直方图
   * @param {Array<Object>} data - 数据数组
   * @param {string} column - 数值列名
   * @param {string} title - 图表标题
   * @returns {Object} Plotly 图表配置
   */
  createHistogram(data, column, title) {
    const values = data.map((row) => row[column]).filter((v) => typeof v === 'number');

    const trace = {
      x: values,
      type: 'histogram',
      marker: {
        color: this.THEME_COLORS.teal,
      },
    };

    const layout = this.applyStandardLayout({
      title: title,
      xaxis: { 
        title: column,
        title_standoff: 15, // x轴标题与轴线的距离
      },
      yaxis: { title: 'Count' },
      showlegend: false,
    });

    return {
      data: [trace],
      layout: layout,
    };
  }

  /**
   * 创建柱状图
   * @param {Array<Object>} data - 数据数组
   * @param {string} xCol - X轴列（分类）
   * @param {string} yCol - Y轴列（数值）
   * @param {string} title - 图表标题
   * @returns {Object} Plotly 图表配置
   */
  createBarChart(data, xCol, yCol, title) {
    // 如果需要聚合，先聚合数据
    const grouped = {};
    for (const row of data) {
      const key = String(row[xCol] || '');

      let value = row[yCol];
      if (typeof value === 'number' && !isNaN(value)) {
        // 已经是数字
      } else if (typeof value === 'string') {
        // 从字符串中提取数字
        // 包含逗号的数字字符串
        const cleanedValue = value.replace(/,/g, '');
        const parsedValue = parseFloat(cleanedValue);
        value = isNaN(parsedValue) ? 0 : parsedValue;
      } else {
        // 其他类型转换为0
        value = 0;
      }

      grouped[key] = (grouped[key] || 0) + value;
    }

    const xValues = Object.keys(grouped);
    const yValues = Object.values(grouped);

    const trace = {
      x: xValues,
      y: yValues,
      type: 'bar',
      marker: {
        color: '#15a8a8', // 绿色
      },
    };

    const layout = this.applyStandardLayout({
      title: title, // 传入title，applyStandardLayout会处理默认值"Chart"
      xaxis: { 
        title: xCol,
        title_standoff: 15, // x轴标题与轴线的距离，避免与柱子名称重叠
      },
      yaxis: { title: yCol },
    });

    return {
      data: [trace],
      layout: layout,
    };
  }

  /**
   * 创建散点图
   * @param {Array<Object>} data - 数据数组
   * @param {string} xCol - X轴列
   * @param {string} yCol - Y轴列
   * @param {string} title - 图表标题
   * @returns {Object} Plotly 图表配置
   */
  createScatterPlot(data, xCol, yCol, title) {
    // 数值转换辅助函数
    const toNumber = (value) => {
      if (typeof value === 'number' && !isNaN(value)) {
        return value;
      } else if (typeof value === 'string') {
        const cleanedValue = value.replace(/,/g, '');
        const parsedValue = parseFloat(cleanedValue);
        return isNaN(parsedValue) ? null : parsedValue;
      }
      return null;
    };

    const validData = data.map((row) => ({
      x: toNumber(row[xCol]),
      y: toNumber(row[yCol])
    })).filter((point) => point.x !== null && point.y !== null);

    const xValues = validData.map((point) => point.x);
    const yValues = validData.map((point) => point.y);

    // 确保长度一致
    const minLength = Math.min(xValues.length, yValues.length);
    const x = xValues.slice(0, minLength);
    const y = yValues.slice(0, minLength);

    const trace = {
      x: x,
      y: y,
      mode: 'markers',
      type: 'scatter',
      marker: {
        color: this.THEME_COLORS.magenta,
      },
    };

    const layout = this.applyStandardLayout({
      title: title,
      xaxis: { 
        title: xCol,
        title_standoff: 15, // x轴标题与轴线的距离
      },
      yaxis: { title: yCol },
    });

    return {
      data: [trace],
      layout: layout,
    };
  }

  /**
   * 创建相关性热力图
   * @param {Array<Object>} data - 数据数组
   * @param {Array<string>} columns - 数值列数组
   * @param {string} title - 图表标题
   * @returns {Object} Plotly 图表配置
   */
  createCorrelationHeatmap(data, columns, title) {
    // 计算相关性矩阵
    const correlationMatrix = this._calculateCorrelationMatrix(data, columns);

    // Vanna 颜色比例尺：navy (负) -> cream (中性) -> teal (正)
    const vannaColorscale = [
      [0.0, this.THEME_COLORS.navy],
      [0.5, this.THEME_COLORS.cream],
      [1.0, this.THEME_COLORS.teal],
    ];

    const trace = {
      z: correlationMatrix.matrix,
      x: columns,
      y: columns,
      type: 'heatmap',
      colorscale: vannaColorscale,
      zmin: -1,
      zmax: 1,
      colorbar: {
        title: 'Correlation',
      },
    };

    const layout = this.applyStandardLayout({
      title: title,
    });

    return {
      data: [trace],
      layout: layout,
    };
  }

  /**
   * 计算相关性矩阵
   * @private
   */
  _calculateCorrelationMatrix(data, columns) {
    const n = columns.length;
    const matrix = [];

    // 提取每列的数据
    const columnData = {};
    for (const col of columns) {
      columnData[col] = data.map((row) => row[col]).filter((v) => typeof v === 'number');
    }

    // 计算相关性
    for (let i = 0; i < n; i++) {
      const row = [];
      for (let j = 0; j < n; j++) {
        const col1 = columns[i];
        const col2 = columns[j];
        const corr = this._calculateCorrelation(columnData[col1], columnData[col2]);
        row.push(corr);
      }
      matrix.push(row);
    }

    return { matrix, columns };
  }

  /**
   * 计算两个数组的皮尔逊相关系数
   * @private
   */
  _calculateCorrelation(x, y) {
    if (x.length !== y.length || x.length === 0) {
      return 0;
    }

    // 找到有效数据对
    const pairs = [];
    for (let i = 0; i < x.length; i++) {
      if (typeof x[i] === 'number' && typeof y[i] === 'number' && !isNaN(x[i]) && !isNaN(y[i])) {
        pairs.push([x[i], y[i]]);
      }
    }

    if (pairs.length < 2) {
      return 0;
    }

    const n = pairs.length;
    const xValues = pairs.map((p) => p[0]);
    const yValues = pairs.map((p) => p[1]);

    // 计算均值
    const xMean = xValues.reduce((a, b) => a + b, 0) / n;
    const yMean = yValues.reduce((a, b) => a + b, 0) / n;

    // 计算协方差和方差
    let covariance = 0;
    let xVariance = 0;
    let yVariance = 0;

    for (let i = 0; i < n; i++) {
      const xDiff = xValues[i] - xMean;
      const yDiff = yValues[i] - yMean;
      covariance += xDiff * yDiff;
      xVariance += xDiff * xDiff;
      yVariance += yDiff * yDiff;
    }

    // 计算相关系数
    const denominator = Math.sqrt(xVariance * yVariance);
    if (denominator === 0) {
      return 0;
    }

    return covariance / denominator;
  }

  /**
   * 创建时间序列图
   * @param {Array<Object>} data - 数据数组
   * @param {string} timeCol - 时间列名
   * @param {Array<string>} valueCols - 数值列数组
   * @param {string} title - 图表标题
   * @returns {Object} Plotly 图表配置
   */
  createTimeSeriesChart(data, timeCol, valueCols, title) {
    const traces = [];
    const limitedCols = valueCols.slice(0, 5); // 限制最多5条线

    for (let i = 0; i < limitedCols.length; i++) {
      const col = limitedCols[i];
      const timeValues = data.map((row) => {
        const val = row[timeCol];
        // 尝试转换为日期
        if (val instanceof Date) {
          return val;
        }
        if (typeof val === 'string') {
          const date = new Date(val);
          return isNaN(date.getTime()) ? val : date;
        }
        return val;
      });
      const yValues = data.map((row) => row[col]).filter((v) => typeof v === 'number');

      // 确保长度一致
      const minLength = Math.min(timeValues.length, yValues.length);
      const x = timeValues.slice(0, minLength);
      const y = yValues.slice(0, minLength);

      traces.push({
        x: x,
        y: y,
        mode: 'lines',
        type: 'scatter',
        name: col,
        line: {
          color: this.COLOR_PALETTE[i % this.COLOR_PALETTE.length],
        },
      });
    }

    const layout = this.applyStandardLayout({
      title: title,
      xaxis: { 
        title: timeCol,
        title_standoff: 15, // x轴标题与轴线的距离
      },
      yaxis: { title: 'Value' },
      hovermode: 'x unified',
    });

    return {
      data: traces,
      layout: layout,
    };
  }

  /**
   * 创建分组柱状图
   * @param {Array<Object>} data - 数据数组
   * @param {Array<string>} categoricalCols - 分类列数组
   * @param {string} title - 图表标题
   * @returns {Object} Plotly 图表配置
   */
  createGroupedBarChart(data, categoricalCols, title) {
    if (categoricalCols.length >= 2) {
      // 使用前两个分类列进行分组
      const col1 = categoricalCols[0];
      const col2 = categoricalCols[1];

      // 计算分组计数
      const grouped = {};
      for (const row of data) {
        const key1 = String(row[col1] || '');
        const key2 = String(row[col2] || '');
        const groupKey = `${key1}|${key2}`;
        grouped[groupKey] = (grouped[groupKey] || 0) + 1;
      }

      // 提取唯一值
      const col1Values = [...new Set(data.map((row) => String(row[col1] || '')))];
      const col2Values = [...new Set(data.map((row) => String(row[col2] || '')))];

      // 构建 traces
      const traces = col2Values.map((col2Val, idx) => {
        const yValues = col1Values.map((col1Val) => {
          const groupKey = `${col1Val}|${col2Val}`;
          return grouped[groupKey] || 0;
        });

        return {
          x: col1Values,
          y: yValues,
          name: col2Val,
          type: 'bar',
          marker: {
            color: this.COLOR_PALETTE[idx % this.COLOR_PALETTE.length],
          },
        };
      });

      const layout = this.applyStandardLayout({
        title: title,
        barmode: 'group',
        xaxis: { 
          title: col1,
          title_standoff: 15, // x轴标题与轴线的距离
        },
        yaxis: { title: 'Count' },
      });

      return {
        data: traces,
        layout: layout,
      };
    } else {
      // 单个分类列：值计数
      const col = categoricalCols[0];
      const counts = {};
      for (const row of data) {
        const key = String(row[col] || '');
        counts[key] = (counts[key] || 0) + 1;
      }

      const xValues = Object.keys(counts);
      const yValues = Object.values(counts);

      const trace = {
        x: xValues,
        y: yValues,
        type: 'bar',
        marker: {
          color: '#15a8a8', // 统一的绿色
        },
      };

      const layout = this.applyStandardLayout({
        title: title,
        xaxis: { 
          title: col,
          title_standoff: 15, // x轴标题与轴线的距离
        },
        yaxis: { title: 'Count' },
      });

      return {
        data: [trace],
        layout: layout,
      };
    }
  }

  /**
   * 创建通用图表（根据列类型自动选择）
   * @param {Array<Object>} data - 数据数组
   * @param {string} col1 - 第一列
   * @param {string} col2 - 第二列
   * @param {string} title - 图表标题
   * @returns {Object} Plotly 图表配置
   */
  createGenericChart(data, col1, col2, title) {
    // 检查列类型
    const sample1 = data.find((row) => row[col1] !== null && row[col1] !== undefined);
    const sample2 = data.find((row) => row[col2] !== null && row[col2] !== undefined);

    const isNumeric1 = typeof sample1?.[col1] === 'number';
    const isNumeric2 = typeof sample2?.[col2] === 'number';

    if (isNumeric1 && isNumeric2) {
      // 两个都是数值，使用散点图
      return this.createScatterPlot(data, col1, col2, title);
    } else {
      // 否则使用柱状图
      return this.createBarChart(data, col1, col2, title);
    }
  }

  /**
   * 创建表格
   * @param {Array<Object>} data - 数据数组
   * @param {string} title - 图表标题
   * @returns {Object} Plotly 图表配置
   */
  createTable(data, title) {
    if (data.length === 0) {
      throw new Error('Cannot create table from empty data');
    }

    const columns = Object.keys(data[0]);
    const headerValues = columns;
    const cellValues = columns.map((col) => data.map((row) => row[col] || ''));

    const trace = {
      type: 'table',
      header: {
        values: headerValues,
        fill: { color: this.THEME_COLORS.navy },
        font: { color: 'white', size: 12 },
        align: 'left',
      },
      cells: {
        values: cellValues,
        fill: {
          color: [
            data.map((_, i) => (i % 2 === 0 ? this.THEME_COLORS.cream : 'white')),
          ],
        },
        font: { color: this.THEME_COLORS.navy, size: 11 },
        align: 'left',
      },
    };

    const layout = this.applyStandardLayout({
      title: title,
    });

    return {
      data: [trace],
      layout: layout,
    };
  }
}

module.exports = PlotlyChartGenerator;

