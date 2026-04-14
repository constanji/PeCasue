/**
 * Chart Type Selector - 图表类型选择器
 * 
 * 根据数据特征自动选择最合适的图表类型
 */

class ChartTypeSelector {
  /**
   * 根据数据特征选择图表类型
   * @param {Array<Object>} data - 数据数组
   * @param {Object} columnTypes - 列类型信息 {numeric: [], categorical: [], datetime: []}
   * @returns {string} 图表类型
   */
  selectChartType(data, columnTypes) {
    if (!data || data.length === 0) {
      throw new Error('Cannot visualize empty data');
    }

    const { numeric, categorical, datetime } = columnTypes;
    const firstRow = data[0];
    const columnCount = Object.keys(firstRow).length;

    // 规则1: 4+ 列 → 表格
    if (columnCount >= 4) {
      return 'table';
    }

    // 规则2: 包含时间列 + 数值列 → 时间序列图
    if (datetime.length > 0 && numeric.length > 0) {
      return 'time_series';
    }

    // 规则3: 1个数值列 → 直方图
    if (numeric.length === 1 && categorical.length === 0 && datetime.length === 0) {
      return 'histogram';
    }

    // 规则4: 1个分类列 + 1个数值列 → 柱状图
    if (numeric.length === 1 && categorical.length === 1) {
      return 'bar';
    }

    // 规则5: 2个数值列 → 散点图
    if (numeric.length === 2 && categorical.length === 0) {
      return 'scatter';
    }

    // 规则6: 3+ 个数值列 → 相关性热力图
    if (numeric.length >= 3) {
      return 'heatmap';
    }

    // 规则7: 多个分类列 → 分组柱状图
    if (categorical.length >= 2) {
      return 'grouped_bar';
    }

    // 规则8: 1个分类列 + 其他 → 柱状图（计数）
    if (categorical.length === 1 && numeric.length === 0) {
      return 'bar';
    }

    // 默认：通用图表（使用前两列）
    if (columnCount >= 2) {
      return 'generic';
    }

    throw new Error('Cannot determine appropriate visualization for this data');
  }

  /**
   * 验证图表类型是否有效
   * @param {string} chartType - 图表类型
   * @returns {boolean} 是否有效
   */
  isValidChartType(chartType) {
    const validTypes = [
      'table',
      'histogram',
      'bar',
      'scatter',
      'heatmap',
      'time_series',
      'grouped_bar',
      'generic',
    ];
    return validTypes.includes(chartType);
  }
}

module.exports = ChartTypeSelector;

