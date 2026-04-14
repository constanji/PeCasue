/**
 * Data Analyzer - 数据分析工具
 * 
 * 提供数据类型识别、数据清洗、统计计算等功能
 */

const _ = require('lodash');

class DataAnalyzer {
  /**
   * 识别列的数据类型
   * @param {Array<Object>} data - 数据数组
   * @returns {Object} 包含 numeric, categorical, datetime 数组的对象
   */
  identifyColumnTypes(data) {
    if (!data || data.length === 0) {
      return { numeric: [], categorical: [], datetime: [] };
    }

    const sampleRow = data[0];
    const columns = Object.keys(sampleRow);
    const columnTypes = {
      numeric: [],
      categorical: [],
      datetime: [],
    };

    for (const col of columns) {
      const type = this._identifyColumnType(data, col);
      if (type === 'numeric') {
        columnTypes.numeric.push(col);
      } else if (type === 'datetime') {
        columnTypes.datetime.push(col);
      } else {
        columnTypes.categorical.push(col);
      }
    }

    return columnTypes;
  }

  /**
   * 识别单个列的数据类型
   * @private
   */
  _identifyColumnType(data, column) {
    // 采样检查（最多检查前100行）
    const sampleSize = Math.min(100, data.length);
    let numericCount = 0;
    let datetimeCount = 0;
    let totalCount = 0;

    for (let i = 0; i < sampleSize; i++) {
      const value = data[i][column];
      if (value === null || value === undefined) {
        continue;
      }
      totalCount++;

      // 检查是否为数值类型
      if (typeof value === 'number' && !isNaN(value)) {
        numericCount++;
      }
      // 检查是否为字符串格式的纯数值（如 "123", "1,234.56"）
      // 注意：含非数字字符的字符串（如 "9.00%", "20岁以下"）应视为分类列
      else if (typeof value === 'string') {
        const cleanedValue = value.replace(/,/g, '').trim();
        const parsedValue = parseFloat(cleanedValue);
        // 仅当字符串为纯数字格式时才视为数值（排除 "9.00%"、"20-29岁" 等）
        const isPureNumber = !isNaN(parsedValue) && /^-?\d*\.?\d+$/.test(cleanedValue);
        if (isPureNumber) {
          numericCount++;
        }
      }
      // 检查是否为日期时间类型
      else if (this._isDateTime(value)) {
        datetimeCount++;
      }
    }

    if (totalCount === 0) {
      return 'categorical'; // 默认返回分类类型
    }

    // 如果超过80%是数值，认为是数值列
    if (numericCount / totalCount > 0.8) {
      return 'numeric';
    }
    // 如果超过50%是日期时间，认为是日期时间列
    if (datetimeCount / totalCount > 0.5) {
      return 'datetime';
    }
    // 否则认为是分类列
    return 'categorical';
  }

  /**
   * 检查值是否为日期时间类型
   * @private
   */
  _isDateTime(value) {
    if (value instanceof Date) {
      return true;
    }
    if (typeof value === 'string') {
      // 检查常见的日期时间格式
      const datePatterns = [
        /^\d{4}-\d{2}-\d{2}$/, // YYYY-MM-DD
        /^\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2}$/, // YYYY-MM-DD HH:MM:SS
        /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}/, // ISO format
      ];
      return datePatterns.some((pattern) => pattern.test(value));
    }
    return false;
  }

  /**
   * 清洗数据（处理 null、undefined、异常值）
   * @param {Array<Object>} data - 原始数据
   * @returns {Array<Object>} 清洗后的数据
   */
  cleanData(data) {
    if (!data || data.length === 0) {
      return [];
    }

    return data.map((row) => {
      const cleanedRow = {};
      for (const [key, value] of Object.entries(row)) {
        // 保留 null 和 undefined，但转换为 null
        cleanedRow[key] = value === undefined ? null : value;
      }
      return cleanedRow;
    });
  }

  /**
   * 获取列的统计信息
   * @param {Array<Object>} data - 数据数组
   * @param {string} column - 列名
   * @returns {Object} 统计信息
   */
  getStatistics(data, column) {
    const values = data
      .map((row) => row[column])
      .filter((v) => v !== null && v !== undefined && typeof v === 'number');

    if (values.length === 0) {
      return {
        count: 0,
        min: null,
        max: null,
        sum: null,
        mean: null,
      };
    }

    const sum = values.reduce((a, b) => a + b, 0);
    const mean = sum / values.length;
    const sorted = [...values].sort((a, b) => a - b);

    return {
      count: values.length,
      min: sorted[0],
      max: sorted[sorted.length - 1],
      sum: sum,
      mean: mean,
    };
  }

  /**
   * 按列分组并聚合
   * @param {Array<Object>} data - 数据数组
   * @param {string} groupCol - 分组列
   * @param {string} sumCol - 求和列
   * @returns {Array<Object>} 聚合后的数据
   */
  groupBySum(data, groupCol, sumCol) {
    const grouped = _.groupBy(data, groupCol);
    return Object.entries(grouped).map(([key, values]) => {
      const sum = values.reduce((acc, row) => {
        const value = row[sumCol];
        return acc + (typeof value === 'number' ? value : 0);
      }, 0);
      return {
        [groupCol]: key,
        [sumCol]: sum,
      };
    });
  }

  /**
   * 计算值计数（value counts）
   * @param {Array<Object>} data - 数据数组
   * @param {string} column - 列名
   * @returns {Array<Object>} 计数结果，格式为 [{value: 'A', count: 10}, ...]
   */
  valueCounts(data, column) {
    const counts = {};
    for (const row of data) {
      const value = row[column];
      if (value !== null && value !== undefined) {
        const key = String(value);
        counts[key] = (counts[key] || 0) + 1;
      }
    }
    return Object.entries(counts).map(([value, count]) => ({
      value: value,
      count: count,
    }));
  }

  /**
   * 限制数据行数（采样）
   * @param {Array<Object>} data - 数据数组
   * @param {number} maxRows - 最大行数
   * @returns {Array<Object>} 采样后的数据
   */
  limitRows(data, maxRows = 1000) {
    if (!data || data.length <= maxRows) {
      return data;
    }
    // 均匀采样
    const step = Math.floor(data.length / maxRows);
    const sampled = [];
    for (let i = 0; i < data.length; i += step) {
      sampled.push(data[i]);
      if (sampled.length >= maxRows) {
        break;
      }
    }
    return sampled;
  }
}

module.exports = DataAnalyzer;

