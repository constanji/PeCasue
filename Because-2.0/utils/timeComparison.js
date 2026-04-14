/**
 * TimeComparison - 时间对比模块
 * 
 * 支持基期/现期对比分析，自定义时间周期：
 * - 同比 (Year-over-Year)
 * - 环比 (Month-over-Month / Period-over-Period)
 * - 自定义时间区间对比
 * - 自动时间字段检测
 */

const StatisticsEngine = require('./statisticsEngine');

class TimeComparison {
  /**
   * 常见时间格式的正则表达式
   */
  static TIME_PATTERNS = [
    { name: 'date', pattern: /^\d{4}-\d{2}-\d{2}$/ },
    { name: 'datetime', pattern: /^\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}/ },
    { name: 'yearMonth', pattern: /^\d{4}-\d{2}$/ },
    { name: 'year', pattern: /^\d{4}$/ },
    { name: 'timestamp', pattern: /^\d{10,13}$/ },
  ];

  /**
   * 自动检测时间字段
   * @param {Object[]} data - 数据行数组
   * @returns {string|null} 时间字段名
   */
  static detectTimeField(data) {
    if (!data || data.length === 0) {
      return null;
    }

    const sample = data[0];
    const timeKeywords = ['date', 'time', 'day', 'month', 'year', 'period', 'week',
      '日期', '时间', '月份', '年份', '周期', '季度'];

    for (const key of Object.keys(sample)) {
      const lowerKey = key.toLowerCase();
      if (timeKeywords.some((kw) => lowerKey.includes(kw))) {
        return key;
      }
    }

    for (const key of Object.keys(sample)) {
      const value = String(sample[key]);
      if (this.TIME_PATTERNS.some((tp) => tp.pattern.test(value))) {
        return key;
      }
    }

    return null;
  }

  /**
   * 解析时间粒度
   * @param {string} timeValue - 时间值示例
   * @returns {'year'|'month'|'day'|'datetime'|'unknown'}
   */
  static detectGranularity(timeValue) {
    const str = String(timeValue);
    for (const { name, pattern } of this.TIME_PATTERNS) {
      if (pattern.test(str)) {
        return name === 'yearMonth' ? 'month' : name;
      }
    }
    return 'unknown';
  }

  /**
   * 时间周期类型定义
   */
  static PERIOD_TYPES = {
    YOY: 'year_over_year',       // 同比
    MOM: 'month_over_month',     // 环比（月）
    WOW: 'week_over_week',       // 环比（周）
    DOD: 'day_over_day',         // 环比（日）
    CUSTOM: 'custom',            // 自定义
  };

  /**
   * 根据周期类型计算基期时间范围
   * @param {Date} currentStart - 现期起始
   * @param {Date} currentEnd - 现期终止
   * @param {string} periodType - 周期类型
   * @returns {{ baseStart: Date, baseEnd: Date }}
   */
  static calculateBasePeriod(currentStart, currentEnd, periodType) {
    const start = new Date(currentStart);
    const end = new Date(currentEnd);
    const duration = end.getTime() - start.getTime();

    let baseStart, baseEnd;

    switch (periodType) {
    case this.PERIOD_TYPES.YOY:
      baseStart = new Date(start);
      baseStart.setFullYear(baseStart.getFullYear() - 1);
      baseEnd = new Date(end);
      baseEnd.setFullYear(baseEnd.getFullYear() - 1);
      break;

    case this.PERIOD_TYPES.MOM:
      baseStart = new Date(start);
      baseStart.setMonth(baseStart.getMonth() - 1);
      baseEnd = new Date(end);
      baseEnd.setMonth(baseEnd.getMonth() - 1);
      break;

    case this.PERIOD_TYPES.WOW:
      baseStart = new Date(start.getTime() - 7 * 24 * 60 * 60 * 1000);
      baseEnd = new Date(end.getTime() - 7 * 24 * 60 * 60 * 1000);
      break;

    case this.PERIOD_TYPES.DOD:
      baseStart = new Date(start.getTime() - 24 * 60 * 60 * 1000);
      baseEnd = new Date(end.getTime() - 24 * 60 * 60 * 1000);
      break;

    default:
      baseStart = new Date(start.getTime() - duration);
      baseEnd = new Date(start);
      break;
    }

    return { baseStart, baseEnd };
  }

  /**
   * 按时间字段分割数据为基期和现期
   * @param {Object[]} data - 完整数据
   * @param {string} timeField - 时间字段名
   * @param {Date|string} basePeriodStart - 基期开始
   * @param {Date|string} basePeriodEnd - 基期结束
   * @param {Date|string} currentPeriodStart - 现期开始
   * @param {Date|string} currentPeriodEnd - 现期结束
   * @returns {{ baseData: Object[], currentData: Object[] }}
   */
  static splitByPeriod(data, timeField, basePeriodStart, basePeriodEnd, currentPeriodStart, currentPeriodEnd) {
    const toDate = (v) => v instanceof Date ? v : new Date(v);
    const bStart = toDate(basePeriodStart).getTime();
    const bEnd = toDate(basePeriodEnd).getTime();
    const cStart = toDate(currentPeriodStart).getTime();
    const cEnd = toDate(currentPeriodEnd).getTime();

    const baseData = [];
    const currentData = [];

    for (const row of data) {
      const t = new Date(row[timeField]).getTime();
      if (t >= bStart && t <= bEnd) {
        baseData.push(row);
      } else if (t >= cStart && t <= cEnd) {
        currentData.push(row);
      }
    }

    return { baseData, currentData };
  }

  /**
   * 执行时间对比分析
   * @param {Object} params
   * @param {Object[]} params.baseData - 基期数据
   * @param {Object[]} params.currentData - 现期数据
   * @param {string[]} params.metricFields - 指标字段列表
   * @param {string[]} params.dimensionFields - 维度字段列表（可选）
   * @returns {Object} 时间对比结果
   */
  static analyze({ baseData, currentData, metricFields, dimensionFields = [] }) {
    const result = {
      overview: {},
      metricComparisons: [],
      dimensionBreakdowns: [],
    };

    for (const metric of metricFields) {
      const baseValues = baseData.map((r) => Number(r[metric]) || 0);
      const currentValues = currentData.map((r) => Number(r[metric]) || 0);

      const baseStats = StatisticsEngine.basicStats(baseValues);
      const currentStats = StatisticsEngine.basicStats(currentValues);

      const absoluteChange = currentStats.sum - baseStats.sum;
      const changeRate = baseStats.sum !== 0 ? absoluteChange / baseStats.sum : 0;
      const meanAbsoluteChange = currentStats.mean - baseStats.mean;

      const comparison = {
        metric,
        basePeriod: {
          sum: baseStats.sum,
          mean: baseStats.mean,
          count: baseStats.count,
          min: baseStats.min,
          max: baseStats.max,
          std: baseStats.std,
        },
        currentPeriod: {
          sum: currentStats.sum,
          mean: currentStats.mean,
          count: currentStats.count,
          min: currentStats.min,
          max: currentStats.max,
          std: currentStats.std,
        },
        change: {
          absolute: absoluteChange,
          rate: changeRate,
          ratePercent: (changeRate * 100).toFixed(2) + '%',
          meanAbsolute: meanAbsoluteChange,
          direction: absoluteChange > 0 ? 'increase' : absoluteChange < 0 ? 'decrease' : 'unchanged',
        },
        significance: this._assessSignificance(baseValues, currentValues),
      };

      result.metricComparisons.push(comparison);
    }

    for (const dim of dimensionFields) {
      const breakdown = this._dimensionTimeDiff(baseData, currentData, dim, metricFields[0]);
      result.dimensionBreakdowns.push({
        dimension: dim,
        metric: metricFields[0],
        breakdown,
      });
    }

    if (result.metricComparisons.length > 0) {
      const primary = result.metricComparisons[0];
      result.overview = {
        primaryMetric: primary.metric,
        direction: primary.change.direction,
        changeRate: primary.change.ratePercent,
        absoluteChange: primary.change.absolute,
      };
    }

    return result;
  }

  /**
   * 简单显著性评估（无需外部库的简化t检验替代）
   */
  static _assessSignificance(baseValues, currentValues) {
    if (baseValues.length < 2 || currentValues.length < 2) {
      return { significant: false, confidence: 'low', reason: '样本量不足' };
    }

    const baseStats = StatisticsEngine.basicStats(baseValues);
    const currentStats = StatisticsEngine.basicStats(currentValues);

    if (baseStats.std === 0 && currentStats.std === 0) {
      const same = baseStats.mean === currentStats.mean;
      return {
        significant: !same,
        confidence: same ? 'high' : 'high',
        reason: same ? '两期数据完全一致' : '两期数据存在明确差异（标准差为0）',
      };
    }

    // Welch's t-test approximation
    const se = Math.sqrt(
      (baseStats.std * baseStats.std) / baseValues.length +
      (currentStats.std * currentStats.std) / currentValues.length,
    );
    const tStat = se > 0 ? Math.abs(currentStats.mean - baseStats.mean) / se : 0;

    // 简化判断：|t| > 2 大致对应 p < 0.05
    const significant = tStat > 2;
    const confidence = tStat > 3 ? 'high' : tStat > 2 ? 'medium' : 'low';

    return {
      significant,
      confidence,
      tStatistic: Number(tStat.toFixed(4)),
      reason: significant
        ? `t=${tStat.toFixed(2)}，两期差异在统计上显著`
        : `t=${tStat.toFixed(2)}，两期差异在统计上不显著`,
    };
  }

  /**
   * 维度时间差异分析
   */
  static _dimensionTimeDiff(baseData, currentData, dimensionKey, metricKey) {
    const contributions = StatisticsEngine.contributionDecomposition(
      baseData, currentData, dimensionKey, metricKey,
    );

    const baseValues = contributions.map((c) => c.baseValue);
    const currentValues = contributions.map((c) => c.currentValue);
    const baseDist = StatisticsEngine.normalize(baseValues);
    const currentDist = StatisticsEngine.normalize(currentValues);

    let jsDivergence = 0;
    if (baseDist.length === currentDist.length && baseDist.length > 0) {
      jsDivergence = StatisticsEngine.jsDivergence(baseDist, currentDist);
    }

    return {
      contributions: contributions.slice(0, 20),
      jsDivergence: Number(jsDivergence.toFixed(6)),
      distributionShift: jsDivergence > 0.1 ? 'significant' : jsDivergence > 0.01 ? 'moderate' : 'minimal',
    };
  }

  /**
   * 生成时间对比SQL
   * @param {Object} params
   * @param {string} params.tableName - 表名
   * @param {string} params.timeField - 时间字段
   * @param {string[]} params.metricFields - 指标字段
   * @param {string[]} params.dimensionFields - 维度字段
   * @param {string} params.basePeriodStart - 基期开始
   * @param {string} params.basePeriodEnd - 基期结束
   * @param {string} params.currentPeriodStart - 现期开始
   * @param {string} params.currentPeriodEnd - 现期结束
   * @returns {{ baseSql: string, currentSql: string }}
   */
  static generateComparisonSQL({
    tableName, timeField, metricFields, dimensionFields = [],
    basePeriodStart, basePeriodEnd, currentPeriodStart, currentPeriodEnd,
  }) {
    const selectParts = [];
    dimensionFields.forEach((d) => selectParts.push(d));
    metricFields.forEach((m) => selectParts.push(`SUM(${m}) AS ${m}`));

    const selectClause = selectParts.join(', ');
    const groupClause = dimensionFields.length > 0
      ? `GROUP BY ${dimensionFields.join(', ')}`
      : '';

    const baseSql = `SELECT ${selectClause} FROM ${tableName} WHERE ${timeField} >= '${basePeriodStart}' AND ${timeField} <= '${basePeriodEnd}' ${groupClause}`;
    const currentSql = `SELECT ${selectClause} FROM ${tableName} WHERE ${timeField} >= '${currentPeriodStart}' AND ${timeField} <= '${currentPeriodEnd}' ${groupClause}`;

    return { baseSql, currentSql };
  }
}

module.exports = TimeComparison;
