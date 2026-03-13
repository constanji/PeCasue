const { Tool } = require('@langchain/core/tools');
const { z } = require('zod');
const { logger } = require('@because/data-schemas');
const StatisticsEngine = require('../../utils/statisticsEngine');
const TimeComparison = require('../../utils/timeComparison');
const DimensionDrillDown = require('../../utils/dimensionDrillDown');
const MetricAttribution = require('../../utils/metricAttribution');

/**
 * ResultAnalysisTool - 结果分析工具（重构版）
 * 
 * 从简单的SQL结构归因升级为全面的数据分析引擎：
 * - 基础统计分析（总和、均值、标准差、中位数、变异系数）
 * - 维度归因（Adtributor算法：解释力、惊喜度、简洁性）
 * - 时间趋势检测与周期对比
 * - 指标关联性分析
 * - 异常值检测
 * - 智能后续建议生成
 */
class ResultAnalysisTool extends Tool {
  name = 'result_analysis';

  description =
    'SQL查询结果分析工具（增强版），提供深度数据洞察。' +
    '功能包括：统计分析、维度归因（Adtributor算法）、时间趋势检测、指标关联分析、异常值检测。' +
    '输出解释力、惊喜度、简洁性等量化归因指标。';

  schema = z.object({
    sql: z
      .string()
      .min(1)
      .describe('执行的SQL查询语句'),
    results: z
      .array(z.any())
      .describe('SQL查询结果数组'),
    row_count: z
      .number()
      .int()
      .nonnegative()
      .optional()
      .describe('结果行数'),
    attribution: z
      .object({
        tables: z.array(z.string()).optional(),
        columns: z.array(z.string()).optional(),
        has_where: z.boolean().optional(),
        has_group_by: z.boolean().optional(),
        has_order_by: z.boolean().optional(),
        has_limit: z.boolean().optional(),
      })
      .optional()
      .describe('归因信息（来自sql_executor）'),
    comparison_results: z
      .array(z.any())
      .optional()
      .describe('对比数据集（用于波动归因分析，作为基期数据）'),
    analysis_depth: z
      .enum(['basic', 'standard', 'deep'])
      .optional()
      .default('standard')
      .describe('分析深度：basic=基础统计, standard=标准分析, deep=深度归因'),
  });

  /**
   * 从SQL中提取结构信息
   */
  extractSQLStructure(sql) {
    const upper = sql.toUpperCase();
    const structure = {
      tables: [],
      columns: [],
      hasWhere: false,
      hasGroupBy: false,
      hasOrderBy: false,
      hasLimit: false,
    };

    try {
      const fromMatch = upper.match(/\bFROM\b([\s\S]+?)(\bWHERE\b|\bGROUP BY\b|\bORDER BY\b|\bLIMIT\b|$)/);
      if (fromMatch && fromMatch[1]) {
        structure.tables = fromMatch[1]
          .split(',')
          .map((t) => t.trim())
          .filter(Boolean)
          .map((t) => t.replace(/\s+AS\s+.+$/i, '').split(/\s+/)[0]);
      }

      const selectMatch = upper.match(/\bSELECT\b([\s\S]+?)\bFROM\b/i);
      if (selectMatch && selectMatch[1]) {
        structure.columns = selectMatch[1]
          .split(',')
          .map((c) => {
            const match = c.trim().match(/(\w+)(?:\s+AS\s+\w+)?$/i);
            return match ? match[1] : c.trim();
          })
          .filter(Boolean);
      }

      structure.hasWhere = /\bWHERE\b/i.test(sql);
      structure.hasGroupBy = /\bGROUP BY\b/i.test(sql);
      structure.hasOrderBy = /\bORDER BY\b/i.test(sql);
      structure.hasLimit = /\bLIMIT\b/i.test(sql);
    } catch (error) {
      logger.warn('[ResultAnalysisTool] 提取SQL结构失败:', error.message);
    }

    return structure;
  }

  /**
   * 自动识别字段类型
   */
  classifyFields(results) {
    if (!results || results.length === 0) {
      return { numericFields: [], categoricalFields: [], timeFields: [], booleanFields: [] };
    }

    const sample = results.slice(0, Math.min(20, results.length));
    const fields = Object.keys(sample[0]);
    const numericFields = [];
    const categoricalFields = [];
    const timeFields = [];
    const booleanFields = [];

    for (const field of fields) {
      const values = sample.map((r) => r[field]).filter((v) => v !== null && v !== undefined);
      if (values.length === 0) continue;

      const numericCount = values.filter((v) => typeof v === 'number' || (!isNaN(Number(v)) && String(v).trim() !== '')).length;
      const boolCount = values.filter((v) => typeof v === 'boolean' || v === 0 || v === 1).length;
      const timeCount = values.filter((v) => {
        if (typeof v !== 'string') return false;
        return /\d{4}[-/]\d{2}/.test(v) || !isNaN(Date.parse(v));
      }).length;

      if (timeCount > values.length * 0.5) {
        timeFields.push(field);
      } else if (boolCount === values.length && new Set(values.map(String)).size <= 2) {
        booleanFields.push(field);
      } else if (numericCount > values.length * 0.7) {
        numericFields.push(field);
      } else {
        categoricalFields.push(field);
      }
    }

    return { numericFields, categoricalFields, timeFields, booleanFields };
  }

  /**
   * 深度统计分析
   */
  performStatisticalAnalysis(results, numericFields) {
    const stats = {};

    for (const field of numericFields) {
      const values = results.map((r) => Number(r[field])).filter((v) => !isNaN(v));
      if (values.length === 0) continue;

      const basic = StatisticsEngine.basicStats(values);
      const cv = StatisticsEngine.coefficientOfVariation(values);

      stats[field] = {
        ...basic,
        coefficientOfVariation: Number(cv.toFixed(4)),
        distribution: cv < 0.1 ? 'very_uniform' : cv < 0.3 ? 'uniform' : cv < 0.7 ? 'moderate' : 'dispersed',
      };
    }

    return stats;
  }

  /**
   * 异常值检测（IQR方法）
   */
  detectAnomalies(results, numericFields) {
    const anomalies = [];

    for (const field of numericFields) {
      const values = results.map((r, i) => ({ value: Number(r[field]), index: i }))
        .filter((v) => !isNaN(v.value))
        .sort((a, b) => a.value - b.value);

      if (values.length < 4) continue;

      const q1 = values[Math.floor(values.length * 0.25)].value;
      const q3 = values[Math.floor(values.length * 0.75)].value;
      const iqr = q3 - q1;
      const lowerBound = q1 - 1.5 * iqr;
      const upperBound = q3 + 1.5 * iqr;

      const outliers = values.filter((v) => v.value < lowerBound || v.value > upperBound);
      if (outliers.length > 0) {
        anomalies.push({
          field,
          outlierCount: outliers.length,
          outlierRate: Number((outliers.length / values.length).toFixed(4)),
          bounds: { lower: Number(lowerBound.toFixed(4)), upper: Number(upperBound.toFixed(4)) },
          extremeValues: outliers.slice(0, 5).map((o) => ({
            rowIndex: o.index,
            value: o.value,
            direction: o.value < lowerBound ? 'below' : 'above',
          })),
        });
      }
    }

    return anomalies;
  }

  /**
   * 维度归因分析（当有对比数据时）
   */
  performDimensionAttribution(currentData, baseData, numericFields, categoricalFields) {
    if (!baseData || baseData.length === 0 || categoricalFields.length === 0 || numericFields.length === 0) {
      return null;
    }

    return DimensionDrillDown.analyze({
      baseData,
      currentData,
      metricField: numericFields[0],
      dimensionFields: categoricalFields.slice(0, 3),
    });
  }

  /**
   * 时间趋势检测
   */
  detectTimeTrend(results, timeField, numericFields) {
    if (!timeField || numericFields.length === 0) {
      return null;
    }

    const sorted = [...results].sort((a, b) => new Date(a[timeField]) - new Date(b[timeField]));
    const trends = {};

    for (const metric of numericFields.slice(0, 3)) {
      const values = sorted.map((r) => Number(r[metric])).filter((v) => !isNaN(v));
      if (values.length < 3) continue;

      const indices = values.map((_, i) => i);
      const regression = StatisticsEngine.linearRegression(indices, values);

      let direction;
      if (regression.rSquared < 0.1) {
        direction = 'no_trend';
      } else if (regression.slope > 0) {
        direction = 'increasing';
      } else {
        direction = 'decreasing';
      }

      trends[metric] = {
        direction,
        slope: Number(regression.slope.toFixed(6)),
        rSquared: Number(regression.rSquared.toFixed(4)),
        strength: regression.rSquared > 0.7 ? 'strong' : regression.rSquared > 0.3 ? 'moderate' : 'weak',
      };
    }

    return trends;
  }

  /**
   * 指标间关联分析
   */
  performCorrelationAnalysis(results, numericFields) {
    if (numericFields.length < 2) {
      return null;
    }

    return MetricAttribution.metricCorrelation(results, numericFields.slice(0, 5));
  }

  /**
   * 生成SQL结构归因（兼容旧版）
   */
  buildStructuralAttribution(sql, sqlStructure, attribution, results) {
    const tables = attribution?.tables || sqlStructure.tables || [];
    const columns = results.length > 0 ? Object.keys(results[0]) : attribution?.columns || sqlStructure.columns || [];

    const tablePart = tables.length > 0
      ? `主要数据来源于以下表：${tables.join('，')}。`
      : '未能从SQL中可靠解析出表名。';

    const columnPart = columns.length > 0
      ? `结果中包含字段：${columns.join('，')}。`
      : '结果中未检测到字段列表。';

    const clauseHints = [];
    if (attribution?.has_where || sqlStructure.hasWhere) clauseHints.push('WHERE过滤条件');
    if (attribution?.has_group_by || sqlStructure.hasGroupBy) clauseHints.push('GROUP BY分组逻辑');
    if (attribution?.has_order_by || sqlStructure.hasOrderBy) clauseHints.push('ORDER BY排序规则');
    if (attribution?.has_limit || sqlStructure.hasLimit) clauseHints.push('LIMIT行数限制');

    const clausePart = clauseHints.length > 0
      ? `查询中包含 ${clauseHints.join('、')}，这些条件影响结果。`
      : '查询中未检测到主要子句。';

    return {
      tables,
      columns,
      filters: clauseHints.includes('WHERE过滤条件') ? '包含WHERE过滤' : '无WHERE条件',
      grouping: clauseHints.includes('GROUP BY分组逻辑') ? '包含GROUP BY' : '无分组',
      data_source: `${tablePart} ${columnPart} ${clausePart}`,
    };
  }

  /**
   * 生成智能后续建议
   */
  generateSmartSuggestions(analysis) {
    const { results, fieldTypes, stats, anomalies, trends, correlation, dimensionAttribution } = analysis;
    const suggestions = [];

    if (results.length === 0) {
      suggestions.push({
        question: '查询结果为空，是否需要调整查询条件？',
        reason: '当前查询未返回任何结果',
        sql_hint: '检查WHERE条件是否过于严格',
        priority: 'high',
      });
      return suggestions;
    }

    // 基于异常检测的建议
    if (anomalies && anomalies.length > 0) {
      const topAnomaly = anomalies[0];
      suggestions.push({
        question: `${topAnomaly.field}存在${topAnomaly.outlierCount}个异常值，是否需要深入调查？`,
        reason: `异常值比例${(topAnomaly.outlierRate * 100).toFixed(1)}%，超出正常范围[${topAnomaly.bounds.lower}, ${topAnomaly.bounds.upper}]`,
        sql_hint: `添加 WHERE ${topAnomaly.field} > ${topAnomaly.bounds.upper} 查看高异常值明细`,
        priority: 'high',
      });
    }

    // 基于趋势的建议
    if (trends) {
      for (const [metric, trend] of Object.entries(trends)) {
        if (trend.strength === 'strong' && trend.direction !== 'no_trend') {
          suggestions.push({
            question: `${metric}呈现${trend.direction === 'increasing' ? '上升' : '下降'}趋势（R²=${trend.rSquared}），需要进一步归因分析吗？`,
            reason: `线性趋势显著，斜率=${trend.slope}`,
            sql_hint: '可以按维度分组查看趋势差异',
            priority: 'medium',
          });
        }
      }
    }

    // 基于相关性的建议
    if (correlation?.keyFindings?.length > 0) {
      const finding = correlation.keyFindings[0];
      suggestions.push({
        question: `${finding.metric1}和${finding.metric2}之间存在${finding.strength === 'very_strong' ? '非常强' : '强'}的相关性(r=${finding.correlation})，是否需要因果分析？`,
        reason: `${finding.direction === 'positive' ? '正' : '负'}相关可能暗示因果关系`,
        sql_hint: '可以用scatter plot可视化两个指标的关系',
        priority: 'medium',
      });
    }

    // 基于维度归因的建议
    if (dimensionAttribution?.dimensionRanking?.length > 0) {
      const topDim = dimensionAttribution.dimensionRanking[0];
      suggestions.push({
        question: `"${topDim.dimension}"维度的Adtributor评分最高(${topDim.adtributorScore})，是否沿此维度下钻？`,
        reason: `解释力=${topDim.explanatoryPower}，惊喜度=${topDim.surprise}`,
        sql_hint: `添加 GROUP BY ${topDim.dimension} 查看维度分布`,
        priority: 'high',
      });
    }

    // 基于数据量的建议
    if (fieldTypes.categoricalFields.length > 0 && !analysis.hasGroupBy) {
      suggestions.push({
        question: `是否按"${fieldTypes.categoricalFields[0]}"维度分组分析？`,
        reason: '检测到分类字段，可以进行分组对比',
        sql_hint: `添加 GROUP BY ${fieldTypes.categoricalFields[0]}`,
        priority: 'low',
      });
    }

    return suggestions.sort((a, b) => {
      const priorityOrder = { high: 0, medium: 1, low: 2 };
      return (priorityOrder[a.priority] || 2) - (priorityOrder[b.priority] || 2);
    });
  }

  async _call(input) {
    const { sql, results = [], row_count, attribution, comparison_results, analysis_depth = 'standard' } = input;

    try {
      logger.info('[ResultAnalysisTool] 开始结果分析:', {
        sql: sql.substring(0, 50),
        rowCount: results.length || row_count || 0,
        depth: analysis_depth,
        hasComparison: !!comparison_results,
      });

      const sqlStructure = this.extractSQLStructure(sql);
      const fieldTypes = this.classifyFields(results);
      const rowCount = results.length || row_count || 0;
      const columns = results.length > 0 ? Object.keys(results[0]) : [];

      // 基础结构归因（保持向后兼容）
      const structuralAttribution = this.buildStructuralAttribution(sql, sqlStructure, attribution, results);

      const analysisResult = {
        results,
        fieldTypes,
        hasGroupBy: sqlStructure.hasGroupBy,
      };

      const result = {
        summary: `查询返回了 ${rowCount} 行数据，包含 ${columns.length} 个字段。${structuralAttribution.data_source}`,
        attribution: structuralAttribution,
        metadata: {
          row_count: rowCount,
          column_count: columns.length,
          field_types: fieldTypes,
        },
      };

      // 统计分析
      if (analysis_depth !== 'basic' && fieldTypes.numericFields.length > 0) {
        const stats = this.performStatisticalAnalysis(results, fieldTypes.numericFields);
        result.statistics = stats;
        analysisResult.stats = stats;
      }

      // 异常检测
      if (analysis_depth !== 'basic' && results.length >= 4) {
        const anomalies = this.detectAnomalies(results, fieldTypes.numericFields);
        if (anomalies.length > 0) {
          result.anomalies = anomalies;
        }
        analysisResult.anomalies = anomalies;
      }

      // 时间趋势
      if (fieldTypes.timeFields.length > 0 && fieldTypes.numericFields.length > 0) {
        const trends = this.detectTimeTrend(results, fieldTypes.timeFields[0], fieldTypes.numericFields);
        if (trends) {
          result.time_trends = trends;
        }
        analysisResult.trends = trends;
      }

      // 指标关联
      if (analysis_depth === 'deep' && fieldTypes.numericFields.length >= 2) {
        const correlation = this.performCorrelationAnalysis(results, fieldTypes.numericFields);
        if (correlation) {
          result.correlation = correlation;
        }
        analysisResult.correlation = correlation;
      }

      // 维度归因（当有对比数据时）
      if (comparison_results && comparison_results.length > 0 &&
          fieldTypes.categoricalFields.length > 0 && fieldTypes.numericFields.length > 0) {
        const dimAttr = this.performDimensionAttribution(
          results, comparison_results, fieldTypes.numericFields, fieldTypes.categoricalFields,
        );
        if (dimAttr) {
          result.dimension_attribution = dimAttr;
        }
        analysisResult.dimensionAttribution = dimAttr;
      }

      // 关键洞察
      result.key_insights = this._generateKeyInsights(analysisResult);

      // 智能后续建议
      result.follow_up_suggestions = this.generateSmartSuggestions(analysisResult);

      // 分析置信度
      result.metadata.analysis_confidence = this._calculateConfidence(results, analysis_depth);
      result.metadata.analysis_depth = analysis_depth;

      logger.info('[ResultAnalysisTool] 分析完成:', {
        rowCount,
        insightsCount: result.key_insights?.length || 0,
        suggestionsCount: result.follow_up_suggestions?.length || 0,
        hasAnomalies: !!result.anomalies,
        hasTrends: !!result.time_trends,
      });

      return JSON.stringify(result, null, 2);
    } catch (error) {
      logger.error('[ResultAnalysisTool] 分析失败:', error);
      return JSON.stringify({
        summary: '结果分析失败',
        error: error.message,
        metadata: {
          row_count: results.length || row_count || 0,
          column_count: 0,
          analysis_confidence: 0.0,
        },
      }, null, 2);
    }
  }

  /**
   * 从分析结果生成关键洞察
   */
  _generateKeyInsights(analysis) {
    const insights = [];
    const { results, fieldTypes, stats, anomalies, trends, correlation, dimensionAttribution } = analysis;

    if (!results || results.length === 0) return insights;

    // 统计洞察
    if (stats) {
      for (const [field, stat] of Object.entries(stats)) {
        if (stat.coefficientOfVariation > 0.5) {
          insights.push({
            type: 'distribution',
            dimension: field,
            value: `变异系数=${stat.coefficientOfVariation}，数据分散度较高`,
            impact: `范围: ${stat.min} ~ ${stat.max}，均值: ${stat.mean.toFixed(2)}`,
            importance: 'medium',
          });
        }
      }
    }

    // 异常洞察
    if (anomalies && anomalies.length > 0) {
      for (const anomaly of anomalies.slice(0, 3)) {
        insights.push({
          type: 'anomaly',
          dimension: anomaly.field,
          value: `发现${anomaly.outlierCount}个异常值`,
          impact: `异常值比例${(anomaly.outlierRate * 100).toFixed(1)}%`,
          importance: 'high',
        });
      }
    }

    // 趋势洞察
    if (trends) {
      for (const [metric, trend] of Object.entries(trends)) {
        if (trend.direction !== 'no_trend' && trend.strength !== 'weak') {
          insights.push({
            type: 'trend',
            dimension: metric,
            value: `${trend.direction === 'increasing' ? '上升' : '下降'}趋势(R²=${trend.rSquared})`,
            impact: `趋势强度: ${trend.strength}`,
            importance: trend.strength === 'strong' ? 'high' : 'medium',
          });
        }
      }
    }

    // 维度归因洞察
    if (dimensionAttribution?.dimensionRanking?.length > 0) {
      const top = dimensionAttribution.dimensionRanking[0];
      insights.push({
        type: 'attribution',
        dimension: top.dimension,
        value: `Adtributor评分=${top.adtributorScore}（EP=${top.explanatoryPower}, Surprise=${top.surprise}）`,
        impact: `该维度是最主要的归因维度`,
        importance: 'high',
      });
    }

    // 相关性洞察
    if (correlation?.keyFindings?.length > 0) {
      for (const finding of correlation.keyFindings.slice(0, 2)) {
        insights.push({
          type: 'correlation',
          dimension: `${finding.metric1} × ${finding.metric2}`,
          value: `相关系数=${finding.correlation}`,
          impact: `${finding.strength === 'very_strong' ? '非常强' : '强'}的${finding.direction === 'positive' ? '正' : '负'}相关`,
          importance: 'medium',
        });
      }
    }

    // 基本数值洞察（当没有高级洞察时兜底）
    if (insights.length === 0 && fieldTypes.numericFields.length > 0) {
      for (const field of fieldTypes.numericFields.slice(0, 3)) {
        const values = results.map((r) => Number(r[field])).filter((v) => !isNaN(v));
        if (values.length > 0) {
          const sum = values.reduce((a, b) => a + b, 0);
          const avg = sum / values.length;
          insights.push({
            type: 'basic_stat',
            dimension: field,
            value: `总计: ${sum.toFixed(2)}, 平均: ${avg.toFixed(2)}`,
            impact: `范围 ${Math.min(...values)} 到 ${Math.max(...values)}`,
            importance: 'low',
          });
        }
      }
    }

    return insights.sort((a, b) => {
      const order = { high: 0, medium: 1, low: 2 };
      return (order[a.importance] || 2) - (order[b.importance] || 2);
    });
  }

  /**
   * 计算分析置信度
   */
  _calculateConfidence(results, depth) {
    if (!results || results.length === 0) return 0;

    let confidence = 0.5;

    if (results.length >= 100) confidence += 0.2;
    else if (results.length >= 20) confidence += 0.15;
    else if (results.length >= 5) confidence += 0.1;

    if (depth === 'deep') confidence += 0.15;
    else if (depth === 'standard') confidence += 0.1;

    const columns = Object.keys(results[0]).length;
    if (columns >= 3) confidence += 0.05;

    return Number(Math.min(0.95, confidence).toFixed(2));
  }
}

module.exports = ResultAnalysisTool;
