/**
 * StatisticsEngine - 统计算法引擎
 * 
 * 提供波动归因所需的核心统计算法：
 * - KL散度 / JS散度
 * - 解释力（Explanatory Power）
 * - 简洁性（Parsimony）
 * - 惊喜度（Surprise）
 * - 基础统计量计算
 */

class StatisticsEngine {
  /**
   * KL散度: D_KL(P||Q) = Σ P(i) * log(P(i)/Q(i))
   * P和Q必须是概率分布（归一化后的数组）
   */
  static klDivergence(P, Q, epsilon = 1e-10) {
    if (P.length !== Q.length) {
      throw new Error('KL散度要求P和Q长度一致');
    }

    let divergence = 0;
    for (let i = 0; i < P.length; i++) {
      const p = Math.max(P[i], epsilon);
      const q = Math.max(Q[i], epsilon);
      divergence += p * Math.log2(p / q);
    }

    return divergence;
  }

  /**
   * JS散度: JSD(P||Q) = 0.5 * D_KL(P||M) + 0.5 * D_KL(Q||M)
   * 其中 M = 0.5 * (P + Q)
   * 返回值在 [0, 1] 之间（使用log2时）
   */
  static jsDivergence(P, Q, epsilon = 1e-10) {
    if (P.length !== Q.length) {
      throw new Error('JS散度要求P和Q长度一致');
    }

    const M = P.map((p, i) => 0.5 * (p + Q[i]));
    return 0.5 * this.klDivergence(P, M, epsilon) + 0.5 * this.klDivergence(Q, M, epsilon);
  }

  /**
   * 将数值数组归一化为概率分布
   */
  static normalize(values) {
    const sum = values.reduce((a, b) => a + Math.abs(b), 0);
    if (sum === 0) {
      return values.map(() => 1 / values.length);
    }
    return values.map((v) => Math.abs(v) / sum);
  }

  /**
   * 解释力（Explanatory Power）
   * EP(e) = 1 - (actual_change - predicted_change)² / actual_change²
   * 
   * @param {number} actualChange - 实际变化量（整体指标变化）
   * @param {number} predictedChange - 由该维度解释的变化量
   * @returns {number} 解释力 [0, 1]，1表示完美解释
   */
  static explanatoryPower(actualChange, predictedChange) {
    if (actualChange === 0) {
      return predictedChange === 0 ? 1 : 0;
    }
    const residual = actualChange - predictedChange;
    const ep = 1 - (residual * residual) / (actualChange * actualChange);
    return Math.max(0, Math.min(1, ep));
  }

  /**
   * 简洁性（Parsimony）
   * 越少的元素组成的解释，简洁性越高
   * 
   * @param {number} numElements - 解释中包含的元素数量
   * @param {number} totalElements - 维度中总元素数量
   * @returns {number} 简洁性 [0, 1]
   */
  static parsimony(numElements, totalElements) {
    if (totalElements <= 0) {
      return 0;
    }
    return 1 - (numElements / totalElements);
  }

  /**
   * 惊喜度（Surprise）
   * 使用JS散度衡量基期和现期在某个维度上的分布差异
   * 
   * @param {number[]} baseDistribution - 基期分布
   * @param {number[]} currentDistribution - 现期分布
   * @returns {number} 惊喜度 [0, 1]
   */
  static surprise(baseDistribution, currentDistribution) {
    const baseNorm = this.normalize(baseDistribution);
    const currentNorm = this.normalize(currentDistribution);
    return this.jsDivergence(baseNorm, currentNorm);
  }

  /**
   * Adtributor综合评分
   * Score = α * EP + β * Surprise + γ * Parsimony
   * 
   * @param {number} ep - 解释力
   * @param {number} surpriseScore - 惊喜度
   * @param {number} parsimonyScore - 简洁性
   * @param {Object} weights - 权重配置
   * @returns {number} 综合评分
   */
  static adtributorScore(ep, surpriseScore, parsimonyScore, weights = {}) {
    const alpha = weights.explanatoryPower || 0.5;
    const beta = weights.surprise || 0.3;
    const gamma = weights.parsimony || 0.2;

    return alpha * ep + beta * surpriseScore + gamma * parsimonyScore;
  }

  /**
   * 计算贡献度分解
   * 将整体变化按维度值分解为各维度值的贡献
   * 
   * @param {Object[]} baseData - 基期数据 [{dimension: 'A', metric: 100}, ...]
   * @param {Object[]} currentData - 现期数据
   * @param {string} dimensionKey - 维度字段名
   * @param {string} metricKey - 指标字段名
   * @returns {Object[]} 各维度值的贡献分解
   */
  static contributionDecomposition(baseData, currentData, dimensionKey, metricKey) {
    const baseMap = new Map();
    const currentMap = new Map();

    baseData.forEach((row) => {
      const key = String(row[dimensionKey]);
      baseMap.set(key, (baseMap.get(key) || 0) + (Number(row[metricKey]) || 0));
    });

    currentData.forEach((row) => {
      const key = String(row[dimensionKey]);
      currentMap.set(key, (currentMap.get(key) || 0) + (Number(row[metricKey]) || 0));
    });

    const allKeys = new Set([...baseMap.keys(), ...currentMap.keys()]);
    const baseTotal = [...baseMap.values()].reduce((a, b) => a + b, 0);
    const currentTotal = [...currentMap.values()].reduce((a, b) => a + b, 0);
    const totalChange = currentTotal - baseTotal;

    const contributions = [];
    for (const key of allKeys) {
      const baseVal = baseMap.get(key) || 0;
      const currentVal = currentMap.get(key) || 0;
      const change = currentVal - baseVal;
      const baseProportion = baseTotal !== 0 ? baseVal / baseTotal : 0;
      const currentProportion = currentTotal !== 0 ? currentVal / currentTotal : 0;

      // 预期变化 = 基期占比 * 整体变化
      const expectedChange = baseProportion * totalChange;
      const unexpectedChange = change - expectedChange;

      contributions.push({
        dimensionValue: key,
        baseValue: baseVal,
        currentValue: currentVal,
        change,
        changeRate: baseVal !== 0 ? (change / baseVal) : (currentVal !== 0 ? Infinity : 0),
        baseProportion,
        currentProportion,
        expectedChange,
        unexpectedChange,
        contributionRate: totalChange !== 0 ? change / totalChange : 0,
      });
    }

    return contributions.sort((a, b) => Math.abs(b.change) - Math.abs(a.change));
  }

  /**
   * 变异系数 (Coefficient of Variation)
   */
  static coefficientOfVariation(values) {
    if (values.length === 0) {
      return 0;
    }
    const mean = values.reduce((a, b) => a + b, 0) / values.length;
    if (mean === 0) {
      return 0;
    }
    const variance = values.reduce((sum, v) => sum + Math.pow(v - mean, 2), 0) / values.length;
    return Math.sqrt(variance) / Math.abs(mean);
  }

  /**
   * 基础统计量
   */
  static basicStats(values) {
    if (!values || values.length === 0) {
      return { count: 0, sum: 0, mean: 0, min: 0, max: 0, std: 0, median: 0 };
    }

    const sorted = [...values].sort((a, b) => a - b);
    const count = values.length;
    const sum = values.reduce((a, b) => a + b, 0);
    const mean = sum / count;
    const min = sorted[0];
    const max = sorted[count - 1];
    const variance = values.reduce((s, v) => s + Math.pow(v - mean, 2), 0) / count;
    const std = Math.sqrt(variance);
    const median = count % 2 === 0
      ? (sorted[count / 2 - 1] + sorted[count / 2]) / 2
      : sorted[Math.floor(count / 2)];

    return { count, sum, mean, min, max, std, median };
  }

  /**
   * 皮尔逊相关系数
   */
  static pearsonCorrelation(X, Y) {
    if (X.length !== Y.length || X.length === 0) {
      return 0;
    }
    const n = X.length;
    const meanX = X.reduce((a, b) => a + b, 0) / n;
    const meanY = Y.reduce((a, b) => a + b, 0) / n;

    let sumXY = 0, sumX2 = 0, sumY2 = 0;
    for (let i = 0; i < n; i++) {
      const dx = X[i] - meanX;
      const dy = Y[i] - meanY;
      sumXY += dx * dy;
      sumX2 += dx * dx;
      sumY2 += dy * dy;
    }

    const denom = Math.sqrt(sumX2 * sumY2);
    return denom === 0 ? 0 : sumXY / denom;
  }

  /**
   * 简单线性回归
   * @returns {{ slope, intercept, rSquared }}
   */
  static linearRegression(X, Y) {
    if (X.length !== Y.length || X.length < 2) {
      return { slope: 0, intercept: 0, rSquared: 0 };
    }

    const n = X.length;
    const meanX = X.reduce((a, b) => a + b, 0) / n;
    const meanY = Y.reduce((a, b) => a + b, 0) / n;

    let sumXY = 0, sumX2 = 0, sumY2 = 0;
    for (let i = 0; i < n; i++) {
      sumXY += (X[i] - meanX) * (Y[i] - meanY);
      sumX2 += (X[i] - meanX) * (X[i] - meanX);
      sumY2 += (Y[i] - meanY) * (Y[i] - meanY);
    }

    const slope = sumX2 !== 0 ? sumXY / sumX2 : 0;
    const intercept = meanY - slope * meanX;
    const rSquared = sumY2 !== 0 ? (sumXY * sumXY) / (sumX2 * sumY2) : 0;

    return { slope, intercept, rSquared };
  }
}

module.exports = StatisticsEngine;
