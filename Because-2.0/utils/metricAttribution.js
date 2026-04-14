/**
 * MetricAttribution - 指标归因模块
 * 
 * 实现指标层面的归因分析：
 * - 多元线性回归分解
 * - ElasticNet正则化回归（简化JS实现）
 * - 特征重要性分析（SHAP近似：置换重要性 + Shapley值近似）
 * - 指标关联性分析
 */

const StatisticsEngine = require('./statisticsEngine');

class MetricAttribution {
  /**
   * 多元线性回归（最小二乘法）
   * Y = β0 + β1*X1 + β2*X2 + ... + βn*Xn
   * 
   * @param {number[][]} X - 特征矩阵 (m x n)
   * @param {number[]} Y - 目标向量 (m x 1)
   * @returns {{ coefficients: number[], intercept: number, rSquared: number, predictions: number[] }}
   */
  static multipleLinearRegression(X, Y) {
    const m = X.length;
    const n = X[0]?.length || 0;

    if (m < n + 1 || m !== Y.length) {
      return { coefficients: new Array(n).fill(0), intercept: 0, rSquared: 0, predictions: [] };
    }

    // 添加截距列（全1列）
    const Xa = X.map((row) => [1, ...row]);
    const p = n + 1;

    // 计算 X^T * X
    const XtX = this._matMul(this._transpose(Xa), Xa);
    // 计算 X^T * Y
    const XtY = this._matVecMul(this._transpose(Xa), Y);

    // 正则化以避免奇异矩阵 (Ridge: XtX + λI)
    const lambda = 0.001;
    for (let i = 0; i < p; i++) {
      XtX[i][i] += lambda;
    }

    // 求解 β = (X^T*X + λI)^(-1) * X^T * Y
    const beta = this._solveLinearSystem(XtX, XtY);
    if (!beta) {
      return { coefficients: new Array(n).fill(0), intercept: 0, rSquared: 0, predictions: [] };
    }

    const intercept = beta[0];
    const coefficients = beta.slice(1);

    // 计算预测值和R²
    const predictions = X.map((row) =>
      intercept + row.reduce((s, x, i) => s + x * coefficients[i], 0),
    );

    const yMean = Y.reduce((a, b) => a + b, 0) / m;
    const ssRes = Y.reduce((s, y, i) => s + Math.pow(y - predictions[i], 2), 0);
    const ssTot = Y.reduce((s, y) => s + Math.pow(y - yMean, 2), 0);
    const rSquared = ssTot > 0 ? 1 - ssRes / ssTot : 0;

    return { coefficients, intercept, rSquared: Math.max(0, rSquared), predictions };
  }

  /**
   * ElasticNet回归（简化实现：坐标下降法）
   * 最小化: 1/(2m) * ||Y - Xβ||² + α*ρ*||β||₁ + α*(1-ρ)/2*||β||₂²
   * 
   * @param {number[][]} X - 特征矩阵
   * @param {number[]} Y - 目标向量
   * @param {number} alpha - 正则化强度
   * @param {number} l1Ratio - L1比率 (0=Ridge, 1=Lasso)
   * @param {number} maxIter - 最大迭代次数
   * @returns {{ coefficients: number[], intercept: number, rSquared: number }}
   */
  static elasticNet(X, Y, alpha = 0.1, l1Ratio = 0.5, maxIter = 1000) {
    const m = X.length;
    const n = X[0]?.length || 0;

    if (m === 0 || n === 0) {
      return { coefficients: [], intercept: 0, rSquared: 0 };
    }

    // 标准化
    const { normalizedX, means, stds } = this._standardize(X);
    const yMean = Y.reduce((a, b) => a + b, 0) / m;
    const normalizedY = Y.map((y) => y - yMean);

    let beta = new Array(n).fill(0);
    const tolerance = 1e-6;

    for (let iter = 0; iter < maxIter; iter++) {
      const oldBeta = [...beta];
      let maxChange = 0;

      for (let j = 0; j < n; j++) {
        let residual = 0;
        for (let i = 0; i < m; i++) {
          let pred = 0;
          for (let k = 0; k < n; k++) {
            if (k !== j) {
              pred += normalizedX[i][k] * beta[k];
            }
          }
          residual += normalizedX[i][j] * (normalizedY[i] - pred);
        }
        residual /= m;

        const l1Penalty = alpha * l1Ratio;
        const l2Penalty = alpha * (1 - l1Ratio);

        // 软阈值操作
        beta[j] = this._softThreshold(residual, l1Penalty) / (1 + l2Penalty);
        maxChange = Math.max(maxChange, Math.abs(beta[j] - oldBeta[j]));
      }

      if (maxChange < tolerance) {
        break;
      }
    }

    // 反标准化系数
    const coefficients = beta.map((b, j) => stds[j] > 0 ? b / stds[j] : 0);
    const intercept = yMean - coefficients.reduce((s, c, j) => s + c * means[j], 0);

    // 计算R²
    const predictions = X.map((row) =>
      intercept + row.reduce((s, x, j) => s + x * coefficients[j], 0),
    );
    const ssRes = Y.reduce((s, y, i) => s + Math.pow(y - predictions[i], 2), 0);
    const ssTot = Y.reduce((s, y) => s + Math.pow(y - yMean, 2), 0);
    const rSquared = ssTot > 0 ? Math.max(0, 1 - ssRes / ssTot) : 0;

    return { coefficients, intercept, rSquared };
  }

  /**
   * 特征重要性分析（置换重要性 + Shapley值近似）
   * 
   * @param {number[][]} X - 特征矩阵
   * @param {number[]} Y - 目标向量
   * @param {string[]} featureNames - 特征名称
   * @returns {Object[]} 特征重要性排序
   */
  static featureImportance(X, Y, featureNames) {
    const m = X.length;
    const n = X[0]?.length || 0;

    if (m < 3 || n === 0) {
      return featureNames.map((name) => ({
        feature: name, importance: 0, direction: 'unknown', method: 'insufficient_data',
      }));
    }

    const fullModel = this.multipleLinearRegression(X, Y);
    const fullMSE = this._calculateMSE(Y, fullModel.predictions);

    const importances = featureNames.map((name, j) => {
      // 方法1: 置换重要性
      const permutedX = X.map((row) => [...row]);
      const col = permutedX.map((row) => row[j]);
      // Fisher-Yates shuffle
      for (let i = col.length - 1; i > 0; i--) {
        const k = Math.floor(Math.random() * (i + 1));
        [col[i], col[k]] = [col[k], col[i]];
      }
      permutedX.forEach((row, i) => { row[j] = col[i]; });

      const permModel = this.multipleLinearRegression(permutedX, Y);
      const permMSE = this._calculateMSE(Y, permModel.predictions);
      const permutationImportance = fullMSE > 0 ? (permMSE - fullMSE) / fullMSE : 0;

      // 方法2: Leave-one-out (Shapley近似)
      const reducedX = X.map((row) => row.filter((_, k) => k !== j));
      const reducedModel = this.multipleLinearRegression(reducedX, Y);
      const shapleyApprox = fullModel.rSquared - reducedModel.rSquared;

      const combinedImportance = 0.5 * Math.max(0, permutationImportance) + 0.5 * Math.max(0, shapleyApprox);

      return {
        feature: name,
        importance: Number(combinedImportance.toFixed(6)),
        coefficient: Number(fullModel.coefficients[j]?.toFixed(6) || 0),
        direction: (fullModel.coefficients[j] || 0) > 0 ? 'positive' : 'negative',
        permutationImportance: Number(Math.max(0, permutationImportance).toFixed(6)),
        shapleyApprox: Number(Math.max(0, shapleyApprox).toFixed(6)),
        method: 'permutation_shapley_hybrid',
      };
    });

    importances.sort((a, b) => b.importance - a.importance);

    // 归一化
    const totalImportance = importances.reduce((s, f) => s + f.importance, 0);
    if (totalImportance > 0) {
      importances.forEach((f) => {
        f.normalizedImportance = Number((f.importance / totalImportance).toFixed(4));
      });
    }

    return importances;
  }

  /**
   * 指标关联性分析
   * @param {Object[]} data - 数据
   * @param {string[]} metricFields - 指标字段列表
   * @returns {Object} 相关性矩阵和关键发现
   */
  static metricCorrelation(data, metricFields) {
    const correlationMatrix = {};
    const keyFindings = [];

    for (let i = 0; i < metricFields.length; i++) {
      correlationMatrix[metricFields[i]] = {};
      const xi = data.map((r) => Number(r[metricFields[i]]) || 0);

      for (let j = 0; j < metricFields.length; j++) {
        const xj = data.map((r) => Number(r[metricFields[j]]) || 0);
        const corr = StatisticsEngine.pearsonCorrelation(xi, xj);
        correlationMatrix[metricFields[i]][metricFields[j]] = Number(corr.toFixed(4));

        if (i < j && Math.abs(corr) > 0.7) {
          keyFindings.push({
            metric1: metricFields[i],
            metric2: metricFields[j],
            correlation: Number(corr.toFixed(4)),
            strength: Math.abs(corr) > 0.9 ? 'very_strong' : 'strong',
            direction: corr > 0 ? 'positive' : 'negative',
          });
        }
      }
    }

    return { correlationMatrix, keyFindings };
  }

  /**
   * 指标分解归因
   * 将复合指标分解为子指标的贡献
   * 例如：收入 = 客单价 × 订单数
   * 
   * @param {Object} params
   * @param {Object[]} params.baseData
   * @param {Object[]} params.currentData
   * @param {string} params.targetMetric - 目标指标
   * @param {string[]} params.componentMetrics - 组成指标
   * @returns {Object} 分解结果
   */
  static metricDecomposition({ baseData, currentData, targetMetric, componentMetrics }) {
    const baseTarget = baseData.reduce((s, r) => s + (Number(r[targetMetric]) || 0), 0);
    const currentTarget = currentData.reduce((s, r) => s + (Number(r[targetMetric]) || 0), 0);
    const totalChange = currentTarget - baseTarget;

    const components = componentMetrics.map((metric) => {
      const baseVal = baseData.reduce((s, r) => s + (Number(r[metric]) || 0), 0);
      const currentVal = currentData.reduce((s, r) => s + (Number(r[metric]) || 0), 0);
      const change = currentVal - baseVal;

      return {
        metric,
        baseValue: baseVal,
        currentValue: currentVal,
        change,
        changeRate: baseVal !== 0 ? change / baseVal : 0,
      };
    });

    // 用线性回归估算各组成指标对目标指标的贡献
    const allData = [...baseData, ...currentData];
    const X = allData.map((r) => componentMetrics.map((m) => Number(r[m]) || 0));
    const Y = allData.map((r) => Number(r[targetMetric]) || 0);

    const featureImps = this.featureImportance(X, Y, componentMetrics);

    return {
      targetMetric,
      baseValue: baseTarget,
      currentValue: currentTarget,
      totalChange,
      changeRate: baseTarget !== 0 ? totalChange / baseTarget : 0,
      components: components.map((c) => {
        const imp = featureImps.find((f) => f.feature === c.metric);
        return {
          ...c,
          importance: imp?.normalizedImportance || 0,
          direction: imp?.direction || 'unknown',
        };
      }),
      modelFit: featureImps.length > 0 ? 'regression' : 'direct',
    };
  }

  // ---- 私有辅助方法 ----

  static _matMul(A, B) {
    const m = A.length;
    const n = B[0].length;
    const p = B.length;
    const C = Array.from({ length: m }, () => new Array(n).fill(0));
    for (let i = 0; i < m; i++) {
      for (let j = 0; j < n; j++) {
        for (let k = 0; k < p; k++) {
          C[i][j] += A[i][k] * B[k][j];
        }
      }
    }
    return C;
  }

  static _transpose(A) {
    const m = A.length;
    const n = A[0].length;
    const T = Array.from({ length: n }, () => new Array(m));
    for (let i = 0; i < m; i++) {
      for (let j = 0; j < n; j++) {
        T[j][i] = A[i][j];
      }
    }
    return T;
  }

  static _matVecMul(A, v) {
    return A.map((row) => row.reduce((s, a, j) => s + a * v[j], 0));
  }

  /**
   * 高斯消元法求解线性方程组 Ax = b
   */
  static _solveLinearSystem(A, b) {
    const n = A.length;
    const aug = A.map((row, i) => [...row, b[i]]);

    for (let col = 0; col < n; col++) {
      let maxRow = col;
      for (let row = col + 1; row < n; row++) {
        if (Math.abs(aug[row][col]) > Math.abs(aug[maxRow][col])) {
          maxRow = row;
        }
      }
      [aug[col], aug[maxRow]] = [aug[maxRow], aug[col]];

      if (Math.abs(aug[col][col]) < 1e-12) {
        continue;
      }

      for (let row = col + 1; row < n; row++) {
        const factor = aug[row][col] / aug[col][col];
        for (let j = col; j <= n; j++) {
          aug[row][j] -= factor * aug[col][j];
        }
      }
    }

    const x = new Array(n).fill(0);
    for (let i = n - 1; i >= 0; i--) {
      if (Math.abs(aug[i][i]) < 1e-12) {
        continue;
      }
      let sum = aug[i][n];
      for (let j = i + 1; j < n; j++) {
        sum -= aug[i][j] * x[j];
      }
      x[i] = sum / aug[i][i];
    }

    return x;
  }

  static _standardize(X) {
    const m = X.length;
    const n = X[0].length;
    const means = new Array(n).fill(0);
    const stds = new Array(n).fill(0);

    for (let j = 0; j < n; j++) {
      for (let i = 0; i < m; i++) {
        means[j] += X[i][j];
      }
      means[j] /= m;

      for (let i = 0; i < m; i++) {
        stds[j] += Math.pow(X[i][j] - means[j], 2);
      }
      stds[j] = Math.sqrt(stds[j] / m);
    }

    const normalizedX = X.map((row) =>
      row.map((val, j) => (stds[j] > 0 ? (val - means[j]) / stds[j] : 0)),
    );

    return { normalizedX, means, stds };
  }

  static _softThreshold(x, lambda) {
    if (x > lambda) {
      return x - lambda;
    }
    if (x < -lambda) {
      return x + lambda;
    }
    return 0;
  }

  static _calculateMSE(actual, predicted) {
    if (actual.length === 0 || actual.length !== predicted.length) {
      return 0;
    }
    return actual.reduce((s, y, i) => s + Math.pow(y - (predicted[i] || 0), 2), 0) / actual.length;
  }
}

module.exports = MetricAttribution;
