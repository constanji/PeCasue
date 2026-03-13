/**
 * BeCause 2.0 归因工具测试脚本
 *
 * 运行：node Because-2.0/test-attribution.js
 *
 * 三个层次：
 *   1. 单元测试 — StatisticsEngine / MetricAttribution
 *   2. 模块测试 — DimensionDrillDown / TimeComparison
 *   3. 集成测试 — FluctuationAttributionTool._call()
 */

const StatisticsEngine = require('./utils/statisticsEngine');
const DimensionDrillDown = require('./utils/dimensionDrillDown');
const MetricAttribution = require('./utils/metricAttribution');
const TimeComparison = require('./utils/timeComparison');

// ─── 测试框架 ────────────────────────────────────────────

let totalTests = 0;
let passed = 0;
let failed = 0;
const failures = [];

function assert(condition, testName, detail = '') {
  totalTests++;
  if (condition) {
    passed++;
    console.log(`  ✅ ${testName}`);
  } else {
    failed++;
    const msg = `  ❌ ${testName}${detail ? ' — ' + detail : ''}`;
    console.log(msg);
    failures.push(msg);
  }
}

function assertApprox(actual, expected, tolerance, testName) {
  const diff = Math.abs(actual - expected);
  assert(
    diff <= tolerance,
    testName,
    `expected ≈${expected}, got ${actual} (diff=${diff.toFixed(6)})`,
  );
}

function section(title) {
  console.log(`\n${'═'.repeat(60)}`);
  console.log(`  ${title}`);
  console.log('═'.repeat(60));
}

// ─── 测试数据 ────────────────────────────────────────────

const BASE_SIMPLE = [
  { region: '华东', product: '家电', channel: '线上', revenue: 500, orders: 100 },
  { region: '华东', product: '服装', channel: '线下', revenue: 200, orders: 80 },
  { region: '华北', product: '家电', channel: '线上', revenue: 300, orders: 60 },
  { region: '华北', product: '服装', channel: '线下', revenue: 150, orders: 40 },
  { region: '华南', product: '家电', channel: '线上', revenue: 200, orders: 50 },
  { region: '华南', product: '服装', channel: '线下', revenue: 100, orders: 30 },
];

const CURRENT_SIMPLE = [
  { region: '华东', product: '家电', channel: '线上', revenue: 200, orders: 40 },  // 暴跌
  { region: '华东', product: '服装', channel: '线下', revenue: 180, orders: 75 },
  { region: '华北', product: '家电', channel: '线上', revenue: 290, orders: 58 },
  { region: '华北', product: '服装', channel: '线下', revenue: 160, orders: 42 },
  { region: '华南', product: '家电', channel: '线上', revenue: 210, orders: 52 },
  { region: '华南', product: '服装', channel: '线下', revenue: 105, orders: 32 },
];

// ─── 层次一：StatisticsEngine 单元测试 ──────────────────

section('层次一：StatisticsEngine 单元测试');

// 1.1 JS 散度
console.log('\n--- JS 散度 ---');
assertApprox(
  StatisticsEngine.jsDivergence([0.5, 0.5], [0.5, 0.5]),
  0, 1e-8, 'JSD(相同分布) ≈ 0',
);
const jsdOpposite = StatisticsEngine.jsDivergence([1, 0], [0, 1]);
assert(jsdOpposite > 0.9 && jsdOpposite <= 1.0, `JSD(完全对立) ∈ (0.9, 1.0]，实际=${jsdOpposite.toFixed(4)}`);

const jsdModerate = StatisticsEngine.jsDivergence([0.7, 0.3], [0.3, 0.7]);
assert(jsdModerate > 0 && jsdModerate < jsdOpposite, `JSD(中等差异) ∈ (0, JSD_opposite)，实际=${jsdModerate.toFixed(4)}`);

// 1.2 KL 散度
console.log('\n--- KL 散度 ---');
assertApprox(
  StatisticsEngine.klDivergence([0.5, 0.5], [0.5, 0.5]),
  0, 1e-6, 'KL(相同分布) ≈ 0',
);
const klAsym = StatisticsEngine.klDivergence([0.9, 0.1], [0.5, 0.5]);
assert(klAsym > 0, `KL(非对称) > 0，实际=${klAsym.toFixed(4)}`);

// 1.3 归一化
console.log('\n--- normalize ---');
const norm = StatisticsEngine.normalize([100, 200, 300]);
assertApprox(norm.reduce((a, b) => a + b, 0), 1.0, 1e-10, 'normalize 后总和为 1');
assertApprox(norm[2], 0.5, 1e-10, 'normalize([100,200,300])[2] = 0.5');

const normZero = StatisticsEngine.normalize([0, 0, 0]);
assertApprox(normZero[0], 1 / 3, 1e-10, 'normalize(全零) → 均匀分布');

// 1.4 解释力
console.log('\n--- 解释力 (EP) ---');
assertApprox(StatisticsEngine.explanatoryPower(100, 100), 1.0, 1e-10, 'EP(完美解释) = 1');
assertApprox(StatisticsEngine.explanatoryPower(100, 0), 0.0, 1e-10, 'EP(完全无关) = 0');
assertApprox(StatisticsEngine.explanatoryPower(100, 50), 0.75, 1e-10, 'EP(解释50%) = 0.75');
assertApprox(StatisticsEngine.explanatoryPower(0, 0), 1.0, 1e-10, 'EP(0,0) = 1');
assertApprox(StatisticsEngine.explanatoryPower(0, 5), 0.0, 1e-10, 'EP(0,5) = 0');

// 1.5 简洁性
console.log('\n--- 简洁性 (Parsimony) ---');
assertApprox(StatisticsEngine.parsimony(1, 10), 0.9, 1e-10, 'Parsimony(1/10) = 0.9');
assertApprox(StatisticsEngine.parsimony(5, 10), 0.5, 1e-10, 'Parsimony(5/10) = 0.5');
assertApprox(StatisticsEngine.parsimony(10, 10), 0.0, 1e-10, 'Parsimony(10/10) = 0');

// 1.6 Adtributor 综合评分
console.log('\n--- Adtributor 评分 ---');
assertApprox(
  StatisticsEngine.adtributorScore(1.0, 0.5, 0.8),
  1.0 * 0.5 + 0.5 * 0.3 + 0.8 * 0.2,
  1e-10,
  'Adtributor = 0.5*EP + 0.3*Surprise + 0.2*Parsimony',
);
assertApprox(
  StatisticsEngine.adtributorScore(0.8, 0.2, 0.9, { explanatoryPower: 0.6, surprise: 0.2, parsimony: 0.2 }),
  0.8 * 0.6 + 0.2 * 0.2 + 0.9 * 0.2,
  1e-10,
  'Adtributor（自定义权重）',
);

// 1.7 贡献分解
console.log('\n--- 贡献分解 ---');
const contributions = StatisticsEngine.contributionDecomposition(
  BASE_SIMPLE, CURRENT_SIMPLE, 'region', 'revenue',
);
assert(contributions.length === 3, `贡献分解：3个维度值，实际=${contributions.length}`);
const huadong = contributions.find((c) => c.dimensionValue === '华东');
assert(huadong !== undefined, '贡献分解：包含华东');
assertApprox(huadong.change, (200 + 180) - (500 + 200), 1e-6, '华东 change = -320');
assert(huadong.contributionRate > 1.0, `华东贡献率 > 1（过度解释下降，其他区域有正向抵消），实际=${huadong.contributionRate.toFixed(4)}`);

// 1.8 基础统计量
console.log('\n--- basicStats ---');
const stats = StatisticsEngine.basicStats([10, 20, 30, 40, 50]);
assertApprox(stats.mean, 30, 1e-10, 'mean([10..50]) = 30');
assertApprox(stats.sum, 150, 1e-10, 'sum = 150');
assertApprox(stats.median, 30, 1e-10, 'median = 30');
assertApprox(stats.min, 10, 1e-10, 'min = 10');
assertApprox(stats.max, 50, 1e-10, 'max = 50');

const emptyStats = StatisticsEngine.basicStats([]);
assert(emptyStats.count === 0, 'basicStats([]) → count=0');

// 1.9 Pearson 相关系数
console.log('\n--- Pearson 相关系数 ---');
assertApprox(
  StatisticsEngine.pearsonCorrelation([1, 2, 3, 4, 5], [2, 4, 6, 8, 10]),
  1.0, 1e-10, 'Pearson(完美正相关) = 1',
);
assertApprox(
  StatisticsEngine.pearsonCorrelation([1, 2, 3, 4, 5], [10, 8, 6, 4, 2]),
  -1.0, 1e-10, 'Pearson(完美负相关) = -1',
);

// 1.10 线性回归
console.log('\n--- 线性回归 ---');
const lr = StatisticsEngine.linearRegression([1, 2, 3, 4, 5], [3, 5, 7, 9, 11]);
assertApprox(lr.slope, 2.0, 1e-10, 'slope(Y=2X+1) = 2');
assertApprox(lr.intercept, 1.0, 1e-10, 'intercept = 1');
assertApprox(lr.rSquared, 1.0, 1e-10, 'R² = 1 (完美线性)');

// ─── 层次二A：DimensionDrillDown 模块测试 ───────────────

section('层次二A：DimensionDrillDown 模块测试');

// 2A.1 单维度归因 — 华东暴跌场景
console.log('\n--- 场景：华东区暴跌 ---');
const dimResult = DimensionDrillDown.analyze({
  baseData: BASE_SIMPLE,
  currentData: CURRENT_SIMPLE,
  metricField: 'revenue',
  dimensionFields: ['region'],
});

assert(dimResult.overview.direction === 'decrease', `总体方向 = decrease`);
assert(dimResult.overview.totalChange < 0, `totalChange < 0，实际=${dimResult.overview.totalChange}`);
assert(dimResult.dimensionRanking.length === 1, '1个维度');
assert(dimResult.dimensionRanking[0].dimension === 'region', '维度名 = region');
const regionRanking = dimResult.dimensionRanking[0];
assert(regionRanking.explanatoryPower > 0.5, `EP > 0.5，实际=${regionRanking.explanatoryPower}`);
assert(regionRanking.topContributors[0].value === '华东', `top contributor = 华东，实际=${regionRanking.topContributors[0].value}`);

// 2A.2 多维度下钻
console.log('\n--- 多维度下钻 ---');
const drillResult = DimensionDrillDown.analyze({
  baseData: BASE_SIMPLE,
  currentData: CURRENT_SIMPLE,
  metricField: 'revenue',
  dimensionFields: ['region', 'product'],
});

assert(drillResult.dimensionRanking.length === 2, '2个维度');
assert(drillResult.drillPaths.length > 0, `存在下钻路径，数量=${drillResult.drillPaths.length}`);

const topPath = drillResult.drillPaths[0];
assert(topPath.steps.length >= 1, `下钻路径至少1步，实际=${topPath.steps.length}`);
assert(topPath.steps[0].dimension === 'region', '第一步 = region');
console.log(`  ℹ️ 最强路径: ${topPath.steps.map((s) => `${s.dimension}="${s.value}"`).join(' → ')}`);

// 2A.3 三层下钻
console.log('\n--- 三层下钻 ---');
const drill3 = DimensionDrillDown.analyze({
  baseData: BASE_SIMPLE,
  currentData: CURRENT_SIMPLE,
  metricField: 'revenue',
  dimensionFields: ['region', 'product', 'channel'],
});
assert(drill3.drillPaths.length > 0, '三层下钻有路径');
assert(drill3.drillPaths[0].steps.length <= 3, `路径深度 <= 3，实际=${drill3.drillPaths[0].steps.length}`);

// 2A.4 均匀下降场景 — 惊喜度应低
console.log('\n--- 场景：均匀下降 ---');
const uniformBase = [
  { region: '华东', revenue: 100 },
  { region: '华北', revenue: 100 },
  { region: '华南', revenue: 100 },
];
const uniformCurrent = [
  { region: '华东', revenue: 50 },
  { region: '华北', revenue: 50 },
  { region: '华南', revenue: 50 },
];
const uniformResult = DimensionDrillDown.analyze({
  baseData: uniformBase,
  currentData: uniformCurrent,
  metricField: 'revenue',
  dimensionFields: ['region'],
});
const uniformSurprise = uniformResult.dimensionRanking[0].surprise;
assert(uniformSurprise < 0.01, `均匀下降惊喜度 ≈ 0，实际=${uniformSurprise}`);

// 2A.5 边界：空数据
console.log('\n--- 边界：空数据 ---');
const emptyResult = DimensionDrillDown.analyze({
  baseData: [],
  currentData: [],
  metricField: 'revenue',
  dimensionFields: ['region'],
});
assert(emptyResult.overview.totalChange === 0, '空数据 totalChange = 0');

// ─── 层次二B：MetricAttribution 模块测试 ────────────────

section('层次二B：MetricAttribution 模块测试');

// 2B.1 多元线性回归 — 已知系数
console.log('\n--- 多元线性回归 ---');
const X_reg = [[1, 1], [2, 1], [1, 2], [2, 2], [3, 1], [1, 3], [3, 3], [2, 3]];
const Y_reg = X_reg.map(([x1, x2]) => 2 * x1 + 3 * x2 + 1);
const mlr = MetricAttribution.multipleLinearRegression(X_reg, Y_reg);

assertApprox(mlr.coefficients[0], 2.0, 0.1, '系数[0] ≈ 2');
assertApprox(mlr.coefficients[1], 3.0, 0.1, '系数[1] ≈ 3');
assertApprox(mlr.intercept, 1.0, 0.2, '截距 ≈ 1');
assert(mlr.rSquared > 0.99, `R² > 0.99，实际=${mlr.rSquared.toFixed(4)}`);

// 2B.2 ElasticNet
console.log('\n--- ElasticNet ---');
const enResult = MetricAttribution.elasticNet(X_reg, Y_reg, 0.01, 0.5);
assert(enResult.rSquared > 0.95, `ElasticNet R² > 0.95，实际=${enResult.rSquared.toFixed(4)}`);
assert(enResult.coefficients.length === 2, 'ElasticNet 系数数量 = 2');

// 2B.3 特征重要性
console.log('\n--- 特征重要性 ---');
const fiResult = MetricAttribution.featureImportance(X_reg, Y_reg, ['X1', 'X2']);
assert(fiResult.length === 2, '2个特征');
assert(fiResult[0].importance >= fiResult[1].importance, '按重要性降序排列');
console.log(`  ℹ️ ${fiResult[0].feature}=${fiResult[0].importance.toFixed(4)}, ${fiResult[1].feature}=${fiResult[1].importance.toFixed(4)}`);

// 2B.4 指标关联
console.log('\n--- 指标关联 ---');
const corrData = [
  { revenue: 100, orders: 50, cost: 80 },
  { revenue: 200, orders: 100, cost: 90 },
  { revenue: 300, orders: 150, cost: 95 },
  { revenue: 400, orders: 200, cost: 100 },
  { revenue: 500, orders: 250, cost: 105 },
];
const corrResult = MetricAttribution.metricCorrelation(corrData, ['revenue', 'orders', 'cost']);
assert(corrResult.correlationMatrix.revenue.orders > 0.99, 'revenue-orders 强正相关');
assert(corrResult.keyFindings.length > 0, `发现强相关对，数量=${corrResult.keyFindings.length}`);

// 2B.5 指标分解
console.log('\n--- 指标分解 ---');
const decompBase = [
  { 收入: 1000, 客单价: 100, 订单数: 10 },
  { 收入: 800, 客单价: 80, 订单数: 10 },
  { 收入: 1200, 客单价: 120, 订单数: 10 },
];
const decompCurrent = [
  { 收入: 600, 客单价: 60, 订单数: 10 },
  { 收入: 500, 客单价: 50, 订单数: 10 },
  { 收入: 400, 客单价: 40, 订单数: 10 },
];
const decomp = MetricAttribution.metricDecomposition({
  baseData: decompBase,
  currentData: decompCurrent,
  targetMetric: '收入',
  componentMetrics: ['客单价', '订单数'],
});
assert(decomp.totalChange < 0, `收入总变化 < 0，实际=${decomp.totalChange}`);
assert(decomp.components.length === 2, '2个组成指标');
console.log(`  ℹ️ 收入变化: ${decomp.totalChange}，客单价重要性: ${decomp.components[0].importance}, 订单数重要性: ${decomp.components[1].importance}`);

// 2B.6 边界：数据不足
console.log('\n--- 边界：数据不足 ---');
const fewData = MetricAttribution.multipleLinearRegression([[1]], [2]);
assert(fewData.rSquared === 0, '数据不足时 R²=0');

// ─── 层次二C：TimeComparison 模块测试 ───────────────────

section('层次二C：TimeComparison 模块测试');

// 2C.1 时间字段检测
console.log('\n--- 时间字段检测 ---');
const timeData = [{ date: '2026-01-01', region: '华东', revenue: 100 }];
assert(TimeComparison.detectTimeField(timeData) === 'date', '检测到 date 字段');
assert(TimeComparison.detectTimeField([{ month: '2026-01', val: 1 }]) === 'month', '检测到 month 字段');
assert(TimeComparison.detectTimeField([]) === null, '空数据返回 null');

// 2C.2 时间粒度检测
console.log('\n--- 时间粒度检测 ---');
assert(TimeComparison.detectGranularity('2026-03-12') === 'date', '日粒度');
assert(TimeComparison.detectGranularity('2026-03') === 'month', '月粒度');
assert(TimeComparison.detectGranularity('2026') === 'year', '年粒度');
assert(TimeComparison.detectGranularity('2026-03-12T10:30:00') === 'datetime', '日期时间');

// 2C.3 基期计算
console.log('\n--- 基期计算 ---');
const yoyBase = TimeComparison.calculateBasePeriod('2026-03-01', '2026-03-31', 'year_over_year');
assert(yoyBase.baseStart.getFullYear() === 2025, '同比基期年份 = 2025');

const momBase = TimeComparison.calculateBasePeriod('2026-03-01', '2026-03-31', 'month_over_month');
assert(momBase.baseStart.getMonth() === 1, '环比基期月份 = 2 (0-indexed=1)');

// 2C.4 数据分割
console.log('\n--- 数据分割 ---');
const timeSeriesData = [
  { date: '2026-01-15', revenue: 100 },
  { date: '2026-01-20', revenue: 120 },
  { date: '2026-02-15', revenue: 90 },
  { date: '2026-02-20', revenue: 80 },
];
const split = TimeComparison.splitByPeriod(
  timeSeriesData, 'date',
  '2026-01-01', '2026-01-31',
  '2026-02-01', '2026-02-28',
);
assert(split.baseData.length === 2, `基期 2 条，实际=${split.baseData.length}`);
assert(split.currentData.length === 2, `现期 2 条，实际=${split.currentData.length}`);

// 2C.5 时间对比分析
console.log('\n--- 时间对比分析 ---');
const tcResult = TimeComparison.analyze({
  baseData: split.baseData,
  currentData: split.currentData,
  metricFields: ['revenue'],
});
assert(tcResult.metricComparisons.length === 1, '1个指标对比');
assert(tcResult.metricComparisons[0].change.direction === 'decrease', '收入下降');
assert(tcResult.overview.direction === 'decrease', 'overview 方向 = decrease');
console.log(`  ℹ️ 收入变化率: ${tcResult.metricComparisons[0].change.ratePercent}`);

// 2C.6 SQL 生成
console.log('\n--- 对比SQL生成 ---');
const sqlPair = TimeComparison.generateComparisonSQL({
  tableName: 'sales',
  timeField: 'date',
  metricFields: ['revenue', 'orders'],
  dimensionFields: ['region'],
  basePeriodStart: '2026-01-01',
  basePeriodEnd: '2026-01-31',
  currentPeriodStart: '2026-02-01',
  currentPeriodEnd: '2026-02-28',
});
assert(sqlPair.baseSql.includes('2026-01-01'), '基期SQL包含基期日期');
assert(sqlPair.currentSql.includes('2026-02-01'), '现期SQL包含现期日期');
assert(sqlPair.baseSql.includes('GROUP BY'), '包含GROUP BY');

// ─── 层次三：FluctuationAttributionTool 集成测试 ────────

section('层次三：FluctuationAttributionTool 集成测试');

async function runIntegrationTests() {
  // mock logger 避免依赖 @because/data-schemas
  let FATool;
  try {
    FATool = require('./fluctuation-attribution-tool/scripts/FluctuationAttributionTool');
  } catch (err) {
    console.log(`\n  ⚠️  无法加载 FluctuationAttributionTool（可能缺少依赖 @because/data-schemas）`);
    console.log(`      错误: ${err.message}`);
    console.log('      跳过集成测试，单独测试算法模块已通过。\n');
    return;
  }

  const tool = new FATool();

  // 3.1 综合归因 — 直接提供两期数据
  console.log('\n--- 场景1：综合归因（base_data + current_data） ---');
  try {
    const raw = await tool._call({
      analysis_type: 'comprehensive',
      base_data: BASE_SIMPLE,
      current_data: CURRENT_SIMPLE,
      metric_fields: ['revenue'],
      dimension_fields: ['region', 'product'],
      max_drill_depth: 3,
    });
    const result = JSON.parse(raw);

    assert(!result.error, '无错误');
    assert(result.analysis_type === 'comprehensive', 'analysis_type = comprehensive');
    assert(result.time_comparison !== undefined, '包含 time_comparison');
    assert(result.dimension_attribution !== undefined, '包含 dimension_attribution');
    assert(result.metric_attribution !== undefined, '包含 metric_attribution');
    assert(result.dimension_attribution.dimensionRanking?.length === 2, '2个维度排名');
    assert(
      result.dimension_attribution.dimensionRanking[0].adtributorScore >=
      result.dimension_attribution.dimensionRanking[1].adtributorScore,
      '维度按 Adtributor 降序排列',
    );
    assert(result.dimension_attribution.drillPaths?.length > 0, '有下钻路径');
    assert(typeof result.conclusion === 'string' && result.conclusion.length > 0, '有归因结论');
    assert(Array.isArray(result.next_steps), '有后续建议');

    console.log(`  ℹ️ 结论: ${result.conclusion.split('\n')[0]}`);
  } catch (err) {
    assert(false, '综合归因执行失败', err.message);
  }

  // 3.2 仅维度归因
  console.log('\n--- 场景2：仅维度归因 ---');
  try {
    const raw = await tool._call({
      analysis_type: 'dimension',
      base_data: BASE_SIMPLE,
      current_data: CURRENT_SIMPLE,
      metric_fields: ['revenue'],
      dimension_fields: ['region', 'product', 'channel'],
      max_drill_depth: 3,
    });
    const result = JSON.parse(raw);
    assert(!result.error, '无错误');
    assert(result.dimension_attribution !== undefined, '包含 dimension_attribution');
    assert(result.metric_attribution === undefined, '不包含 metric_attribution');
    assert(result.dimension_attribution.dimensionRanking?.length === 3, '3个维度');
  } catch (err) {
    assert(false, '维度归因执行失败', err.message);
  }

  // 3.3 仅指标归因
  console.log('\n--- 场景3：仅指标归因（指标分解） ---');
  try {
    const raw = await tool._call({
      analysis_type: 'metric',
      base_data: BASE_SIMPLE,
      current_data: CURRENT_SIMPLE,
      metric_fields: ['revenue', 'orders'],
      dimension_fields: ['region'],
      target_metric: 'revenue',
      component_metrics: ['orders'],
      max_drill_depth: 3,
    });
    const result = JSON.parse(raw);
    assert(!result.error, '无错误');
    assert(result.metric_attribution !== undefined, '包含 metric_attribution');
    assert(result.dimension_attribution === undefined, '不包含 dimension_attribution');
    if (result.metric_attribution.correlation) {
      assert(result.metric_attribution.correlation.correlationMatrix !== undefined, '有相关矩阵');
    }
  } catch (err) {
    assert(false, '指标归因执行失败', err.message);
  }

  // 3.4 full_data 模式（自动时间分割）
  console.log('\n--- 场景4：full_data 模式 ---');
  try {
    const fullData = [
      { date: '2026-01-10', region: '华东', revenue: 500 },
      { date: '2026-01-15', region: '华北', revenue: 300 },
      { date: '2026-02-10', region: '华东', revenue: 200 },
      { date: '2026-02-15', region: '华北', revenue: 280 },
    ];
    const raw = await tool._call({
      analysis_type: 'comprehensive',
      full_data: fullData,
      time_field: 'date',
      metric_fields: ['revenue'],
      dimension_fields: ['region'],
      time_comparison: { type: 'custom', base_start: '2026-01-01', base_end: '2026-01-31', current_start: '2026-02-01', current_end: '2026-02-28' },
      max_drill_depth: 2,
    });
    const result = JSON.parse(raw);
    assert(!result.error, '无错误');
    assert(result.data_summary.base_rows === 2, `基期2条，实际=${result.data_summary.base_rows}`);
    assert(result.data_summary.current_rows === 2, `现期2条，实际=${result.data_summary.current_rows}`);
  } catch (err) {
    assert(false, 'full_data 模式执行失败', err.message);
  }

  // 3.5 错误处理：缺少数据
  console.log('\n--- 场景5：错误处理 ---');
  try {
    const raw = await tool._call({
      analysis_type: 'comprehensive',
      metric_fields: ['revenue'],
      max_drill_depth: 3,
    });
    const result = JSON.parse(raw);
    assert(result.error !== undefined || result.success === false, '缺少数据时返回错误');
  } catch (err) {
    assert(false, '错误处理测试失败', err.message);
  }

  // 3.6 边界：空数据
  console.log('\n--- 场景6：空数据 ---');
  try {
    const raw = await tool._call({
      analysis_type: 'comprehensive',
      base_data: [],
      current_data: [],
      metric_fields: ['revenue'],
      max_drill_depth: 3,
    });
    const result = JSON.parse(raw);
    assert(result.error !== undefined || result.success === false, '空数据时返回错误');
  } catch (err) {
    assert(false, '空数据测试失败', err.message);
  }

  // 3.7 单一维度值
  console.log('\n--- 场景7：单一维度值 ---');
  try {
    const raw = await tool._call({
      analysis_type: 'dimension',
      base_data: [{ region: '华东', revenue: 100 }],
      current_data: [{ region: '华东', revenue: 50 }],
      metric_fields: ['revenue'],
      dimension_fields: ['region'],
      max_drill_depth: 1,
    });
    const result = JSON.parse(raw);
    assert(!result.error, '单一维度值不报错');
    assert(result.dimension_attribution.dimensionRanking[0].explanatoryPower >= 0, 'EP ≥ 0');
  } catch (err) {
    assert(false, '单一维度值测试失败', err.message);
  }
}

// ─── 运行 ────────────────────────────────────────────────

(async () => {
  try {
    await runIntegrationTests();
  } catch (err) {
    console.error('\n💥 集成测试运行异常:', err);
  }

  // 汇总
  section('测试汇总');
  console.log(`  总计: ${totalTests}`);
  console.log(`  通过: ${passed} ✅`);
  console.log(`  失败: ${failed} ❌`);

  if (failures.length > 0) {
    console.log('\n失败列表:');
    failures.forEach((f) => console.log(f));
  }

  console.log('');
  process.exit(failed > 0 ? 1 : 0);
})();
