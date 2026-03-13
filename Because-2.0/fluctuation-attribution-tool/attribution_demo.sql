-- ================================================================
-- 波动归因测试数据库（SQLite）
--
-- 场景：某零售企业 2025-10 ~ 2026-03 的销售数据
-- 覆盖 5个地区 × 5个品类 × 3个渠道，共约 4500 条日粒度记录
--
-- 内嵌的异常场景（用于验证归因工具）：
--   1. 华东线上家电 2026-03 暴跌 40%（供应链中断）
--   2. 直播渠道 2026-02 起量爆发（新渠道红利）
--   3. 食品品类 6 个月持续上升趋势（品类增长）
--   4. 西南美妆 2026-01 新客激增（春节营销活动）
--   5. 全品类 2026-01 春节波峰 → 2026-02 回落
--   6. 华北线下服装 2026-03 突然萎缩（竞对开业）
-- ================================================================

PRAGMA journal_mode = WAL;

-- ================================
-- 维度表
-- ================================

DROP TABLE IF EXISTS fact_sales;
DROP TABLE IF EXISTS dim_region;
DROP TABLE IF EXISTS dim_product;
DROP TABLE IF EXISTS dim_channel;
DROP TABLE IF EXISTS dim_customer_type;
DROP TABLE IF EXISTS event_log;

CREATE TABLE dim_region (
    region_id   INTEGER PRIMARY KEY,
    region_name TEXT NOT NULL,
    tier        TEXT NOT NULL  -- 一线/二线/三线
);

INSERT INTO dim_region VALUES
(1, '华东', '一线'),
(2, '华北', '一线'),
(3, '华南', '一线'),
(4, '西南', '二线'),
(5, '西北', '三线');

CREATE TABLE dim_product (
    product_id       INTEGER PRIMARY KEY,
    product_category TEXT NOT NULL,
    avg_unit_cost    REAL NOT NULL  -- 平均单位成本
);

INSERT INTO dim_product VALUES
(1, '家电',  2800.00),
(2, '服装',   320.00),
(3, '食品',    85.00),
(4, '美妆',   260.00),
(5, '数码', 1500.00);

CREATE TABLE dim_channel (
    channel_id   INTEGER PRIMARY KEY,
    channel_name TEXT NOT NULL
);

INSERT INTO dim_channel VALUES
(1, '线上'),
(2, '线下'),
(3, '直播');

CREATE TABLE dim_customer_type (
    type_id   INTEGER PRIMARY KEY,
    type_name TEXT NOT NULL
);

INSERT INTO dim_customer_type VALUES
(1, '新客'),
(2, '老客');

-- ================================
-- 事件知识库（归因参考）
-- ================================

CREATE TABLE event_log (
    event_id    INTEGER PRIMARY KEY,
    event_date  TEXT NOT NULL,
    event_end   TEXT,
    region      TEXT,
    category    TEXT,
    channel     TEXT,
    event_type  TEXT NOT NULL,   -- supply_chain / marketing / competition / seasonal / policy
    description TEXT NOT NULL,
    impact      TEXT             -- positive / negative / neutral
);

INSERT INTO event_log VALUES
(1, '2026-03-01', '2026-03-15', '华东', '家电', '线上',
 'supply_chain', '华东仓库因暴雨导致物流中断，家电发货延迟7-10天', 'negative'),
(2, '2026-02-01', '2026-02-28', NULL, NULL, '直播',
 'marketing', '签约头部主播开启直播带货，直播渠道全品类推广', 'positive'),
(3, '2025-10-01', '2026-03-31', NULL, '食品', NULL,
 'marketing', '食品品类全年健康饮食营销战役持续投放', 'positive'),
(4, '2026-01-10', '2026-01-31', '西南', '美妆', NULL,
 'marketing', '西南区春节美妆营销活动「新年焕新颜」，拉新补贴力度大', 'positive'),
(5, '2026-01-01', '2026-02-05', NULL, NULL, NULL,
 'seasonal', '春节消费旺季，全品类销售额上升', 'positive'),
(6, '2026-03-05', NULL, '华北', '服装', '线下',
 'competition', '华北某商圈竞对品牌旗舰店开业，分流客户', 'negative');

-- ================================
-- 事实表
-- ================================

CREATE TABLE fact_sales (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    sale_date       TEXT    NOT NULL,  -- YYYY-MM-DD
    region_id       INTEGER NOT NULL,
    product_id      INTEGER NOT NULL,
    channel_id      INTEGER NOT NULL,
    customer_type_id INTEGER NOT NULL,
    orders          INTEGER NOT NULL,
    unit_price      REAL    NOT NULL,  -- 客单价
    revenue         REAL    NOT NULL,  -- 收入
    cost            REAL    NOT NULL,  -- 成本
    discount_rate   REAL    NOT NULL DEFAULT 0.0,  -- 折扣率
    return_orders   INTEGER NOT NULL DEFAULT 0,    -- 退货单数
    FOREIGN KEY (region_id)        REFERENCES dim_region(region_id),
    FOREIGN KEY (product_id)       REFERENCES dim_product(product_id),
    FOREIGN KEY (channel_id)       REFERENCES dim_channel(channel_id),
    FOREIGN KEY (customer_type_id) REFERENCES dim_customer_type(type_id)
);

-- ================================================================
-- 数据生成：用 recursive CTE 生成日期序列，再交叉维度填充
--
-- 基线逻辑：
--   base_orders 由地区权重 × 品类权重 × 渠道权重决定
--   叠加月份季节系数、趋势系数、异常系数
--   revenue = orders × unit_price
--   cost    = orders × avg_unit_cost × 0.65
-- ================================================================

WITH RECURSIVE
-- 日期序列：2025-10-01 ~ 2026-03-31 (183天)
dates(d) AS (
    SELECT '2025-10-01'
    UNION ALL
    SELECT date(d, '+1 day') FROM dates WHERE d < '2026-03-31'
),

-- 地区基线订单权重
region_weight(rid, rw) AS (
    VALUES (1, 1.0), (2, 0.85), (3, 0.90), (4, 0.55), (5, 0.35)
),

-- 品类基线订单权重 + 客单价
product_weight(pid, pw, base_price) AS (
    VALUES (1, 0.30, 3200.0), (2, 0.50, 380.0), (3, 0.70, 110.0), (4, 0.40, 320.0), (5, 0.25, 1800.0)
),

-- 渠道基线权重
channel_weight(cid, cw) AS (
    VALUES (1, 1.0), (2, 0.70), (3, 0.15)
),

-- 客户类型权重
ctype_weight(tid, tw) AS (
    VALUES (1, 0.40), (2, 0.60)
),

-- 月份季节系数（春节1月高峰，2月回落，3月恢复）
month_factor(m, mf) AS (
    VALUES ('10', 1.00), ('11', 1.05), ('12', 1.15),
           ('01', 1.35), ('02', 0.90), ('03', 1.05)
),

-- 组装所有维度组合 × 日期
combos AS (
    SELECT
        d.d                       AS sale_date,
        rw.rid                    AS region_id,
        pw.pid                    AS product_id,
        cw.cid                    AS channel_id,
        ct.tid                    AS customer_type_id,
        rw.rw                     AS region_w,
        pw.pw                     AS product_w,
        pw.base_price             AS base_price,
        cw.cw                     AS channel_w,
        ct.tw                     AS ctype_w,
        CAST(strftime('%m', d.d) AS TEXT) AS mon,
        strftime('%Y-%m', d.d)    AS ym,
        -- 用日期的 julianday 做伪随机种子
        ABS(CAST(
            (julianday(d.d) * 1000 + rw.rid * 137 + pw.pid * 251 + cw.cid * 397 + ct.tid * 53)
            % 1000 AS INTEGER
        )) / 1000.0               AS noise_seed
    FROM dates d, region_weight rw, product_weight pw, channel_weight cw, ctype_weight ct
),

-- 稀疏过滤：不是所有组合都有数据（模拟真实稀疏性）
-- 西北直播渠道在2026-02之前不存在；部分低权重组合按比例过滤
filtered AS (
    SELECT * FROM combos
    WHERE
        -- 西北直播2026-02前不存在
        NOT (region_id = 5 AND channel_id = 3 AND sale_date < '2026-02-01')
        -- 低权重组合随机稀疏（保留约 60%）
        AND (region_w * product_w * channel_w * ctype_w > 0.03 OR noise_seed > 0.4)
),

-- 计算各异常场景的调整系数
adjusted AS (
    SELECT
        f.*,
        -- 月份系数
        COALESCE((SELECT mf FROM month_factor WHERE m = CASE WHEN LENGTH(f.mon)=1 THEN '0'||f.mon ELSE f.mon END), 1.0) AS season_f,

        -- 异常1：华东线上家电 2026-03 暴跌
        CASE WHEN f.region_id = 1 AND f.product_id = 1 AND f.channel_id = 1 AND f.ym = '2026-03'
             THEN 0.55 ELSE 1.0 END AS anomaly_east_elec,

        -- 异常2：直播渠道 2026-02 起量
        CASE WHEN f.channel_id = 3 AND f.ym >= '2026-02'
             THEN 2.8 ELSE 1.0 END AS anomaly_live,

        -- 异常3：食品品类趋势（每月+5%）
        CASE WHEN f.product_id = 3
             THEN 1.0 + (julianday(f.sale_date) - julianday('2025-10-01')) / 183.0 * 0.30
             ELSE 1.0 END AS trend_food,

        -- 异常4：西南美妆 2026-01 新客激增
        CASE WHEN f.region_id = 4 AND f.product_id = 4 AND f.customer_type_id = 1 AND f.ym = '2026-01'
             THEN 2.5 ELSE 1.0 END AS anomaly_sw_beauty,

        -- 异常6：华北线下服装 2026-03 萎缩
        CASE WHEN f.region_id = 2 AND f.product_id = 2 AND f.channel_id = 2 AND f.ym = '2026-03'
             THEN 0.45 ELSE 1.0 END AS anomaly_north_cloth,

        -- 日内波动（±15%）
        (1.0 + (f.noise_seed - 0.5) * 0.30) AS daily_noise

    FROM filtered f
),

-- 最终计算
final AS (
    SELECT
        a.sale_date,
        a.region_id,
        a.product_id,
        a.channel_id,
        a.customer_type_id,

        -- 基础订单 = 50 × 地区权重 × 品类权重 × 渠道权重 × 客户类型权重
        -- × 季节系数 × 各异常系数 × 日波动
        MAX(1, CAST(
            50.0 * a.region_w * a.product_w * a.channel_w * a.ctype_w
            * a.season_f
            * a.anomaly_east_elec * a.anomaly_live * a.trend_food
            * a.anomaly_sw_beauty * a.anomaly_north_cloth
            * a.daily_noise
        AS INTEGER)) AS orders,

        -- 客单价 = 基础价格 × (0.92~1.08波动) × 折扣影响
        ROUND(a.base_price * (0.92 + a.noise_seed * 0.16)
            -- 家电2026-03降价促销
            * CASE WHEN a.product_id = 1 AND a.ym = '2026-03' THEN 0.88 ELSE 1.0 END
            -- 直播渠道低价策略
            * CASE WHEN a.channel_id = 3 THEN 0.82 ELSE 1.0 END
        , 2) AS unit_price,

        -- 折扣率
        ROUND(
            CASE
                WHEN a.channel_id = 3 THEN 0.15 + a.noise_seed * 0.10  -- 直播折扣大
                WHEN a.ym = '2026-01' THEN 0.08 + a.noise_seed * 0.05  -- 春节促销
                ELSE 0.02 + a.noise_seed * 0.05
            END, 3) AS discount_rate,

        -- 退货率
        MAX(0, CAST(
            50.0 * a.region_w * a.product_w * a.channel_w * a.ctype_w
            * a.season_f * a.daily_noise
            -- 直播退货率高
            * CASE WHEN a.channel_id = 3 THEN 0.12 ELSE 0.03 END
        AS INTEGER)) AS return_orders

    FROM adjusted a
)

INSERT INTO fact_sales (sale_date, region_id, product_id, channel_id, customer_type_id,
                        orders, unit_price, revenue, cost, discount_rate, return_orders)
SELECT
    sale_date, region_id, product_id, channel_id, customer_type_id,
    orders,
    unit_price,
    ROUND(orders * unit_price * (1.0 - discount_rate), 2) AS revenue,
    ROUND(orders * (SELECT avg_unit_cost FROM dim_product WHERE product_id = f.product_id) * 0.65, 2) AS cost,
    discount_rate,
    return_orders
FROM final f;

-- ================================
-- 分析视图
-- ================================

DROP VIEW IF EXISTS v_sales;
CREATE VIEW v_sales AS
SELECT
    f.id,
    f.sale_date,
    strftime('%Y-%m', f.sale_date) AS year_month,
    r.region_name                  AS region,
    r.tier                         AS region_tier,
    p.product_category             AS category,
    c.channel_name                 AS channel,
    ct.type_name                   AS customer_type,
    f.orders,
    f.unit_price,
    f.revenue,
    f.cost,
    ROUND(f.revenue - f.cost, 2)   AS profit,
    f.discount_rate,
    f.return_orders,
    CASE WHEN f.orders > 0
         THEN ROUND(f.return_orders * 1.0 / f.orders, 4)
         ELSE 0 END               AS return_rate
FROM fact_sales f
JOIN dim_region        r  ON f.region_id        = r.region_id
JOIN dim_product       p  ON f.product_id       = p.product_id
JOIN dim_channel       c  ON f.channel_id       = c.channel_id
JOIN dim_customer_type ct ON f.customer_type_id  = ct.type_id;

-- ================================
-- 常用归因分析 SQL 示例
-- ================================

-- 示例1：月度收入汇总（发现整体趋势）
-- SELECT year_month, SUM(revenue) AS total_revenue, SUM(orders) AS total_orders
-- FROM v_sales GROUP BY year_month ORDER BY year_month;

-- 示例2：2026-02 vs 2026-03 地区收入对比（维度归因基期/现期数据）
-- SELECT region, SUM(revenue) AS revenue, SUM(orders) AS orders
-- FROM v_sales WHERE year_month = '2026-02' GROUP BY region;
--
-- SELECT region, SUM(revenue) AS revenue, SUM(orders) AS orders
-- FROM v_sales WHERE year_month = '2026-03' GROUP BY region;

-- 示例3：多维下钻（地区×品类×渠道）
-- SELECT region, category, channel,
--        SUM(revenue) AS revenue, SUM(orders) AS orders, ROUND(AVG(unit_price),2) AS avg_price
-- FROM v_sales WHERE year_month = '2026-03'
-- GROUP BY region, category, channel
-- ORDER BY revenue DESC;

-- 示例4：新老客结构变化（JS散度高的场景）
-- SELECT year_month, customer_type,
--        SUM(revenue) AS revenue, SUM(orders) AS orders
-- FROM v_sales WHERE region = '西南' AND category = '美妆'
-- GROUP BY year_month, customer_type ORDER BY year_month;

-- 示例5：渠道结构变化（直播渠道起量）
-- SELECT year_month, channel,
--        SUM(revenue) AS revenue, SUM(orders) AS orders,
--        ROUND(SUM(return_orders)*1.0/SUM(orders),4) AS return_rate
-- FROM v_sales GROUP BY year_month, channel ORDER BY year_month, channel;

-- 示例6：查看事件知识库
-- SELECT * FROM event_log ORDER BY event_date;
