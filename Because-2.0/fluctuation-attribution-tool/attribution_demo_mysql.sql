-- ================================================================
-- 波动归因测试数据库（MySQL 8.0+）
--
-- 场景：某零售企业 2025-10 ~ 2026-03 的销售数据
-- 覆盖 5个地区 × 5个品类 × 3个渠道 × 2个客户类型
-- 约 23000+ 条日粒度记录
--
-- 内嵌的异常场景（用于验证归因工具）：
--   1. 华东线上家电 2026-03 暴跌 40%（供应链中断）
--   2. 直播渠道 2026-02 起量爆发（新渠道红利）
--   3. 食品品类 6 个月持续上升趋势（品类增长）
--   4. 西南美妆 2026-01 新客激增（春节营销活动）
--   5. 全品类 2026-01 春节波峰 → 2026-02 回落
--   6. 华北线下服装 2026-03 突然萎缩（竞对开业）
--
-- 运行方式：mysql -u root -p < attribution_demo_mysql.sql
-- ================================================================

DROP DATABASE IF EXISTS attribution_demo;
CREATE DATABASE attribution_demo CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
USE attribution_demo;

-- ================================
-- 维度表
-- ================================

CREATE TABLE dim_region (
    region_id   INT PRIMARY KEY,
    region_name VARCHAR(20) NOT NULL,
    tier        VARCHAR(10) NOT NULL COMMENT '一线/二线/三线'
) ENGINE=InnoDB;

INSERT INTO dim_region VALUES
(1, '华东', '一线'),
(2, '华北', '一线'),
(3, '华南', '一线'),
(4, '西南', '二线'),
(5, '西北', '三线');

CREATE TABLE dim_product (
    product_id       INT PRIMARY KEY,
    product_category VARCHAR(20) NOT NULL,
    avg_unit_cost    DECIMAL(10,2) NOT NULL COMMENT '平均单位成本'
) ENGINE=InnoDB;

INSERT INTO dim_product VALUES
(1, '家电',  2800.00),
(2, '服装',   320.00),
(3, '食品',    85.00),
(4, '美妆',   260.00),
(5, '数码', 1500.00);

CREATE TABLE dim_channel (
    channel_id   INT PRIMARY KEY,
    channel_name VARCHAR(20) NOT NULL
) ENGINE=InnoDB;

INSERT INTO dim_channel VALUES
(1, '线上'),
(2, '线下'),
(3, '直播');

CREATE TABLE dim_customer_type (
    type_id   INT PRIMARY KEY,
    type_name VARCHAR(20) NOT NULL
) ENGINE=InnoDB;

INSERT INTO dim_customer_type VALUES
(1, '新客'),
(2, '老客');

-- ================================
-- 事件知识库（归因参考）
-- ================================

CREATE TABLE event_log (
    event_id    INT PRIMARY KEY,
    event_date  DATE NOT NULL,
    event_end   DATE DEFAULT NULL,
    region      VARCHAR(20) DEFAULT NULL,
    category    VARCHAR(20) DEFAULT NULL,
    channel     VARCHAR(20) DEFAULT NULL,
    event_type  VARCHAR(30) NOT NULL COMMENT 'supply_chain / marketing / competition / seasonal / policy',
    description TEXT NOT NULL,
    impact      VARCHAR(10) DEFAULT NULL COMMENT 'positive / negative / neutral'
) ENGINE=InnoDB;

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
    id               INT AUTO_INCREMENT PRIMARY KEY,
    sale_date        DATE          NOT NULL,
    region_id        INT           NOT NULL,
    product_id       INT           NOT NULL,
    channel_id       INT           NOT NULL,
    customer_type_id INT           NOT NULL,
    orders           INT           NOT NULL,
    unit_price       DECIMAL(10,2) NOT NULL COMMENT '客单价',
    revenue          DECIMAL(14,2) NOT NULL COMMENT '收入',
    cost             DECIMAL(14,2) NOT NULL COMMENT '成本',
    discount_rate    DECIMAL(5,3)  NOT NULL DEFAULT 0.000 COMMENT '折扣率',
    return_orders    INT           NOT NULL DEFAULT 0     COMMENT '退货单数',
    INDEX idx_date       (sale_date),
    INDEX idx_region     (region_id),
    INDEX idx_product    (product_id),
    INDEX idx_channel    (channel_id),
    INDEX idx_date_region(sale_date, region_id),
    FOREIGN KEY (region_id)        REFERENCES dim_region(region_id),
    FOREIGN KEY (product_id)       REFERENCES dim_product(product_id),
    FOREIGN KEY (channel_id)       REFERENCES dim_channel(channel_id),
    FOREIGN KEY (customer_type_id) REFERENCES dim_customer_type(type_id)
) ENGINE=InnoDB;

-- ================================================================
-- 数据生成
--
-- MySQL 8.0+ 支持 recursive CTE + 窗口函数
-- 用 CRC32 做确定性伪随机（保证每次执行结果一致）
--
-- 基线逻辑：
--   base_orders = 50 × 地区权重 × 品类权重 × 渠道权重 × 客户类型权重
--   × 季节系数 × 各异常系数 × 日波动
--   revenue = orders × unit_price × (1 - discount_rate)
--   cost    = orders × avg_unit_cost × 0.65
-- ================================================================

INSERT INTO fact_sales
    (sale_date, region_id, product_id, channel_id, customer_type_id,
     orders, unit_price, revenue, cost, discount_rate, return_orders)
WITH RECURSIVE
-- 日期序列：2025-10-01 ~ 2026-03-31
dates(d) AS (
    SELECT DATE '2025-10-01'
    UNION ALL
    SELECT d + INTERVAL 1 DAY FROM dates WHERE d < '2026-03-31'
),

-- 权重表
region_w(rid, rw) AS (
    SELECT 1, 1.00 UNION ALL SELECT 2, 0.85 UNION ALL SELECT 3, 0.90
    UNION ALL SELECT 4, 0.55 UNION ALL SELECT 5, 0.35
),
product_w(pid, pw, base_price) AS (
    SELECT 1, 0.30, 3200.0 UNION ALL SELECT 2, 0.50, 380.0 UNION ALL SELECT 3, 0.70, 110.0
    UNION ALL SELECT 4, 0.40, 320.0 UNION ALL SELECT 5, 0.25, 1800.0
),
channel_w(cid, cw) AS (
    SELECT 1, 1.00 UNION ALL SELECT 2, 0.70 UNION ALL SELECT 3, 0.15
),
ctype_w(tid, tw) AS (
    SELECT 1, 0.40 UNION ALL SELECT 2, 0.60
),

-- 所有维度组合 × 日期
combos AS (
    SELECT
        d.d                                              AS sale_date,
        rw.rid                                           AS region_id,
        pw.pid                                           AS product_id,
        cw.cid                                           AS channel_id,
        ct.tid                                           AS customer_type_id,
        rw.rw, pw.pw, pw.base_price, cw.cw, ct.tw,
        DATE_FORMAT(d.d, '%Y-%m')                        AS ym,
        MONTH(d.d)                                       AS mon,
        -- 确定性伪随机 [0,1)
        (CRC32(CONCAT(d.d, '-', rw.rid, '-', pw.pid, '-', cw.cid, '-', ct.tid)) % 10000) / 10000.0
                                                         AS noise_seed
    FROM dates d
    CROSS JOIN region_w rw
    CROSS JOIN product_w pw
    CROSS JOIN channel_w cw
    CROSS JOIN ctype_w ct
),

-- 稀疏过滤
filtered AS (
    SELECT * FROM combos
    WHERE
        NOT (region_id = 5 AND channel_id = 3 AND sale_date < '2026-02-01')
        AND (rw * pw * cw * tw > 0.03 OR noise_seed > 0.4)
),

-- 叠加季节 + 异常系数
adjusted AS (
    SELECT
        f.*,
        -- 季节系数
        CASE mon
            WHEN 10 THEN 1.00 WHEN 11 THEN 1.05 WHEN 12 THEN 1.15
            WHEN  1 THEN 1.35 WHEN  2 THEN 0.90 WHEN  3 THEN 1.05
            ELSE 1.0
        END AS season_f,

        -- 异常1：华东线上家电 2026-03 暴跌
        IF(region_id = 1 AND product_id = 1 AND channel_id = 1 AND ym = '2026-03', 0.55, 1.0)
            AS anom_east_elec,

        -- 异常2：直播渠道 2026-02 起量
        IF(channel_id = 3 AND ym >= '2026-02', 2.8, 1.0)
            AS anom_live,

        -- 异常3：食品持续增长趋势
        IF(product_id = 3,
           1.0 + DATEDIFF(sale_date, '2025-10-01') / 183.0 * 0.30,
           1.0)
            AS trend_food,

        -- 异常4：西南美妆 2026-01 新客激增
        IF(region_id = 4 AND product_id = 4 AND customer_type_id = 1 AND ym = '2026-01', 2.5, 1.0)
            AS anom_sw_beauty,

        -- 异常6：华北线下服装 2026-03 萎缩
        IF(region_id = 2 AND product_id = 2 AND channel_id = 2 AND ym = '2026-03', 0.45, 1.0)
            AS anom_north_cloth,

        -- 日内波动 ±15%
        (1.0 + (noise_seed - 0.5) * 0.30) AS daily_noise

    FROM filtered f
),

-- 最终计算
final_calc AS (
    SELECT
        a.sale_date,
        a.region_id,
        a.product_id,
        a.channel_id,
        a.customer_type_id,

        GREATEST(1, CAST(
            50.0 * a.rw * a.pw * a.cw * a.tw
            * a.season_f
            * a.anom_east_elec * a.anom_live * a.trend_food
            * a.anom_sw_beauty * a.anom_north_cloth
            * a.daily_noise
        AS UNSIGNED)) AS calc_orders,

        ROUND(a.base_price * (0.92 + a.noise_seed * 0.16)
            * IF(a.product_id = 1 AND a.ym = '2026-03', 0.88, 1.0)
            * IF(a.channel_id = 3, 0.82, 1.0)
        , 2) AS calc_unit_price,

        ROUND(
            CASE
                WHEN a.channel_id = 3 THEN 0.15 + a.noise_seed * 0.10
                WHEN a.ym = '2026-01' THEN 0.08 + a.noise_seed * 0.05
                ELSE 0.02 + a.noise_seed * 0.05
            END, 3) AS calc_discount_rate,

        GREATEST(0, CAST(
            50.0 * a.rw * a.pw * a.cw * a.tw
            * a.season_f * a.daily_noise
            * IF(a.channel_id = 3, 0.12, 0.03)
        AS UNSIGNED)) AS calc_return_orders,

        a.product_id AS pid_for_cost

    FROM adjusted a
)

SELECT
    sale_date,
    region_id,
    product_id,
    channel_id,
    customer_type_id,
    calc_orders                                                                         AS orders,
    calc_unit_price                                                                     AS unit_price,
    ROUND(calc_orders * calc_unit_price * (1.0 - calc_discount_rate), 2)                AS revenue,
    ROUND(calc_orders * (SELECT avg_unit_cost FROM dim_product dp WHERE dp.product_id = fc.pid_for_cost) * 0.65, 2)
                                                                                        AS cost,
    calc_discount_rate                                                                  AS discount_rate,
    calc_return_orders                                                                  AS return_orders
FROM final_calc fc;

-- ================================
-- 分析视图
-- ================================

DROP VIEW IF EXISTS v_sales;
CREATE VIEW v_sales AS
SELECT
    f.id,
    f.sale_date,
    DATE_FORMAT(f.sale_date, '%Y-%m')  AS `year_month`,
    r.region_name                      AS region,
    r.tier                             AS region_tier,
    p.product_category                 AS category,
    c.channel_name                     AS channel,
    ct.type_name                       AS customer_type,
    f.orders,
    f.unit_price,
    f.revenue,
    f.cost,
    ROUND(f.revenue - f.cost, 2)       AS profit,
    f.discount_rate,
    f.return_orders,
    IF(f.orders > 0,
       ROUND(f.return_orders / f.orders, 4),
       0)                              AS return_rate
FROM fact_sales f
JOIN dim_region        r  ON f.region_id        = r.region_id
JOIN dim_product       p  ON f.product_id       = p.product_id
JOIN dim_channel       c  ON f.channel_id       = c.channel_id
JOIN dim_customer_type ct ON f.customer_type_id  = ct.type_id;

-- ================================
-- 常用归因分析 SQL 示例
-- ================================

-- 示例1：月度收入汇总（发现整体趋势）
-- SELECT `year_month`, SUM(revenue) AS total_revenue, SUM(orders) AS total_orders
-- FROM v_sales GROUP BY `year_month` ORDER BY `year_month`;

-- 示例2：2026-02 vs 2026-03 地区收入对比（维度归因基期/现期数据）
-- SELECT region, SUM(revenue) AS revenue, SUM(orders) AS orders
-- FROM v_sales WHERE `year_month` = '2026-02' GROUP BY region;
--
-- SELECT region, SUM(revenue) AS revenue, SUM(orders) AS orders
-- FROM v_sales WHERE `year_month` = '2026-03' GROUP BY region;

-- 示例3：多维下钻（地区×品类×渠道）
-- SELECT region, category, channel,
--        SUM(revenue) AS revenue, SUM(orders) AS orders, ROUND(AVG(unit_price),2) AS avg_price
-- FROM v_sales WHERE `year_month` = '2026-03'
-- GROUP BY region, category, channel
-- ORDER BY revenue DESC;

-- 示例4：新老客结构变化（JS散度高的场景）
-- SELECT `year_month`, customer_type,
--        SUM(revenue) AS revenue, SUM(orders) AS orders
-- FROM v_sales WHERE region = '西南' AND category = '美妆'
-- GROUP BY `year_month`, customer_type ORDER BY `year_month`;

-- 示例5：渠道结构变化（直播渠道起量）
-- SELECT `year_month`, channel,
--        SUM(revenue) AS revenue, SUM(orders) AS orders,
--        ROUND(SUM(return_orders)/SUM(orders),4) AS return_rate
-- FROM v_sales GROUP BY `year_month`, channel ORDER BY `year_month`, channel;

-- 示例6：查看事件知识库
-- SELECT * FROM event_log ORDER BY event_date;

-- ================================================================
-- 数据校验（执行后可删除）
-- ================================================================
-- SELECT '总行数' AS metric, COUNT(*) AS value FROM fact_sales
-- UNION ALL
-- SELECT '日期范围', CONCAT(MIN(sale_date), ' ~ ', MAX(sale_date)) FROM fact_sales
-- UNION ALL
-- SELECT '地区数', COUNT(DISTINCT region_id) FROM fact_sales
-- UNION ALL
-- SELECT '品类数', COUNT(DISTINCT product_id) FROM fact_sales
-- UNION ALL
-- SELECT '渠道数', COUNT(DISTINCT channel_id) FROM fact_sales;
