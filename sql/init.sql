-- ============================================================
-- 电表日结核对：源表结构 + 示例数据
-- 容器首次启动时由 /docker-entrypoint-initdb.d 自动执行
-- ============================================================

-- 表计档案：表号 -> 产线
CREATE TABLE IF NOT EXISTS meters (
    meter_id  bigint PRIMARY KEY,          -- 表号
    line_name text   NOT NULL              -- 产线
);

-- 读数流水：表号、时刻、累计电量 Wh
CREATE TABLE IF NOT EXISTS readings (
    meter_id   bigint NOT NULL REFERENCES meters(meter_id),
    read_time  timestamptz NOT NULL,       -- 时刻
    wh         bigint NOT NULL,            -- 累计电量（Wh）
    PRIMARY KEY (meter_id, read_time)
);

-- 日结常用过滤索引
CREATE INDEX IF NOT EXISTS idx_readings_time ON readings (read_time);

-- ---------- 示例档案：3 条产线、6 块表 ----------
INSERT INTO meters (meter_id, line_name) VALUES
    (1001, 'A线'),
    (1002, 'A线'),
    (2001, 'B线'),
    (2002, 'B线'),
    (3001, 'C线'),
    (3002, 'C线')
ON CONFLICT DO NOTHING;

-- ---------- 示例读数：2026-11-15 当日整点 ----------
INSERT INTO readings (meter_id, read_time, wh) VALUES
    (1001, TIMESTAMPTZ '2026-11-15 08:00:00+00', 1000),
    (1001, TIMESTAMPTZ '2026-11-15 09:00:00+00', 1150),
    (1001, TIMESTAMPTZ '2026-11-15 10:00:00+00', 1320),
    (1002, TIMESTAMPTZ '2026-11-15 08:00:00+00',  500),
    (1002, TIMESTAMPTZ '2026-11-15 09:00:00+00',  640),
    (2001, TIMESTAMPTZ '2026-11-15 08:00:00+00', 2000),
    (2001, TIMESTAMPTZ '2026-11-15 09:00:00+00', 2210),
    (2002, TIMESTAMPTZ '2026-11-15 08:00:00+00',  900),
    (3001, TIMESTAMPTZ '2026-11-15 08:00:00+00',  700),
    (3001, TIMESTAMPTZ '2026-11-15 09:00:00+00',  880),
    (3002, TIMESTAMPTZ '2026-11-15 08:00:00+00',  300),
    (3002, TIMESTAMPTZ '2026-11-15 09:00:00+00',  420)
ON CONFLICT DO NOTHING;

-- ============================================================
-- 示例核对查询（两条都只输出 bigint / text，列数、类型一致）
-- 场景：按“产线 + 小时”汇总当日每块表相对上一读数的增量电量（Wh）
--   left  —— 数仓工程师改写后的写法（CTE + 窗口差值 + 外连接聚合）
--   right —— 基准写法（子查询 + CASE 判空 + 内连接聚合）
-- 把这两条分别贴进 POST /jobs 的 left_sql / right_sql 即可。
-- 注意所有数值列都显式 ::bigint，文本列为 text，满足列类型约束。
-- ============================================================

-- LEFT（改写版）：
-- WITH delta AS (
--     SELECT r.meter_id,
--            EXTRACT(HOUR FROM r.read_time)::bigint AS hr,
--            (r.wh - LAG(r.wh, 1, r.wh)
--                   OVER (PARTITION BY r.meter_id ORDER BY r.read_time))::bigint AS d
--     FROM readings r
--     WHERE r.read_time >= TIMESTAMPTZ '2026-11-15 00:00:00+00'
--       AND r.read_time <  TIMESTAMPTZ '2026-11-16 00:00:00+00'
-- )
-- SELECT m.line_name,
--        d.hr,
--        SUM(d.d)::bigint AS wh_delta
-- FROM delta d
-- JOIN meters m ON m.meter_id = d.meter_id
-- GROUP BY m.line_name, d.hr
-- ORDER BY m.line_name, d.hr;

-- RIGHT（基准版，写法不同但结果应一致；ORDER BY 会被忽略，行序不参与比较）：
-- SELECT line_name, hr, SUM(d)::bigint AS wh_delta FROM (
--     SELECT m.line_name AS line_name,
--            EXTRACT(HOUR FROM r.read_time)::bigint AS hr,
--            COALESCE(r.wh - LAG(r.wh) OVER w, 0)::bigint AS d
--     FROM readings r
--     JOIN meters m ON m.meter_id = r.meter_id
--     WHERE r.read_time >= TIMESTAMPTZ '2026-11-15 00:00:00+00'
--       AND r.read_time <  TIMESTAMPTZ '2026-11-16 00:00:00+00'
--     WINDOW w AS (PARTITION BY r.meter_id ORDER BY r.read_time)
-- ) baseline
-- GROUP BY line_name, hr
-- ORDER BY line_name, hr;
