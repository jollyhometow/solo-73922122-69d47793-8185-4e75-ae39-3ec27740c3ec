"""/docs 一键演示用的示例 SQL。

两条查询语义相同、写法不同，都只输出 text / bigint 列，可直接在
Swagger UI（/docs）的 `POST /jobs/demo` 上点击执行，看到一致的核对结论。
数据来自 sql/init.sql 中的 2026-11-15 示例读数。
"""

# 改写版：CTE + 窗口差值（首行差值取 0）+ 外连接聚合
DEMO_LEFT_SQL = """
WITH delta AS (
    SELECT r.meter_id,
           EXTRACT(HOUR FROM r.read_time)::bigint AS hr,
           (r.wh - LAG(r.wh, 1, r.wh)
                  OVER (PARTITION BY r.meter_id ORDER BY r.read_time))::bigint AS d
    FROM readings r
    WHERE r.read_time >= TIMESTAMPTZ '2026-11-15 00:00:00+00'
      AND r.read_time <  TIMESTAMPTZ '2026-11-16 00:00:00+00'
)
SELECT m.line_name,
       d.hr,
       SUM(d.d)::bigint AS wh_delta
FROM delta d
JOIN meters m ON m.meter_id = d.meter_id
GROUP BY m.line_name, d.hr
"""

# 基准版：子查询 + COALESCE + 内连接聚合（行序不参与比较，无需 ORDER BY）
DEMO_RIGHT_SQL = """
SELECT line_name, hr, SUM(d)::bigint AS wh_delta FROM (
    SELECT m.line_name AS line_name,
           EXTRACT(HOUR FROM r.read_time)::bigint AS hr,
           COALESCE(r.wh - LAG(r.wh) OVER w, 0)::bigint AS d
    FROM readings r
    JOIN meters m ON m.meter_id = r.meter_id
    WHERE r.read_time >= TIMESTAMPTZ '2026-11-15 00:00:00+00'
      AND r.read_time <  TIMESTAMPTZ '2026-11-16 00:00:00+00'
    WINDOW w AS (PARTITION BY r.meter_id ORDER BY r.read_time)
) baseline
GROUP BY line_name, hr
"""
