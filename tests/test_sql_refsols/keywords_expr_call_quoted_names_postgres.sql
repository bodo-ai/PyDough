WITH _s0 AS (
  SELECT
    MAX("WHERE") AS max_where
  FROM keywords."PARTITION"
), _s1 AS (
  SELECT
    AVG(CAST("= ""QUOTE""" AS DECIMAL)) AS avg_quote,
    COUNT("`cast`") AS count_cast,
    MAX("`name""[") AS max_name,
    MIN("= ""QUOTE""") AS min_quote,
    SUM("`name""[") AS sum_name
  FROM keywords."""QUOTED TABLE_NAME"""
)
SELECT
  _s0.max_where,
  _s1.min_quote,
  _s1.max_name,
  _s1.count_cast,
  _s1.avg_quote AS quote_avg,
  COALESCE(_s1.sum_name, 0) AS sum_name
FROM _s0 AS _s0
CROSS JOIN _s1 AS _s1
