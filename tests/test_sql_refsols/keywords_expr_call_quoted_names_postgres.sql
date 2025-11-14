WITH _s0 AS (
  SELECT
    MAX("WHERE") AS "max_WHERE"
  FROM keywords."PARTITION"
), _s1 AS (
  SELECT
    AVG(CAST("= ""QUOTE""" AS DECIMAL)) AS "avg_= ""QUOTE""",
    COUNT("`cast`") AS "count_``cast``",
    MAX("`name""[") AS "max_`name""[",
    MIN("= ""QUOTE""") AS "min_= ""QUOTE""",
    SUM("`name""[") AS "sum_`name""["
  FROM keywords."""QUOTED TABLE_NAME"""
)
SELECT
  _s0."max_WHERE" AS max_where,
  _s1."min_= ""QUOTE""" AS min_quote,
  _s1."max_`name""[" AS max_name,
  _s1."count_``cast``" AS count_cast,
  _s1."avg_= ""QUOTE""" AS quote_avg,
  COALESCE(_s1."sum_`name""[", 0) AS sum_name
FROM _s0 AS _s0
CROSS JOIN _s1 AS _s1
