WITH _s0 AS (
  SELECT
    MAX("where") AS "max_where"
  FROM keywords."partition"
), _s1 AS (
  SELECT
    AVG("= ""quote""") AS "avg_= ""quote""",
    COUNT("`cast`") AS "count_``cast``",
    MAX("`name""[") AS "max_`name""[",
    MIN("= ""quote""") AS "min_= ""quote""",
    SUM("`name""[") AS "sum_`name""["
  FROM keywords."""quoted table_name"""
)
SELECT
  _s0."max_where",
  _s1."min_= ""quote""" AS min_quote,
  _s1."max_`name""[" AS max_name,
  _s1."count_``cast``" AS count_cast,
  _s1."avg_= ""quote""" AS quote_avg,
  COALESCE(_s1."sum_`name""[", 0) AS sum_name
FROM _s0 AS _s0
CROSS JOIN _s1 AS _s1
