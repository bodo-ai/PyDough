WITH "_s0" AS (
  SELECT
    MAX("WHERE") AS MAX_WHERE
  FROM KEYWORDS."PARTITION"
), "_s1" AS (
  SELECT
    AVG("= ""QUOTE""") AS AVG_QUOTE,
    COUNT("`cast`") AS COUNT_CAST,
    MAX("`name""[") AS MAX_NAME,
    MIN("= ""QUOTE""") AS MIN_QUOTE,
    SUM("`name""[") AS SUM_NAME
  FROM KEYWORDS."""QUOTED TABLE_NAME"""
)
SELECT
  "_s0".MAX_WHERE AS max_where,
  "_s1".MIN_QUOTE AS min_quote,
  "_s1".MAX_NAME AS max_name,
  "_s1".COUNT_CAST AS count_cast,
  "_s1".AVG_QUOTE AS quote_avg,
  COALESCE("_s1".SUM_NAME, 0) AS sum_name
FROM "_s0" "_s0"
CROSS JOIN "_s1" "_s1"
