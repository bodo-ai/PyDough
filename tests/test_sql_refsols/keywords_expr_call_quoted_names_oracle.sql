WITH _S0 AS (
  SELECT
    MAX("WHERE") AS MAX_WHERE
  FROM KEYWORDS."PARTITION"
), _S1 AS (
  SELECT
    AVG("= ""QUOTE""") AS AVG_QUOTE,
    COUNT("`cast`") AS COUNT_CAST,
    MAX("`name""[") AS MAX_NAME,
    MIN("= ""QUOTE""") AS MIN_QUOTE,
    SUM("`name""[") AS SUM_NAME
  FROM KEYWORDS."""QUOTED TABLE_NAME"""
)
SELECT
  _S0.MAX_WHERE AS max_where,
  _S1.MIN_QUOTE AS min_quote,
  _S1.MAX_NAME AS max_name,
  _S1.COUNT_CAST AS count_cast,
  _S1.AVG_QUOTE AS quote_avg,
  NVL(_S1.SUM_NAME, 0) AS sum_name
FROM _S0 _S0
CROSS JOIN _S1 _S1
