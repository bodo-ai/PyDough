WITH "_t0" AS (
  SELECT
    COUNT(*) AS N_ROWS
  FROM TPCH.PART
  WHERE
    P_NAME LIKE '%smoke%'
  GROUP BY
    P_BRAND
), "_s0" AS (
  SELECT
    MAX(N_ROWS) AS MAX_N_ROWS
  FROM "_t0"
), "_t3" AS (
  SELECT
    COUNT(*) AS N_ROWS
  FROM TPCH.PART
  WHERE
    P_NAME LIKE '%rose%'
  GROUP BY
    P_BRAND
), "_s1" AS (
  SELECT
    MAX(N_ROWS) AS MAX_N_ROWS
  FROM "_t3"
), "_t6" AS (
  SELECT
    COUNT(*) AS N_ROWS
  FROM TPCH.PART
  WHERE
    P_NAME LIKE '%turquoise%'
  GROUP BY
    P_BRAND
), "_s3" AS (
  SELECT
    MAX(N_ROWS) AS MAX_N_ROWS
  FROM "_t6"
), "_t9" AS (
  SELECT
    COUNT(*) AS N_ROWS
  FROM TPCH.PART
  WHERE
    P_NAME LIKE '%spring%'
  GROUP BY
    P_BRAND
), "_s5" AS (
  SELECT
    MAX(N_ROWS) AS MAX_N_ROWS
  FROM "_t9"
)
SELECT
  "_s0".MAX_N_ROWS AS max_smoke,
  "_s1".MAX_N_ROWS AS max_rose,
  "_s3".MAX_N_ROWS AS max_turquoise,
  "_s5".MAX_N_ROWS AS max_spring
FROM "_s0" "_s0"
CROSS JOIN "_s1" "_s1"
CROSS JOIN "_s3" "_s3"
CROSS JOIN "_s5" "_s5"
