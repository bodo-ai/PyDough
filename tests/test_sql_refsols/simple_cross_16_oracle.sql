WITH "_s0" AS (
  SELECT
    C_ACCTBAL
  FROM TPCH.CUSTOMER
), "_s1" AS (
  SELECT
    MIN(C_ACCTBAL) AS MIN_C_ACCTBAL
  FROM "_s0"
), "_s4" AS (
  SELECT
    COUNT(*) AS N_ROWS
  FROM "_s0" "_s0"
  JOIN "_s1" "_s1"
    ON "_s0".C_ACCTBAL <= (
      "_s1".MIN_C_ACCTBAL + 10.0
    )
), "_s2" AS (
  SELECT
    S_ACCTBAL
  FROM TPCH.SUPPLIER
), "_s3" AS (
  SELECT
    MAX(S_ACCTBAL) AS MAX_S_ACCTBAL
  FROM "_s2"
), "_s5" AS (
  SELECT
    COUNT(*) AS N_ROWS
  FROM "_s2" "_s2"
  JOIN "_s3" "_s3"
    ON "_s2".S_ACCTBAL >= (
      "_s3".MAX_S_ACCTBAL - 10.0
    )
)
SELECT
  "_s4".N_ROWS AS n1,
  "_s5".N_ROWS AS n2
FROM "_s4" "_s4"
CROSS JOIN "_s5" "_s5"
