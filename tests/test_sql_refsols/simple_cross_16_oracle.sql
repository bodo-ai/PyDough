WITH "_S0" AS (
  SELECT
    c_acctbal AS C_ACCTBAL
  FROM TPCH.CUSTOMER
), "_S1" AS (
  SELECT
    MIN(C_ACCTBAL) AS MIN_C_ACCTBAL
  FROM "_S0"
), "_S4" AS (
  SELECT
    COUNT(*) AS N_ROWS
  FROM "_S0" "_S0"
  JOIN "_S1" "_S1"
    ON "_S0".C_ACCTBAL <= (
      "_S1".MIN_C_ACCTBAL + 10.0
    )
), "_S2" AS (
  SELECT
    s_acctbal AS S_ACCTBAL
  FROM TPCH.SUPPLIER
), "_S3" AS (
  SELECT
    MAX(S_ACCTBAL) AS MAX_S_ACCTBAL
  FROM "_S2"
), "_S5" AS (
  SELECT
    COUNT(*) AS N_ROWS
  FROM "_S2" "_S2"
  JOIN "_S3" "_S3"
    ON "_S2".S_ACCTBAL >= (
      "_S3".MAX_S_ACCTBAL - 10.0
    )
)
SELECT
  "_S4".N_ROWS AS n1,
  "_S5".N_ROWS AS n2
FROM "_S4" "_S4"
CROSS JOIN "_S5" "_S5"
