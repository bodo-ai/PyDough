WITH _S1 AS (
  SELECT
    c_nationkey AS C_NATIONKEY,
    SUM(GREATEST(c_acctbal, 0)) AS EXPR_0,
    COUNT(GREATEST(c_acctbal, 0)) AS EXPR_1_0
  FROM TPCH.CUSTOMER
  GROUP BY
    c_nationkey
), _S3 AS (
  SELECT
    NATION.n_regionkey AS N_REGIONKEY,
    SUM(_S1.EXPR_0) AS SUM_EXPR,
    SUM(_S1.EXPR_1_0) AS SUM_EXPR_1
  FROM TPCH.NATION NATION
  JOIN _S1 _S1
    ON NATION.n_nationkey = _S1.C_NATIONKEY
  GROUP BY
    NATION.n_regionkey
)
SELECT
  REGION.r_name AS region_name,
  _S3.SUM_EXPR / _S3.SUM_EXPR_1 AS avg_bal_without_debt_erasure
FROM TPCH.REGION REGION
JOIN _S3 _S3
  ON REGION.r_regionkey = _S3.N_REGIONKEY
