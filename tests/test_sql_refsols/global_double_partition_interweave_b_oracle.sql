WITH "_S2" AS (
  SELECT
    AVG(c_acctbal) AS AVG_C_ACCTBAL
  FROM TPCH.CUSTOMER
), "_S0" AS (
  SELECT
    c_mktsegment AS C_MKTSEGMENT,
    c_nationkey AS C_NATIONKEY,
    COUNT(c_acctbal) AS COUNT_C_ACCTBAL,
    SUM(c_acctbal) AS SUM_C_ACCTBAL
  FROM TPCH.CUSTOMER
  GROUP BY
    c_mktsegment,
    c_nationkey
), "_S3" AS (
  SELECT
    "_S0".C_MKTSEGMENT,
    SUM("_S0".COUNT_C_ACCTBAL) AS SUM_COUNT_C_ACCTBAL,
    SUM("_S0".SUM_C_ACCTBAL) AS SUM_SUM_C_ACCTBAL
  FROM "_S0" "_S0"
  JOIN TPCH.NATION NATION
    ON NATION.n_nationkey = "_S0".C_NATIONKEY
  GROUP BY
    "_S0".C_MKTSEGMENT,
    NATION.n_name
)
SELECT
  "_S3".C_MKTSEGMENT AS market_segment,
  ROUND(
    MAX(
      (
        100.0 * (
          (
            "_S3".SUM_SUM_C_ACCTBAL / "_S3".SUM_COUNT_C_ACCTBAL
          ) - "_S2".AVG_C_ACCTBAL
        )
      ) / "_S2".AVG_C_ACCTBAL
    ),
    2
  ) AS max_nation_pct_diff
FROM "_S2" "_S2"
CROSS JOIN "_S3" "_S3"
GROUP BY
  "_S3".C_MKTSEGMENT
