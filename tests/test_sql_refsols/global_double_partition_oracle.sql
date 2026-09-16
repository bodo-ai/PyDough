WITH "_S0" AS (
  SELECT
    AVG(c_acctbal) AS AVG_C_ACCTBAL
  FROM TPCH.CUSTOMER
), "_S2" AS (
  SELECT
    CUSTOMER.c_mktsegment AS C_MKTSEGMENT,
    CUSTOMER.c_nationkey AS C_NATIONKEY,
    MAX(
      (
        100.0 * (
          CUSTOMER.c_acctbal - "_S0".AVG_C_ACCTBAL
        )
      ) / "_S0".AVG_C_ACCTBAL
    ) AS MAX_PCT_DIFF
  FROM "_S0" "_S0"
  CROSS JOIN TPCH.CUSTOMER CUSTOMER
  GROUP BY
    CUSTOMER.c_mktsegment,
    CUSTOMER.c_nationkey
), "_T1" AS (
  SELECT
    "_S2".C_MKTSEGMENT,
    MAX("_S2".MAX_PCT_DIFF) AS MAX_MAX_PCT_DIFF
  FROM "_S2" "_S2"
  JOIN TPCH.NATION NATION
    ON NATION.n_nationkey = "_S2".C_NATIONKEY
  GROUP BY
    "_S2".C_MKTSEGMENT,
    NATION.n_name
)
SELECT
  C_MKTSEGMENT AS market_segment,
  ROUND(AVG(MAX_MAX_PCT_DIFF), 2) AS avg_max_pct_diff
FROM "_T1"
GROUP BY
  C_MKTSEGMENT
