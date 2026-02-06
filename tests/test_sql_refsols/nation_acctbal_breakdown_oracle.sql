WITH _T1 AS (
  SELECT
    CUSTOMER.c_acctbal AS C_ACCTBAL,
    CUSTOMER.c_nationkey AS C_NATIONKEY,
    NATION.n_name AS N_NAME,
    CASE
      WHEN ABS(
        (
          ROW_NUMBER() OVER (PARTITION BY CUSTOMER.c_nationkey ORDER BY CASE WHEN CUSTOMER.c_acctbal >= 0 THEN CUSTOMER.c_acctbal ELSE NULL END DESC NULLS LAST) - 1.0
        ) - (
          (
            COUNT(CASE WHEN CUSTOMER.c_acctbal >= 0 THEN CUSTOMER.c_acctbal ELSE NULL END) OVER (PARTITION BY CUSTOMER.c_nationkey) - 1.0
          ) / 2.0
        )
      ) < 1.0
      THEN CASE WHEN CUSTOMER.c_acctbal >= 0 THEN CUSTOMER.c_acctbal ELSE NULL END
      ELSE NULL
    END AS EXPR_5,
    CASE
      WHEN ABS(
        (
          ROW_NUMBER() OVER (PARTITION BY CUSTOMER.c_nationkey ORDER BY CUSTOMER.c_acctbal DESC NULLS LAST) - 1.0
        ) - (
          (
            COUNT(CUSTOMER.c_acctbal) OVER (PARTITION BY CUSTOMER.c_nationkey) - 1.0
          ) / 2.0
        )
      ) < 1.0
      THEN CUSTOMER.c_acctbal
      ELSE NULL
    END AS EXPR_6,
    CASE
      WHEN ABS(
        (
          ROW_NUMBER() OVER (PARTITION BY CUSTOMER.c_nationkey ORDER BY CASE WHEN CUSTOMER.c_acctbal < 0 THEN CUSTOMER.c_acctbal ELSE NULL END DESC NULLS LAST) - 1.0
        ) - (
          (
            COUNT(CASE WHEN CUSTOMER.c_acctbal < 0 THEN CUSTOMER.c_acctbal ELSE NULL END) OVER (PARTITION BY CUSTOMER.c_nationkey) - 1.0
          ) / 2.0
        )
      ) < 1.0
      THEN CASE WHEN CUSTOMER.c_acctbal < 0 THEN CUSTOMER.c_acctbal ELSE NULL END
      ELSE NULL
    END AS EXPR_7
  FROM TPCH.NATION NATION
  JOIN TPCH.REGION REGION
    ON NATION.n_regionkey = REGION.r_regionkey AND REGION.r_name = 'AMERICA'
  JOIN TPCH.CUSTOMER CUSTOMER
    ON CUSTOMER.c_nationkey = NATION.n_nationkey
)
SELECT
  ANY_VALUE(N_NAME) AS nation_name,
  COUNT(CASE WHEN C_ACCTBAL < 0 THEN C_ACCTBAL ELSE NULL END) AS n_red_acctbal,
  COUNT(CASE WHEN C_ACCTBAL >= 0 THEN C_ACCTBAL ELSE NULL END) AS n_black_acctbal,
  AVG(EXPR_7) AS median_red_acctbal,
  AVG(EXPR_5) AS median_black_acctbal,
  AVG(EXPR_6) AS median_overall_acctbal
FROM _T1
GROUP BY
  C_NATIONKEY
ORDER BY
  1 NULLS FIRST
