WITH _t1 AS (
  SELECT
    CUSTOMER.c_acctbal,
    CUSTOMER.c_nationkey,
    NATION.n_name,
    CASE
      WHEN ABS(
        (
          ROW_NUMBER() OVER (PARTITION BY CUSTOMER.c_nationkey ORDER BY CASE WHEN CUSTOMER.c_acctbal >= 0 THEN CUSTOMER.c_acctbal ELSE NULL END DESC) - 1.0
        ) - (
          (
            COUNT(CASE WHEN CUSTOMER.c_acctbal >= 0 THEN CUSTOMER.c_acctbal ELSE NULL END) OVER (PARTITION BY CUSTOMER.c_nationkey) - 1.0
          ) / 2.0
        )
      ) < 1.0
      THEN CASE WHEN CUSTOMER.c_acctbal >= 0 THEN CUSTOMER.c_acctbal ELSE NULL END
      ELSE NULL
    END AS expr_5,
    CASE
      WHEN ABS(
        (
          ROW_NUMBER() OVER (PARTITION BY CUSTOMER.c_nationkey ORDER BY CUSTOMER.c_acctbal DESC) - 1.0
        ) - (
          (
            COUNT(CUSTOMER.c_acctbal) OVER (PARTITION BY CUSTOMER.c_nationkey) - 1.0
          ) / 2.0
        )
      ) < 1.0
      THEN CUSTOMER.c_acctbal
      ELSE NULL
    END AS expr_6,
    CASE
      WHEN ABS(
        (
          ROW_NUMBER() OVER (PARTITION BY CUSTOMER.c_nationkey ORDER BY CASE WHEN CUSTOMER.c_acctbal < 0 THEN CUSTOMER.c_acctbal ELSE NULL END DESC) - 1.0
        ) - (
          (
            COUNT(CASE WHEN CUSTOMER.c_acctbal < 0 THEN CUSTOMER.c_acctbal ELSE NULL END) OVER (PARTITION BY CUSTOMER.c_nationkey) - 1.0
          ) / 2.0
        )
      ) < 1.0
      THEN CASE WHEN CUSTOMER.c_acctbal < 0 THEN CUSTOMER.c_acctbal ELSE NULL END
      ELSE NULL
    END AS expr_7
  FROM tpch.NATION AS NATION
  JOIN tpch.REGION AS REGION
    ON NATION.n_regionkey = REGION.r_regionkey AND REGION.r_name = 'AMERICA'
  JOIN tpch.CUSTOMER AS CUSTOMER
    ON CUSTOMER.c_nationkey = NATION.n_nationkey
)
SELECT
  ANY_VALUE(n_name) COLLATE utf8mb4_bin AS nation_name,
  COUNT(CASE WHEN c_acctbal < 0 THEN c_acctbal ELSE NULL END) AS n_red_acctbal,
  COUNT(CASE WHEN c_acctbal >= 0 THEN c_acctbal ELSE NULL END) AS n_black_acctbal,
  AVG(expr_7) AS median_red_acctbal,
  AVG(expr_5) AS median_black_acctbal,
  AVG(expr_6) AS median_overall_acctbal
FROM _t1
GROUP BY
  c_nationkey
ORDER BY
  1
