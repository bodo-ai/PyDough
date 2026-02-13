WITH _t1 AS (
  SELECT
    customer.c_acctbal,
    customer.c_nationkey,
    nation.n_name,
    CASE
      WHEN ABS(
        (
          ROW_NUMBER() OVER (PARTITION BY customer.c_nationkey ORDER BY CASE WHEN customer.c_acctbal >= 0 THEN customer.c_acctbal ELSE NULL END DESC) - 1.0
        ) - (
          CAST((
            COUNT(CASE WHEN customer.c_acctbal >= 0 THEN customer.c_acctbal ELSE NULL END) OVER (PARTITION BY customer.c_nationkey) - 1.0
          ) AS REAL) / 2.0
        )
      ) < 1.0
      THEN CASE WHEN customer.c_acctbal >= 0 THEN customer.c_acctbal ELSE NULL END
      ELSE NULL
    END AS expr_5,
    CASE
      WHEN ABS(
        (
          ROW_NUMBER() OVER (PARTITION BY customer.c_nationkey ORDER BY customer.c_acctbal DESC) - 1.0
        ) - (
          CAST((
            COUNT(customer.c_acctbal) OVER (PARTITION BY customer.c_nationkey) - 1.0
          ) AS REAL) / 2.0
        )
      ) < 1.0
      THEN customer.c_acctbal
      ELSE NULL
    END AS expr_6,
    CASE
      WHEN ABS(
        (
          ROW_NUMBER() OVER (PARTITION BY customer.c_nationkey ORDER BY CASE WHEN customer.c_acctbal < 0 THEN customer.c_acctbal ELSE NULL END DESC) - 1.0
        ) - (
          CAST((
            COUNT(CASE WHEN customer.c_acctbal < 0 THEN customer.c_acctbal ELSE NULL END) OVER (PARTITION BY customer.c_nationkey) - 1.0
          ) AS REAL) / 2.0
        )
      ) < 1.0
      THEN CASE WHEN customer.c_acctbal < 0 THEN customer.c_acctbal ELSE NULL END
      ELSE NULL
    END AS expr_7
  FROM tpch.nation AS nation
  JOIN tpch.region AS region
    ON nation.n_regionkey = region.r_regionkey AND region.r_name = 'AMERICA'
  JOIN tpch.customer AS customer
    ON customer.c_nationkey = nation.n_nationkey
)
SELECT
  MAX(n_name) AS nation_name,
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
