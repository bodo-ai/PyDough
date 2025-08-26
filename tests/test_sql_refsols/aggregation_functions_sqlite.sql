WITH _s1 AS (
  SELECT
    COUNT(*) AS n_rows,
    o_custkey
  FROM tpch.orders
  GROUP BY
    2
), _t2 AS (
  SELECT
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
    END AS expr_15,
    CASE
      WHEN CAST(0.19999999999999996 * COUNT(customer.c_acctbal) OVER (PARTITION BY customer.c_nationkey) AS INTEGER) < ROW_NUMBER() OVER (PARTITION BY customer.c_nationkey ORDER BY customer.c_acctbal DESC)
      THEN customer.c_acctbal
      ELSE NULL
    END AS expr_16,
    customer.c_acctbal,
    customer.c_nationkey,
    _s1.n_rows
  FROM tpch.customer AS customer
  LEFT JOIN _s1 AS _s1
    ON _s1.o_custkey = customer.c_custkey
), _t1 AS (
  SELECT
    MAX(c_acctbal) AS anything_c_acctbal,
    AVG(c_acctbal) AS avg_c_acctbal,
    AVG(expr_15) AS avg_expr_15,
    COUNT(c_acctbal) AS count_c_acctbal,
    MAX(c_acctbal) AS max_c_acctbal,
    MAX(expr_16) AS max_expr_16,
    MIN(c_acctbal) AS min_c_acctbal,
    COUNT(DISTINCT c_acctbal) AS ndistinct_c_acctbal,
    POWER(
      (
        CAST((
          SUM((
            POWER(c_acctbal, 2)
          )) - (
            CAST((
              POWER(SUM(c_acctbal), 2)
            ) AS REAL) / COUNT(c_acctbal)
          )
        ) AS REAL) / (
          COUNT(c_acctbal) - 1
        )
      ),
      0.5
    ) AS sample_std_c_acctbal,
    CAST((
      SUM((
        POWER(c_acctbal, 2)
      )) - (
        CAST((
          POWER(SUM(c_acctbal), 2)
        ) AS REAL) / COUNT(c_acctbal)
      )
    ) AS REAL) / (
      COUNT(c_acctbal) - 1
    ) AS sample_variance_c_acctbal,
    SUM(c_acctbal) AS sum_c_acctbal,
    SUM(n_rows) AS sum_n_rows
  FROM _t2
  GROUP BY
    c_nationkey
)
SELECT
  COALESCE(sum_c_acctbal, 0) AS sum_value,
  avg_c_acctbal AS avg_value,
  avg_expr_15 AS median_value,
  min_c_acctbal AS min_value,
  max_c_acctbal AS max_value,
  max_expr_16 AS quantile_value,
  anything_c_acctbal AS anything_value,
  count_c_acctbal AS count_value,
  ndistinct_c_acctbal AS count_distinct_value,
  sample_variance_c_acctbal AS variance_value,
  sample_std_c_acctbal AS stddev_value
FROM _t1
WHERE
  sum_n_rows = 0 OR sum_n_rows IS NULL
