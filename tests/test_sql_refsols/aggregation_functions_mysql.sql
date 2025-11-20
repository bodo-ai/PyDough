WITH _s1 AS (
  SELECT
    o_custkey,
    COUNT(*) AS n_rows
  FROM tpch.ORDERS
  GROUP BY
    1
), _t2 AS (
  SELECT
    CUSTOMER.c_acctbal,
    CUSTOMER.c_nationkey,
    _s1.n_rows,
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
    END AS expr_17,
    CASE
      WHEN TRUNCATE(
        CAST(0.19999999999999996 * COUNT(CUSTOMER.c_acctbal) OVER (PARTITION BY CUSTOMER.c_nationkey) AS FLOAT),
        0
      ) < ROW_NUMBER() OVER (PARTITION BY CUSTOMER.c_nationkey ORDER BY CUSTOMER.c_acctbal DESC)
      THEN CUSTOMER.c_acctbal
      ELSE NULL
    END AS expr_18
  FROM tpch.CUSTOMER AS CUSTOMER
  LEFT JOIN _s1 AS _s1
    ON CUSTOMER.c_custkey = _s1.o_custkey
), _t1 AS (
  SELECT
    ANY_VALUE(c_acctbal) AS anything_cacctbal,
    AVG(c_acctbal) AS avg_cacctbal,
    AVG(expr_17) AS avg_expr17,
    COUNT(c_acctbal) AS count_cacctbal,
    MAX(c_acctbal) AS max_cacctbal,
    MAX(expr_18) AS max_expr18,
    MIN(c_acctbal) AS min_cacctbal,
    COUNT(DISTINCT c_acctbal) AS ndistinct_cacctbal,
    POWER(
      (
        (
          SUM((
            POWER(c_acctbal, 2)
          )) - (
            (
              POWER(SUM(c_acctbal), 2)
            ) / COUNT(c_acctbal)
          )
        ) / COUNT(c_acctbal)
      ),
      0.5
    ) AS population_std_cacctbal,
    (
      SUM((
        POWER(c_acctbal, 2)
      )) - (
        (
          POWER(SUM(c_acctbal), 2)
        ) / COUNT(c_acctbal)
      )
    ) / COUNT(c_acctbal) AS population_var_cacctbal,
    POWER(
      (
        (
          SUM((
            POWER(c_acctbal, 2)
          )) - (
            (
              POWER(SUM(c_acctbal), 2)
            ) / COUNT(c_acctbal)
          )
        ) / (
          COUNT(c_acctbal) - 1
        )
      ),
      0.5
    ) AS sample_std_cacctbal,
    (
      SUM((
        POWER(c_acctbal, 2)
      )) - (
        (
          POWER(SUM(c_acctbal), 2)
        ) / COUNT(c_acctbal)
      )
    ) / (
      COUNT(c_acctbal) - 1
    ) AS sample_var_cacctbal,
    SUM(c_acctbal) AS sum_cacctbal,
    SUM(n_rows) AS sum_nrows
  FROM _t2
  GROUP BY
    c_nationkey
)
SELECT
  COALESCE(sum_cacctbal, 0) AS sum_value,
  avg_cacctbal AS avg_value,
  avg_expr17 AS median_value,
  min_cacctbal AS min_value,
  max_cacctbal AS max_value,
  max_expr18 AS quantile_value,
  anything_cacctbal AS anything_value,
  count_cacctbal AS count_value,
  ndistinct_cacctbal AS count_distinct_value,
  sample_var_cacctbal AS variance_s_value,
  population_var_cacctbal AS variance_p_value,
  sample_std_cacctbal AS stddev_s_value,
  population_std_cacctbal AS stddev_p_value
FROM _t1
WHERE
  sum_nrows = 0 OR sum_nrows IS NULL
