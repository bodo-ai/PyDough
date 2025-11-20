WITH _t1 AS (
  SELECT
    CUSTOMER.c_acctbal,
    NATION.n_regionkey,
    CASE
      WHEN ABS(
        (
          ROW_NUMBER() OVER (PARTITION BY NATION.n_regionkey ORDER BY CASE WHEN CUSTOMER.c_acctbal >= 0 THEN CUSTOMER.c_acctbal ELSE NULL END DESC) - 1.0
        ) - (
          (
            COUNT(CASE WHEN CUSTOMER.c_acctbal >= 0 THEN CUSTOMER.c_acctbal ELSE NULL END) OVER (PARTITION BY NATION.n_regionkey) - 1.0
          ) / 2.0
        )
      ) < 1.0
      THEN CASE WHEN CUSTOMER.c_acctbal >= 0 THEN CUSTOMER.c_acctbal ELSE NULL END
      ELSE NULL
    END AS expr_5,
    CASE
      WHEN ABS(
        (
          ROW_NUMBER() OVER (PARTITION BY NATION.n_regionkey ORDER BY CUSTOMER.c_acctbal DESC) - 1.0
        ) - (
          (
            COUNT(CUSTOMER.c_acctbal) OVER (PARTITION BY NATION.n_regionkey) - 1.0
          ) / 2.0
        )
      ) < 1.0
      THEN CUSTOMER.c_acctbal
      ELSE NULL
    END AS expr_6,
    CASE
      WHEN ABS(
        (
          ROW_NUMBER() OVER (PARTITION BY NATION.n_regionkey ORDER BY CASE WHEN CUSTOMER.c_acctbal < 0 THEN CUSTOMER.c_acctbal ELSE NULL END DESC) - 1.0
        ) - (
          (
            COUNT(CASE WHEN CUSTOMER.c_acctbal < 0 THEN CUSTOMER.c_acctbal ELSE NULL END) OVER (PARTITION BY NATION.n_regionkey) - 1.0
          ) / 2.0
        )
      ) < 1.0
      THEN CASE WHEN CUSTOMER.c_acctbal < 0 THEN CUSTOMER.c_acctbal ELSE NULL END
      ELSE NULL
    END AS expr_7
  FROM tpch.NATION AS NATION
  JOIN tpch.CUSTOMER AS CUSTOMER
    ON CUSTOMER.c_nationkey = NATION.n_nationkey
), _s3 AS (
  SELECT
    n_regionkey,
    AVG(expr_5) AS avg_expr5,
    AVG(expr_6) AS avg_expr6,
    AVG(expr_7) AS avg_expr7,
    COUNT(CASE WHEN c_acctbal < 0 THEN c_acctbal ELSE NULL END) AS count_negativeacctbal,
    COUNT(CASE WHEN c_acctbal >= 0 THEN c_acctbal ELSE NULL END) AS count_nonnegativeacctbal
  FROM _t1
  GROUP BY
    1
)
SELECT
  REGION.r_name COLLATE utf8mb4_bin AS region_name,
  _s3.count_negativeacctbal AS n_red_acctbal,
  _s3.count_nonnegativeacctbal AS n_black_acctbal,
  _s3.avg_expr7 AS median_red_acctbal,
  _s3.avg_expr5 AS median_black_acctbal,
  _s3.avg_expr6 AS median_overall_acctbal
FROM tpch.REGION AS REGION
JOIN _s3 AS _s3
  ON REGION.r_regionkey = _s3.n_regionkey
ORDER BY
  1
