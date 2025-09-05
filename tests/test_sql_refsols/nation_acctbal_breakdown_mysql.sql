WITH _t2 AS (
  SELECT
    CASE
      WHEN ABS(
        (
          ROW_NUMBER() OVER (PARTITION BY c_nationkey ORDER BY CASE WHEN c_acctbal >= 0 THEN c_acctbal ELSE NULL END DESC) - 1.0
        ) - (
          (
            COUNT(CASE WHEN c_acctbal >= 0 THEN c_acctbal ELSE NULL END) OVER (PARTITION BY c_nationkey) - 1.0
          ) / 2.0
        )
      ) < 1.0
      THEN CASE WHEN c_acctbal >= 0 THEN c_acctbal ELSE NULL END
      ELSE NULL
    END AS expr_5,
    CASE
      WHEN ABS(
        (
          ROW_NUMBER() OVER (PARTITION BY c_nationkey ORDER BY c_acctbal DESC) - 1.0
        ) - (
          (
            COUNT(c_acctbal) OVER (PARTITION BY c_nationkey) - 1.0
          ) / 2.0
        )
      ) < 1.0
      THEN c_acctbal
      ELSE NULL
    END AS expr_6,
    CASE
      WHEN ABS(
        (
          ROW_NUMBER() OVER (PARTITION BY c_nationkey ORDER BY CASE WHEN c_acctbal < 0 THEN c_acctbal ELSE NULL END DESC) - 1.0
        ) - (
          (
            COUNT(CASE WHEN c_acctbal < 0 THEN c_acctbal ELSE NULL END) OVER (PARTITION BY c_nationkey) - 1.0
          ) / 2.0
        )
      ) < 1.0
      THEN CASE WHEN c_acctbal < 0 THEN c_acctbal ELSE NULL END
      ELSE NULL
    END AS expr_7,
    c_acctbal,
    c_nationkey
  FROM tpch.CUSTOMER
), _s3 AS (
  SELECT
    AVG(expr_5) AS avg_expr_5,
    AVG(expr_6) AS avg_expr_6,
    AVG(expr_7) AS avg_expr_7,
    COUNT(CASE WHEN c_acctbal < 0 THEN c_acctbal ELSE NULL END) AS count_negative_acctbal,
    COUNT(CASE WHEN c_acctbal >= 0 THEN c_acctbal ELSE NULL END) AS count_non_negative_acctbal,
    c_nationkey
  FROM _t2
  GROUP BY
    6
)
SELECT
  NATION.n_name COLLATE utf8mb4_bin AS nation_name,
  _s3.count_negative_acctbal AS n_red_acctbal,
  _s3.count_non_negative_acctbal AS n_black_acctbal,
  _s3.avg_expr_7 AS median_red_acctbal,
  _s3.avg_expr_5 AS median_black_acctbal,
  _s3.avg_expr_6 AS median_overall_acctbal
FROM tpch.NATION AS NATION
JOIN tpch.REGION AS REGION
  ON NATION.n_regionkey = REGION.r_regionkey AND REGION.r_name = 'AMERICA'
JOIN _s3 AS _s3
  ON NATION.n_nationkey = _s3.c_nationkey
ORDER BY
  1
