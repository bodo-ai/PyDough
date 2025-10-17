WITH _s3 AS (
  SELECT
    c_acctbal,
    c_nationkey,
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
    END AS expr_7
  FROM tpch.CUSTOMER
)
SELECT
  ANY_VALUE(NATION.n_name) COLLATE utf8mb4_bin AS nation_name,
  COUNT(CASE WHEN _s3.c_acctbal < 0 THEN _s3.c_acctbal ELSE NULL END) AS n_red_acctbal,
  COUNT(CASE WHEN _s3.c_acctbal >= 0 THEN _s3.c_acctbal ELSE NULL END) AS n_black_acctbal,
  AVG(_s3.expr_7) AS median_red_acctbal,
  AVG(_s3.expr_5) AS median_black_acctbal,
  AVG(_s3.expr_6) AS median_overall_acctbal
FROM tpch.NATION AS NATION
JOIN tpch.REGION AS REGION
  ON NATION.n_regionkey = REGION.r_regionkey AND REGION.r_name = 'AMERICA'
JOIN _s3 AS _s3
  ON NATION.n_nationkey = _s3.c_nationkey
GROUP BY
  _s3.c_nationkey
ORDER BY
  1
