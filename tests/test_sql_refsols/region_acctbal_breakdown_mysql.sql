WITH _t1 AS (
  SELECT
    CASE
      WHEN ABS(
        (
          ROW_NUMBER() OVER (PARTITION BY nation.n_regionkey ORDER BY CASE WHEN customer.c_acctbal >= 0 THEN customer.c_acctbal ELSE NULL END DESC) - 1.0
        ) - (
          (
            COUNT(CASE WHEN customer.c_acctbal >= 0 THEN customer.c_acctbal ELSE NULL END) OVER (PARTITION BY nation.n_regionkey) - 1.0
          ) / 2.0
        )
      ) < 1.0
      THEN CASE WHEN customer.c_acctbal >= 0 THEN customer.c_acctbal ELSE NULL END
      ELSE NULL
    END AS expr_5,
    CASE
      WHEN ABS(
        (
          ROW_NUMBER() OVER (PARTITION BY nation.n_regionkey ORDER BY customer.c_acctbal DESC) - 1.0
        ) - (
          (
            COUNT(customer.c_acctbal) OVER (PARTITION BY nation.n_regionkey) - 1.0
          ) / 2.0
        )
      ) < 1.0
      THEN customer.c_acctbal
      ELSE NULL
    END AS expr_6,
    CASE
      WHEN ABS(
        (
          ROW_NUMBER() OVER (PARTITION BY nation.n_regionkey ORDER BY CASE WHEN customer.c_acctbal < 0 THEN customer.c_acctbal ELSE NULL END DESC) - 1.0
        ) - (
          (
            COUNT(CASE WHEN customer.c_acctbal < 0 THEN customer.c_acctbal ELSE NULL END) OVER (PARTITION BY nation.n_regionkey) - 1.0
          ) / 2.0
        )
      ) < 1.0
      THEN CASE WHEN customer.c_acctbal < 0 THEN customer.c_acctbal ELSE NULL END
      ELSE NULL
    END AS expr_7,
    customer.c_acctbal,
    nation.n_regionkey
  FROM tpch.nation AS nation
  JOIN tpch.customer AS customer
    ON customer.c_nationkey = nation.n_nationkey
), _s3 AS (
  SELECT
    AVG(expr_5) AS median_black_acctbal,
    AVG(expr_6) AS median_overall_acctbal,
    AVG(expr_7) AS median_red_acctbal,
    COUNT(CASE WHEN c_acctbal >= 0 THEN c_acctbal ELSE NULL END) AS n_black_acctbal,
    COUNT(CASE WHEN c_acctbal < 0 THEN c_acctbal ELSE NULL END) AS n_red_acctbal,
    n_regionkey
  FROM _t1
  GROUP BY
    6
)
SELECT
  region.r_name COLLATE utf8mb4_bin AS region_name,
  _s3.n_red_acctbal,
  _s3.n_black_acctbal,
  _s3.median_red_acctbal,
  _s3.median_black_acctbal,
  _s3.median_overall_acctbal
FROM tpch.region AS region
JOIN _s3 AS _s3
  ON _s3.n_regionkey = region.r_regionkey
ORDER BY
  1
