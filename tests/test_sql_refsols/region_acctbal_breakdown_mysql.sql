WITH _t1 AS (
  SELECT
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
    END AS expr_7,
    NATION.n_regionkey,
    CASE WHEN CUSTOMER.c_acctbal < 0 THEN CUSTOMER.c_acctbal ELSE NULL END AS negative_acctbal,
    CASE WHEN CUSTOMER.c_acctbal >= 0 THEN CUSTOMER.c_acctbal ELSE NULL END AS non_negative_acctbal
  FROM tpch.NATION AS NATION
  JOIN tpch.CUSTOMER AS CUSTOMER
    ON CUSTOMER.c_nationkey = NATION.n_nationkey
), _s3 AS (
  SELECT
    AVG(expr_5) AS median_black_acctbal,
    AVG(expr_6) AS median_overall_acctbal,
    AVG(expr_7) AS median_red_acctbal,
    COUNT(non_negative_acctbal) AS n_black_acctbal,
    COUNT(negative_acctbal) AS n_red_acctbal,
    n_regionkey
  FROM _t1
  GROUP BY
    n_regionkey
)
SELECT
  REGION.r_name AS region_name,
  _s3.n_red_acctbal,
  _s3.n_black_acctbal,
  _s3.median_red_acctbal,
  _s3.median_black_acctbal,
  _s3.median_overall_acctbal
FROM tpch.REGION AS REGION
JOIN _s3 AS _s3
  ON REGION.r_regionkey = _s3.n_regionkey
ORDER BY
  REGION.r_name
