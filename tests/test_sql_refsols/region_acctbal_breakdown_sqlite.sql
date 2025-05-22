WITH _t1 AS (
  SELECT
    CASE
      WHEN ABS(
        (
          ROW_NUMBER() OVER (PARTITION BY nation.n_regionkey ORDER BY CASE WHEN customer.c_acctbal >= 0 THEN customer.c_acctbal ELSE NULL END DESC) - 1.0
        ) - (
          CAST((
            COUNT(CASE WHEN customer.c_acctbal >= 0 THEN customer.c_acctbal ELSE NULL END) OVER (PARTITION BY nation.n_regionkey) - 1.0
          ) AS REAL) / 2.0
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
          CAST((
            COUNT(customer.c_acctbal) OVER (PARTITION BY nation.n_regionkey) - 1.0
          ) AS REAL) / 2.0
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
          CAST((
            COUNT(CASE WHEN customer.c_acctbal < 0 THEN customer.c_acctbal ELSE NULL END) OVER (PARTITION BY nation.n_regionkey) - 1.0
          ) AS REAL) / 2.0
        )
      ) < 1.0
      THEN CASE WHEN customer.c_acctbal < 0 THEN customer.c_acctbal ELSE NULL END
      ELSE NULL
    END AS expr_7,
    CASE WHEN customer.c_acctbal < 0 THEN customer.c_acctbal ELSE NULL END AS negative_acctbal,
    CASE WHEN customer.c_acctbal >= 0 THEN customer.c_acctbal ELSE NULL END AS non_negative_acctbal,
    nation.n_regionkey AS region_key
  FROM tpch.nation AS nation
  JOIN tpch.customer AS customer
    ON customer.c_nationkey = nation.n_nationkey
), _s3 AS (
  SELECT
    AVG(expr_5) AS agg_0,
    AVG(expr_6) AS agg_1,
    AVG(expr_7) AS agg_2,
    COUNT(non_negative_acctbal) AS agg_3,
    COUNT(negative_acctbal) AS agg_4,
    region_key
  FROM _t1
  GROUP BY
    region_key
)
SELECT
  region.r_name AS region_name,
  COALESCE(_s3.agg_4, 0) AS n_red_acctbal,
  COALESCE(_s3.agg_3, 0) AS n_black_acctbal,
  _s3.agg_2 AS median_red_acctbal,
  _s3.agg_0 AS median_black_acctbal,
  _s3.agg_1 AS median_overall_acctbal
FROM tpch.region AS region
LEFT JOIN _s3 AS _s3
  ON _s3.region_key = region.r_regionkey
ORDER BY
  region_name
