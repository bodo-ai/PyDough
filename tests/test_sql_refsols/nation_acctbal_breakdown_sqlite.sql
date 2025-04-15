WITH _t3 AS (
  SELECT
    CASE
      WHEN ABS(
        (
          ROW_NUMBER() OVER (PARTITION BY c_nationkey ORDER BY c_acctbal DESC) - 1.0
        ) - (
          CAST((
            COUNT(c_acctbal) OVER (PARTITION BY c_nationkey) - 1.0
          ) AS REAL) / 2.0
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
          CAST((
            COUNT(CASE WHEN c_acctbal < 0 THEN c_acctbal ELSE NULL END) OVER (PARTITION BY c_nationkey) - 1.0
          ) AS REAL) / 2.0
        )
      ) < 1.0
      THEN CASE WHEN c_acctbal < 0 THEN c_acctbal ELSE NULL END
      ELSE NULL
    END AS expr_7,
    CASE
      WHEN ABS(
        (
          ROW_NUMBER() OVER (PARTITION BY c_nationkey ORDER BY CASE WHEN c_acctbal >= 0 THEN c_acctbal ELSE NULL END DESC) - 1.0
        ) - (
          CAST((
            COUNT(CASE WHEN c_acctbal >= 0 THEN c_acctbal ELSE NULL END) OVER (PARTITION BY c_nationkey) - 1.0
          ) AS REAL) / 2.0
        )
      ) < 1.0
      THEN CASE WHEN c_acctbal >= 0 THEN c_acctbal ELSE NULL END
      ELSE NULL
    END AS expr_5,
    c_nationkey AS nation_key,
    CASE WHEN c_acctbal < 0 THEN c_acctbal ELSE NULL END AS negative_acctbal,
    CASE WHEN c_acctbal >= 0 THEN c_acctbal ELSE NULL END AS non_negative_acctbal
  FROM tpch.customer
), _s3 AS (
  SELECT
    AVG(expr_5) AS agg_0,
    AVG(expr_6) AS agg_1,
    AVG(expr_7) AS agg_2,
    COUNT(negative_acctbal) AS agg_4,
    COUNT(non_negative_acctbal) AS agg_3,
    nation_key
  FROM _t3
  GROUP BY
    nation_key
)
SELECT
  nation.n_name AS nation_name,
  COALESCE(_s3.agg_4, 0) AS n_red_acctbal,
  COALESCE(_s3.agg_3, 0) AS n_black_acctbal,
  _s3.agg_2 AS median_red_acctbal,
  _s3.agg_0 AS median_black_acctbal,
  _s3.agg_1 AS median_overall_acctbal
FROM tpch.nation AS nation
JOIN tpch.region AS region
  ON nation.n_regionkey = region.r_regionkey AND region.r_name = 'AMERICA'
LEFT JOIN _s3 AS _s3
  ON _s3.nation_key = nation.n_nationkey
ORDER BY
  nation_name
