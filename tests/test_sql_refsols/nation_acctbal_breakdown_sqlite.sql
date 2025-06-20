WITH _t1 AS (
  SELECT
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
    c_nationkey AS nation_key,
    CASE WHEN c_acctbal < 0 THEN c_acctbal ELSE NULL END AS negative_acctbal,
    CASE WHEN c_acctbal >= 0 THEN c_acctbal ELSE NULL END AS non_negative_acctbal
  FROM tpch.customer
), _s6 AS (
  SELECT
    AVG(expr_5) AS median_black_acctbal,
    AVG(expr_6) AS median_overall_acctbal,
    AVG(expr_7) AS median_red_acctbal,
    COUNT(non_negative_acctbal) AS n_black_acctbal,
    COUNT(negative_acctbal) AS n_red_acctbal,
    nation_key
  FROM _t1
  GROUP BY
    nation_key
)
SELECT
  _s0.n_name AS nation_name,
  _s6.n_red_acctbal,
  _s6.n_black_acctbal,
  _s6.median_red_acctbal,
  _s6.median_black_acctbal,
  _s6.median_overall_acctbal
FROM tpch.nation AS _s0
JOIN tpch.region AS _s1
  ON _s0.n_regionkey = _s1.r_regionkey AND _s1.r_name = 'AMERICA'
JOIN _s6 AS _s6
  ON _s0.n_nationkey = _s6.nation_key
ORDER BY
  nation_name
