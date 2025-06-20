WITH _t0 AS (
  SELECT
    CASE
      WHEN ABS(
        (
          ROW_NUMBER() OVER (PARTITION BY _s1.n_regionkey ORDER BY CASE WHEN _s2.c_acctbal >= 0 THEN _s2.c_acctbal ELSE NULL END DESC) - 1.0
        ) - (
          CAST((
            COUNT(CASE WHEN _s2.c_acctbal >= 0 THEN _s2.c_acctbal ELSE NULL END) OVER (PARTITION BY _s1.n_regionkey) - 1.0
          ) AS REAL) / 2.0
        )
      ) < 1.0
      THEN CASE WHEN _s2.c_acctbal >= 0 THEN _s2.c_acctbal ELSE NULL END
      ELSE NULL
    END AS expr_5,
    CASE
      WHEN ABS(
        (
          ROW_NUMBER() OVER (PARTITION BY _s1.n_regionkey ORDER BY _s2.c_acctbal DESC) - 1.0
        ) - (
          CAST((
            COUNT(_s2.c_acctbal) OVER (PARTITION BY _s1.n_regionkey) - 1.0
          ) AS REAL) / 2.0
        )
      ) < 1.0
      THEN _s2.c_acctbal
      ELSE NULL
    END AS expr_6,
    CASE
      WHEN ABS(
        (
          ROW_NUMBER() OVER (PARTITION BY _s1.n_regionkey ORDER BY CASE WHEN _s2.c_acctbal < 0 THEN _s2.c_acctbal ELSE NULL END DESC) - 1.0
        ) - (
          CAST((
            COUNT(CASE WHEN _s2.c_acctbal < 0 THEN _s2.c_acctbal ELSE NULL END) OVER (PARTITION BY _s1.n_regionkey) - 1.0
          ) AS REAL) / 2.0
        )
      ) < 1.0
      THEN CASE WHEN _s2.c_acctbal < 0 THEN _s2.c_acctbal ELSE NULL END
      ELSE NULL
    END AS expr_7,
    CASE WHEN _s2.c_acctbal < 0 THEN _s2.c_acctbal ELSE NULL END AS negative_acctbal,
    CASE WHEN _s2.c_acctbal >= 0 THEN _s2.c_acctbal ELSE NULL END AS non_negative_acctbal,
    _s1.n_regionkey AS region_key
  FROM tpch.nation AS _s1
  JOIN tpch.customer AS _s2
    ON _s1.n_nationkey = _s2.c_nationkey
), _s6 AS (
  SELECT
    AVG(expr_5) AS median_black_acctbal,
    AVG(expr_6) AS median_overall_acctbal,
    AVG(expr_7) AS median_red_acctbal,
    COUNT(non_negative_acctbal) AS n_black_acctbal,
    COUNT(negative_acctbal) AS n_red_acctbal,
    region_key
  FROM _t0
  GROUP BY
    region_key
)
SELECT
  _s0.r_name AS region_name,
  _s6.n_red_acctbal,
  _s6.n_black_acctbal,
  _s6.median_red_acctbal,
  _s6.median_black_acctbal,
  _s6.median_overall_acctbal
FROM tpch.region AS _s0
JOIN _s6 AS _s6
  ON _s0.r_regionkey = _s6.region_key
ORDER BY
  region_name
