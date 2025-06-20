WITH _s6 AS (
  SELECT
    MEDIAN(CASE WHEN _s2.c_acctbal >= 0 THEN _s2.c_acctbal ELSE NULL END) AS median_black_acctbal,
    MEDIAN(_s2.c_acctbal) AS median_overall_acctbal,
    MEDIAN(CASE WHEN _s2.c_acctbal < 0 THEN _s2.c_acctbal ELSE NULL END) AS median_red_acctbal,
    COUNT(CASE WHEN _s2.c_acctbal >= 0 THEN _s2.c_acctbal ELSE NULL END) AS n_black_acctbal,
    COUNT(CASE WHEN _s2.c_acctbal < 0 THEN _s2.c_acctbal ELSE NULL END) AS n_red_acctbal,
    _s1.n_regionkey AS region_key
  FROM tpch.nation AS _s1
  JOIN tpch.customer AS _s2
    ON _s1.n_nationkey = _s2.c_nationkey
  GROUP BY
    _s1.n_regionkey
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
