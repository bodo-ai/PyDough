WITH _s6 AS (
  SELECT
    MEDIAN(CASE WHEN c_acctbal >= 0 THEN c_acctbal ELSE NULL END) AS median_black_acctbal,
    MEDIAN(c_acctbal) AS median_overall_acctbal,
    MEDIAN(CASE WHEN c_acctbal < 0 THEN c_acctbal ELSE NULL END) AS median_red_acctbal,
    COUNT(CASE WHEN c_acctbal >= 0 THEN c_acctbal ELSE NULL END) AS n_black_acctbal,
    COUNT(CASE WHEN c_acctbal < 0 THEN c_acctbal ELSE NULL END) AS n_red_acctbal,
    c_nationkey AS nation_key
  FROM tpch.customer
  GROUP BY
    c_nationkey
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
