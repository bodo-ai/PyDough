WITH _s1 AS (
  SELECT
    SUM(CASE WHEN c_acctbal >= 0 THEN c_acctbal WHEN c_acctbal <= 0 THEN 0 END) AS expr_0,
    COUNT(CASE WHEN c_acctbal >= 0 THEN c_acctbal WHEN c_acctbal <= 0 THEN 0 END) AS expr_1,
    c_nationkey AS nation_key
  FROM tpch.customer
  GROUP BY
    c_nationkey
), _t0 AS (
  SELECT
    SUM(_s1.expr_0) AS expr_0,
    SUM(_s1.expr_1) AS expr_1,
    nation.n_regionkey AS region_key
  FROM tpch.nation AS nation
  JOIN _s1 AS _s1
    ON _s1.nation_key = nation.n_nationkey
  GROUP BY
    nation.n_regionkey
)
SELECT
  region.r_name AS region_name,
  _t0.expr_0 / _t0.expr_1 AS avg_bal_without_debt_erasure
FROM tpch.region AS region
JOIN _t0 AS _t0
  ON _t0.region_key = region.r_regionkey
