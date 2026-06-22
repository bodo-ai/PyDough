WITH _s1 AS (
  SELECT
    c_nationkey,
    SUM(GREATEST(c_acctbal, 0)) AS expr_0,
    COUNT(GREATEST(c_acctbal, 0)) AS expr_1_0
  FROM tpch.customer
  GROUP BY
    1
), _s3 AS (
  SELECT
    nation.n_regionkey,
    SUM(_s1.expr_0) AS sum_expr,
    SUM(_s1.expr_1_0) AS sum_expr_1
  FROM tpch.nation AS nation
  JOIN _s1 AS _s1
    ON _s1.c_nationkey = nation.n_nationkey
  GROUP BY
    1
)
SELECT
  region.r_name AS region_name,
  _s3.sum_expr / _s3.sum_expr_1 AS avg_bal_without_debt_erasure
FROM tpch.region AS region
JOIN _s3 AS _s3
  ON _s3.n_regionkey = region.r_regionkey
