WITH _s3 AS (
  SELECT
    c_nationkey,
    SUM(c_acctbal) AS sum_c_acctbal
  FROM tpch.customer
  GROUP BY
    1
), _s5 AS (
  SELECT
    nation.n_regionkey,
    SUM(_s3.sum_c_acctbal) AS sum_sum_c_acctbal
  FROM tpch.nation AS nation
  JOIN _s3 AS _s3
    ON _s3.c_nationkey = nation.n_nationkey
  GROUP BY
    1
)
SELECT
  nation.n_nationkey AS k,
  ROUND(COALESCE(_s5.sum_sum_c_acctbal, 0), 2) AS total
FROM tpch.nation AS nation
JOIN tpch.region AS region
  ON region.r_regionkey = 0
JOIN _s5 AS _s5
  ON _s5.n_regionkey = region.r_regionkey
WHERE
  nation.n_nationkey <= 1
ORDER BY
  1 NULLS FIRST
