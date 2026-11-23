WITH _s3 AS (
  SELECT
    c_nationkey,
    SUM(c_acctbal) AS sum_c_acctbal
  FROM tpch.CUSTOMER
  GROUP BY
    1
), _s5 AS (
  SELECT
    NATION.n_regionkey,
    SUM(_s3.sum_c_acctbal) AS sum_sum_c_acctbal
  FROM tpch.NATION AS NATION
  JOIN _s3 AS _s3
    ON NATION.n_nationkey = _s3.c_nationkey
  GROUP BY
    1
)
SELECT
  NATION.n_nationkey AS k,
  ROUND(COALESCE(_s5.sum_sum_c_acctbal, 0), 2) AS total
FROM tpch.NATION AS NATION
JOIN tpch.REGION AS REGION
  ON REGION.r_regionkey = 0
JOIN _s5 AS _s5
  ON REGION.r_regionkey = _s5.n_regionkey
WHERE
  NATION.n_nationkey <= 1
ORDER BY
  1
