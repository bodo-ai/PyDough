WITH _t2 AS (
  SELECT
    customer.c_custkey,
    nation.n_regionkey
  FROM tpch.nation AS nation
  JOIN tpch.customer AS customer
    ON customer.c_nationkey = nation.n_nationkey
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY customer.c_nationkey ORDER BY customer.c_acctbal DESC NULLS FIRST, customer.c_name NULLS LAST) = 1
), _s3 AS (
  SELECT
    o_custkey,
    COUNT(*) AS n_rows
  FROM tpch.orders
  GROUP BY
    1
), _s5 AS (
  SELECT
    _t2.n_regionkey,
    SUM(_s3.n_rows) AS sum_n_rows
  FROM _t2 AS _t2
  JOIN _s3 AS _s3
    ON _s3.o_custkey = _t2.c_custkey
  GROUP BY
    1
)
SELECT
  region.r_name AS region_name,
  COALESCE(_s5.sum_n_rows, 0) AS n_orders
FROM tpch.region AS region
LEFT JOIN _s5 AS _s5
  ON _s5.n_regionkey = region.r_regionkey
ORDER BY
  1
