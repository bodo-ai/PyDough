WITH _t AS (
  SELECT
    CUSTOMER.c_custkey,
    NATION.n_regionkey,
    ROW_NUMBER() OVER (PARTITION BY CUSTOMER.c_nationkey ORDER BY CASE WHEN CUSTOMER.c_acctbal IS NULL THEN 1 ELSE 0 END DESC, CUSTOMER.c_acctbal DESC, CASE WHEN CUSTOMER.c_name COLLATE utf8mb4_bin IS NULL THEN 1 ELSE 0 END, CUSTOMER.c_name COLLATE utf8mb4_bin) AS _w
  FROM tpch.NATION AS NATION
  JOIN tpch.CUSTOMER AS CUSTOMER
    ON CUSTOMER.c_nationkey = NATION.n_nationkey
), _s3 AS (
  SELECT
    o_custkey,
    COUNT(*) AS n_rows
  FROM tpch.ORDERS
  GROUP BY
    1
), _s5 AS (
  SELECT
    _t.n_regionkey,
    SUM(_s3.n_rows) AS sum_n_rows
  FROM _t AS _t
  JOIN _s3 AS _s3
    ON _s3.o_custkey = _t.c_custkey
  WHERE
    _t._w = 1
  GROUP BY
    1
)
SELECT
  REGION.r_name COLLATE utf8mb4_bin AS region_name,
  COALESCE(_s5.sum_n_rows, 0) AS n_orders
FROM tpch.REGION AS REGION
LEFT JOIN _s5 AS _s5
  ON REGION.r_regionkey = _s5.n_regionkey
ORDER BY
  1
