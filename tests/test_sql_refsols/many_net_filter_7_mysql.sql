WITH _s1 AS (
  SELECT
    n_nationkey,
    n_regionkey
  FROM tpch.NATION
)
SELECT
  COUNT(*) AS n
FROM tpch.SUPPLIER AS SUPPLIER
JOIN _s1 AS _s1
  ON SUPPLIER.s_nationkey = _s1.n_nationkey
JOIN _s1 AS _s3
  ON _s1.n_regionkey = _s3.n_regionkey
JOIN tpch.CUSTOMER AS CUSTOMER
  ON CUSTOMER.c_custkey = SUPPLIER.s_suppkey
  AND CUSTOMER.c_nationkey = 7
  AND CUSTOMER.c_nationkey = _s3.n_nationkey
