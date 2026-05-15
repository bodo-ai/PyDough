WITH _t1 AS (
  SELECT
    n_nationkey,
    n_regionkey
  FROM tpch.NATION
  WHERE
    n_regionkey = 2
)
SELECT
  COUNT(*) AS n
FROM tpch.SUPPLIER AS SUPPLIER
JOIN _t1 AS _t1
  ON SUPPLIER.s_nationkey = _t1.n_nationkey
JOIN _t1 AS _t2
  ON _t1.n_regionkey = _t2.n_regionkey
JOIN tpch.CUSTOMER AS CUSTOMER
  ON CUSTOMER.c_custkey = SUPPLIER.s_suppkey
  AND CUSTOMER.c_nationkey = _t2.n_nationkey
