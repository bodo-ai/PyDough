WITH _t1 AS (
  SELECT
    n_nationkey,
    n_regionkey
  FROM tpch.nation
  WHERE
    n_regionkey = 2
)
SELECT
  COUNT(*) AS n
FROM tpch.supplier AS supplier
JOIN _t1 AS _t1
  ON _t1.n_nationkey = supplier.s_nationkey
JOIN _t1 AS _t2
  ON _t1.n_regionkey = _t2.n_regionkey
JOIN tpch.customer AS customer
  ON _t2.n_nationkey = customer.c_nationkey
  AND customer.c_custkey = supplier.s_suppkey
