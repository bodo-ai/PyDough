WITH _t AS (
  SELECT
    o_custkey,
    o_orderkey,
    ROW_NUMBER() OVER (PARTITION BY o_custkey ORDER BY o_totalprice DESC, o_orderkey) AS _w
  FROM tpch.orders
), _s1 AS (
  SELECT
    o_custkey,
    o_orderkey
  FROM _t
  WHERE
    _w = 1
)
SELECT
  customer.c_custkey AS key,
  _s1.o_orderkey AS best_order
FROM tpch.customer AS customer
LEFT JOIN _s1 AS _s1
  ON _s1.o_custkey = customer.c_custkey
WHERE
  customer.c_custkey <= 3
ORDER BY
  1 NULLS FIRST
