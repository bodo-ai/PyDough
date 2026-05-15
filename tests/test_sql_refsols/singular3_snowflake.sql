WITH _s0 AS (
  SELECT
    c_custkey,
    c_name
  FROM tpch.customer
  ORDER BY
    2 NULLS FIRST
  LIMIT 5
), _t1 AS (
  SELECT
    o_custkey,
    o_orderdate
  FROM tpch.orders
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY o_custkey ORDER BY o_totalprice DESC) = 1
)
SELECT
  _s0.c_name AS name
FROM _s0 AS _s0
LEFT JOIN _t1 AS _t1
  ON _s0.c_custkey = _t1.o_custkey
ORDER BY
  _t1.o_orderdate
