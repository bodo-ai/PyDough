WITH _s0 AS (
  SELECT
    c_custkey
  FROM tpch.customer
  ORDER BY
    c_acctbal DESC
  LIMIT 10
)
SELECT
  _s0.c_custkey AS ck,
  orders.o_orderkey AS ok,
  orders.o_totalprice AS tp
FROM _s0 AS _s0
JOIN tpch.orders AS orders
  ON _s0.c_custkey = orders.o_custkey
ORDER BY
  3 DESC
LIMIT 5
