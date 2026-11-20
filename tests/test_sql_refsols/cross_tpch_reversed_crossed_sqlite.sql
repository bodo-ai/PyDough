WITH _s0 AS (
  SELECT
    o_custkey,
    o_orderkey,
    SUM(o_totalprice) OVER (
      PARTITION BY o_custkey
      ORDER BY o_orderkey
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS running
  FROM tpch.orders
  WHERE
    o_custkey <= 2
)
SELECT
  _s0.o_custkey AS c,
  _s0.o_orderkey AS oid,
  _s0.running
FROM _s0 AS _s0
JOIN tpch.customer AS customer
  ON _s0.o_custkey = customer.c_custkey AND customer.c_custkey <= 2
