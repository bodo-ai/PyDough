WITH _t0 AS (
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
  o_custkey AS c,
  o_orderkey AS oid,
  running,
  LAG(o_orderkey, 1) OVER (PARTITION BY o_custkey ORDER BY o_orderkey) AS prev_oid
FROM _t0
