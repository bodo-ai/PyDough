SELECT
  o_custkey AS c,
  o_orderkey AS oid,
  SUM(o_totalprice) OVER (
    PARTITION BY o_custkey
    ORDER BY o_orderkey
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
  ) AS running
FROM tpch.orders
WHERE
  o_custkey <= 2 AND o_totalprice > 100000
