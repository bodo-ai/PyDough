SELECT
  o_custkey AS c1,
  o_custkey AS c2,
  o_orderkey AS oid,
  SUM(o_totalprice) OVER (
    PARTITION BY o_custkey
    ORDER BY o_orderkey NULLS LAST
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
  ) AS running
FROM tpch.orders
WHERE
  o_custkey <= 2
ORDER BY
  1,
  3
