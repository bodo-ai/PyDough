SELECT
  rng.column1 AS i,
  orders.o_orderkey AS oid,
  ROW_NUMBER() OVER (PARTITION BY rng.column1 ORDER BY orders.o_orderkey) AS r,
  SUM(orders.o_totalprice) OVER (PARTITION BY rng.column1) AS tot
FROM (VALUES
  (0),
  (1)) AS rng
JOIN tpch.orders AS orders
  ON orders.o_custkey <= 2
