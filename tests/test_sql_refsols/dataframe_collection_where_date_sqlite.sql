SELECT
  dates.column1 AS clerk_id,
  COUNT(*) AS n_orders
FROM (VALUES
  ('Clerk#000000456', '1996-01-01 00:00:00', '1996-02-01 00:00:00'),
  ('Clerk#000000743', '1995-06-01 00:00:00', '1995-07-01 00:00:00'),
  ('Clerk#000000547', '1995-11-01 00:00:00', '1995-12-01 00:00:00')) AS dates
JOIN tpch.orders AS orders
  ON dates.column1 = orders.o_clerk
  AND dates.column2 <= DATETIME(orders.o_orderdate)
  AND dates.column3 >= DATETIME(orders.o_orderdate)
GROUP BY
  1
