SELECT
  dates.clerk_id,
  COUNT(*) AS n_orders
FROM (VALUES
  (
    'Clerk#000000456',
    CAST('1996-01-01 00:00:00' AS TIMESTAMP),
    CAST('1996-02-01 00:00:00' AS TIMESTAMP)
  ),
  (
    'Clerk#000000743',
    CAST('1995-06-01 00:00:00' AS TIMESTAMP),
    CAST('1995-07-01 00:00:00' AS TIMESTAMP)
  ),
  (
    'Clerk#000000547',
    CAST('1995-11-01 00:00:00' AS TIMESTAMP),
    CAST('1995-12-01 00:00:00' AS TIMESTAMP)
  )) AS dates(clerk_id, start_date, end_date)
JOIN tpch.orders AS orders
  ON dates.clerk_id = orders.o_clerk
  AND dates.end_date >= CAST(orders.o_orderdate AS TIMESTAMP)
  AND dates.start_date <= CAST(orders.o_orderdate AS TIMESTAMP)
GROUP BY
  1
