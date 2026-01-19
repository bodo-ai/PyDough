SELECT
  dates.clerk_id,
  COUNT(*) AS n_orders
FROM (VALUES
  ROW(
    'Clerk#000000456',
    CAST('1996-01-01 00:00:00' AS DATETIME),
    CAST('1996-02-01 00:00:00' AS DATETIME)
  ),
  ROW(
    'Clerk#000000743',
    CAST('1995-06-01 00:00:00' AS DATETIME),
    CAST('1995-07-01 00:00:00' AS DATETIME)
  ),
  ROW(
    'Clerk#000000547',
    CAST('1995-11-01 00:00:00' AS DATETIME),
    CAST('1995-12-01 00:00:00' AS DATETIME)
  )) AS dates(clerk_id, start_date, end_date)
JOIN tpch.ORDERS AS ORDERS
  ON ORDERS.o_clerk = dates.clerk_id
  AND dates.end_date >= CAST(ORDERS.o_orderdate AS DATETIME)
  AND dates.start_date <= CAST(ORDERS.o_orderdate AS DATETIME)
GROUP BY
  1
