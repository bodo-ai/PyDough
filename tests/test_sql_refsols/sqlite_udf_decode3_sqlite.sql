SELECT
  o_orderkey AS key,
  CASE
    WHEN CAST(SUBSTRING(o_orderpriority, 1, 1) AS INTEGER) = 1
    THEN 'A'
    WHEN CAST(SUBSTRING(o_orderpriority, 1, 1) AS INTEGER) = 2
    THEN 'B'
    WHEN CAST(SUBSTRING(o_orderpriority, 1, 1) AS INTEGER) = 3
    THEN 'C'
    ELSE 'D'
  END AS _expr0
FROM tpch.orders
WHERE
  o_clerk = 'Clerk#000000951'
ORDER BY
  o_orderkey
LIMIT 10
