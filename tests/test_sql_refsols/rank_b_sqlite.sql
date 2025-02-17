SELECT
  RANK() OVER (ORDER BY order_priority) AS rank
FROM (
  SELECT
    o_orderpriority AS order_priority
  FROM tpch.ORDERS
)
