WITH _t2 AS (
  SELECT
    COUNT() AS n_orders,
    o_orderpriority AS order_priority,
    EXTRACT(YEAR FROM o_orderdate) AS order_year
  FROM tpch.orders
  GROUP BY
    o_orderpriority,
    EXTRACT(YEAR FROM o_orderdate)
), _t1 AS (
  SELECT
    order_priority AS highest_priority,
    (
      100.0 * n_orders
    ) / SUM(n_orders) OVER (PARTITION BY order_year) AS priority_pct,
    order_year
  FROM _t2
), _t0 AS (
  SELECT
    highest_priority,
    priority_pct,
    order_year
  FROM _t1
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY order_year ORDER BY priority_pct DESC NULLS FIRST) = 1
)
SELECT
  order_year,
  highest_priority,
  priority_pct
FROM _t0
ORDER BY
  order_year
