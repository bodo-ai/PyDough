WITH _t2 AS (
  SELECT
    COUNT() AS n_orders,
    o_orderpriority AS order_priority,
    CAST(STRFTIME('%Y', o_orderdate) AS INTEGER) AS order_year
  FROM tpch.orders
  GROUP BY
    o_orderpriority,
    CAST(STRFTIME('%Y', o_orderdate) AS INTEGER)
), _t1 AS (
  SELECT
    order_priority AS highest_priority,
    CAST((
      100.0 * n_orders
    ) AS REAL) / SUM(n_orders) OVER (PARTITION BY order_year) AS priority_pct,
    order_year
  FROM _t2
), _t AS (
  SELECT
    highest_priority,
    priority_pct,
    order_year,
    ROW_NUMBER() OVER (PARTITION BY order_year ORDER BY priority_pct DESC) AS _w
  FROM _t1
)
SELECT
  order_year,
  highest_priority,
  priority_pct
FROM _t
WHERE
  _w = 1
ORDER BY
  order_year
