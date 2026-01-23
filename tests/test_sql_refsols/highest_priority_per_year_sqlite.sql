WITH _t3 AS (
  SELECT
    CAST(STRFTIME('%Y', o_orderdate) AS INTEGER) AS year_o_orderdate,
    o_orderpriority,
    COUNT(*) AS n_rows
  FROM tpch.orders
  GROUP BY
    1,
    2
), _t2 AS (
  SELECT
    o_orderpriority,
    year_o_orderdate,
    CAST((
      100.0 * n_rows
    ) AS REAL) / SUM(n_rows) OVER (PARTITION BY year_o_orderdate) AS priority_pct
  FROM _t3
), _t AS (
  SELECT
    o_orderpriority,
    year_o_orderdate,
    priority_pct,
    ROW_NUMBER() OVER (PARTITION BY year_o_orderdate ORDER BY priority_pct DESC) AS _w
  FROM _t2
)
SELECT
  year_o_orderdate AS order_year,
  o_orderpriority AS highest_priority,
  priority_pct
FROM _t
WHERE
  _w = 1
ORDER BY
  1
