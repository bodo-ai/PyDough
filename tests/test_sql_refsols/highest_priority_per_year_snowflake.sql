WITH _t3 AS (
  SELECT
    YEAR(CAST(o_orderdate AS TIMESTAMP)) AS year_o_orderdate,
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
    (
      100.0 * n_rows
    ) / SUM(n_rows) OVER (PARTITION BY year_o_orderdate) AS priority_pct
  FROM _t3
), _t1 AS (
  SELECT
    o_orderpriority,
    year_o_orderdate,
    priority_pct
  FROM _t2
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY year_o_orderdate ORDER BY priority_pct DESC) = 1
)
SELECT
  year_o_orderdate AS order_year,
  o_orderpriority AS highest_priority,
  priority_pct
FROM _t1
ORDER BY
  1 NULLS FIRST
