WITH _t0 AS (
  SELECT
    CAST(STRFTIME('%m', o_orderdate) AS INTEGER) AS month_oorderdate,
    COUNT(*) AS n_rows
  FROM tpch.orders
  WHERE
    CAST(STRFTIME('%Y', o_orderdate) AS INTEGER) = 1994
    AND o_orderpriority = '1-URGENT'
  GROUP BY
    1
)
SELECT
  month_oorderdate AS month,
  n_rows AS n_orders,
  MIN(n_rows) OVER () AS m1,
  MIN(n_rows) OVER (ORDER BY month_oorderdate ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS m2,
  MIN(n_rows) OVER (ORDER BY month_oorderdate ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS m3
FROM _t0
ORDER BY
  1
