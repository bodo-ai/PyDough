WITH _t5 AS (
  SELECT
    o_orderdate,
    o_totalprice
  FROM tpch.orders
  WHERE
    CAST(STRFTIME('%Y', o_orderdate) AS INTEGER) < 1994
), _t3 AS (
  SELECT
    AVG(o_totalprice) AS avg_o_totalprice,
    CAST(STRFTIME('%m', o_orderdate) AS INTEGER) AS month,
    CAST(STRFTIME('%Y', o_orderdate) AS INTEGER) AS year
  FROM _t5
  GROUP BY
    CAST(STRFTIME('%Y', o_orderdate) AS INTEGER),
    CAST(STRFTIME('%m', o_orderdate) AS INTEGER)
), _s0 AS (
  SELECT
    LAG(avg_o_totalprice, 1) OVER (ORDER BY year, month) AS prev_month_avg_price,
    avg_o_totalprice,
    month,
    year
  FROM _t3
)
SELECT
  MAX(_s0.year) AS year,
  MAX(_s0.month) AS month,
  COUNT(*) AS n_orders_in_range
FROM _s0 AS _s0
JOIN _t5 AS _t6
  ON (
    _s0.avg_o_totalprice <= _t6.o_totalprice
    OR _s0.avg_o_totalprice >= _t6.o_totalprice
  )
  AND (
    _s0.avg_o_totalprice <= _t6.o_totalprice
    OR _s0.prev_month_avg_price <= _t6.o_totalprice
  )
  AND (
    _s0.avg_o_totalprice >= _t6.o_totalprice
    OR _s0.prev_month_avg_price >= _t6.o_totalprice
  )
  AND _s0.month = CAST(STRFTIME('%m', _t6.o_orderdate) AS INTEGER)
  AND (
    _s0.prev_month_avg_price <= _t6.o_totalprice
    OR _s0.prev_month_avg_price >= _t6.o_totalprice
  )
  AND _s0.year = CAST(STRFTIME('%Y', _t6.o_orderdate) AS INTEGER)
GROUP BY
  _s0.month,
  _s0.year
ORDER BY
  MAX(_s0.year),
  MAX(_s0.month)
