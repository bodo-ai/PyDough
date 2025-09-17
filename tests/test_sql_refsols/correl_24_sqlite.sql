WITH _t4 AS (
  SELECT
    o_orderdate,
    o_totalprice
  FROM tpch.orders
  WHERE
    CAST(STRFTIME('%Y', o_orderdate) AS INTEGER) < 1994
), _t2 AS (
  SELECT
    CAST(STRFTIME('%m', o_orderdate) AS INTEGER) AS month_o_orderdate,
    CAST(STRFTIME('%Y', o_orderdate) AS INTEGER) AS year_o_orderdate,
    AVG(o_totalprice) AS avg_o_totalprice
  FROM _t4
  GROUP BY
    1,
    2
), _s0 AS (
  SELECT
    avg_o_totalprice,
    month_o_orderdate,
    year_o_orderdate,
    LAG(avg_o_totalprice, 1) OVER (ORDER BY year_o_orderdate, month_o_orderdate) AS prev_month_avg_price
  FROM _t2
)
SELECT
  _s0.year_o_orderdate AS year,
  _s0.month_o_orderdate AS month,
  COUNT(*) AS n_orders_in_range
FROM _s0 AS _s0
JOIN _t4 AS _t5
  ON (
    _s0.avg_o_totalprice <= _t5.o_totalprice
    OR _s0.avg_o_totalprice >= _t5.o_totalprice
  )
  AND (
    _s0.avg_o_totalprice <= _t5.o_totalprice
    OR _s0.prev_month_avg_price <= _t5.o_totalprice
  )
  AND (
    _s0.avg_o_totalprice >= _t5.o_totalprice
    OR _s0.prev_month_avg_price >= _t5.o_totalprice
  )
  AND _s0.month_o_orderdate = CAST(STRFTIME('%m', _t5.o_orderdate) AS INTEGER)
  AND (
    _s0.prev_month_avg_price <= _t5.o_totalprice
    OR _s0.prev_month_avg_price >= _t5.o_totalprice
  )
  AND _s0.year_o_orderdate = CAST(STRFTIME('%Y', _t5.o_orderdate) AS INTEGER)
GROUP BY
  1,
  2
ORDER BY
  1,
  2
