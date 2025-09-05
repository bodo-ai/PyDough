WITH _t3 AS (
  SELECT
    o_orderdate,
    o_totalprice
  FROM tpch.orders
  WHERE
    CAST(STRFTIME('%Y', o_orderdate) AS INTEGER) < 1994
), _t1 AS (
  SELECT
    AVG(o_totalprice) AS avg_o_totalprice,
    CAST(STRFTIME('%m', o_orderdate) AS INTEGER) AS month,
    CAST(STRFTIME('%Y', o_orderdate) AS INTEGER) AS year
  FROM _t3
  GROUP BY
    2,
    3
), _s0 AS (
  SELECT
    LAG(avg_o_totalprice, 1) OVER (ORDER BY year, month) AS prev_month_avg_price_1,
    avg_o_totalprice,
    month,
    year
  FROM _t1
)
SELECT
  _s0.year,
  _s0.month,
  COUNT(*) AS n_orders_in_range
FROM _s0 AS _s0
JOIN _t3 AS _t4
  ON (
    _s0.avg_o_totalprice <= _t4.o_totalprice
    OR _s0.avg_o_totalprice >= _t4.o_totalprice
  )
  AND (
    _s0.avg_o_totalprice <= _t4.o_totalprice
    OR _s0.prev_month_avg_price_1 <= _t4.o_totalprice
  )
  AND (
    _s0.avg_o_totalprice >= _t4.o_totalprice
    OR _s0.prev_month_avg_price_1 >= _t4.o_totalprice
  )
  AND _s0.month = CAST(STRFTIME('%m', _t4.o_orderdate) AS INTEGER)
  AND (
    _s0.prev_month_avg_price_1 <= _t4.o_totalprice
    OR _s0.prev_month_avg_price_1 >= _t4.o_totalprice
  )
  AND _s0.year = CAST(STRFTIME('%Y', _t4.o_orderdate) AS INTEGER)
GROUP BY
  1,
  2
ORDER BY
  1,
  2
