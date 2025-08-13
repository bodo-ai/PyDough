WITH _t3 AS (
  SELECT
    o_orderdate,
    o_totalprice
  FROM tpch.orders
  WHERE
    EXTRACT(YEAR FROM CAST(o_orderdate AS DATETIME)) < 1994
), _t1 AS (
  SELECT
    AVG(o_totalprice) AS avg_o_totalprice,
    EXTRACT(MONTH FROM CAST(o_orderdate AS DATETIME)) AS month,
    EXTRACT(YEAR FROM CAST(o_orderdate AS DATETIME)) AS year
  FROM _t3
  GROUP BY
    EXTRACT(MONTH FROM CAST(o_orderdate AS DATETIME)),
    EXTRACT(YEAR FROM CAST(o_orderdate AS DATETIME))
), _s0 AS (
  SELECT
    LAG(avg_o_totalprice, 1) OVER (ORDER BY year NULLS LAST, month NULLS LAST) AS prev_month_avg_price,
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
    OR _s0.prev_month_avg_price <= _t4.o_totalprice
  )
  AND (
    _s0.avg_o_totalprice >= _t4.o_totalprice
    OR _s0.prev_month_avg_price >= _t4.o_totalprice
  )
  AND _s0.month = EXTRACT(MONTH FROM CAST(_t4.o_orderdate AS DATETIME))
  AND (
    _s0.prev_month_avg_price <= _t4.o_totalprice
    OR _s0.prev_month_avg_price >= _t4.o_totalprice
  )
  AND _s0.year = EXTRACT(YEAR FROM CAST(_t4.o_orderdate AS DATETIME))
GROUP BY
  _s0.month,
  _s0.year
ORDER BY
  year,
  month
