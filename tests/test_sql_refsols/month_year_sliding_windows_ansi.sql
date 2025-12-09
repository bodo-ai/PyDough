WITH _t5 AS (
  SELECT
    EXTRACT(YEAR FROM CAST(o_orderdate AS DATETIME)) AS year_o_orderdate,
    SUM(o_totalprice) AS sum_o_totalprice
  FROM tpch.orders
  WHERE
    o_orderpriority = '1-URGENT'
  GROUP BY
    1
), _t4 AS (
  SELECT
    sum_o_totalprice,
    year_o_orderdate,
    LEAD(COALESCE(sum_o_totalprice, 0), 1, 0.0) OVER (ORDER BY year_o_orderdate NULLS LAST) AS next_year_total_spent
  FROM _t5
), _t1 AS (
  SELECT
    EXTRACT(MONTH FROM CAST(orders.o_orderdate AS DATETIME)) AS month_o_orderdate,
    EXTRACT(YEAR FROM CAST(orders.o_orderdate AS DATETIME)) AS year_o_orderdate
  FROM _t4 AS _t4
  JOIN tpch.orders AS orders
    ON _t4.year_o_orderdate = EXTRACT(YEAR FROM CAST(orders.o_orderdate AS DATETIME))
    AND orders.o_orderpriority = '1-URGENT'
  WHERE
    _t4.next_year_total_spent < COALESCE(_t4.sum_o_totalprice, 0)
  GROUP BY
    1,
    2
  QUALIFY
    COALESCE(sum_o_totalprice, 0) > LAG(COALESCE(sum_o_totalprice, 0), 1, 0.0) OVER (ORDER BY EXTRACT(YEAR FROM CAST(orders.o_orderdate AS DATETIME)) NULLS LAST, EXTRACT(MONTH FROM CAST(orders.o_orderdate AS DATETIME)) NULLS LAST)
    AND COALESCE(sum_o_totalprice, 0) > LEAD(COALESCE(sum_o_totalprice, 0), 1, 0.0) OVER (ORDER BY EXTRACT(YEAR FROM CAST(orders.o_orderdate AS DATETIME)) NULLS LAST, EXTRACT(MONTH FROM CAST(orders.o_orderdate AS DATETIME)) NULLS LAST)
)
SELECT
  year_o_orderdate AS year,
  month_o_orderdate AS month
FROM _t1
ORDER BY
  1,
  2
