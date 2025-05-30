WITH _t8 AS (
  SELECT
    o_orderdate AS order_date,
    o_totalprice AS total_price
  FROM tpch.orders
), _t6 AS (
  SELECT
    SUM(total_price) AS agg_0,
    EXTRACT(YEAR FROM order_date) AS year
  FROM _t8
  GROUP BY
    EXTRACT(MONTH FROM order_date),
    EXTRACT(YEAR FROM order_date)
), _t4 AS (
  SELECT
    SUM(COALESCE(agg_0, 0)) AS agg_0,
    year
  FROM _t6
  GROUP BY
    year
), _t3 AS (
  SELECT
    COALESCE(agg_0, 0) AS curr_year_total_spent,
    LEAD(COALESCE(agg_0, 0), 1, 0.0) OVER (ORDER BY year NULLS LAST) AS next_year_total_spent,
    year
  FROM _t4
), _t9 AS (
  SELECT
    SUM(total_price) AS agg_0,
    EXTRACT(MONTH FROM order_date) AS month,
    EXTRACT(YEAR FROM order_date) AS year
  FROM _t8
  GROUP BY
    EXTRACT(MONTH FROM order_date),
    EXTRACT(YEAR FROM order_date)
), _t1 AS (
  SELECT
    _t9.month AS month_2,
    _t9.year AS year_4
  FROM _t3 AS _t3
  JOIN _t9 AS _t9
    ON _t3.year = _t9.year
  WHERE
    _t3.curr_year_total_spent > _t3.next_year_total_spent
  QUALIFY
    COALESCE(_t9.agg_0, 0) > LAG(COALESCE(_t9.agg_0, 0), 1, 0.0) OVER (ORDER BY _t9.year NULLS LAST, _t9.month NULLS LAST)
    AND COALESCE(_t9.agg_0, 0) > LEAD(COALESCE(_t9.agg_0, 0), 1, 0.0) OVER (ORDER BY _t9.year NULLS LAST, _t9.month NULLS LAST)
)
SELECT
  year_4 AS year,
  month_2 AS month
FROM _t1
ORDER BY
  year_4,
  month_2
