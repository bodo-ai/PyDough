WITH _t1 AS (
  SELECT
    COUNT() AS agg_1,
    SUM(sale_price) AS agg_0,
    salesperson_id
  FROM main.sales
  WHERE
    CAST((
      JULIANDAY(DATE(DATETIME('now'), 'start of day')) - JULIANDAY(DATE(sale_date, 'start of day'))
    ) AS INTEGER) <= 30
  GROUP BY
    salesperson_id
), _t0_2 AS (
  SELECT
    salespersons.first_name,
    salespersons.last_name,
    _t1.agg_1 AS ordering_2,
    COALESCE(_t1.agg_0, 0) AS total_revenue,
    _t1.agg_1 AS total_sales
  FROM main.salespersons AS salespersons
  JOIN _t1 AS _t1
    ON _t1.salesperson_id = salespersons._id
  ORDER BY
    ordering_2 DESC
  LIMIT 5
)
SELECT
  first_name,
  last_name,
  total_sales,
  total_revenue
FROM _t0_2
ORDER BY
  ordering_2 DESC
