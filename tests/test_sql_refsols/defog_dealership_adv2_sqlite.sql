WITH _t1_2 AS (
  SELECT
    COUNT() AS agg_0,
    salesperson_id
  FROM main.sales
  WHERE
    CAST((
      JULIANDAY(DATE(DATETIME('now'), 'start of day')) - JULIANDAY(DATE(sale_date, 'start of day'))
    ) AS INTEGER) <= 30
  GROUP BY
    salesperson_id
)
SELECT
  salespersons._id,
  salespersons.first_name,
  salespersons.last_name,
  _t1.agg_0 AS num_sales
FROM main.salespersons AS salespersons
JOIN _t1_2 AS _t1
  ON _t1.salesperson_id = salespersons._id
ORDER BY
  _t1.agg_0 DESC
