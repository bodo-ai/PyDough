WITH _s3 AS (
  SELECT
    COUNT(*) AS num_sales,
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
  _s0._id,
  _s0.first_name,
  _s0.last_name,
  _s3.num_sales
FROM main.salespersons AS _s0
JOIN _s3 AS _s3
  ON _s0._id = _s3.salesperson_id
ORDER BY
  num_sales DESC
