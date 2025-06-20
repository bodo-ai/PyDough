WITH _s3 AS (
  SELECT
    SUM(sale_price) AS agg_0,
    salesperson_id
  FROM main.sales
  GROUP BY
    salesperson_id
)
SELECT
  _s0._id,
  _s0.first_name,
  _s0.last_name,
  COALESCE(_s3.agg_0, 0) AS total
FROM main.salespersons AS _s0
LEFT JOIN _s3 AS _s3
  ON _s0._id = _s3.salesperson_id
ORDER BY
  total DESC
LIMIT 5
