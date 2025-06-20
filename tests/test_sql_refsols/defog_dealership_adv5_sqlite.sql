WITH _s3 AS (
  SELECT
    COUNT(*) AS agg_0,
    SUM(sale_price) AS agg_1,
    salesperson_id
  FROM main.sales
  GROUP BY
    salesperson_id
)
SELECT
  _s0.first_name,
  _s0.last_name,
  COALESCE(_s3.agg_1, 0) AS total_sales,
  _s3.agg_0 AS num_sales,
  RANK() OVER (ORDER BY COALESCE(_s3.agg_1, 0) DESC) AS sales_rank
FROM main.salespersons AS _s0
JOIN _s3 AS _s3
  ON _s0._id = _s3.salesperson_id
ORDER BY
  COALESCE(_s3.agg_1, 0) DESC
