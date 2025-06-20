WITH _s1 AS (
  SELECT
    COUNT(*) AS agg_0,
    SUM(sale_price) AS agg_1,
    salesperson_id
  FROM main.sales
  GROUP BY
    salesperson_id
)
SELECT
  salespersons.first_name,
  salespersons.last_name,
  COALESCE(_s1.agg_1, 0) AS total_sales,
  _s1.agg_0 AS num_sales,
  RANK() OVER (ORDER BY COALESCE(_s1.agg_1, 0) DESC) AS sales_rank
FROM main.salespersons AS salespersons
JOIN _s1 AS _s1
  ON _s1.salesperson_id = salespersons._id
ORDER BY
  COALESCE(_s1.agg_1, 0) DESC
