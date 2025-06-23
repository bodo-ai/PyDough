WITH _s1 AS (
  SELECT
    COUNT(*) AS agg_0,
    SUM(sale_price) AS agg_1,
    salesperson_id
  FROM main.sales
  GROUP BY
    salesperson_id
), _t0 AS (
  SELECT
    _s1.agg_0 AS num_sales,
    RANK() OVER (ORDER BY COALESCE(_s1.agg_1, 0) DESC) AS sales_rank,
    COALESCE(_s1.agg_1, 0) AS total_sales,
    salespersons.first_name,
    salespersons.last_name
  FROM main.salespersons AS salespersons
  JOIN _s1 AS _s1
    ON _s1.salesperson_id = salespersons._id
)
SELECT
  first_name,
  last_name,
  total_sales,
  num_sales,
  sales_rank
FROM _t0
ORDER BY
  total_sales DESC
