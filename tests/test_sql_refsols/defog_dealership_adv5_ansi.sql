WITH _t1 AS (
  SELECT
    COUNT() AS agg_0,
    SUM(sale_price) AS agg_1,
    salesperson_id
  FROM main.sales
  GROUP BY
    salesperson_id
)
SELECT
  salespersons.first_name,
  salespersons.last_name,
  COALESCE(_t1.agg_1, 0) AS total_sales,
  _t1.agg_0 AS num_sales,
  RANK() OVER (ORDER BY COALESCE(_t1.agg_1, 0) DESC NULLS FIRST) AS sales_rank
FROM main.salespersons AS salespersons
JOIN _t1 AS _t1
  ON _t1.salesperson_id = salespersons._id
ORDER BY
  COALESCE(_t1.agg_1, 0) DESC
