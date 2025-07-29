WITH _s1 AS (
  SELECT
    COUNT(*) AS n_rows,
    SUM(sale_price) AS sum_sale_price,
    salesperson_id
  FROM main.sales
  GROUP BY
    salesperson_id
)
SELECT
  salespersons.first_name,
  salespersons.last_name,
  COALESCE(_s1.sum_sale_price, 0) AS total_sales,
  _s1.n_rows AS num_sales,
  RANK() OVER (ORDER BY COALESCE(_s1.sum_sale_price, 0) DESC) AS sales_rank
FROM main.salespersons AS salespersons
JOIN _s1 AS _s1
  ON _s1.salesperson_id = salespersons._id
ORDER BY
  COALESCE(_s1.sum_sale_price, 0) DESC NULLS LAST
