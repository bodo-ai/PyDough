WITH _s1 AS (
  SELECT
    COUNT(*) AS n_rows,
    SUM(sale_price) AS sum_sale_price,
    salesperson_id
  FROM main.sales
  GROUP BY
    salesperson_id
), _t0 AS (
  SELECT
    RANK() OVER (ORDER BY 0 DESC, COALESCE(_s1.sum_sale_price, 0) DESC) AS sales_rank,
    COALESCE(_s1.sum_sale_price, 0) AS total_sales,
    salespersons.first_name,
    salespersons.last_name,
    _s1.n_rows
  FROM main.salespersons AS salespersons
  JOIN _s1 AS _s1
    ON _s1.salesperson_id = salespersons._id
)
SELECT
  first_name,
  last_name,
  total_sales,
  n_rows AS num_sales,
  sales_rank
FROM _t0
ORDER BY
  total_sales DESC
