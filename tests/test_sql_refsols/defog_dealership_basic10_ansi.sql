WITH _s1 AS (
  SELECT
    COUNT(*) AS n_rows,
    SUM(sale_price) AS sum_sale_price,
    salesperson_id
  FROM main.sales
  WHERE
    sale_date >= DATE_ADD(CURRENT_TIMESTAMP(), -3, 'MONTH')
  GROUP BY
    3
)
SELECT
  salespersons.first_name,
  salespersons.last_name,
  COALESCE(_s1.n_rows, 0) AS total_sales,
  COALESCE(_s1.sum_sale_price, 0) AS total_revenue
FROM main.salespersons AS salespersons
LEFT JOIN _s1 AS _s1
  ON _s1.salesperson_id = salespersons._id
ORDER BY
  4 DESC
LIMIT 3
