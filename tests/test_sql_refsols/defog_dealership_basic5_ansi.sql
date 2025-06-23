WITH _t0 AS (
  SELECT
    COUNT(*) AS n_rows,
    SUM(sale_price) AS sum_sale_price,
    salesperson_id
  FROM main.sales
  WHERE
    DATEDIFF(CURRENT_TIMESTAMP(), CAST(sale_date AS DATETIME), DAY) <= 30
  GROUP BY
    salesperson_id
)
SELECT
  salespersons.first_name,
  salespersons.last_name,
  _t0.n_rows AS total_sales,
  COALESCE(_t0.sum_sale_price, 0) AS total_revenue
FROM main.salespersons AS salespersons
JOIN _t0 AS _t0
  ON _t0.salesperson_id = salespersons._id
ORDER BY
  _t0.n_rows DESC
LIMIT 5
