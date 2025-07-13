WITH _s1 AS (
  SELECT
    COUNT(*) AS n_rows_1,
    SUM(sale_price) AS sum_sale_price,
    salesperson_id
  FROM main.sales
  WHERE
    DATEDIFF(CURRENT_TIMESTAMP(), CAST(sale_date AS DATETIME), DAY) <= 30
  GROUP BY
    salesperson_id
), _t0 AS (
  SELECT
    salespersons.first_name AS first_name_1,
    salespersons.last_name AS last_name_1,
    _s1.n_rows_1 AS n_rows,
    _s1.sum_sale_price
  FROM main.salespersons AS salespersons
  JOIN _s1 AS _s1
    ON _s1.salesperson_id = salespersons._id
  ORDER BY
    n_rows DESC
  LIMIT 5
)
SELECT
  first_name_1 AS first_name,
  last_name_1 AS last_name,
  n_rows AS total_sales,
  COALESCE(sum_sale_price, 0) AS total_revenue
FROM _t0
ORDER BY
  n_rows DESC
