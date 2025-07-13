WITH _s1 AS (
  SELECT
    COUNT(*) AS n_rows,
    SUM(sale_price) AS sum_sale_price,
    salesperson_id
  FROM main.sales
  WHERE
    sale_date >= DATETIME('now', '-3 month')
  GROUP BY
    salesperson_id
), _t0 AS (
  SELECT
    salespersons.first_name AS first_name_1,
    salespersons.last_name AS last_name_1,
    COALESCE(_s1.sum_sale_price, 0) AS total_revenue_1,
    _s1.n_rows
  FROM main.salespersons AS salespersons
  LEFT JOIN _s1 AS _s1
    ON _s1.salesperson_id = salespersons._id
  ORDER BY
    COALESCE(_s1.sum_sale_price, 0) DESC
  LIMIT 3
)
SELECT
  first_name_1 AS first_name,
  last_name_1 AS last_name,
  COALESCE(n_rows, 0) AS total_sales,
  total_revenue_1 AS total_revenue
FROM _t0
ORDER BY
  total_revenue_1 DESC
