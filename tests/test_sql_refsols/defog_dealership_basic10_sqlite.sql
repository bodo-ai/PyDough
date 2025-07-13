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
    salespersons.first_name,
    salespersons.last_name,
    _s1.n_rows,
    COALESCE(_s1.sum_sale_price, 0) AS total_revenue
  FROM main.salespersons AS salespersons
  LEFT JOIN _s1 AS _s1
    ON _s1.salesperson_id = salespersons._id
  ORDER BY
    total_revenue DESC
  LIMIT 3
)
SELECT
  first_name,
  last_name,
  COALESCE(n_rows, 0) AS total_sales,
  total_revenue
FROM _t0
ORDER BY
  total_revenue DESC
