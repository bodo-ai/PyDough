WITH _s1 AS (
  SELECT
    COALESCE(SUM(sale_price), 0) AS total_revenue,
    COUNT(*) AS n_rows,
    salesperson_id
  FROM main.sales
  WHERE
    DATEDIFF(CURRENT_TIMESTAMP(), CAST(sale_date AS DATETIME)) <= 30
  GROUP BY
    salesperson_id
)
SELECT
  salespersons.first_name,
  salespersons.last_name,
  _s1.n_rows AS total_sales,
  _s1.total_revenue
FROM main.salespersons AS salespersons
JOIN _s1 AS _s1
  ON _s1.salesperson_id = salespersons._id
ORDER BY
  _s1.n_rows DESC
LIMIT 5
