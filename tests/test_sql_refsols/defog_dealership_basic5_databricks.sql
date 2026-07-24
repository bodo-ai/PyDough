WITH _s1 AS (
  SELECT
    salesperson_id,
    COUNT(*) AS n_rows,
    SUM(sale_price) AS sum_sale_price
  FROM defog.dealership.sales
  WHERE
    DATEDIFF(DAY, CAST(sale_date AS DATE), CAST(CURRENT_TIMESTAMP() AS DATE)) <= 30
  GROUP BY
    1
)
SELECT
  salespersons.first_name,
  salespersons.last_name,
  _s1.n_rows AS total_sales,
  COALESCE(_s1.sum_sale_price, 0) AS total_revenue
FROM defog.dealership.salespersons AS salespersons
JOIN _s1 AS _s1
  ON _s1.salesperson_id = salespersons.id
ORDER BY
  3 DESC
LIMIT 5
