WITH _s1 AS (
  SELECT
    SUM(sale_price) AS sum_sale_price,
    salesperson_id
  FROM main.sales
  GROUP BY
    salesperson_id
)
SELECT
  salespersons._id,
  salespersons.first_name,
  salespersons.last_name,
  COALESCE(_s1.sum_sale_price, 0) AS total
FROM main.salespersons AS salespersons
LEFT JOIN _s1 AS _s1
  ON _s1.salesperson_id = salespersons._id
ORDER BY
  COALESCE(_s1.sum_sale_price, 0) DESC NULLS LAST
LIMIT 5
