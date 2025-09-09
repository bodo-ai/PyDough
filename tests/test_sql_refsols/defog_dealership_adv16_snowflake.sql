WITH _s1 AS (
  SELECT
    salesperson_id,
    SUM(sale_price) AS sum_sale_price
  FROM main.sales
  GROUP BY
    1
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
  4 DESC NULLS LAST
LIMIT 5
