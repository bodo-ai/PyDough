WITH _s1 AS (
  SELECT
    salesperson_id,
    SUM(sale_price) AS sum_sale_price
  FROM defog.dealership.sales
  GROUP BY
    1
)
SELECT
  salespersons.id AS _id,
  salespersons.first_name,
  salespersons.last_name,
  COALESCE(_s1.sum_sale_price, 0) AS total
FROM defog.dealership.salespersons AS salespersons
LEFT JOIN _s1 AS _s1
  ON _s1.salesperson_id = salespersons.id
ORDER BY
  4 DESC
LIMIT 5
