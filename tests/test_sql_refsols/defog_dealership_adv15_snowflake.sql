WITH _s1 AS (
  SELECT
    salesperson_id,
    AVG(sale_price) AS avg_sale_price
  FROM main.sales
  GROUP BY
    1
)
SELECT
  salespersons.first_name,
  salespersons.last_name,
  _s1.avg_sale_price AS ASP
FROM main.salespersons AS salespersons
JOIN _s1 AS _s1
  ON _s1.salesperson_id = salespersons._id
ORDER BY
  3 DESC NULLS LAST
LIMIT 3
