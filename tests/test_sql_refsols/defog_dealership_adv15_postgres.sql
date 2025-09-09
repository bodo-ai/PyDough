WITH _s1 AS (
  SELECT
    AVG(CAST(sale_price AS DECIMAL)) AS avg_sale_price,
    salesperson_id
  FROM main.sales
  GROUP BY
    2
)
SELECT
  salespersons.first_name,
  salespersons.last_name,
  _s1.avg_sale_price AS ASP
FROM main.salespersons AS salespersons
LEFT JOIN _s1 AS _s1
  ON _s1.salesperson_id = salespersons._id
ORDER BY
  3 DESC NULLS LAST
LIMIT 3
