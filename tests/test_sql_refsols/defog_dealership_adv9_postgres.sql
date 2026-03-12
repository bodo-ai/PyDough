SELECT
  AVG(CAST(sale_price AS DECIMAL)) AS ASP
FROM main.sales
WHERE
  sale_date <= CAST('2023-03-31' AS DATE)
  AND sale_date >= CAST('2023-01-01' AS DATE)
