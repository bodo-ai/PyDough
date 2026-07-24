SELECT
  AVG(sale_price) AS ASP
FROM cassandra.defog.sales
WHERE
  sale_date <= CAST('2023-03-31' AS DATE)
  AND sale_date >= CAST('2023-01-01' AS DATE)
