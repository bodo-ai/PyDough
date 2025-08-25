SELECT
  AVG(sale_price) AS ASP
FROM MAIN.SALES
WHERE
  sale_date <= '2023-03-31' AND sale_date >= '2023-01-01'
