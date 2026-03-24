SELECT
  AVG(sale_price) AS ASP
FROM MAIN.SALES
WHERE
  sale_date <= TO_DATE('2023-03-31', 'YYYY-MM-DD')
  AND sale_date >= TO_DATE('2023-01-01', 'YYYY-MM-DD')
