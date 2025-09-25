SELECT
  ROUND(AVG(total_amount), 2) AS n
FROM bodo.retail.transactions
WHERE
  MONTH(CAST(transaction_date AS TIMESTAMP)) = 8
  AND YEAR(CAST(transaction_date AS TIMESTAMP)) = 2022
