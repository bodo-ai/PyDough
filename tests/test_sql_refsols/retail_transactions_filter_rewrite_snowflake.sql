SELECT
  ROUND(AVG(total_amount), 2) AS n
FROM bodo.retail.transactions
WHERE
  MONTH(CAST(PTY_UNPROTECT_TS(transaction_date) AS TIMESTAMP)) = 7
  AND YEAR(CAST(PTY_UNPROTECT_TS(transaction_date) AS TIMESTAMP)) = 2025
