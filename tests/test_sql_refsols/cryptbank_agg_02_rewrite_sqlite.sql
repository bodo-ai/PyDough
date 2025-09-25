SELECT
  SUBSTRING(a_type, -1) || SUBSTRING(a_type, 1, LENGTH(a_type) - 1) AS account_type,
  COUNT(*) AS n,
  ROUND(AVG(SQRT(a_balance)), 2) AS avg_bal
FROM crbnk.accounts
GROUP BY
  1
