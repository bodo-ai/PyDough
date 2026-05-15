SELECT
  COUNT(*) AS n
FROM crbnk.accounts
WHERE
  ABS(SQRT(a_balance) - 7250) <= 600 AND ABS(SQRT(a_balance) - 7250) >= 200
