SELECT
  COUNT(*) AS n
FROM bodo.retail.transactions
WHERE
  PTY_UNPROTECT_ACCOUNT(payment_method) = 'Cash'
