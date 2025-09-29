SELECT
  COUNT(*) AS n
FROM bodo.retail.transactions
WHERE
  NOT PTY_UNPROTECT_ACCOUNT(payment_method) IN ('Mobile Payment', 'Gift Card')
