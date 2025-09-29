SELECT
  COUNT(*) AS n
FROM bodo.retail.transactions
WHERE
  NOT payment_method IN ('Mobile Payment', 'Gift Card')
