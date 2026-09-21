SELECT
  COUNT(*) AS transaction_count
FROM broker.sbTransaction AS sbTransaction
JOIN broker.sbCustomer AS sbCustomer
  ON sbCustomer.sbCustId = sbTransaction.sbTxCustId
  AND sbCustomer.sbCustJoinDate >= CAST(DATE_SUB(CURRENT_TIMESTAMP(), INTERVAL '70' DAY) AS DATE)
