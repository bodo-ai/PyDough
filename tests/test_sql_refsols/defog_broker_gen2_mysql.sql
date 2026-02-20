SELECT
  COUNT(*) AS transaction_count
FROM broker.sbTransaction AS sbTransaction
JOIN broker.sbCustomer AS sbCustomer
  ON sbCustomer.sbcustid = sbTransaction.sbtxcustid
  AND sbCustomer.sbcustjoindate >= CAST(DATE_SUB(CURRENT_TIMESTAMP(), INTERVAL '70' DAY) AS DATE)
