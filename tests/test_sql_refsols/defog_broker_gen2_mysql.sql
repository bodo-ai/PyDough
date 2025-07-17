SELECT
  COUNT(sbTransaction.sbtxcustid) AS transaction_count
FROM main.sbTransaction AS sbTransaction
JOIN main.sbCustomer AS sbCustomer
  ON sbCustomer.sbcustid = sbTransaction.sbtxcustid
  AND sbCustomer.sbcustjoindate >= DATE(DATE_ADD(CURRENT_TIMESTAMP(), INTERVAL '-70' DAY))
