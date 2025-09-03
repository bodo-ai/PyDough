SELECT
  COUNT(sbtransaction.sbtxcustid) AS transaction_count
FROM main.sbtransaction AS sbtransaction
JOIN main.sbcustomer AS sbcustomer
  ON sbcustomer.sbcustid = sbtransaction.sbtxcustid
  AND sbcustomer.sbcustjoindate >= DATE_TRUNC('DAY', DATE_SUB(CURRENT_TIMESTAMP(), 70, DAY))
